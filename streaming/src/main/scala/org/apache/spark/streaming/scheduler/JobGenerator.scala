/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.streaming.scheduler

import scala.util.{Failure, Success, Try}

import org.apache.spark.{SparkEnv, Logging}
import org.apache.spark.streaming.{Checkpoint, CheckpointWriter, Time}
import org.apache.spark.streaming.util.RecurringTimer
import org.apache.spark.util.{Utils, Clock, EventLoop, ManualClock}

/** 
 *  Event classes for JobGenerator 
 *  对于jobgenerator事件类
 *  */
private[scheduler] sealed trait JobGeneratorEvent
private[scheduler] case class GenerateJobs(time: Time) extends JobGeneratorEvent
private[scheduler] case class ClearMetadata(time: Time) extends JobGeneratorEvent
private[scheduler] case class DoCheckpoint(
    time: Time, clearCheckpointDataLater: Boolean) extends JobGeneratorEvent
private[scheduler] case class ClearCheckpointData(time: Time) extends JobGeneratorEvent

/**
 * This class generates jobs from DStreams as well as drives checkpointing and cleaning
 * up DStream metadata.
 * JobScheduler 将每个 batch 的 RDD DAG 具体生成工作委托给 JobGenerator,而将源头输入数据的记录工作委托给 ReceiverTracker
 */
private[streaming]
/**
 * JobGenerator 维护了一个定时器,周期就是我们刚刚提到的 batchDuration,定时为每个 batch 生成 RDD DAG 的实例
 */
class JobGenerator(jobScheduler: JobScheduler) extends Logging {

  private val ssc = jobScheduler.ssc
  private val conf = ssc.conf
  private val graph = ssc.graph

  val clock = {
    val clockClass = ssc.sc.conf.get(
      "spark.streaming.clock", "org.apache.spark.util.SystemClock")
    try {
      Utils.classForName(clockClass).newInstance().asInstanceOf[Clock]
    } catch {
      case e: ClassNotFoundException if clockClass.startsWith("org.apache.spark.streaming") =>
        val newClockClass = clockClass.replace("org.apache.spark.streaming", "org.apache.spark")
        Utils.classForName(newClockClass).newInstance().asInstanceOf[Clock]
    }
  }

  private val timer = new RecurringTimer(clock, ssc.graph.batchDuration.milliseconds,
    longTime => eventLoop.post(GenerateJobs(new Time(longTime))), "JobGenerator")

  // This is marked lazy so that this is initialized after checkpoint duration has been set  
  // in the context and the generator has been started.
  //这是延迟加载,所以这是初始化后检查点的持续时间已被设置在上下文和生成器已启动
  private lazy val shouldCheckpoint = ssc.checkpointDuration != null && ssc.checkpointDir != null

  private lazy val checkpointWriter = if (shouldCheckpoint) {
    new CheckpointWriter(this, ssc.conf, ssc.checkpointDir, ssc.sparkContext.hadoopConfiguration)
  } else {
    null
  }

  // eventLoop is created when generator starts.
  //当生成器启动时创建事件循环
  // This not being null means the scheduler has been started and not stopped
  //这不是空的意思是调度程序已经启动,而不是停止
  private var eventLoop: EventLoop[JobGeneratorEvent] = null

  // last batch whose completion,checkpointing and metadata cleanup has been completed
  //最后一批的完成,检查点和元数据清除已完成
  private var lastProcessedBatch: Time = null

  /** 
   *  Start generation of jobs
   *  开始运行Job 
   *  */
  def start(): Unit = synchronized {
    //判断是否已经启动
    if (eventLoop != null) return // generator has already been started

    // Call checkpointWriter here to initialize it before eventLoop uses it to avoid a deadlock.
    //调用checkpointwriter来初始化之前eventloop,使用它来避免死锁
    // See SPARK-10125
    checkpointWriter

    eventLoop = new EventLoop[JobGeneratorEvent]("JobGenerator") {
      override protected def onReceive(event: JobGeneratorEvent): Unit = processEvent(event)

      override protected def onError(e: Throwable): Unit = {
        jobScheduler.reportError("Error in job generator", e)
      }
    }
    eventLoop.start()

    if (ssc.isCheckpointPresent) {
      restart()
    } else {
      startFirstTime()
    }
  }

  /**
   * Stop generation of jobs. processReceivedData = true makes this wait until jobs
   * of current ongoing time interval has been generated, processed and corresponding
   * 使此等待,直到当前正在进行的时间间隔的作业已被生成,处理和相应的检查点写
   * checkpoints written.
   * 停止jobs,
   */
  def stop(processReceivedData: Boolean): Unit = synchronized {
    //产生已被停止
    if (eventLoop == null) return // generator has already been stopped

    if (processReceivedData) {
      logInfo("Stopping JobGenerator gracefully")
      val timeWhenStopStarted = System.currentTimeMillis()
      val stopTimeoutMs = conf.getTimeAsMs(
        "spark.streaming.gracefulStopTimeout", s"${10 * ssc.graph.batchDuration.milliseconds}ms")
      val pollTime = 100

      // To prevent graceful stop to get stuck permanently
      //防止优雅的永久停止
      def hasTimedOut: Boolean = {
        val timedOut = (System.currentTimeMillis() - timeWhenStopStarted) > stopTimeoutMs
        if (timedOut) {
          logWarning("Timed out while stopping the job generator (timeout = " + stopTimeoutMs + ")")
        }
        timedOut
      }

      // Wait until all the received blocks in the network input tracker has
      // been consumed by network input DStreams, and jobs have been generated with them
      //等到所有接收到的块的网络输入跟踪已通过网络输入dstreams消耗,和工作已经产生与他们
      logInfo("Waiting for all received blocks to be consumed for job generation")
      while(!hasTimedOut && jobScheduler.receiverTracker.hasUnallocatedBlocks) {
        Thread.sleep(pollTime)
      }
      logInfo("Waited for all received blocks to be consumed for job generation")

      // Stop generating jobs
      //停止产生Jobs
      val stopTime = timer.stop(interruptTimer = false)
      graph.stop()
      logInfo("Stopped generation timer")

      // Wait for the jobs to complete and checkpoints to be written
      //等待工作完成和检查点写
      def haveAllBatchesBeenProcessed: Boolean = {
        lastProcessedBatch != null && lastProcessedBatch.milliseconds == stopTime
      }
      logInfo("Waiting for jobs to be processed and checkpoints to be written")
      while (!hasTimedOut && !haveAllBatchesBeenProcessed) {
        Thread.sleep(pollTime)
      }
      logInfo("Waited for jobs to be processed and checkpoints to be written")
    } else {
      logInfo("Stopping JobGenerator immediately")
      // Stop timer and graph immediately, ignore unprocessed data and pending jobs
      //立即停止定时器图,忽略了未处理的数据和未决的工作
      timer.stop(true)
      graph.stop()
    }

    // Stop the event loop and checkpoint writer
    //停止事件循环和检查点写
    if (shouldCheckpoint) checkpointWriter.stop()
    eventLoop.stop()
    logInfo("Stopped JobGenerator")
  }

  /**
   * Callback called when a batch has been completely processed.
   * 回调调用时,一个批处理已完全处理
   */
  def onBatchCompletion(time: Time) {
    eventLoop.post(ClearMetadata(time))
  }

  /**
   * Callback called when the checkpoint of a batch has been written.
   * 当一个批处理的检查点被写入时调用,
   */
  def onCheckpointCompletion(time: Time, clearCheckpointDataLater: Boolean) {
    if (clearCheckpointDataLater) {
      eventLoop.post(ClearCheckpointData(time))
    }
  }

  /** 
   *  Processes all events 
   *  处理所有的事件
   *  */
  private def processEvent(event: JobGeneratorEvent) {
    logDebug("Got event " + event)
    event match {
      case GenerateJobs(time) => generateJobs(time)
      case ClearMetadata(time) => clearMetadata(time)
      //是异步地收到 DoCheckpoint消息后,在一个线程池里执行 doCheckpoint()方法
      case DoCheckpoint(time, clearCheckpointDataLater) =>
        doCheckpoint(time, clearCheckpointDataLater)
      case ClearCheckpointData(time) => clearCheckpointData(time)
    }
  }

  /** 
   *  Starts the generator for the first time 
   *  开始第一次启动
   *  */
  private def startFirstTime() {
    val startTime = new Time(timer.getStartTime())
    graph.start(startTime - graph.batchDuration)
    timer.start(startTime.milliseconds)
    logInfo("Started JobGenerator at " + startTime)
  }

  /** 
   *  Restarts the generator based on the information in checkpoint 
   *  重新启动产生基于信息的检查点
   *  */
  private def restart() {
    // If manual clock is being used for testing, then
    //如果手动时钟被用于测试
    // either set the manual clock to the last checkpointed time,
    // or if the property is defined set it to that time
    if (clock.isInstanceOf[ManualClock]) {
      val lastTime = ssc.initialCheckpoint.checkpointTime.milliseconds
      val jumpTime = ssc.sc.conf.getLong("spark.streaming.manualClock.jump", 0)
      clock.asInstanceOf[ManualClock].setTime(lastTime + jumpTime)
    }

    val batchDuration = ssc.graph.batchDuration

    // Batches when the master was down, that is,
    // between the checkpoint and current restart time
    val checkpointTime = ssc.initialCheckpoint.checkpointTime
    val restartTime = new Time(timer.getRestartTime(graph.zeroTime.milliseconds))
    val downTimes = checkpointTime.until(restartTime, batchDuration)
    logInfo("Batches during down time (" + downTimes.size + " batches): "
      + downTimes.mkString(", "))

    // Batches that were unprocessed before failure
    val pendingTimes = ssc.initialCheckpoint.pendingTimes.sorted(Time.ordering)
    logInfo("Batches pending processing (" + pendingTimes.size + " batches): " +
      pendingTimes.mkString(", "))
    // Reschedule jobs for these times
    val timesToReschedule = (pendingTimes ++ downTimes).distinct.sorted(Time.ordering)
    logInfo("Batches to reschedule (" + timesToReschedule.size + " batches): " +
      timesToReschedule.mkString(", "))
    timesToReschedule.foreach { time =>
      // Allocate the related blocks when recovering from failure, because some blocks that were
      // added but not allocated, are dangling in the queue after recovering, we have to allocate
      // those blocks to the next batch, which is the batch they were supposed to go.
      jobScheduler.receiverTracker.allocateBlocksToBatch(time) // allocate received blocks to batch
      jobScheduler.submitJobSet(JobSet(time, graph.generateJobs(time)))
    }

    // Restart the timer
    timer.start(restartTime.milliseconds)
    logInfo("Restarted JobGenerator at " + restartTime)
  }

  /** 
   *  Generate jobs and perform checkpoint for the given `time`.  
   *  为给定的时间生成作业和执行检查点
   *  */
  private def generateJobs(time: Time) {
    // Set the SparkEnv in this thread, so that job generation code can access the environment
    //设置sparkenv在这个线程,因此,作业生成代码可以访问环境
    // Example: BlockRDDs are created in this thread, and it needs to access BlockManager
    // 例如:blockrdds在这个线程创建的,它需要访问blockmanager
    // Update: This is probably redundant after threadlocal stuff in SparkEnv has been removed.
    //
    SparkEnv.set(ssc.env)
    Try {
      //分配接收到的块到批处理
      jobScheduler.receiverTracker.allocateBlocksToBatch(time) // allocate received blocks to batch
      //生成作业使用分配的块
      graph.generateJobs(time) // generate jobs using allocated block
    } match {
      case Success(jobs) =>
        val streamIdToInputInfos = jobScheduler.inputInfoTracker.getInfo(time)
        jobScheduler.submitJobSet(JobSet(time, jobs, streamIdToInputInfos))
      case Failure(e) =>
        jobScheduler.reportError("Error generating jobs for time " + time, e)
    }
    //异步地收到 DoCheckpoint消息后
    eventLoop.post(DoCheckpoint(time, clearCheckpointDataLater = false))
  }

  /** 
   *  Clear DStream metadata for the given `time`.
   *  给定时间清理dstream检查点数据  
   *  */
  private def clearMetadata(time: Time) {
    ssc.graph.clearMetadata(time)

    // If checkpointing is enabled, then checkpoint,
    //如果检查点已启用,然后调用checkpoint
    // else mark batch to be fully processed
    //其他标记批处理完全处理
    if (shouldCheckpoint) {
      eventLoop.post(DoCheckpoint(time, clearCheckpointDataLater = true))
    } else {
      // If checkpointing is not enabled, then delete metadata information about
      //如果检查点未启用,然后删除接收到的块的元数据信息(没有保存在任何情况下的块数据)
      // received blocks (block data not saved in any case). Otherwise, wait for
      // checkpointing of this batch to complete.
      //否则,这批等待检查点完成
      
      val maxRememberDuration = graph.getMaxInputStreamRememberDuration()
      jobScheduler.receiverTracker.cleanupOldBlocksAndBatches(time - maxRememberDuration)
      jobScheduler.inputInfoTracker.cleanup(time - maxRememberDuration)
      markBatchFullyProcessed(time)
    }
  }

  /** 
   *  Clear DStream checkpoint data for the given `time`.
   *  给定时间清理dstream检查点数据 
   *  */
  private def clearCheckpointData(time: Time) {
    ssc.graph.clearCheckpointData(time)

    // All the checkpoint information about which batches have been processed, etc have
    //已处理的批处理的所有检查点信息.等已保存到检查点,因此,它的安全删除块元数据和数据
    // been saved to checkpoints, so its safe to delete block metadata and data WAL files
    val maxRememberDuration = graph.getMaxInputStreamRememberDuration()
    jobScheduler.receiverTracker.cleanupOldBlocksAndBatches(time - maxRememberDuration)
    jobScheduler.inputInfoTracker.cleanup(time - maxRememberDuration)
    markBatchFullyProcessed(time)
  }

  /**
   *  Perform checkpoint for the give `time`. 
   *  Checkpoint发起的间隔默认的是和 batchDuration一致,即每次 batch 发起、提交了需要运行的job后就做 Checkpoint
   *  */
  private def doCheckpoint(time: Time, clearCheckpointDataLater: Boolean) {
    if (shouldCheckpoint && (time - graph.zeroTime).isMultipleOf(ssc.checkpointDuration)) {
      logInfo("Checkpointing graph for time " + time)
      ssc.graph.updateCheckpointData(time)
      //new一个当前状态的 Checkpoint,然后通过 CheckpointWriter 写出去
      checkpointWriter.write(new Checkpoint(ssc, time), clearCheckpointDataLater)
    }
  }

  private def markBatchFullyProcessed(time: Time) {
    lastProcessedBatch = time
  }
}
