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

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import scala.collection.JavaConversions._
import scala.util.{Failure, Success}

import org.apache.spark.Logging
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.streaming._
import org.apache.spark.streaming.ui.UIUtils
import org.apache.spark.util.{EventLoop, ThreadUtils}


private[scheduler] sealed trait JobSchedulerEvent
private[scheduler] case class JobStarted(job: Job) extends JobSchedulerEvent
private[scheduler] case class JobCompleted(job: Job) extends JobSchedulerEvent
private[scheduler] case class ErrorReported(msg: String, e: Throwable) extends JobSchedulerEvent

/**
 * This class schedules jobs to be run on Spark. It uses the JobGenerator to generate
 * the jobs and runs them using a thread pool.
 * 总体负责动态作业调度
 */
private[streaming]
class JobScheduler(val ssc: StreamingContext) extends Logging {

  // Use of ConcurrentHashMap.keySet later causes an odd runtime problem due to Java 7/8 diff
  // https://gist.github.com/AlainODea/1375759b8720a3f9f094
  private val jobSets: java.util.Map[Time, JobSet] = new ConcurrentHashMap[Time, JobSet]
  private val numConcurrentJobs = ssc.conf.getInt("spark.streaming.concurrentJobs", 1)
  private val jobExecutor =
    ThreadUtils.newDaemonFixedThreadPool(numConcurrentJobs, "streaming-job-executor")
  //JobScheduler 将每个 batch 的 RDD DAG 具体生成工作委托给 JobGenerator
  private val jobGenerator = new JobGenerator(this)
  val clock = jobGenerator.clock
  val listenerBus = new StreamingListenerBus()

  // These two are created only when scheduler starts.
  //只有当调度程序启动时才创建InputInfoTracker,ReceiverTracker
  // eventLoop not being null means the scheduler has been started and not stopped
  //eventloop不空意味着调度程序已经开始运行
  //JobScheduler将源头输入数据的记录工作委托给 ReceiverTracker
  var receiverTracker: ReceiverTracker = null
  // A tracker to track all the input stream information as well as processed record number
  //跟踪所有输入流信息及处理记录数
  var inputInfoTracker: InputInfoTracker = null

  private var eventLoop: EventLoop[JobSchedulerEvent] = null

  def start(): Unit = synchronized {
    //如果调度程序已经运行,则返回
    if (eventLoop != null) return // scheduler has already been started

    logDebug("Starting JobScheduler")
    //创建EventLoop主要用于处理各类JobScheduler事件
    eventLoop = new EventLoop[JobSchedulerEvent]("JobScheduler") {
      override protected def onReceive(event: JobSchedulerEvent): Unit = processEvent(event)

      override protected def onError(e: Throwable): Unit = reportError("Error in job scheduler", e)
    }
    //启动事件
    eventLoop.start()

    // attach rate controllers of input streams to receive batch completion updates
    //将输入流的速率控制器连接到接收批处理完成更新
    for {
      inputDStream <- ssc.graph.getInputStreams
      rateController <- inputDStream.rateController
    } ssc.addStreamingListener(rateController)
    //主要用于更新SparkUI中StreamTab的内容
    listenerBus.start(ssc.sparkContext)
    //创建并启动ReceiverTracker,用于处理数据接收,数据缓存,Block生成等工作
    receiverTracker = new ReceiverTracker(ssc)
    //创建并启动InputInfoTracker,用于处理数据输入
    inputInfoTracker = new InputInfoTracker(ssc)
    receiverTracker.start()
    //启动JobGenerator负责对DstreamGraph的初始化,Dstream与RDD的转换,生成Job,提交执行等工作
    jobGenerator.start()
    logInfo("Started JobScheduler")
  }

  def stop(processAllReceivedData: Boolean): Unit = synchronized {
    //如果调度程序已经暂停,则返回
    if (eventLoop == null) return // scheduler has already been stopped
    logDebug("Stopping JobScheduler")

    // First, stop receiving
    // 第一,停止接收
    receiverTracker.stop(processAllReceivedData)

    // Second, stop generating jobs. If it has to process all received data,
    // then this will wait for all the processing through JobScheduler to be over.
    //第二,停止产生作业,处理所有接收到的数据.
    jobGenerator.stop(processAllReceivedData)

    // Stop the executor for receiving new jobs
    //停止接收执行新的Job
    logDebug("Stopping job executor")
    jobExecutor.shutdown()

    // Wait for the queued jobs to complete if indicated
    //等待队列的工作完成,如果true
    val terminated = if (processAllReceivedData) {
      //只是一个非常大的时间
      jobExecutor.awaitTermination(1, TimeUnit.HOURS)  // just a very large period of time
    } else {
      jobExecutor.awaitTermination(2, TimeUnit.SECONDS)
    }
    if (!terminated) {
      jobExecutor.shutdownNow()
    }
    logDebug("Stopped job executor")

    // Stop everything else
    //停止一切
    listenerBus.stop()
    eventLoop.stop()
    eventLoop = null
    logInfo("Stopped JobScheduler")
  }

  def submitJobSet(jobSet: JobSet) {
    if (jobSet.jobs.isEmpty) {
      logInfo("No jobs added for time " + jobSet.time)
    } else {
      listenerBus.post(StreamingListenerBatchSubmitted(jobSet.toBatchInfo))
      jobSets.put(jobSet.time, jobSet)
      jobSet.jobs.foreach(job => jobExecutor.execute(new JobHandler(job)))
      logInfo("Added jobs for time " + jobSet.time)
    }
  }

  def getPendingTimes(): Seq[Time] = {
    jobSets.keySet.toSeq
  }

  def reportError(msg: String, e: Throwable) {
    eventLoop.post(ErrorReported(msg, e))
  }

  def isStarted(): Boolean = synchronized {
    eventLoop != null
  }

  private def processEvent(event: JobSchedulerEvent) {
    try {
      event match {
        case JobStarted(job) => handleJobStart(job)
        case JobCompleted(job) => handleJobCompletion(job)
        case ErrorReported(m, e) => handleError(m, e)
      }
    } catch {
      case e: Throwable =>
        reportError("Error in job scheduler", e)
    }
  }

  private def handleJobStart(job: Job) {
    val jobSet = jobSets.get(job.time)
    val isFirstJobOfJobSet = !jobSet.hasStarted
    jobSet.handleJobStart(job)
    if (isFirstJobOfJobSet) {
      // "StreamingListenerBatchStarted" should be posted after calling "handleJobStart" to get the
      // correct "jobSet.processingStartTime".
      listenerBus.post(StreamingListenerBatchStarted(jobSet.toBatchInfo))
    }
    logInfo("Starting job " + job.id + " from job set of time " + jobSet.time)
  }

  private def handleJobCompletion(job: Job) {
    val jobSet = jobSets.get(job.time)
    jobSet.handleJobCompletion(job)
    logInfo("Finished job " + job.id + " from job set of time " + jobSet.time)
    if (jobSet.hasCompleted) {
      jobSets.remove(jobSet.time)
      jobGenerator.onBatchCompletion(jobSet.time)
      logInfo("Total delay: %.3f s for time %s (execution: %.3f s)".format(
        jobSet.totalDelay / 1000.0, jobSet.time.toString,
        jobSet.processingDelay / 1000.0
      ))
      listenerBus.post(StreamingListenerBatchCompleted(jobSet.toBatchInfo))
    }
    job.result match {
      case Failure(e) =>
        reportError("Error running job " + job, e)
      case _ =>
    }
  }

  private def handleError(msg: String, e: Throwable) {
    logError(msg, e)
    ssc.waiter.notifyError(e)
  }

  private class JobHandler(job: Job) extends Runnable with Logging {
    import JobScheduler._

    def run() {
      try {
        val formattedTime = UIUtils.formatBatchTime(
          job.time.milliseconds, ssc.graph.batchDuration.milliseconds, showYYYYMMSS = false)
        val batchUrl = s"/streaming/batch/?id=${job.time.milliseconds}"
        val batchLinkText = s"[output operation ${job.outputOpId}, batch time ${formattedTime}]"

        ssc.sc.setJobDescription(
          s"""Streaming job from <a href="$batchUrl">$batchLinkText</a>""")
        ssc.sc.setLocalProperty(BATCH_TIME_PROPERTY_KEY, job.time.milliseconds.toString)
        ssc.sc.setLocalProperty(OUTPUT_OP_ID_PROPERTY_KEY, job.outputOpId.toString)

        // We need to assign `eventLoop` to a temp variable. Otherwise, because
        // `JobScheduler.stop(false)` may set `eventLoop` to null when this method is running, then
        // it's possible that when `post` is called, `eventLoop` happens to null.
        var _eventLoop = eventLoop
        if (_eventLoop != null) {
          _eventLoop.post(JobStarted(job))
          // Disable checks for existing output directories in jobs launched by the streaming
          // scheduler, since we may need to write output to an existing directory during checkpoint
          // recovery; see SPARK-4835 for more details.
          PairRDDFunctions.disableOutputSpecValidation.withValue(true) {
            job.run()
          }
          _eventLoop = eventLoop
          if (_eventLoop != null) {
            _eventLoop.post(JobCompleted(job))
          }
        } else {
          // JobScheduler has been stopped.
        }
      } finally {
        ssc.sc.setLocalProperty(JobScheduler.BATCH_TIME_PROPERTY_KEY, null)
        ssc.sc.setLocalProperty(JobScheduler.OUTPUT_OP_ID_PROPERTY_KEY, null)
      }
    }
  }
}

private[streaming] object JobScheduler {
  val BATCH_TIME_PROPERTY_KEY = "spark.streaming.internal.batchTime"
  val OUTPUT_OP_ID_PROPERTY_KEY = "spark.streaming.internal.outputOpId"
}
