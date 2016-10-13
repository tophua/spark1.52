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

package org.apache.spark.scheduler

import java.util.concurrent.Semaphore

import scala.collection.mutable
import scala.collection.JavaConversions._

import org.scalatest.Matchers

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.util.ResetSystemProperties
import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkFunSuite}

class SparkListenerSuite extends SparkFunSuite with LocalSparkContext with Matchers
  with ResetSystemProperties {

  /** 
   *  Length of time to wait while draining listener events.
   *  等待在侦听侦听事件事件时等待的时间
   *   */
  val WAIT_TIMEOUT_MILLIS = 10000

  val jobCompletionTime = 1421191296660L

  test("basic creation and shutdown of LiveListenerBus") {
    val counter = new BasicJobCounter
    val bus = new LiveListenerBus
    bus.addListener(counter)

    // Listener bus hasn't started yet, so posting events should not increment counter
    //侦听器总线尚未启动,因此不应将事件增量计数器
    (1 to 5).foreach { _ => bus.post(SparkListenerJobEnd(0, jobCompletionTime, JobSucceeded)) }
    assert(counter.count === 0)

    // Starting listener bus should flush all buffered events
    //启动侦听器总线应刷新所有缓冲事件
    bus.start(sc)
    bus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(counter.count === 5)

    // After listener bus has stopped, posting events should not increment counter
    //侦听器总线停止后,发布事件不应增量计数器
    bus.stop()
    (1 to 5).foreach { _ => bus.post(SparkListenerJobEnd(0, jobCompletionTime, JobSucceeded)) }
    assert(counter.count === 5)

    // Listener bus must not be started twice
    //侦听器总线不能启动两次
    intercept[IllegalStateException] {
      val bus = new LiveListenerBus
      bus.start(sc)
      bus.start(sc)
    }

    // ... or stopped before starting
    //或停止之前开始
    intercept[IllegalStateException] {
      val bus = new LiveListenerBus
      bus.stop()
    }
  }
  //等待事件队列完全耗尽
  test("bus.stop() waits for the event queue to completely drain") {
    @volatile var drained = false

    // When Listener has started
    //当监听器已经开始
    val listenerStarted = new Semaphore(0)

    // Tells the listener to stop blocking
    //告诉监听器停止阻塞
    val listenerWait = new Semaphore(0)

    // When stopper has started
    //当阻塞已经开始
    val stopperStarted = new Semaphore(0)

    // When stopper has returned
    //阻塞返回
    val stopperReturned = new Semaphore(0)

    class BlockingListener extends SparkListener {
      override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
        listenerStarted.release()
        listenerWait.acquire()
        drained = true
      }
    }

    val bus = new LiveListenerBus
    val blockingListener = new BlockingListener

    bus.addListener(blockingListener)
    bus.start(sc)
    bus.post(SparkListenerJobEnd(0, jobCompletionTime, JobSucceeded))

    listenerStarted.acquire()
    // Listener should be blocked after start
    //监听开始后阻塞
    assert(!drained)

    new Thread("ListenerBusStopper") {
      override def run() {
        stopperStarted.release()
        // stop() will block until notify() is called below
        bus.stop()
        stopperReturned.release()
      }
    }.start()

    stopperStarted.acquire()
    // Listener should remain blocked after stopper started
    assert(!drained)

    // unblock Listener to let queue drain
    listenerWait.release()
    stopperReturned.acquire()
    assert(drained)
  }

  test("basic creation of StageInfo") {//创建基本StageInfo
    sc = new SparkContext("local", "SparkListenerSuite")
    val listener = new SaveStageAndTaskInfo
    sc.addSparkListener(listener)
    val rdd1 = sc.parallelize(1 to 100, 4)
    val rdd2 = rdd1.map(_.toString)
    rdd2.setName("Target RDD")
    rdd2.count()

    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)

    listener.stageInfos.size should be {1}
    val (stageInfo, taskInfoMetrics) = listener.stageInfos.head
    stageInfo.rddInfos.size should be {2}
    stageInfo.rddInfos.forall(_.numPartitions == 4) should be {true}
    stageInfo.rddInfos.exists(_.name == "Target RDD") should be {true}
    stageInfo.numTasks should be {4}
    stageInfo.submissionTime should be ('defined)
    stageInfo.completionTime should be ('defined)
    taskInfoMetrics.length should be {4}
  }

  test("basic creation of StageInfo with shuffle") {//创建基本StageInfo具有shuffle
    sc = new SparkContext("local", "SparkListenerSuite")
    val listener = new SaveStageAndTaskInfo
    sc.addSparkListener(listener)
    val rdd1 = sc.parallelize(1 to 100, 4)
    val rdd2 = rdd1.filter(_ % 2 == 0).map(i => (i, i))
    val rdd3 = rdd2.reduceByKey(_ + _)
    rdd1.setName("Un")
    rdd2.setName("Deux")
    rdd3.setName("Trois")

    rdd1.count()
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    listener.stageInfos.size should be {1}
    val stageInfo1 = listener.stageInfos.keys.find(_.stageId == 0).get
    stageInfo1.rddInfos.size should be {1} // ParallelCollectionRDD
    stageInfo1.rddInfos.forall(_.numPartitions == 4) should be {true}
    stageInfo1.rddInfos.exists(_.name == "Un") should be {true}
    listener.stageInfos.clear()

    rdd2.count()
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    listener.stageInfos.size should be {1}
    val stageInfo2 = listener.stageInfos.keys.find(_.stageId == 1).get
    stageInfo2.rddInfos.size should be {3} // ParallelCollectionRDD, FilteredRDD, MappedRDD
    stageInfo2.rddInfos.forall(_.numPartitions == 4) should be {true}
    stageInfo2.rddInfos.exists(_.name == "Deux") should be {true}
    listener.stageInfos.clear()

    rdd3.count()
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    listener.stageInfos.size should be {2} // Shuffle map stage + result stage
    val stageInfo3 = listener.stageInfos.keys.find(_.stageId == 3).get
    stageInfo3.rddInfos.size should be {1} // ShuffledRDD
    stageInfo3.rddInfos.forall(_.numPartitions == 4) should be {true}
    stageInfo3.rddInfos.exists(_.name == "Trois") should be {true}
  }

  test("StageInfo with fewer tasks than partitions") {//StageInfo较少的任务分区
    sc = new SparkContext("local", "SparkListenerSuite")
    val listener = new SaveStageAndTaskInfo
    sc.addSparkListener(listener)
    val rdd1 = sc.parallelize(1 to 100, 4)
    val rdd2 = rdd1.map(_.toString)
    sc.runJob(rdd2, (items: Iterator[String]) => items.size, Seq(0, 1))

    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)

    listener.stageInfos.size should be {1}
    val (stageInfo, _) = listener.stageInfos.head
    stageInfo.numTasks should be {2}
    stageInfo.rddInfos.size should be {2}
    stageInfo.rddInfos.forall(_.numPartitions == 4) should be {true}
  }

  test("local metrics") {//本地测量
    sc = new SparkContext("local", "SparkListenerSuite")
    val listener = new SaveStageAndTaskInfo
    sc.addSparkListener(listener)
    sc.addSparkListener(new StatsReportListener)
    // just to make sure some of the tasks take a noticeable amount of time    
    val w = { i: Int =>
      if (i == 0) {
        Thread.sleep(100)
      }
      i
    }

    val d = sc.parallelize(0 to 1e4.toInt, 64).map(w)
    d.count()
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    listener.stageInfos.size should be (1)

    val d2 = d.map { i => w(i) -> i * 2 }.setName("shuffle input 1")
    val d3 = d.map { i => w(i) -> (0 to (i % 5)) }.setName("shuffle input 2")
    val d4 = d2.cogroup(d3, 64).map { case (k, (v1, v2)) =>
      w(k) -> (v1.size, v2.size)
    }
    d4.setName("A Cogroup")
    d4.collectAsMap()

    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    listener.stageInfos.size should be (4)
    listener.stageInfos.foreach { case (stageInfo, taskInfoMetrics) =>
      /**
       * Small test, so some tasks might take less than 1 millisecond, but average should be greater
       * than 0 ms.
       */
      checkNonZeroAvg(
        taskInfoMetrics.map(_._2.executorRunTime),
        stageInfo + " executorRunTime")
      checkNonZeroAvg(
        taskInfoMetrics.map(_._2.executorDeserializeTime),
        stageInfo + " executorDeserializeTime")

      /* Test is disabled (SEE SPARK-2208)
      if (stageInfo.rddInfos.exists(_.name == d4.name)) {
        checkNonZeroAvg(
          taskInfoMetrics.map(_._2.shuffleReadMetrics.get.fetchWaitTime),
          stageInfo + " fetchWaitTime")
      }
      */

      taskInfoMetrics.foreach { case (taskInfo, taskMetrics) =>
        taskMetrics.resultSize should be > (0L)
        if (stageInfo.rddInfos.exists(info => info.name == d2.name || info.name == d3.name)) {
          taskMetrics.inputMetrics should not be ('defined)
          taskMetrics.outputMetrics should not be ('defined)
          taskMetrics.shuffleWriteMetrics should be ('defined)
          taskMetrics.shuffleWriteMetrics.get.shuffleBytesWritten should be > (0L)
        }
        if (stageInfo.rddInfos.exists(_.name == d4.name)) {
          taskMetrics.shuffleReadMetrics should be ('defined)
          val sm = taskMetrics.shuffleReadMetrics.get
          sm.totalBlocksFetched should be (128)
          sm.localBlocksFetched should be (128)
          sm.remoteBlocksFetched should be (0)
          sm.remoteBytesRead should be (0L)
        }
      }
    }
  }
  //onTaskGettingResult调用获取远程结果
  test("onTaskGettingResult() called when result fetched remotely") {
   //以MB为单位的driver和executor之间通信信息的大小,设置值越大,driver可以接受越大的计算结果
    val conf = new SparkConf().set("spark.akka.frameSize", "1")
    sc = new SparkContext("local", "SparkListenerSuite", conf)
    val listener = new SaveTaskEvents
    sc.addSparkListener(listener)

    // Make a task whose result is larger than the akka frame size
    //做一个任务的结果大于Akka框架大小
    val akkaFrameSize =
      sc.env.actorSystem.settings.config.getBytes("akka.remote.netty.tcp.maximum-frame-size").toInt
    assert(akkaFrameSize === 1024 * 1024)
    val result = sc.parallelize(Seq(1), 1)
      .map { x => 1.to(akkaFrameSize).toArray }
      .reduce { case (x, y) => x }
    assert(result === 1.to(akkaFrameSize).toArray)

    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    val TASK_INDEX = 0
    assert(listener.startedTasks.contains(TASK_INDEX))
    assert(listener.startedGettingResultTasks.contains(TASK_INDEX))
    assert(listener.endedTasks.contains(TASK_INDEX))
  }
  //onTaskGettingResult()不调用,结果直接发送
  test("onTaskGettingResult() not called when result sent directly") {
    sc = new SparkContext("local", "SparkListenerSuite")
    val listener = new SaveTaskEvents
    sc.addSparkListener(listener)

    // Make a task whose result is larger than the akka frame size
    //做一个任务的结果大于Akka框架大小
    val result = sc.parallelize(Seq(1), 1).map(2 * _).reduce { case (x, y) => x }
    assert(result === 2)

    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    val TASK_INDEX = 0
    assert(listener.startedTasks.contains(TASK_INDEX))
    assert(listener.startedGettingResultTasks.isEmpty)
    assert(listener.endedTasks.contains(TASK_INDEX))
  }
   //应该所有开始的任务,事件后工作被杀死了
  test("onTaskEnd() should be called for all started tasks, even after job has been killed") {
    sc = new SparkContext("local", "SparkListenerSuite")
    val WAIT_TIMEOUT_MILLIS = 10000
    val listener = new SaveTaskEvents
    sc.addSparkListener(listener)

    val numTasks = 10
    val f = sc.parallelize(1 to 10000, numTasks).map { i => Thread.sleep(10); i }.countAsync()
    // Wait until one task has started (because we want to make sure that any tasks that are started
    // have corresponding end events sent to the listener).
    var finishTime = System.currentTimeMillis + WAIT_TIMEOUT_MILLIS
    listener.synchronized {
      var remainingWait = finishTime - System.currentTimeMillis
      while (listener.startedTasks.isEmpty && remainingWait > 0) {
        listener.wait(remainingWait)
        remainingWait = finishTime - System.currentTimeMillis
      }
      assert(!listener.startedTasks.isEmpty)
    }

    f.cancel()

    // Ensure that onTaskEnd is called for all started tasks.
    //确保ontaskend被称为所有启动任务
    finishTime = System.currentTimeMillis + WAIT_TIMEOUT_MILLIS
    listener.synchronized {
      var remainingWait = finishTime - System.currentTimeMillis
      while (listener.endedTasks.size < listener.startedTasks.size && remainingWait > 0) {
        listener.wait(finishTime - System.currentTimeMillis)
        remainingWait = finishTime - System.currentTimeMillis
      }
      assert(listener.endedTasks.size === listener.startedTasks.size)
    }
  }
  //sparklistener删除,如果监听抛出一个异常
  test("SparkListener moves on if a listener throws an exception") {
    val badListener = new BadListener
    val jobCounter1 = new BasicJobCounter
    val jobCounter2 = new BasicJobCounter
    val bus = new LiveListenerBus

    // Propagate events to bad listener first
    //传播事件第一个坏的监听事件
    bus.addListener(badListener)
    bus.addListener(jobCounter1)
    bus.addListener(jobCounter2)
    bus.start(sc)

    // Post events to all listeners, and wait until the queue is drained
    //提交事件全部监听,等待队列耗尽
    (1 to 5).foreach { _ => bus.post(SparkListenerJobEnd(0, jobCompletionTime, JobSucceeded)) }
    bus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)

    // The exception should be caught, and the event should be propagated to other listeners
    //事件应该传播给其他监听
    assert(bus.listenerThreadIsAlive)
    assert(jobCounter1.count === 5)
    assert(jobCounter2.count === 5)
  }

  test("registering listeners via spark.extraListeners") {//通过spark.extraListeners注册监听器
    val conf = new SparkConf().setMaster("local").setAppName("test")
      .set("spark.extraListeners", classOf[ListenerThatAcceptsSparkConf].getName + "," +
        classOf[BasicJobCounter].getName)
    sc = new SparkContext(conf)
    sc.listenerBus.listeners.collect { case x: BasicJobCounter => x}.size should be (1)
    sc.listenerBus.listeners.collect {
      case x: ListenerThatAcceptsSparkConf => x
    }.size should be (1)
  }

  /**
   * Assert that the given list of numbers has an average that is greater than zero.
   * 断言给定的数字列表的平均值大于零。
   */
  private def checkNonZeroAvg(m: Traversable[Long], msg: String) {
    assert(m.sum / m.size.toDouble > 0.0, msg)
  }

  /**
   * A simple listener that saves all task infos and task metrics.
   * 一个简单的侦听器,保存所有的任务信息和任务指标
   */
  private class SaveStageAndTaskInfo extends SparkListener {
    val stageInfos = mutable.Map[StageInfo, Seq[(TaskInfo, TaskMetrics)]]()
    var taskInfoMetrics = mutable.Buffer[(TaskInfo, TaskMetrics)]()

    override def onTaskEnd(task: SparkListenerTaskEnd) {
      val info = task.taskInfo
      val metrics = task.taskMetrics
      if (info != null && metrics != null) {
        taskInfoMetrics += ((info, metrics))
      }
    }

    override def onStageCompleted(stage: SparkListenerStageCompleted) {
      stageInfos(stage.stageInfo) = taskInfoMetrics
      taskInfoMetrics = mutable.Buffer.empty
    }
  }

  /**
   * A simple listener that saves the task indices for all task events.
   * 一个简单的侦听器，可以保存所有任务事件的任务索引
   */
  private class SaveTaskEvents extends SparkListener {
    val startedTasks = new mutable.HashSet[Int]()
    val startedGettingResultTasks = new mutable.HashSet[Int]()
    val endedTasks = new mutable.HashSet[Int]()

    override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = synchronized {
      startedTasks += taskStart.taskInfo.index
      notify()
    }

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {
      endedTasks += taskEnd.taskInfo.index
      notify()
    }

    override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult) {
      startedGettingResultTasks += taskGettingResult.taskInfo.index
    }
  }

  /**
   * A simple listener that throws an exception on job end.
   * 一个简单的侦听器,工作结束将异常一个抛出
   */
  private class BadListener extends SparkListener {
    override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = { throw new Exception }
  }

}

// These classes can't be declared inside of the SparkListenerSuite class because we don't want
// their constructors to contain references to SparkListenerSuite:

/**
 * A simple listener that counts the number of jobs observed.
 * 一个简单的侦听器,计数观察到的工作的数量
 */
private class BasicJobCounter extends SparkListener {
  var count = 0
  override def onJobEnd(job: SparkListenerJobEnd): Unit = count += 1
}

private class ListenerThatAcceptsSparkConf(conf: SparkConf) extends SparkListener {
  var count = 0
  override def onJobEnd(job: SparkListenerJobEnd): Unit = count += 1
}
