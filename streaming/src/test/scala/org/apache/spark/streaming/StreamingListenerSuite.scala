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

package org.apache.spark.streaming

import scala.collection.mutable.{ ArrayBuffer, SynchronizedBuffer }
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.scheduler._

import org.scalatest.Matchers
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._
import org.apache.spark.Logging
/**
 * Streaming监听测试套件
 */
class StreamingListenerSuite extends TestSuiteBase with Matchers {
  //input= Vector(List(1), List(2), List(3), List(4))
  val input = (1 to 4).map(Seq(_)).toSeq
  val operation = (d: DStream[Int]) => d.map(x => x)

  var ssc: StreamingContext = _

  override def afterFunction() {
    super.afterFunction()
    if (ssc != null) {
      ssc.stop()
    }
  }

  // To make sure that the processing start and end times in collected
  //为了确保集合信息的处理开始和结束时间是不同的连续批次
  // information are different for successive batches
  override def batchDuration: Duration = Milliseconds(100)
  override def actuallyWait: Boolean = true

  test("batch info reporting") { //批量信息报告
     //input= Vector(List(1), List(2), List(3), List(4))
    ssc = setupStreams(input, operation)
    val collector = new BatchInfoCollector
    ssc.addStreamingListener(collector)
    runStreams(ssc, input.size, input.size)

    // SPARK-6766: batch info should be submitted 批量信息提交
    val batchInfosSubmitted = collector.batchInfosSubmitted
    batchInfosSubmitted should have size 4

    batchInfosSubmitted.foreach(info => {
      //调用延迟
      info.schedulingDelay should be(None)
      //处理延迟
      info.processingDelay should be(None)
      //总的延迟
      info.totalDelay should be(None)
    })

    batchInfosSubmitted.foreach { info =>
      println("==="+info.numRecords+"==="+info.batchTime)
       println("==="+info.processingStartTime+"==="+info.processingEndTime)
    println("==="+info.submissionTime+"==="+info.numOutputOp)
    //Map(0 -> StreamInputInfo(0,1,Map()))
    println("==="+info.streamIdToInputInfo)
      //记录数
      info.numRecords should be(1L)
  
      info.streamIdToInputInfo should be(Map(0 -> StreamInputInfo(0, 1L)))
    }
    batchInfosSubmitted.map(x=>{println(x.submissionTime)})
    //检查一个数字序列是否在递增顺序
    /**
     * batchInfosSubmitted.map(_.submissionTime)提交时间
     *1482309346865
      1482309346880
      1482309346917
      1482309347006
     */
    isInIncreasingOrder(batchInfosSubmitted.map(_.submissionTime)) should be(true)

    // SPARK-6766: processingStartTime of batch info should not be None when starting
    //启动批量信息的处理开始时间不应该是没有的
    val batchInfosStarted = collector.batchInfosStarted
    batchInfosStarted should have size 4

    batchInfosStarted.foreach(info => {
      //println(info.schedulingDelay)
      //println(info.schedulingDelay.get)
      //println( info.processingDelay)
      //println(info.totalDelay)
      //调度延迟Some(19)
      info.schedulingDelay should not be None
      info.schedulingDelay.get should be >= 0L
      //处理延迟 None
      info.processingDelay should be(None)
      info.totalDelay should be(None)
    })

    batchInfosStarted.foreach { info =>
      //记录数
      //info.numRecords should be(1L)
      info.streamIdToInputInfo should be(Map(0 -> StreamInputInfo(0, 1L)))
    }
   /* batchInfosStarted.map(x=>println("===="+x.submissionTime))
    batchInfosStarted.map(x=>println("===="+x.processingStartTime.get))*/
    /**
     *submissionTime 提交时间
     *1482310060175
     *1482310060191
     *1482310060230
     *1482310060309
     */
    isInIncreasingOrder(batchInfosStarted.map(_.submissionTime)) should be(true)
     /**
     *processingStartTime 处理开始时间
     *1482310060189
     *1482310061004
     *1482310061142
     *1482310061246
     */
    isInIncreasingOrder(batchInfosStarted.map(_.processingStartTime.get)) should be(true)

    // test onBatchCompleted 测试批处理完成
    val batchInfosCompleted = collector.batchInfosCompleted
    batchInfosCompleted should have size 4
    /**
     * 注意完成之后有处理时间和总记录数
     */
    batchInfosCompleted.foreach(info => {
     /* println("schedulingDelay:"+info.schedulingDelay)
      println("processingDelay:"+info.processingDelay)
      println("totalDelay:"+info.totalDelay)
      println("schedulingDelay.get:"+info.schedulingDelay.get)
      println("info.processingDelay.get:"+info.processingDelay.get)      
       println("info.totalDelay.get:"+info.totalDelay.get)*/
      //Some(15)
      info.schedulingDelay should not be None
      //Some(919)
      info.processingDelay should not be None
      //Some(934)
      info.totalDelay should not be None
      //15
      info.schedulingDelay.get should be >= 0L
      //919
      info.processingDelay.get should be >= 0L
      //934
      info.totalDelay.get should be >= 0L
    })

    batchInfosCompleted.foreach { info =>
      info.numRecords should be(1L)
      info.streamIdToInputInfo should be(Map(0 -> StreamInputInfo(0, 1L)))
    }
    //submissionTime:1482311354971===processingStartTime:1482311354983===processingEndTime:1482311355753
    batchInfosCompleted.map(x=>println(x.submissionTime+"==="+x.processingStartTime.get+"==="+x.processingEndTime.get))
    //判断是否自增顺序
    isInIncreasingOrder(batchInfosCompleted.map(_.submissionTime)) should be(true)
    isInIncreasingOrder(batchInfosCompleted.map(_.processingStartTime.get)) should be(true)    
    isInIncreasingOrder(batchInfosCompleted.map(_.processingEndTime.get)) should be(true)
  }

  test("receiver info reporting") { //接收信息报告
    ssc = new StreamingContext("local[2]", "test", Milliseconds(1000))
    val inputStream = ssc.receiverStream(new StreamingListenerSuiteReceiver)
    inputStream.foreachRDD(x=>{
     // println("====="+x.count)
      x.count
      })

    val collector = new ReceiverInfoCollector
    ssc.addStreamingListener(collector)

    ssc.start()
    try {
     // println("======")
      eventually(timeout(30 seconds), interval(20 millis)) {
        //接收数据
        collector.startedReceiverStreamIds.size should equal(1)
        //接收数据数据
        collector.startedReceiverStreamIds(0) should equal(0)
        //暂停接收数据数
        collector.stoppedReceiverStreamIds should have size 1
        //暂停接收数据
        collector.stoppedReceiverStreamIds(0) should equal(0)
        //接收错误的数据
        collector.receiverErrors should have size 1
        //接收数据错误ID
        collector.receiverErrors(0)._1 should equal(0)
        //错误异常信息
        collector.receiverErrors(0)._2 should include("report error")
        //
        collector.receiverErrors(0)._3 should include("report exception")
      }
    } finally {
      ssc.stop()
    }
  }

  test("onBatchCompleted with successful batch") { //批处理完成与成功
    ssc = new StreamingContext("local[2]", "test", Milliseconds(1000))
    val inputStream = ssc.receiverStream(new StreamingListenerSuiteReceiver)
    inputStream.foreachRDD(_.count)

    val failureReasons = startStreamingContextAndCollectFailureReasons(ssc)
    //一个成功的批量没有设置错误信息
    assert(failureReasons != null && failureReasons.isEmpty,
      "A successful batch should not set errorMessage")
  }

  test("onBatchCompleted with failed batch and one failed job") { //批处理失败和一个失败的Job
  //分隔的时间叫作批次间隔
    ssc = new StreamingContext("local[2]", "test", Milliseconds(1000))
    val inputStream = ssc.receiverStream(new StreamingListenerSuiteReceiver)
    inputStream.foreachRDD { _ =>
      throw new RuntimeException("This is a failed job")
    }

    // Check if failureReasons contains the correct error message
    // 检查是否包含正确的错误消息
    val failureReasons = startStreamingContextAndCollectFailureReasons(ssc, isFailed = true)
    assert(failureReasons != null)
    assert(failureReasons.size === 1)
    assert(failureReasons.contains(0))
    //失败原因
    assert(failureReasons(0).contains("This is a failed job"))
  }

  test("onBatchCompleted with failed batch and multiple failed jobs") {
    //分隔的时间叫作批次间隔
    ssc = new StreamingContext("local[2]", "test", Milliseconds(1000))
    val inputStream = ssc.receiverStream(new StreamingListenerSuiteReceiver)
    inputStream.foreachRDD { _ =>
      throw new RuntimeException("This is a failed job")
    }
    inputStream.foreachRDD { _ =>
      throw new RuntimeException("This is another failed job")
    }

    // Check if failureReasons contains the correct error messages
    //检查是否包含正确的错误消息
    val failureReasons =
      startStreamingContextAndCollectFailureReasons(ssc, isFailed = true)
    assert(failureReasons != null)
    assert(failureReasons.size === 2)
    assert(failureReasons.contains(0))
    assert(failureReasons.contains(1))
    assert(failureReasons(0).contains("This is a failed job"))
    assert(failureReasons(1).contains("This is another failed job"))
  }

  private def startStreamingContextAndCollectFailureReasons(
    _ssc: StreamingContext, isFailed: Boolean = false): Map[Int, String] = {
    //添加失败原因集合
    val failureReasonsCollector = new FailureReasonsCollector()
    _ssc.addStreamingListener(failureReasonsCollector)
    val batchCounter = new BatchCounter(_ssc)
    _ssc.start()
    // Make sure running at least one batch
    //确保运行至少一个批
    batchCounter.waitUntilBatchesCompleted(expectedNumCompletedBatches = 1, timeout = 10000)
    if (isFailed) {
      intercept[RuntimeException] {
        _ssc.awaitTerminationOrTimeout(10000)
      }
    }
    _ssc.stop()
    failureReasonsCollector.failureReasons
  }

  /**
   *  Check if a sequence of numbers is in increasing order
   *  检查一个数字序列是否在递增顺序
   */
  def isInIncreasingOrder(seq: Seq[Long]): Boolean = {
    for (i <- 1 until seq.size) {
      if (seq(i - 1) > seq(i)) {
        return false
      }
    }
    true
  }
}

/**
 *  用户利用这个类中的方法可以获得作业当前处理的情况,获得作业内部信息,
 *  当用户需要监控或者输出内部状态时
 *  Listener that collects information on processed batches
 *
 */
class BatchInfoCollector extends StreamingListener {
  val batchInfosCompleted = new ArrayBuffer[BatchInfo] with SynchronizedBuffer[BatchInfo]
  val batchInfosStarted = new ArrayBuffer[BatchInfo] with SynchronizedBuffer[BatchInfo]
  val batchInfosSubmitted = new ArrayBuffer[BatchInfo] with SynchronizedBuffer[BatchInfo]
  //提交作业时间
  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted) {
    batchInfosSubmitted += batchSubmitted.batchInfo
  }
  //批处理开始运行
  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted) {
    batchInfosStarted += batchStarted.batchInfo
  }
  //批处理完成
  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
    batchInfosCompleted += batchCompleted.batchInfo
  }
}

/** 
 *  Listener that collects information on processed batches
 *  侦听器,处理的批次集合的信息 
 *  */
class ReceiverInfoCollector extends StreamingListener {
  val startedReceiverStreamIds = new ArrayBuffer[Int] with SynchronizedBuffer[Int]
  val stoppedReceiverStreamIds = new ArrayBuffer[Int] with SynchronizedBuffer[Int]
  val receiverErrors =
    new ArrayBuffer[(Int, String, String)] with SynchronizedBuffer[(Int, String, String)]
  //接收开始
  override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted) {
    startedReceiverStreamIds += receiverStarted.receiverInfo.streamId
  }
  //接收暂停
  override def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped) {
    stoppedReceiverStreamIds += receiverStopped.receiverInfo.streamId
  }
  //接收错误
  override def onReceiverError(receiverError: StreamingListenerReceiverError) {
    receiverErrors += ((receiverError.receiverInfo.streamId,
      receiverError.receiverInfo.lastErrorMessage, receiverError.receiverInfo.lastError))
  }
}

class StreamingListenerSuiteReceiver extends Receiver[Any](StorageLevel.MEMORY_ONLY) with Logging {
  def onStart() {
    //Future 只能写一次： 当一个 future 完成后,它就不能再被改变了,
    //Future 只提供了读取计算值的接口,写入计算值的任务交给了 Promise
    Future {
      logInfo("Started receiver and sleeping")
      println("Started receiver and sleeping")
      Thread.sleep(10)
      println("Reporting error and sleeping")
      logInfo("Reporting error and sleeping")
      
      reportError("test report error", new Exception("test report exception"))
      Thread.sleep(10)
      println("Stopping")
      logInfo("Stopping")
      stop("test stop error")
    }
  }
  def onStop() {}
}

/**
 * A StreamingListener that saves the latest `failureReasons` in `BatchInfo` to the `failureReasons`
 * field.
 * Streaming监听器最新失败原因在'BatchInfo'保存到FailureReasonsCollector字段失败
 */
class FailureReasonsCollector extends StreamingListener {

  @volatile var failureReasons: Map[Int, String] = null

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    failureReasons = batchCompleted.batchInfo.failureReasons
  }
}
