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

import scala.collection.mutable.Queue

import org.apache.spark.util.Distribution
import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 * Base trait for events related to StreamingListener
 * 有关StreamingListener事件的基础特征
 */
@DeveloperApi
sealed trait StreamingListenerEvent

@DeveloperApi
case class StreamingListenerBatchSubmitted(batchInfo: BatchInfo) extends StreamingListenerEvent

@DeveloperApi
case class StreamingListenerBatchCompleted(batchInfo: BatchInfo) extends StreamingListenerEvent

@DeveloperApi
case class StreamingListenerBatchStarted(batchInfo: BatchInfo) extends StreamingListenerEvent

@DeveloperApi
case class StreamingListenerReceiverStarted(receiverInfo: ReceiverInfo)
  extends StreamingListenerEvent

@DeveloperApi
case class StreamingListenerReceiverError(receiverInfo: ReceiverInfo)
  extends StreamingListenerEvent

@DeveloperApi
case class StreamingListenerReceiverStopped(receiverInfo: ReceiverInfo)
  extends StreamingListenerEvent

/**
 * :: DeveloperApi ::
 * A listener interface for receiving information about an ongoing streaming
 * computation.
 * 用于接收正在进行的流计算的信息的侦听器接口
 */
@DeveloperApi
trait StreamingListener {

  /** 
   *  Called when a receiver has been started 
   *  调用一个接收器已经启动
   *  */
  def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted) { }

  /** 
   *  Called when a receiver has reported an error 
   *  调用一个接收器报告了一个错误
   *  */
  def onReceiverError(receiverError: StreamingListenerReceiverError) { }

  /** 
   *  Called when a receiver has been stopped 
   *  调用一个接收器被停止
   *  */
  def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped) { }

  /** 
   *  Called when a batch of jobs has been submitted for processing. 
   *  调用一批工作已提交处理时
   *  */
  def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted) { }

  /** 
   *  Called when processing of a batch of jobs has started. 
   *  调用处理一批工作已经开始时
   *   */
  def onBatchStarted(batchStarted: StreamingListenerBatchStarted) { }

  /** 
   *  Called when processing of a batch of jobs has completed. 
   *  调用处理一批作业完成时调用
   *  */
  def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) { }
}


/**
 * :: DeveloperApi ::
 * A simple StreamingListener that logs summary statistics across Spark Streaming batches
 * @param numBatchInfos Number of last batches to consider for generating statistics (default: 10)
 */
@DeveloperApi
class StatsReportListener(numBatchInfos: Int = 10) extends StreamingListener {
  // Queue containing latest completed batches
  val batchInfos = new Queue[BatchInfo]()

  override def onBatchCompleted(batchStarted: StreamingListenerBatchCompleted) {
    batchInfos.enqueue(batchStarted.batchInfo)
    if (batchInfos.size > numBatchInfos) batchInfos.dequeue()
    printStats()
  }

  def printStats() {
    showMillisDistribution("Total delay: ", _.totalDelay)
    showMillisDistribution("Processing time: ", _.processingDelay)
  }

  def showMillisDistribution(heading: String, getMetric: BatchInfo => Option[Long]) {
    org.apache.spark.scheduler.StatsReportListener.showMillisDistribution(
      heading, extractDistribution(getMetric))
  }

  def extractDistribution(getMetric: BatchInfo => Option[Long]): Option[Distribution] = {
    Distribution(batchInfos.flatMap(getMetric(_)).map(_.toDouble))
  }
}
