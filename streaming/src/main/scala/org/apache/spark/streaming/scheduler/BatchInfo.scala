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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.streaming.Time

/**
 * :: DeveloperApi ::
 * Class having information on completed batches.
 * @param batchTime   Time of the batch,批次时间
 * @param streamIdToInputInfo A map of input stream id to its input info
 * 														输入流ID的映射到它的输入信息
 * @param submissionTime  Clock time of when jobs of this batch was submitted to
 *                        the streaming scheduler queue
 *                        提交作业时间
 * @param processingStartTime Clock time of when the first job of this batch started processing
 *                            开始运行作业时间
 * @param processingEndTime Clock time of when the last job of this batch finished processing
 *                          作业结束时间
 */
@DeveloperApi
case class BatchInfo(
    batchTime: Time,//批次时间
    streamIdToInputInfo: Map[Int, StreamInputInfo],//输入信息的流标识
    submissionTime: Long,//提交时间
    processingStartTime: Option[Long],//处理开始时间
    processingEndTime: Option[Long]//处理结果时间
  ) {

  private var _failureReasons: Map[Int, String] = Map.empty

  private var _numOutputOp: Int = 0

  @deprecated("Use streamIdToInputInfo instead", "1.5.0")
  def streamIdToNumRecords: Map[Int, Long] = streamIdToInputInfo.mapValues(_.numRecords)

  /**
   * Time taken for the first job of this batch to start processing from the time this batch
   * was submitted to the streaming scheduler. Essentially, it is
   * 从批处理开始的时间开始处理第一批作业的时间,被提交给流调度
   * `processingStartTime` - `submissionTime`.
   */
  def schedulingDelay: Option[Long] = processingStartTime.map(_ - submissionTime)

  /**
   * Time taken for the all jobs of this batch to finish processing from the time they started
   * processing. Essentially, it is `processingEndTime` - `processingStartTime`.
   *  从提交Job(作业)到该批处理的开始的时间.
   */
  def processingDelay: Option[Long] = processingEndTime.zip(processingStartTime)
    .map(x => x._1 - x._2).headOption

  /**
   * Time taken for all the jobs of this batch to finish processing from the time they
   * were submitted.  Essentially, it is `processingDelay` + `schedulingDelay`.
   * 从提交Job(作业)到该批处理的完成的时间.
   */
  def totalDelay: Option[Long] = schedulingDelay.zip(processingDelay)
    .map(x => x._1 + x._2).headOption

  /**
   * The number of recorders received by the receivers in this batch.
   * 批理接收数据数
   */
  def numRecords: Long = streamIdToInputInfo.values.map(_.numRecords).sum

  /** 
   *  Set the failure reasons corresponding to every output ops in the batch 
   *  设置对应于批处理中的每一个输出操作的失败原因
   *  */
  private[streaming] def setFailureReason(reasons: Map[Int, String]): Unit = {
    _failureReasons = reasons
  }

  /** 
   *  Failure reasons corresponding to every output ops in the batch 
   *  批处理中的每一个输出操作失败的原因
   *  */
  private[streaming] def failureReasons = _failureReasons

  /** 
   *  Set the number of output operations in this batch 
   *  在该批处理中设置输出操作数
   *  */
  private[streaming] def setNumOutputOp(numOutputOp: Int): Unit = {
    _numOutputOp = numOutputOp
  }

  /** 
   *  Return the number of output operations in this batch 
   *  返回此批中的输出操作数
   *  */
  private[streaming] def numOutputOp: Int = _numOutputOp
}
