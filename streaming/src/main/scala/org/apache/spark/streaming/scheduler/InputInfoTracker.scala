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

import scala.collection.mutable

import org.apache.spark.Logging
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.streaming.{Time, StreamingContext}

/**
 * :: DeveloperApi ::
 * Track the information of input stream at specified batch time.
 * 在指定的批处理时间跟踪输入流的信息
 * @param inputStreamId the input stream id 输入流标识
 * @param numRecords the number of records in a batch 批次中记录的数量
 * @param metadata metadata for this batch. It should contain at least one standard field named
 *                 "Description" which maps to the content that will be shown in the UI.
 *                 它应该包含至少一个名为"描述"映射到将在用户界面中显示的内容
 */
@DeveloperApi
case class StreamInputInfo(
    inputStreamId: Int, numRecords: Long, metadata: Map[String, Any] = Map.empty) {
  require(numRecords >= 0, "numRecords must not be negative")

  def metadataDescription: Option[String] =
    metadata.get(StreamInputInfo.METADATA_KEY_DESCRIPTION).map(_.toString)
}

@DeveloperApi
object StreamInputInfo {

  /**
   * The key for description in `StreamInputInfo.metadata`.
   * 在键值的'streaminputinfo描述元数据'
   */
  val METADATA_KEY_DESCRIPTION: String = "Description"
}

/**
 * This class manages all the input streams as well as their input data statistics. The information
 * will be exposed through StreamingListener for monitoring.
 * 这个类管理所有的输入流以及它们的输入数据统计,信息将通过StreamingListener的监测
 */
private[streaming] class InputInfoTracker(ssc: StreamingContext) extends Logging {

  // Map to track all the InputInfo related to specific batch time and input stream.
  //Map追踪所有的inputinfo相关特定的批处理时间和输入流
  private val batchTimeToInputInfos =
    new mutable.HashMap[Time, mutable.HashMap[Int, StreamInputInfo]]

  /** 
   *  Report the input information with batch time to the tracker 
   *  向跟踪者报告批处理时间的输入信息
   *  */
  def reportInfo(batchTime: Time, inputInfo: StreamInputInfo): Unit = synchronized {
    val inputInfos = batchTimeToInputInfos.getOrElseUpdate(batchTime,
      new mutable.HashMap[Int, StreamInputInfo]())

    if (inputInfos.contains(inputInfo.inputStreamId)) {
      throw new IllegalStateException(s"Input stream ${inputInfo.inputStreamId} for batch" +
        s"$batchTime is already added into InputInfoTracker, this is a illegal state")
    }
    inputInfos += ((inputInfo.inputStreamId, inputInfo))
  }

  /** 
   *  Get the all the input stream's information of specified batch time 
   *  获取指定批处理时间的所有输入流的信息
   *  */
  def getInfo(batchTime: Time): Map[Int, StreamInputInfo] = synchronized {
    val inputInfos = batchTimeToInputInfos.get(batchTime)
    // Convert mutable HashMap to immutable Map for the caller
    //将可变的HashMap转换成不变的Map
    inputInfos.map(_.toMap).getOrElse(Map[Int, StreamInputInfo]())
  }

  /** 
   *  Cleanup the tracked input information older than threshold batch time 
   *  清除比阈值批处理时间更大的跟踪输入信息
   *  */
  def cleanup(batchThreshTime: Time): Unit = synchronized {
    val timesToCleanup = batchTimeToInputInfos.keys.filter(_ < batchThreshTime)
    logInfo(s"remove old batch metadata: ${timesToCleanup.mkString(" ")}")
    batchTimeToInputInfos --= timesToCleanup
  }
}
