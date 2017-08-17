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

package org.apache.spark.shuffle

import org.apache.spark.storage.BlockManagerId
import org.apache.spark.{FetchFailed, TaskEndReason}
import org.apache.spark.util.Utils

/**
 * Failed to fetch a shuffle block. The executor catches this exception and propagates it
 * back to DAGScheduler (through TaskEndReason) so we'd resubmit the previous stage.
  * 无法获取随机播放块,执行器捕获此异常并将其传播回DAGScheduler（通过TaskEndReason），以便我们重新提交上一个阶段。
 *
 * Note that bmAddress can be null.
  * 请注意,bmAddress可以为null
 */
private[spark] class FetchFailedException(
    bmAddress: BlockManagerId,
    shuffleId: Int,
    mapId: Int,//mapId对应RDD的partionsID
    reduceId: Int,
    message: String,
    cause: Throwable = null)
  extends Exception(message, cause) {

  def this(
      bmAddress: BlockManagerId,
      shuffleId: Int,
      mapId: Int,//mapId对应RDD的partionsID
      reduceId: Int,
      cause: Throwable) {
    //mapId对应RDD的partionsID
    this(bmAddress, shuffleId, mapId, reduceId, cause.getMessage, cause)
  }
  //mapId对应RDD的partionsID
  def toTaskEndReason: TaskEndReason = FetchFailed(bmAddress, shuffleId, mapId, reduceId,
    Utils.exceptionString(this))
}

/**
 * Failed to get shuffle metadata from [[org.apache.spark.MapOutputTracker]].
  * 无法从[[org.apache.spark.MapOutputTracker]]中获取随机元数据
 */
private[spark] class MetadataFetchFailedException(
    shuffleId: Int,
    reduceId: Int,
    message: String)
  extends FetchFailedException(null, shuffleId, -1, reduceId, message)
