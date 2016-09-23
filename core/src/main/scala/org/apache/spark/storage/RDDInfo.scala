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

package org.apache.spark.storage

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.{RDDOperationScope, RDD}
import org.apache.spark.util.Utils

@DeveloperApi
class RDDInfo(
    val id: Int,
    val name: String,
    val numPartitions: Int,//分区数
    var storageLevel: StorageLevel,//存储级别
    val parentIds: Seq[Int],//父RDD列表
    val scope: Option[RDDOperationScope] = None)
  extends Ordered[RDDInfo] {

  var numCachedPartitions = 0//缓存分区数
  var memSize = 0L//内存大小
  var diskSize = 0L//硬盘大小
  var externalBlockStoreSize = 0L//扩展块存储大小
  //是否缓存,
  def isCached: Boolean =
    (memSize + diskSize + externalBlockStoreSize > 0) && numCachedPartitions > 0

  override def toString: String = {
    import Utils.bytesToString
    ("RDD \"%s\" (%d) StorageLevel: %s; CachedPartitions: %d; TotalPartitions: %d; " +
      "MemorySize: %s; ExternalBlockStoreSize: %s; DiskSize: %s").format(
        name, id, storageLevel.toString, numCachedPartitions, numPartitions,
        bytesToString(memSize), bytesToString(externalBlockStoreSize), bytesToString(diskSize))
  }

  override def compare(that: RDDInfo): Int = {
    this.id - that.id
  }
}

private[spark] object RDDInfo {
  def fromRdd(rdd: RDD[_]): RDDInfo = {
    val rddName = Option(rdd.name).getOrElse(Utils.getFormattedClassName(rdd))
    val parentIds = rdd.dependencies.map(_.rdd.id)//返回RDD依赖的父列表
    new RDDInfo(rdd.id, rddName, rdd.partitions.length, rdd.getStorageLevel, parentIds, rdd.scope)
  }
}
