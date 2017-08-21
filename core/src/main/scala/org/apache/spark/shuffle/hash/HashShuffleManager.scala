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

package org.apache.spark.shuffle.hash

import org.apache.spark._
import org.apache.spark.shuffle._

/**
 * Hash Shuffle适合中小型规模的数据计算
 * A ShuffleManager using hashing, that creates one output file per reduce partition on each
 * mapper (possibly reusing these across waves of tasks).
  * 一个ShuffleManager使用哈希,每个mapper上的reduce分区创建一个输出文件(可能会重复使用这些跨越任务)
 */
private[spark] class HashShuffleManager(conf: SparkConf) extends ShuffleManager {

  private val fileShuffleBlockResolver = new FileShuffleBlockResolver(conf)

  /* Register a shuffle with the manager and obtain a handle for it to pass to tasks.
  * 与manager注册一个洗牌,并获得一个句柄来传递给任务*/
  override def registerShuffle[K, V, C](
      shuffleId: Int,
      numMaps: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    new BaseShuffleHandle(shuffleId, numMaps, dependency)
  }

  /**
   * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive).
   * Called on executors by reduce tasks.
    * 获取一系列读取减少分区（startPartition到endPartition-1，包括）通过减少任务指定执行程序。
   */
  override def getReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext): ShuffleReader[K, C] = {
    new HashShuffleReader(
      handle.asInstanceOf[BaseShuffleHandle[K, _, C]], startPartition, endPartition, context)
  }

  /** Get a writer for a given partition. Called on executors by map tasks.
    * 获取一个给定写的分区,通过Map任务调用执行器,mapId对应RDD的partionsID*/
  override def getWriter[K, V](handle: ShuffleHandle, mapId: Int, context: TaskContext)
      : ShuffleWriter[K, V] = {
    new HashShuffleWriter(//mapId对应RDD的partionsID
      shuffleBlockResolver, handle.asInstanceOf[BaseShuffleHandle[K, V, _]], mapId, context)
  }

  /** Remove a shuffle's metadata from the ShuffleManager.
    * 从ShuffleManager中删除shuffle的元数据 */
  override def unregisterShuffle(shuffleId: Int): Boolean = {
    shuffleBlockResolver.removeShuffle(shuffleId)
  }

  override def shuffleBlockResolver: FileShuffleBlockResolver = {
    fileShuffleBlockResolver
  }

  /** Shut down this ShuffleManager.
    * 关闭这个ShuffleManager*/
  override def stop(): Unit = {
    shuffleBlockResolver.stop()
  }
}
