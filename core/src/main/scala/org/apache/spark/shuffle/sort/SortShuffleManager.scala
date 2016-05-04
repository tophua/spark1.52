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

package org.apache.spark.shuffle.sort

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.{SparkConf, TaskContext, ShuffleDependency}
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.hash.HashShuffleReader
/**
 * sorted Shuffle就会极大的节省内存和磁盘的访问，所以更有利于更大规模的计算
 */
private[spark] class SortShuffleManager(conf: SparkConf) extends ShuffleManager {

  private val indexShuffleBlockResolver = new IndexShuffleBlockResolver(conf)
  private val shuffleMapNumber = new ConcurrentHashMap[Int, Int]()

  /**
   * Register a shuffle with the manager and obtain a handle for it to pass to tasks.
   */
  override def registerShuffle[K, V, C](
      shuffleId: Int,
      numMaps: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    new BaseShuffleHandle(shuffleId, numMaps, dependency)
  }

  /**
   * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive).
   * Called on executors by reduce tasks.
   */
  override def getReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext): ShuffleReader[K, C] = {
    // We currently use the same block store shuffle fetcher as the hash-based shuffle.
    //创建HashShuffleReader,然后执行getReader
    new HashShuffleReader(
      handle.asInstanceOf[BaseShuffleHandle[K, _, C]], startPartition, endPartition, context)
  }

  /** 
   *  Get a writer for a given partition. Called on executors by map tasks.
   *  mapId 是partitionId
   *   */
  override def getWriter[K, V](handle: ShuffleHandle, mapId: Int, context: TaskContext)
      : ShuffleWriter[K, V] = {
    val baseShuffleHandle = handle.asInstanceOf[BaseShuffleHandle[K, V, _]]
    //ConcurrentHashMap put和putIfAbsent的区别就是一个是直接放入并替换，另一个是有就不替换
    shuffleMapNumber.putIfAbsent(baseShuffleHandle.shuffleId, baseShuffleHandle.numMaps)
    
    //创建SortShuffleWriter对象,shuffleBlockResolver：index文件
    new SortShuffleWriter(
      shuffleBlockResolver, baseShuffleHandle, mapId, context)
  }

  /** Remove a shuffle's metadata from the ShuffleManager. */
  override def unregisterShuffle(shuffleId: Int): Boolean = {
    if (shuffleMapNumber.containsKey(shuffleId)) {
      val numMaps = shuffleMapNumber.remove(shuffleId)
      (0 until numMaps).map{ mapId =>
        shuffleBlockResolver.removeDataByMap(shuffleId, mapId)
      }
    }
    true
  }

  override val shuffleBlockResolver: IndexShuffleBlockResolver = {
    indexShuffleBlockResolver
  }

  /** Shut down this ShuffleManager. */
  override def stop(): Unit = {
    shuffleBlockResolver.stop()
  }
}

