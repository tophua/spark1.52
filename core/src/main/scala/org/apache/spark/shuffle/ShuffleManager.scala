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

import org.apache.spark.{TaskContext, ShuffleDependency}

/** 
 * Pluggable interface for shuffle systems. A ShuffleManager is created in SparkEnv on the driver
 * and on each executor, based on the spark.shuffle.manager setting. The driver registers shuffles
 * with it, and executors (or tasks running locally in the driver) can ask to read and write data.
 * Driver中的ShuffleManager负责注册Shuffle的元数据,比如Shuffle ID,map task的数量等。
 * Executor中的ShuffleManager 则负责读和写Shuffle的数据
 * NOTE: this will be instantiated by SparkEnv so its constructor can take a SparkConf and
 * boolean isDriver as parameters.
  * 注意：这将由SparkEnv实例化，因此其构造函数可以使用SparkConf和boolean isDriver作为参数。
 */
private[spark] trait ShuffleManager {
  /**
   * Register a shuffle with the manager and obtain a handle for it to pass to tasks.
   *  由Driver注册元数据信息,Driver中的------负责注册Shuffle的元数据 
   */
  def registerShuffle[K, V, C](
      shuffleId: Int,
      numMaps: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle

  /** 
   *  Get a writer for a given partition. Called on executors by map tasks. 
   *  获得Shuffle Writer, 根据Shuffle Map Task的ID为其创建Shuffle Writer。  
   *  */
  def getWriter[K, V](handle: ShuffleHandle, mapId: Int, context: TaskContext): ShuffleWriter[K, V]

  /**
   * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive).
   * Called on executors by reduce tasks.
   * 获得Shuffle Reader,根据Shuffle ID和partition的ID为其创建ShuffleReader
   */
  def getReader[K, C](
      handle: ShuffleHandle,//Shuffle ID
      startPartition: Int,//partition的ID
      endPartition: Int,
      context: TaskContext): ShuffleReader[K, C]

  /**
   * 从ShuffleManager中删除shuffle的元数据
    * Remove a shuffle's metadata from the ShuffleManager.
    * @return true if the metadata removed successfully, otherwise false.
    *         如果元数据成功移除,则为true,否则为false
    */
  def unregisterShuffle(shuffleId: Int): Boolean

  /**
   * Return a resolver capable of retrieving shuffle block data based on block coordinates.
    * 返回能够根据块坐标检索shuffle块数据的解析器
   * 返回一个基于Shuffle数据块位置的解析器
   */
  def shuffleBlockResolver: ShuffleBlockResolver

  /** Shut down this ShuffleManager.
    * 关闭这个ShuffleManager*/
  def stop(): Unit
}
