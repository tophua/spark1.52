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

package org.apache.spark.rdd

import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.serializer.Serializer

private[spark] class ShuffledRDDPartition(val idx: Int) extends Partition {
  override val index: Int = idx
  override def hashCode(): Int = idx
}

/**
 * :: DeveloperApi ::
 * The resulting RDD from a shuffle (e.g. repartitioning of data).
  * 来自洗牌的RDD(例如重新分配数据)
 * 
 * @param prev the parent RDD.父RDD
 * @param part the partitioner used to partition the RDD 使用那个分区
 * @tparam K the key class.
 * @tparam V the value class.
 * @tparam C the combiner class. 合成
 */
// TODO: Make this return RDD[Product2[K, C]] or have some way to configure mutable pairs
@DeveloperApi
class ShuffledRDD[K, V, C](
    @transient var prev: RDD[_ <: Product2[K, V]],
    part: Partitioner)//分区
  extends RDD[(K, C)](prev.context, Nil) {

  private var serializer: Option[Serializer] = None

  private var keyOrdering: Option[Ordering[K]] = None

  private var aggregator: Option[Aggregator[K, V, C]] = None
//是否需要在worker端进行combine操作
  private var mapSideCombine: Boolean = false

  /** 
   *  Set a serializer for this RDD's shuffle, or null to use the default (spark.serializer)
   *  设置RDD shuffle序列化,如果空使用默认序列 spark.serializer
   *  */
  def setSerializer(serializer: Serializer): ShuffledRDD[K, V, C] = {
    this.serializer = Option(serializer)
    this
  }

  /** 
   *  Set key ordering for RDD's shuffle. 
   *  RDD Shffle key值的排序
   *  */
  def setKeyOrdering(keyOrdering: Ordering[K]): ShuffledRDD[K, V, C] = {
    this.keyOrdering = Option(keyOrdering)
    this
  }

  /** 
   *  Set aggregator for RDD's shuffle. 
   *  RDD Shffle 聚合
   *  */
  def setAggregator(aggregator: Aggregator[K, V, C]): ShuffledRDD[K, V, C] = {
    this.aggregator = Option(aggregator)
    this
  }

  /** 
   *  Set mapSideCombine flag for RDD's shuffle. 
   *  设置mapSideCombine标记
   *  */
  def setMapSideCombine(mapSideCombine: Boolean): ShuffledRDD[K, V, C] = {
    this.mapSideCombine = mapSideCombine
    this
  }
  //ShuffledRDD 依赖于ShuffleDependency 
  override def getDependencies: Seq[Dependency[_]] = {
    List(new ShuffleDependency(prev, part, serializer, keyOrdering, aggregator, mapSideCombine))
  }

  override val partitioner = Some(part) //设置分区

  override def getPartitions: Array[Partition] = {
    //返回包含一个给定的函数的值超过从0开始的范围内的整数值的数组
    Array.tabulate[Partition](part.numPartitions)(i => new ShuffledRDDPartition(i))
  }
  /**
   * 读取ShuffleMapTask计算结果的触发点
   */
  override def compute(split: Partition, context: TaskContext): Iterator[(K, C)] = {
    val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, V, C]]
    //shuffleManager.getReader返回HashShuffle中的reader函数的具体实现
    //首先调用SortShuffleManager的getReader方法创建HashShuffleReader,然后执行HashShuffleReader的read方法
    //读取依赖任务的中间计算结果
    SparkEnv.get.shuffleManager.getReader(dep.shuffleHandle, split.index, split.index + 1, context)
      .read()
      .asInstanceOf[Iterator[(K, C)]]
  }

  override def clearDependencies() {
    super.clearDependencies()
    prev = null
  }
}
