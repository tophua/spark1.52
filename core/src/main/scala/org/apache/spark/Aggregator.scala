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

package org.apache.spark

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.util.collection.{AppendOnlyMap, ExternalAppendOnlyMap}

/**
 * :: DeveloperApi ::
 * A set of functions used to aggregate data.
 * 当数据集键值对形式组织的时候,聚合具有相同键的元素进行一些统计
 * 
 * shuffle中的aggregate操作实际是把一个KV对的集合,变成一个KC对的map,C是指combiner,是V聚合成的结果.
 * Aggregator的三个类型参数K,V,C即代表Key的类型,Value的类型和Combiner的类型。
 * 
 * @param createCombiner function to create the initial value of the aggregation.
 * @param mergeValue function to merge a new value into the aggregation result.
 * @param mergeCombiners function to merge outputs from multiple mergeValue function.
 */
@DeveloperApi
case class Aggregator[K, V, C] (
    createCombiner: V => C,//描述了对于原KV对里由一个Value生成Combiner,以作为聚合的起始点
    mergeValue: (C, V) => C,//描述了如何把一个新的Value(类型为V)合并到之前聚合的结果(类型为C)里
    mergeCombiners: (C, C) => C) {//描述了如何把两个分别聚合好了的Combiner再聚合

  // When spilling is enabled sorting will happen externally, but not necessarily with an
  // ExternalSorter.
  //如果为true,在shuffle期间通过溢出数据保存到磁盘来降低了内存使用总量,溢出值是spark.shuffle.memoryFraction指定
  private val isSpillEnabled = SparkEnv.get.conf.getBoolean("spark.shuffle.spill", true)

  @deprecated("use combineValuesByKey with TaskContext argument", "0.9.0")
  def combineValuesByKey(iter: Iterator[_ <: Product2[K, V]]): Iterator[(K, C)] =
    combineValuesByKey(iter, null)
/**
 * combineByKey()的处理流程如下：
 * 1,如果是一个新的元素，此时使用createCombiner()来创建那个键对应的累加器的初始值。
 *   (注意:这个过程会在每个分区第一次出现各个键时发生，而不是在整个RDD中第一次出现一个键时发生)
 * 2,如果这是一个在处理当前分区中之前已经遇到键，此时combineByKey()使用mergeValue()将该键的累加器对应的当前值与这个新值进行合并。
 * 3,由于每个分区都是独立处理的，因此对于同一个键可以有多个累加器。如果有两个或者更多的分区都有对应同一个键的累加器，
 *   就需要使用用户提供的mergeCombiners()将各个分区的结果进行合并.
 **/
  def combineValuesByKey(iter: Iterator[_ <: Product2[K, V]],
                         context: TaskContext): Iterator[(K, C)] = {
    if (!isSpillEnabled) {//是否保存磁盘
      val combiners = new AppendOnlyMap[K, C]
      var kv: Product2[K, V] = null
      val update = (hadValue: Boolean, oldValue: C) => {
        if (hadValue) mergeValue(oldValue, kv._2) else createCombiner(kv._2)
      }
      while (iter.hasNext) {
        kv = iter.next()
        combiners.changeValue(kv._1, update)
      }
      combiners.iterator
    } else {
      val combiners = new ExternalAppendOnlyMap[K, V, C](createCombiner, mergeValue, mergeCombiners)
      combiners.insertAll(iter)
      updateMetrics(context, combiners)
      combiners.iterator
    }
  }

  @deprecated("use combineCombinersByKey with TaskContext argument", "0.9.0")
  def combineCombinersByKey(iter: Iterator[_ <: Product2[K, C]]) : Iterator[(K, C)] =
    combineCombinersByKey(iter, null)

  def combineCombinersByKey(iter: Iterator[_ <: Product2[K, C]], context: TaskContext)
    : Iterator[(K, C)] =
  {
    if (!isSpillEnabled) {
      val combiners = new AppendOnlyMap[K, C]
      var kc: Product2[K, C] = null
      val update = (hadValue: Boolean, oldValue: C) => {
        if (hadValue) mergeCombiners(oldValue, kc._2) else kc._2
      }
      while (iter.hasNext) {
        kc = iter.next()
        combiners.changeValue(kc._1, update)
      }
      combiners.iterator
    } else {
      val combiners = new ExternalAppendOnlyMap[K, C, C](identity, mergeCombiners, mergeCombiners)
      combiners.insertAll(iter)
      updateMetrics(context, combiners)
      combiners.iterator
    }
  }

  /** Update task metrics after populating the external map. */
  private def updateMetrics(context: TaskContext, map: ExternalAppendOnlyMap[_, _, _]): Unit = {
    Option(context).foreach { c =>
      c.taskMetrics().incMemoryBytesSpilled(map.memoryBytesSpilled)
      c.taskMetrics().incDiskBytesSpilled(map.diskBytesSpilled)
      c.internalMetricsToAccumulators(
        InternalAccumulator.PEAK_EXECUTION_MEMORY).add(map.peakMemoryUsedBytes)
    }
  }
}
