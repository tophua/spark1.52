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
 * @param createCombiner function to create the initial value of the aggregation.函数创建聚合的初始值
 * @param mergeValue function to merge a new value into the aggregation result.将新值合并到聚合结果中的功能
 * @param mergeCombiners function to merge outputs from multiple mergeValue function.用于合并来自多个mergeValue函数的输出
 */

@DeveloperApi
case class Aggregator[K, V, C] (
    createCombiner: V => C,//描述了对于原KV对里由一个Value生成Combiner,以作为聚合的起始点
    mergeValue: (C, V) => C,//描述了如何把一个新的Value(类型为V)合并到之前聚合的结果(类型为C)里
    mergeCombiners: (C, C) => C) {//描述了如何把两个分别聚合好了的Combiner再聚合

  // When spilling is enabled sorting will happen externally, but not necessarily with an
  // ExternalSorter.
  //spark.shuffle.spill用于指定Shuffle过程中如果内存中的数据超过阈值(参考spark.shuffle.memoryFraction的设置),
  //那么是否需要将部分数据临时写入外部存储。如果设置为false，那么这个过程就会一直使用内,最后再合并到最终的Shuffle输出文件中去。
  private val isSpillEnabled = SparkEnv.get.conf.getBoolean("spark.shuffle.spill", true)

  @deprecated("use combineValuesByKey with TaskContext argument", "0.9.0")
  def combineValuesByKey(iter: Iterator[_ <: Product2[K, V]]): Iterator[(K, C)] =
    combineValuesByKey(iter, null)
/**
 * reduce端在缓存中对中间计算结果执行聚合和排序
 **/
  def combineValuesByKey(iter: Iterator[_ <: Product2[K, V]],
                         context: TaskContext): Iterator[(K, C)] = {
    //spark.shuffle.spill用于指定Shuffle过程中如果内存中的数据超过阈值(参考spark.shuffle.memoryFraction的设置),
    //那么是否需要将部分数据临时写入外部存储。如果设置为false，那么这个过程就会一直使用内,最后再合并到最终的Shuffle输出文件中去。
    if (!isSpillEnabled) {//如果为false
      val combiners = new AppendOnlyMap[K, C]
      var kv: Product2[K, V] = null
      val update = (hadValue: Boolean, oldValue: C) => {
        if (hadValue) mergeValue(oldValue, kv._2) else createCombiner(kv._2)
      }
      //用map来去重,用update方法来更新值,如果没值的时候,返回值,如果有值的时候,通过mergeValue方法来合并
      //mergeValue方法就是我们在reduceByKey里面写的那个匿名函数,在这里就是（_ + _）
      while (iter.hasNext) {
        kv = iter.next()
        //再次调用AppendOnlyMap的changeValue方法
        combiners.changeValue(kv._1, update)
      }
      combiners.iterator
    } else {
      //isSpillEnabled为true,会使用ExternalAppendOnlyMap完成聚合
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

  /** 
   *  Update task metrics after populating the external map. 
   *  更新任务指标后填充外部Map任务
   *  */
  private def updateMetrics(context: TaskContext, map: ExternalAppendOnlyMap[_, _, _]): Unit = {
    Option(context).foreach { c =>
      c.taskMetrics().incMemoryBytesSpilled(map.memoryBytesSpilled)
      c.taskMetrics().incDiskBytesSpilled(map.diskBytesSpilled)
      c.internalMetricsToAccumulators(
        InternalAccumulator.PEAK_EXECUTION_MEMORY).add(map.peakMemoryUsedBytes)
    }
  }
}
