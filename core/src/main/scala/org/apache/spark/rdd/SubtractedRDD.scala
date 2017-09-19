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

import java.util.{HashMap => JHashMap}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.spark.Dependency
import org.apache.spark.OneToOneDependency
import org.apache.spark.Partition
import org.apache.spark.Partitioner
import org.apache.spark.ShuffleDependency
import org.apache.spark.SparkEnv
import org.apache.spark.TaskContext
import org.apache.spark.serializer.Serializer

/**
 * An optimized version of cogroup for set difference/subtraction.
  * 用于设置差分/减法的cogroup的优化版本
 *
 * It is possible to implement this operation with just `cogroup`, but
 * that is less efficient because all of the entries from `rdd2`, for
 * both matching and non-matching values in `rdd1`, are kept in the
 * JHashMap until the end.
  *
  * 可以使用`cogroup`来实现这个操作,但是效率不太高，
  * 因为`rdd2`中的所有条目对于'rdd1'中的匹配值和非匹配值都保留在JHashMap中，直到结束
 *
 * With this implementation, only the entries from `rdd1` are kept in-memory,
 * and the entries from `rdd2` are essentially streamed, as we only need to
 * touch each once to decide if the value needs to be removed.
  *
  * 通过这种实现，只有`rdd1`的条目保存在内存中,“rdd2”中的条目基本上是流式传输的,
  * 因为我们只需要触摸一次,以决定是否需要删除该值。
 *
 * This is particularly helpful when `rdd1` is much smaller than `rdd2`, as
 * you can use `rdd1`'s partitioner/partition size and not worry about running
 * out of memory because of the size of `rdd2`.
  *
  * 当`rdd1`比`rdd2`小得多时这是特别有用的,因为你可以使用rdd1的分区/分区大小,
  * 而不用担心由于rdd2的大小而导致内存不足,
 */
private[spark] class SubtractedRDD[K: ClassTag, V: ClassTag, W: ClassTag](
    @transient var rdd1: RDD[_ <: Product2[K, V]],
    @transient var rdd2: RDD[_ <: Product2[K, W]],
    part: Partitioner)
  extends RDD[(K, V)](rdd1.context, Nil) {

  private var serializer: Option[Serializer] = None

  /** Set a serializer for this RDD's shuffle, or null to use the default (spark.serializer)
    * 为此RDD的随机播放器设置一个串行化器,或者使用默认值(spark.serializer)*/
  def setSerializer(serializer: Serializer): SubtractedRDD[K, V, W] = {
    this.serializer = Option(serializer)
    this
  }

  override def getDependencies: Seq[Dependency[_]] = {
    Seq(rdd1, rdd2).map { rdd =>
      if (rdd.partitioner == Some(part)) {
        logDebug("Adding one-to-one dependency with " + rdd)
        new OneToOneDependency(rdd)
      } else {
        logDebug("Adding shuffle dependency with " + rdd)
        new ShuffleDependency(rdd, part, serializer)
      }
    }
  }

  override def getPartitions: Array[Partition] = {
    val array = new Array[Partition](part.numPartitions)
    for (i <- 0 until array.length) {
      // Each CoGroupPartition will depend on rdd1 and rdd2
      //每个CoGroupPartition将依赖于rdd1和rdd2
      array(i) = new CoGroupPartition(i, Seq(rdd1, rdd2).zipWithIndex.map { case (rdd, j) =>
        dependencies(j) match {
          case s: ShuffleDependency[_, _, _] =>
            None
          case _ =>
            Some(new NarrowCoGroupSplitDep(rdd, i, rdd.partitions(i)))
        }
      }.toArray)
    }
    array
  }

  override val partitioner = Some(part)

  override def compute(p: Partition, context: TaskContext): Iterator[(K, V)] = {
    val partition = p.asInstanceOf[CoGroupPartition]
    val map = new JHashMap[K, ArrayBuffer[V]]
    def getSeq(k: K): ArrayBuffer[V] = {
      val seq = map.get(k)
      if (seq != null) {
        seq
      } else {
        val seq = new ArrayBuffer[V]()
        map.put(k, seq)
        seq
      }
    }
    def integrate(depNum: Int, op: Product2[K, V] => Unit) = {
      dependencies(depNum) match {
        case oneToOneDependency: OneToOneDependency[_] =>
          val dependencyPartition = partition.narrowDeps(depNum).get.split
          oneToOneDependency.rdd.iterator(dependencyPartition, context)
            .asInstanceOf[Iterator[Product2[K, V]]].foreach(op)

        case shuffleDependency: ShuffleDependency[_, _, _] =>
          val iter = SparkEnv.get.shuffleManager
            .getReader(
              shuffleDependency.shuffleHandle, partition.index, partition.index + 1, context)
            .read()
          iter.foreach(op)
      }
    }

    // the first dep is rdd1; add all values to the map
    //第一个dep是rdd1; 将所有值添加到map
    integrate(0, t => getSeq(t._1) += t._2)
    // the second dep is rdd2; remove all of its keys
    //第二个dep是rdd2; 删除所有的键
    integrate(1, t => map.remove(t._1))
    map.iterator.map { t => t._2.iterator.map { (t._1, _) } }.flatten
  }

  override def clearDependencies() {
    super.clearDependencies()
    rdd1 = null
    rdd2 = null
  }

}
