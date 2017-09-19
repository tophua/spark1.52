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

import scala.reflect.ClassTag

import org.apache.spark.{Logging, Partitioner, RangePartitioner}
import org.apache.spark.annotation.DeveloperApi

/**
 * 该扩展类的方法需要输入数据是2元tuple,并且key能够排序
 * Extra functions available on RDDs of (key, value) pairs where the key is sortable through
 * an implicit conversion. They will work with any key type `K` that has an implicit `Ordering[K]`
 * in scope. Ordering objects already exist for all of the standard primitive types. Users can also
 * define their own orderings for custom types, or to override the default ordering. The implicit
 * ordering that is in the closest scope will be used.
  *
  * key可通过隐式转换排序的（key，值）对的RDD上的额外功能。 它们将使用范围内具有隐式“Ordering [K]”的任何键类型“K”。
  * 已经存在所有标准原语类型的订购对象。 用户还可以为自定义类型定义自己的订单，或者覆盖默认排序。 将使用最接近的范围内的隐式排序。
 *
 * {{{
 *   import org.apache.spark.SparkContext._
 *
 *   val rdd: RDD[(String, Int)] = ...
 *   implicit val caseInsensitiveOrdering = new Ordering[String] {
 *     override def compare(a: String, b: String) = a.toLowerCase.compare(b.toLowerCase)
 *   }
 *
 *   // Sort by key, using the above case insensitive ordering.
 *   rdd.sortByKey()
 * }}}
 */
class OrderedRDDFunctions[K : Ordering : ClassTag,
                          V: ClassTag,
                          P <: Product2[K, V] : ClassTag] @DeveloperApi() (
    self: RDD[P])
  extends Logging with Serializable
{
  private val ordering = implicitly[Ordering[K]]

  /**
   * 这个方法按Key进行排序,并输出新RDD。输出的RDD数据是经过shuffled之后的,因为在在输出的reducer里数据已经被shuffle了
   * Sort the RDD by key, so that each partition contains a sorted range of the elements. Calling
   * `collect` or `save` on the resulting RDD will return or output an ordered list of records
   * (in the `save` case, they will be written to multiple `part-X` files in the filesystem, in
   * order of the keys).
   * sortByKey函数作用于Key-Value形式的RDD,并对Key进行排序
   * 它主要接受两个函数,含义和sortBy一样,这里就不进行解释了。该函数返回的RDD一定是ShuffledRDD类型的,
   * 因为对源RDD进行排序,必须进行Shuffle操作,而Shuffle操作的结果RDD就是ShuffledRDD
   */
  // TODO: this currently doesn't work on P other than Tuple2!
  def sortByKey(ascending: Boolean = true, numPartitions: Int = self.partitions.length)
      : RDD[(K, V)] = self.withScope
  {
    val part = new RangePartitioner(numPartitions, self, ascending)
    new ShuffledRDD[K, V, V](self, part)
      .setKeyOrdering(if (ascending) ordering else ordering.reverse)
  }

  /**
   * Repartition the RDD according to the given partitioner and, within each resulting partition,
   * sort records by their keys.
   * 再分配的RDD根据给定的分割,在每个分区内按键值排序
   *
   * This is more efficient than calling `repartition` and then sorting within each partition
   * because it can push the sorting down into the shuffle machinery.
    * 这比调用“repartition”更有效,然后在每个分区中进行排序,因为它可以将排序推入洗牌机器
   */
  def repartitionAndSortWithinPartitions(partitioner: Partitioner): RDD[(K, V)] = self.withScope {
    new ShuffledRDD[K, V, V](self, partitioner).setKeyOrdering(ordering)
  }

  /**
   * Returns an RDD containing only the elements in the the inclusive range `lower` to `upper`.
   * If the RDD has been partitioned using a `RangePartitioner`, then this operation can be
   * performed efficiently by only scanning the partitions that might contain matching elements.
   * Otherwise, a standard `filter` is applied to all partitions.
    *
    * 返回一个只包含“lower”到“upper”范围内的元素的RDD,如果使用“RangePartitioner”对RDD进行了分区,
    * 则只能扫描可能包含匹配元素的分区,才能有效地执行此操作,否则，将标准的“filter”应用于所有分区。
   */
  def filterByRange(lower: K, upper: K): RDD[P] = self.withScope {

    def inRange(k: K): Boolean = ordering.gteq(k, lower) && ordering.lteq(k, upper)

    val rddToFilter: RDD[P] = self.partitioner match {
      case Some(rp: RangePartitioner[K, V]) => {
        val partitionIndicies = (rp.getPartition(lower), rp.getPartition(upper)) match {
          case (l, u) => Math.min(l, u) to Math.max(l, u)
        }
        PartitionPruningRDD.create(self, partitionIndicies.contains)
      }
      case _ =>
        self
    }
    rddToFilter.filter { case (k, v) => inRange(k) }
  }

}
