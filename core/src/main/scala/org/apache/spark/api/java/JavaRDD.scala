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

package org.apache.spark.api.java

import java.util.Comparator

import scala.language.implicitConversions
import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.api.java.JavaSparkContext.fakeClassTag
import org.apache.spark.api.java.function.{Function => JFunction}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils

class JavaRDD[T](val rdd: RDD[T])(implicit val classTag: ClassTag[T])
  extends AbstractJavaRDDLike[T, JavaRDD[T]] {

  override def wrapRDD(rdd: RDD[T]): JavaRDD[T] = JavaRDD.fromRDD(rdd)

  // Common RDD functions

  /** Persist this RDD with the default storage level (`MEMORY_ONLY`).
    * 使用默认存储级别（“MEMORY_ONLY”）保留此RDD*/
  def cache(): JavaRDD[T] = wrapRDD(rdd.cache())

  /**
   * Set this RDD's storage level to persist its values across operations after the first time
   * it is computed. This can only be used to assign a new storage level if the RDD does not
   * have a storage level set yet..
    * 设置此RDD的存储级别,以便在第一次计算后将其值保持在操作之间,
    * 如果RDD尚未设置存储级别,则此选项仅可用于分配新的存储级别
   */
  def persist(newLevel: StorageLevel): JavaRDD[T] = wrapRDD(rdd.persist(newLevel))

  /**
   * Mark the RDD as non-persistent, and remove all blocks for it from memory and disk.
    * 将RDD标记为非持久性,并从内存和磁盘中删除它的所有块
   * This method blocks until all blocks are deleted.
    * 此方法将阻塞,直到删除所有块
   */
  def unpersist(): JavaRDD[T] = wrapRDD(rdd.unpersist())

  /**
   * Mark the RDD as non-persistent, and remove all blocks for it from memory and disk.
    * 将RDD标记为非持久性,并从内存和磁盘中删除它的所有块
   *
   * @param blocking Whether to block until all blocks are deleted.
   */
  def unpersist(blocking: Boolean): JavaRDD[T] = wrapRDD(rdd.unpersist(blocking))

  // Transformations (return a new RDD)

  /**
   * Return a new RDD containing the distinct elements in this RDD.
    * 返回包含此RDD中不同元素的新RDD
   */
  def distinct(): JavaRDD[T] = wrapRDD(rdd.distinct())

  /**
   * Return a new RDD containing the distinct elements in this RDD.
    * 返回包含此RDD中不同元素的新RDD
   */
  def distinct(numPartitions: Int): JavaRDD[T] = wrapRDD(rdd.distinct(numPartitions))

  /**
   * Return a new RDD containing only the elements that satisfy a predicate.
    * 返回仅包含满足谓词的元素的新RDD
   */
  def filter(f: JFunction[T, java.lang.Boolean]): JavaRDD[T] =
    wrapRDD(rdd.filter((x => f.call(x).booleanValue())))

  /**
   * Return a new RDD that is reduced into `numPartitions` partitions.
    * 返回一个新的RDD,它被缩减为`numPartitions`分区
   */
  def coalesce(numPartitions: Int): JavaRDD[T] = rdd.coalesce(numPartitions)

  /**
   * Return a new RDD that is reduced into `numPartitions` partitions.
    * 返回一个新的RDD,它被缩减为`numPartitions`分区
   */
  def coalesce(numPartitions: Int, shuffle: Boolean): JavaRDD[T] =
    rdd.coalesce(numPartitions, shuffle)

  /**
   * Return a new RDD that has exactly numPartitions partitions.
    * 返回一个具有正确numPartitions分区的新RDD
   *
   * Can increase or decrease the level of parallelism in this RDD. Internally, this uses
   * a shuffle to redistribute data.
    * 可以增加或减少此RDD中的并行度,在内部,它使用shuffle重新分配数据
   *
   * If you are decreasing the number of partitions in this RDD, consider using `coalesce`,
   * which can avoid performing a shuffle.
    * 如果要减少此RDD中的分区数,请考虑使用`coalesce`,这可以避免执行shuffle
   */
  def repartition(numPartitions: Int): JavaRDD[T] = rdd.repartition(numPartitions)

  /**
   * Return a sampled subset of this RDD.
    * 返回此RDD的采样子集
   *
   * @param withReplacement can elements be sampled multiple times (replaced when sampled out)
   * @param fraction expected size of the sample as a fraction of this RDD's size
   *  without replacement: probability that each element is chosen; fraction must be [0, 1]
   *  with replacement: expected number of times each element is chosen; fraction must be >= 0
   */
  def sample(withReplacement: Boolean, fraction: Double): JavaRDD[T] =
    sample(withReplacement, fraction, Utils.random.nextLong)

  /**
   * Return a sampled subset of this RDD.
   * 返回此RDD的采样子集
   * @param withReplacement can elements be sampled multiple times (replaced when sampled out)
   * @param fraction expected size of the sample as a fraction of this RDD's size
   *  without replacement: probability that each element is chosen; fraction must be [0, 1]
   *  with replacement: expected number of times each element is chosen; fraction must be >= 0
   * @param seed seed for the random number generator
   */
  def sample(withReplacement: Boolean, fraction: Double, seed: Long): JavaRDD[T] =
    wrapRDD(rdd.sample(withReplacement, fraction, seed))


  /**
   * Randomly splits this RDD with the provided weights.
    * 随机地使用提供的权重拆分此RDD
   *
   * @param weights weights for splits, will be normalized if they don't sum to 1
   *
   * @return split RDDs in an array
   */
  def randomSplit(weights: Array[Double]): Array[JavaRDD[T]] =
    randomSplit(weights, Utils.random.nextLong)

  /**
   * Randomly splits this RDD with the provided weights.
    * 随机地使用提供的权重拆分此RDD
   *
   * @param weights weights for splits, will be normalized if they don't sum to 1
   * @param seed random seed
   *
   * @return split RDDs in an array
   */
  def randomSplit(weights: Array[Double], seed: Long): Array[JavaRDD[T]] =
    rdd.randomSplit(weights, seed).map(wrapRDD)

  /**
   * Return the union of this RDD and another one. Any identical elements will appear multiple
   * times (use `.distinct()` to eliminate them).
    * 返回此RDD与另一个RDD的并集,任何相同的元素都会出现多次(使用`.distinct（）`来消除它们)
   */
  def union(other: JavaRDD[T]): JavaRDD[T] = wrapRDD(rdd.union(other.rdd))


  /**
   * Return the intersection of this RDD and another one. The output will not contain any duplicate
   * elements, even if the input RDDs did.
    *
    * 返回此RDD和另一个RDD的交集,输出不包含任何重复
   *
   * Note that this method performs a shuffle internally.
   */
  def intersection(other: JavaRDD[T]): JavaRDD[T] = wrapRDD(rdd.intersection(other.rdd))

  /**
   * Return an RDD with the elements from `this` that are not in `other`.
    * 返回带有`this`中不在`other`中的元素的RDD
   *
   * Uses `this` partitioner/partition size, because even if `other` is huge, the resulting
   * RDD will be <= us.
   */
  def subtract(other: JavaRDD[T]): JavaRDD[T] = wrapRDD(rdd.subtract(other))

  /**
   * Return an RDD with the elements from `this` that are not in `other`.
    *
    * 返回带有`this`中不在`other`中的元素的RDD
   */
  def subtract(other: JavaRDD[T], numPartitions: Int): JavaRDD[T] =
    wrapRDD(rdd.subtract(other, numPartitions))

  /**
   * Return an RDD with the elements from `this` that are not in `other`.
    * 返回带有`this`中不在`other`中的元素的RDD
   */
  def subtract(other: JavaRDD[T], p: Partitioner): JavaRDD[T] =
    wrapRDD(rdd.subtract(other, p))

  override def toString: String = rdd.toString

  /** Assign a name to this RDD
    * 为RDD指定名称*/
  def setName(name: String): JavaRDD[T] = {
    rdd.setName(name)
    this
  }

  /**
   * Return this RDD sorted by the given key function.
    * 返回按给定键函数排序的RDD
   */
  def sortBy[S](f: JFunction[T, S], ascending: Boolean, numPartitions: Int): JavaRDD[T] = {
    import scala.collection.JavaConverters._
    def fn: (T) => S = (x: T) => f.call(x)
    import com.google.common.collect.Ordering  // shadows scala.math.Ordering
    implicit val ordering = Ordering.natural().asInstanceOf[Ordering[S]]
    implicit val ctag: ClassTag[S] = fakeClassTag
    wrapRDD(rdd.sortBy(fn, ascending, numPartitions))
  }

}

object JavaRDD {

  implicit def fromRDD[T: ClassTag](rdd: RDD[T]): JavaRDD[T] = new JavaRDD[T](rdd)

  implicit def toRDD[T](rdd: JavaRDD[T]): RDD[T] = rdd.rdd
}
