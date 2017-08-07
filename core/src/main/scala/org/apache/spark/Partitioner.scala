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

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.{ClassTag, classTag}
import scala.util.hashing.byteswap32

import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.util.{CollectionsUtils, Utils}
import org.apache.spark.util.random.{XORShiftRandom, SamplingUtils}

/**
 * An object that defines how the elements in a key-value pair RDD are partitioned by key.
 * 一个RDD对象中定义在一个键值对中查找分区元素
 * Maps each key to a partition ID, from 0 to `numPartitions - 1`.
 * 将每个键映射到分区标识,从0开始到`numPartitions - 1`.
 */
abstract class Partitioner extends Serializable {
  def numPartitions: Int
  def getPartition(key: Any): Int
}

object Partitioner {
  /**
   * Choose a partitioner to use for a cogroup-like operation between a number of RDDs.
   * 选择一个分区器,用于在多个RDD之间进行类同组操作。
   * If any of the RDDs already has a partitioner, choose that one.
   * 如果任何RDD已经有分区器,请选择该分区器。
   * Otherwise, we use a default HashPartitioner. For the number of partitions, if
   * spark.default.parallelism is set, then we'll use the value from SparkContext
   * defaultParallelism, otherwise we'll use the max number of upstream partitions.
   * 否则,我们使用默认的HashPartitioner,对于分区的数量,如果spark.default.parallelism被设置
    * 那么我们将使用SparkContext的值defaultParallelism，否则我们将使用最大数量的上游分区。
   * Unless spark.default.parallelism is set, the number of partitions will be the
   * same as the number of partitions in the largest upstream RDD, as this should
   * be least likely to cause out-of-memory errors.
   * 除非设置了spark.default.parallelism,否则分区数将与最大上游RDD中的分区数相同,因为这应该是最不可能导致内存不足错误。
   * We use two method parameters (rdd, others) to enforce callers passing at least 1 RDD.
    * 我们使用两个方法参数（rdd,其他）来强制传递至少1个RDD的调用
   * 1)将RDD转换为Seq,然后对Seq按照RDD的partitioner的size倒序排列
   * 2)创建HashPartitioner对象,如果配置parallelis属性,则使用属性值作为分区数量,否则使用Seq中所有RDD
   *   的partitions函数返回值的最大值作为分区数量
    *   RDD[_]*函数传入可变长度参数列,others可以不传递值,[_]是泛型
   */
  def defaultPartitioner(rdd: RDD[_], others: RDD[_]*): Partitioner = {
  //1)将RDD转换为Seq,然后对Seq按照RDD的partitions的大小倒序排列
    //将RDD转换为Seq才可以进行++操作
    val bySize = (Seq(rdd) ++ others).sortBy(_.partitions.size).reverse
    //isDefined  如果该选项实例返回true,否则为false,
    for (r <- bySize if r.partitioner.isDefined && r.partitioner.get.numPartitions > 0) {
      //还回最大分区数
      return r.partitioner.get
    }
    // 2)创建HashPartitioner对象,如果配置parallelis属性,则使用属性值作为分区数量,否则使用Seq中所有RDD
   //   的partitions函数返回值的最大值作为分区数量
    if (rdd.context.conf.contains("spark.default.parallelism")) {//本地模式：机器核数
      new HashPartitioner(rdd.context.defaultParallelism)
    } else {
      //取第一个列表分区大小(倒序最大一个)
      new HashPartitioner(bySize.head.partitions.size)
    }
  }
}

/**
 * A [[org.apache.spark.Partitioner]] that implements hash-based partitioning using
 * Java's `Object.hashCode`.A [[org.apache.spark.Partitioner]]）实现基于哈希的分区
  * Java的“Object.hashCode”。
 *
 * Java arrays have hashCodes that are based on the arrays' identities rather than their contents,
 * so attempting to partition an RDD[Array[_]] or RDD[(Array[_], _)] using a HashPartitioner will
 * produce an unexpected or incorrect result.
  *
  * java数组的哈希码是基于数组的身份而不是他们的内容,因此尝试使用HashPartitioner对RDD [Array [_]]
  * 或RDD [（Array [_]，_）]进行分区将导致意外或不正确结果
 */
class HashPartitioner(partitions: Int) extends Partitioner {
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  def numPartitions: Int = partitions

  def getPartition(key: Any): Int = key match {
    case null => 0
    case _ => Utils.nonNegativeMod(key.hashCode, numPartitions)
  }

  override def equals(other: Any): Boolean = other match {
    case h: HashPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions
}

/**
 * A [[org.apache.spark.Partitioner]] that partitions sortable records by range into roughly
 * equal ranges. The ranges are determined by sampling the content of the RDD passed in.
 *A [[org.apache.spark.Partitioner]]，将可分类的记录按范围划分为大致等号,范围通过对传入的RDD的内容进行抽样来确定。
 * Note that the actual number of partitions created by the RangePartitioner might not be the same
 * as the `partitions` parameter, in the case where the number of sampled records is less than
 * the value of `partitions`.
  * 请注意，RangePartitioner创建的分区的实际数量可能与“partitions”参数不同，在采样记录数小于“partitions”的值的情况下。
 */
class RangePartitioner[K : Ordering : ClassTag, V](
    @transient partitions: Int,
    @transient rdd: RDD[_ <: Product2[K, V]],
    private var ascending: Boolean = true)
  extends Partitioner {

  // We allow partitions = 0, which happens when sorting an empty RDD under the default settings.
  //我们允许分区= 0,这是在默认设置下排序空RDD时发生的。
  require(partitions >= 0, s"Number of partitions cannot be negative but found $partitions.")

  private var ordering = implicitly[Ordering[K]]

  // An array of upper bounds for the first (partitions - 1) partitions
  //上限数组的第一个分区
  private var rangeBounds: Array[K] = {
    if (partitions <= 1) {
      Array.empty
    } else {
      // This is the sample size we need to have roughly balanced output partitions, capped at 1M.
      // 样本的大小,我们需要有大致平衡的输出分区
      val sampleSize = math.min(20.0 * partitions, 1e6)
      // Assume the input partitions are roughly balanced and over-sample a little bit.
      //假设输入分区大致平衡,并且过采样一点。
      val sampleSizePerPartition = math.ceil(3.0 * sampleSize / rdd.partitions.size).toInt
      val (numItems, sketched) = RangePartitioner.sketch(rdd.map(_._1), sampleSizePerPartition)
      if (numItems == 0L) {
        Array.empty
      } else {
        // If a partition contains much more than the average number of items, we re-sample from it
        // to ensure that enough items are collected from that partition.
        //如果一个分区包含的数量远远超过平均数量,我们将从中重新采样,以确保从该分区收集足够的项目。
        val fraction = math.min(sampleSize / math.max(numItems, 1L), 1.0)
        val candidates = ArrayBuffer.empty[(K, Float)]
        val imbalancedPartitions = mutable.Set.empty[Int]
        sketched.foreach { case (idx, n, sample) =>
          if (fraction * n > sampleSizePerPartition) {
            imbalancedPartitions += idx
          } else {
            // The weight is 1 over the sampling probability.
            //重量是抽样概率1
            val weight = (n.toDouble / sample.size).toFloat
            for (key <- sample) {
              candidates += ((key, weight))
            }
          }
        }
        if (imbalancedPartitions.nonEmpty) {
          // Re-sample imbalanced partitions with the desired sampling probability.
          //以所需的采样概率重新采样不平衡分区
          val imbalanced = new PartitionPruningRDD(rdd.map(_._1), imbalancedPartitions.contains)
          val seed = byteswap32(-rdd.id - 1)
          val reSampled = imbalanced.sample(withReplacement = false, fraction, seed).collect()
          val weight = (1.0 / fraction).toFloat
          candidates ++= reSampled.map(x => (x, weight))
        }
        RangePartitioner.determineBounds(candidates, partitions)
      }
    }
  }

  def numPartitions: Int = rangeBounds.length + 1

  private var binarySearch: ((Array[K], K) => Int) = CollectionsUtils.makeBinarySearch[K]

  def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[K]
    var partition = 0
    if (rangeBounds.length <= 128) {
      // If we have less than 128 partitions naive search
      //如果不到128个分区天真的搜索
      while (partition < rangeBounds.length && ordering.gt(k, rangeBounds(partition))) {
        partition += 1
      }
    } else {
      // Determine which binary search method to use only once.
      // 确定二进制搜索方法只使用一次
      partition = binarySearch(rangeBounds, k)      
      // binarySearch either returns the match location or -[insertion point]-1
      //二进制搜索返回匹配位置
      if (partition < 0) {
        partition = -partition-1
      }
      if (partition > rangeBounds.length) {
        partition = rangeBounds.length
      }
    }
    if (ascending) {
      partition
    } else {
      rangeBounds.length - partition
    }
  }

  override def equals(other: Any): Boolean = other match {
    case r: RangePartitioner[_, _] =>
      r.rangeBounds.sameElements(rangeBounds) && r.ascending == ascending
    case _ =>
      false
  }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    var i = 0
    while (i < rangeBounds.length) {
      result = prime * result + rangeBounds(i).hashCode
      i += 1
    }
    result = prime * result + ascending.hashCode
    result
  }

  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    val sfactory = SparkEnv.get.serializer
    sfactory match {
      case js: JavaSerializer => out.defaultWriteObject()
      case _ =>
        out.writeBoolean(ascending)
        out.writeObject(ordering)
        out.writeObject(binarySearch)

        val ser = sfactory.newInstance()
        Utils.serializeViaNestedStream(out, ser) { stream =>
          stream.writeObject(scala.reflect.classTag[Array[K]])
          stream.writeObject(rangeBounds)
        }
    }
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    val sfactory = SparkEnv.get.serializer
    sfactory match {
      case js: JavaSerializer => in.defaultReadObject()
      case _ =>
        ascending = in.readBoolean()
        ordering = in.readObject().asInstanceOf[Ordering[K]]
        binarySearch = in.readObject().asInstanceOf[(Array[K], K) => Int]

        val ser = sfactory.newInstance()
        Utils.deserializeViaNestedStream(in, ser) { ds =>
          implicit val classTag = ds.readObject[ClassTag[Array[K]]]()
          rangeBounds = ds.readObject[Array[K]]()
        }
    }
  }
}

private[spark] object RangePartitioner {

  /**
   * Sketches the input RDD via reservoir sampling on each partition.
   * 通过每个分区上的储层抽样绘制输入RDD。
   * @param rdd the input RDD to sketch
   * @param sampleSizePerPartition max sample size per partition
   * @return (total number of items, an array of (partitionId, number of items, sample))
   */
  def sketch[K : ClassTag](
      rdd: RDD[K],
      sampleSizePerPartition: Int): (Long, Array[(Int, Int, Array[K])]) = {
    val shift = rdd.id
    // val classTagK = classTag[K] // to avoid serializing the entire partitioner object
    //val classTagK = classTag [K] //以避免序列化整个分区对象
    val sketched = rdd.mapPartitionsWithIndex { (idx, iter) =>
      val seed = byteswap32(idx ^ (shift << 16))
      val (sample, n) = SamplingUtils.reservoirSampleAndCount(
        iter, sampleSizePerPartition, seed)
      Iterator((idx, n, sample))
    }.collect()
    val numItems = sketched.map(_._2.toLong).sum
    (numItems, sketched)
  }

  /**
   * Determines the bounds for range partitioning from candidates with weights indicating how many
   * items each represents. Usually this is 1 over the probability used to sample this candidate.
   * 确定从候选人的范围划分的范围,权重表示每个表示多少项。通常这比用于抽样这个候选人的概率高1
   * @param candidates unordered candidates with weights
   * @param partitions number of partitions
   * @return selected bounds
   */
  def determineBounds[K : Ordering : ClassTag](
      candidates: ArrayBuffer[(K, Float)],
      partitions: Int): Array[K] = {
    val ordering = implicitly[Ordering[K]]
    val ordered = candidates.sortBy(_._1)
    val numCandidates = ordered.size
    val sumWeights = ordered.map(_._2.toDouble).sum
    val step = sumWeights / partitions
    var cumWeight = 0.0
    var target = step
    val bounds = ArrayBuffer.empty[K]
    var i = 0
    var j = 0
    var previousBound = Option.empty[K]
    while ((i < numCandidates) && (j < partitions - 1)) {
      val (key, weight) = ordered(i)
      cumWeight += weight
      if (cumWeight > target) {
        // Skip duplicate values.跳过重复值
        if (previousBound.isEmpty || ordering.gt(key, previousBound.get)) {
          bounds += key
          target += step
          j += 1
          previousBound = Some(key)
        }
      }
      i += 1
    }
    bounds.toArray
  }
}
