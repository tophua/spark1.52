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

package org.apache.spark.util.random

import scala.collection.Map
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.commons.math3.distribution.PoissonDistribution

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD

/**
 * Auxiliary functions and data structures for the sampleByKey method in PairRDDFunctions.
 *
 * Essentially, when exact sample size is necessary, we make additional passes over the RDD to
 * compute the exact threshold value to use for each stratum to guarantee exact sample size with
 * high probability. This is achieved by maintaining a waitlist of size O(log(s)), where s is the
 * desired sample size for each stratum.
 *
 * Like in simple random sampling, we generate a random value for each item from the
 * uniform  distribution [0.0, 1.0]. All items with values <= min(values of items in the waitlist)
 * are accepted into the sample instantly. The threshold for instant accept is designed so that
 * s - numAccepted = O(sqrt(s)), where s is again the desired sample size. Thus, by maintaining a
 * waitlist size = O(sqrt(s)), we will be able to create a sample of the exact size s by adding
 * a portion of the waitlist to the set of items that are instantly accepted. The exact threshold
 * is computed by sorting the values in the waitlist and picking the value at (s - numAccepted).
 *
 * Note that since we use the same seed for the RNG when computing the thresholds and the actual
 * sample, our computed thresholds are guaranteed to produce the desired sample size.
 *
 * For more theoretical background on the sampling techniques used here, please refer to
 * http://jmlr.org/proceedings/papers/v28/meng13a.html
 */

private[spark] object StratifiedSamplingUtils extends Logging {

  /**
   * Count the number of items instantly accepted and generate the waitlist for each stratum.
   *计算立即接受的项目数，并生成每个层的等待列表
   * This is only invoked when exact sample size is required.
    * 仅当需要确切的样品量时才调用此方法,
   */
  def getAcceptanceResults[K, V](rdd: RDD[(K, V)],
      withReplacement: Boolean,
      fractions: Map[K, Double],
      counts: Option[Map[K, Long]],
      seed: Long): mutable.Map[K, AcceptanceResult] = {
    val combOp = getCombOp[K]
    val mappedPartitionRDD = rdd.mapPartitionsWithIndex { case (partition, iter) =>
      val zeroU: mutable.Map[K, AcceptanceResult] = new mutable.HashMap[K, AcceptanceResult]()
      val rng = new RandomDataGenerator()
      rng.reSeed(seed + partition)
      val seqOp = getSeqOp(withReplacement, fractions, rng, counts)
      Iterator(iter.aggregate(zeroU)(seqOp, combOp))
    }
    mappedPartitionRDD.reduce(combOp)
  }

  /**
   * Returns the function used by aggregate to collect sampling statistics for each partition.
    * 返回聚合使用的函数来收集每个分区的抽样统计信息
   */
  def getSeqOp[K, V](withReplacement: Boolean,
      fractions: Map[K, Double],
      rng: RandomDataGenerator,
      counts: Option[Map[K, Long]]):
    (mutable.Map[K, AcceptanceResult], (K, V)) => mutable.Map[K, AcceptanceResult] = {
    val delta = 5e-5
    (result: mutable.Map[K, AcceptanceResult], item: (K, V)) => {
      val key = item._1
      val fraction = fractions(key)
      if (!result.contains(key)) {
        result += (key -> new AcceptanceResult())
      }
      val acceptResult = result(key)

      if (withReplacement) {
        // compute acceptBound and waitListBound only if they haven't been computed already
        // since they don't change from iteration to iteration.
        //只有在尚未计算acceptBound和waitListBound时才会计算acceptBound和waitListBound，因为它们不会从迭代更改为迭代。
        // TODO change this to the streaming version
        if (acceptResult.areBoundsEmpty) {
          val n = counts.get(key)
          val sampleSize = math.ceil(n * fraction).toLong
          val lmbd1 = PoissonBounds.getLowerBound(sampleSize)
          val lmbd2 = PoissonBounds.getUpperBound(sampleSize)
          acceptResult.acceptBound = lmbd1 / n
          acceptResult.waitListBound = (lmbd2 - lmbd1) / n
        }
        val acceptBound = acceptResult.acceptBound
        val copiesAccepted = if (acceptBound == 0.0) 0L else rng.nextPoisson(acceptBound)
        if (copiesAccepted > 0) {
          acceptResult.numAccepted += copiesAccepted
        }
        val copiesWaitlisted = rng.nextPoisson(acceptResult.waitListBound)
        if (copiesWaitlisted > 0) {
          acceptResult.waitList ++= ArrayBuffer.fill(copiesWaitlisted)(rng.nextUniform())
        }
      } else {
        // We use the streaming version of the algorithm for sampling without replacement to avoid
        // using an extra pass over the RDD for computing the count.
        // Hence, acceptBound and waitListBound change on every iteration.
        //我们使用流版本的算法进行抽样，无需更换即可避免使用额外的RDD来计算计数, 因此，acceptBound和waitListBound在每次迭代时都会发生变化。
        acceptResult.acceptBound =
          BinomialBounds.getLowerBound(delta, acceptResult.numItems, fraction)
        acceptResult.waitListBound =
          BinomialBounds.getUpperBound(delta, acceptResult.numItems, fraction)

        val x = rng.nextUniform()
        if (x < acceptResult.acceptBound) {
          acceptResult.numAccepted += 1
        } else if (x < acceptResult.waitListBound) {
          acceptResult.waitList += x
        }
      }
      acceptResult.numItems += 1
      result
    }
  }

  /**
   * Returns the function used combine results returned by seqOp from different partitions.
    * 返回使用seqOp从不同分区返回的组合结果的函数。
   */
  def getCombOp[K]: (mutable.Map[K, AcceptanceResult], mutable.Map[K, AcceptanceResult])
    => mutable.Map[K, AcceptanceResult] = {
    (result1: mutable.Map[K, AcceptanceResult], result2: mutable.Map[K, AcceptanceResult]) => {
      // take union of both key sets in case one partition doesn't contain all keys
      //如果一个分区不包含所有键，则将两个密钥集合的并集
      result1.keySet.union(result2.keySet).foreach { key =>
        // Use result2 to keep the combined result since r1 is usual empty
        //使用result2保留组合结果，因为r1通常为空
        val entry1 = result1.get(key)
        if (result2.contains(key)) {
          result2(key).merge(entry1)
        } else {
          if (entry1.isDefined) {
            result2 += (key -> entry1.get)
          }
        }
      }
      result2
    }
  }

  /**
   * Given the result returned by getCounts, determine the threshold for accepting items to
   * generate exact sample size.
   *给定getCounts返回的结果,确定接受项目以生成精确样本大小的阈值。
   * To do so, we compute sampleSize = math.ceil(size * samplingRate) for each stratum and compare
   * it to the number of items that were accepted instantly and the number of items in the waitlist
   * for that stratum. Most of the time, numAccepted <= sampleSize <= (numAccepted + numWaitlisted),
   * which means we need to sort the elements in the waitlist by their associated values in order
   * to find the value T s.t. |{elements in the stratum whose associated values <= T}| = sampleSize.
   * Note that all elements in the waitlist have values >= bound for instant accept, so a T value
   * in the waitlist range would allow all elements that were instantly accepted on the first pass
   * to be included in the sample.
   */
  def computeThresholdByKey[K](finalResult: Map[K, AcceptanceResult],
      fractions: Map[K, Double]): Map[K, Double] = {
    val thresholdByKey = new mutable.HashMap[K, Double]()
    for ((key, acceptResult) <- finalResult) {
      val sampleSize = math.ceil(acceptResult.numItems * fractions(key)).toLong
      if (acceptResult.numAccepted > sampleSize) {
        logWarning("Pre-accepted too many")
        thresholdByKey += (key -> acceptResult.acceptBound)
      } else {
        val numWaitListAccepted = (sampleSize - acceptResult.numAccepted).toInt
        if (numWaitListAccepted >= acceptResult.waitList.size) {
          logWarning("WaitList too short")
          thresholdByKey += (key -> acceptResult.waitListBound)
        } else {
          thresholdByKey += (key -> acceptResult.waitList.sorted.apply(numWaitListAccepted))
        }
      }
    }
    thresholdByKey
  }

  /**
   * Return the per partition sampling function used for sampling without replacement.
   *返回每个分区采样功能用于采样而不更换
   * When exact sample size is required, we make an additional pass over the RDD to determine the
   * exact sampling rate that guarantees sample size with high confidence.
    * 当需要精确的样品量时,我们再对RDD进行额外的检测,以确定以高置信度保证样品量的确切采样率。
   *
   * The sampling function has a unique seed per partition.
    * 采样函数每个分区具有唯一的种子
   */
  def getBernoulliSamplingFunction[K, V](rdd: RDD[(K, V)],
      fractions: Map[K, Double],
      exact: Boolean,
      seed: Long): (Int, Iterator[(K, V)]) => Iterator[(K, V)] = {
    var samplingRateByKey = fractions
    if (exact) {
      // determine threshold for each stratum and resample
      //确定每个层的阈值并重新采样
      val finalResult = getAcceptanceResults(rdd, false, fractions, None, seed)
      samplingRateByKey = computeThresholdByKey(finalResult, fractions)
    }
    (idx: Int, iter: Iterator[(K, V)]) => {
      val rng = new RandomDataGenerator()
      rng.reSeed(seed + idx)
      // Must use the same invoke pattern on the rng as in getSeqOp for without replacement
      // in order to generate the same sequence of random numbers when creating the sample
      //必须在rn上使用与getSeqOp中相同的调用模式,无需替换,以便在创建样本时生成相同的随机数序列
      iter.filter(t => rng.nextUniform() < samplingRateByKey(t._1))
    }
  }

  /**
   * Return the per partition sampling function used for sampling with replacement.
    * 返回用于取代的每个分区采样功能
   *
   * When exact sample size is required, we make two additional passed over the RDD to determine
   * the exact sampling rate that guarantees sample size with high confidence. The first pass
   * counts the number of items in each stratum (group of items with the same key) in the RDD, and
   * the second pass uses the counts to determine exact sampling rates.
   *当需要精确的样本量时，我们再通过RDD传递两个以确定以高置信度保证样本大小的确切采样率。
    * 第一次通过计算RDD中每个层（具有相同键的项目组）中的项目数，第二次通过计数确定精确的采样率。
   * The sampling function has a unique seed per partition.
    * 采样函数每个分区具有唯一的种子
   */
  def getPoissonSamplingFunction[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)],
      fractions: Map[K, Double],
      exact: Boolean,
      seed: Long): (Int, Iterator[(K, V)]) => Iterator[(K, V)] = {
    // TODO implement the streaming version of sampling w/ replacement that doesn't require counts
    if (exact) {
      val counts = Some(rdd.countByKey())
      val finalResult = getAcceptanceResults(rdd, true, fractions, counts, seed)
      val thresholdByKey = computeThresholdByKey(finalResult, fractions)
      (idx: Int, iter: Iterator[(K, V)]) => {
        val rng = new RandomDataGenerator()
        rng.reSeed(seed + idx)
        iter.flatMap { item =>
          val key = item._1
          val acceptBound = finalResult(key).acceptBound
          // Must use the same invoke pattern on the rng as in getSeqOp for with replacement
          // in order to generate the same sequence of random numbers when creating the sample
          //必须在rn上使用与getSeqOp中相同的调用模式进行替换，以便在创建样本时生成相同的随机数序列
          val copiesAccepted = if (acceptBound == 0) 0L else rng.nextPoisson(acceptBound)
          val copiesWaitlisted = rng.nextPoisson(finalResult(key).waitListBound)
          val copiesInSample = copiesAccepted +
            (0 until copiesWaitlisted).count(i => rng.nextUniform() < thresholdByKey(key))
          if (copiesInSample > 0) {
            Iterator.fill(copiesInSample.toInt)(item)
          } else {
            Iterator.empty
          }
        }
      }
    } else {
      (idx: Int, iter: Iterator[(K, V)]) => {
        val rng = new RandomDataGenerator()
        rng.reSeed(seed + idx)
        iter.flatMap { item =>
          val count = rng.nextPoisson(fractions(item._1))
          if (count == 0) {
            Iterator.empty
          } else {
            Iterator.fill(count)(item)
          }
        }
      }
    }
  }

  /** A random data generator that generates both uniform values and Poisson values.
    * 一个产生均匀值和泊松值的随机数据生成器
    * */
  private class RandomDataGenerator {
    val uniform = new XORShiftRandom()
    // commons-math3 doesn't have a method to generate Poisson from an arbitrary mean;
    // maintain a cache of Poisson(m) distributions for various m
    //commons-math3没有一种从任意平均值生成泊松的方法;保持各种m的Poisson（m）分布缓存
    val poissonCache = mutable.Map[Double, PoissonDistribution]()
    var poissonSeed = 0L

    def reSeed(seed: Long): Unit = {
      uniform.setSeed(seed)
      poissonSeed = seed
      poissonCache.clear()
    }

    def nextPoisson(mean: Double): Int = {
      val poisson = poissonCache.getOrElseUpdate(mean, {
        val newPoisson = new PoissonDistribution(mean)
        newPoisson.reseedRandomGenerator(poissonSeed)
        newPoisson
      })
      poisson.sample()
    }

    def nextUniform(): Double = {
      uniform.nextDouble()
    }
  }
}

/**
 * Object used by seqOp to keep track of the number of items accepted and items waitlisted per
 * stratum, as well as the bounds for accepting and waitlisting items.
  * seqOp使用的对象跟踪每层接受的项目数量和候补列表，以及接受和等待列表项目的边界
 *
 * `[random]` here is necessary since it's in the return type signature of seqOp defined above
 */
private[random] class AcceptanceResult(var numItems: Long = 0L, var numAccepted: Long = 0L)
  extends Serializable {

  val waitList = new ArrayBuffer[Double]
  var acceptBound: Double = Double.NaN // upper bound for accepting item instantly
  var waitListBound: Double = Double.NaN // upper bound for adding item to waitlist

  def areBoundsEmpty: Boolean = acceptBound.isNaN || waitListBound.isNaN

  def merge(other: Option[AcceptanceResult]): Unit = {
    if (other.isDefined) {
      waitList ++= other.get.waitList
      numAccepted += other.get.numAccepted
      numItems += other.get.numItems
    }
  }
}
