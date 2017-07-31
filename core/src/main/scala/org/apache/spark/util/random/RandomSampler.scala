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

import java.util.Random

import scala.reflect.ClassTag
import scala.collection.mutable.ArrayBuffer

import org.apache.commons.math3.distribution.PoissonDistribution

import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 * A pseudorandom sampler. It is possible to change the sampled item type. For example, we might
 * want to add weights for stratified sampling or importance sampling. Should only use
 * transformations that are tied to the sampler and cannot be applied after sampling.
  * 伪随机取样器,可以更改采样项目类型,例如，我们可能需要为分层抽样或重要性抽样添加权重,
  * 只应使用与采样器相关的变换，并且在采样后不能应用。
 *
 * @tparam T item type
 * @tparam U sampled item type
 */
@DeveloperApi
trait RandomSampler[T, U] extends Pseudorandom with Cloneable with Serializable {

  /** take a random sample
    * 随机抽样
    * */
  def sample(items: Iterator[T]): Iterator[U]

  /** return a copy of the RandomSampler object
    * 返回一个RandomSampler对象的副本
    * */
  override def clone: RandomSampler[T, U] =
    throw new NotImplementedError("clone() is not implemented.")
}

private[spark]
object RandomSampler {
  /** Default random number generator used by random samplers.
    * 随机采样器使用的默认随机数生成器。
    * */
  def newDefaultRNG: Random = new XORShiftRandom

  /**
   * Default maximum gap-sampling fraction.
    * 默认最大间隙采样分数
   * For sampling fractions <= this value, the gap sampling optimization will be applied.
   * Above this value, it is assumed that "tradtional" Bernoulli sampling is faster.  The
   * optimal value for this will depend on the RNG.  More expensive RNGs will tend to make
   * the optimal value higher.  The most reliable way to determine this value for a new RNG
   * is to experiment.  When tuning for a new RNG, I would expect a value of 0.5 to be close
   * in most cases, as an initial guess.
   */
  val defaultMaxGapSamplingFraction = 0.4

  /**
   * Default epsilon for floating point numbers sampled from the RNG.
    * 从RNG采样的浮点数的默认epsilon
   * The gap-sampling compute logic requires taking log(x), where x is sampled from an RNG.
   * To guard against errors from taking log(0), a positive epsilon lower bound is applied.
   * A good value for this parameter is at or near the minimum positive floating
   * point value returned by "nextDouble()" (or equivalent), for the RNG being used.
   */
  val rngEpsilon = 5e-11

  /**
   * Sampling fraction arguments may be results of computation, and subject to floating
   * point jitter.  I check the arguments with this epsilon slop factor to prevent spurious
   * warnings for cases such as summing some numbers to get a sampling fraction of 1.000000001
    * 采样分数参数可能是计算结果，并受到浮点抖动。
   */
  val roundingEpsilon = 1e-6
}

/**
 * :: DeveloperApi ::
 * A sampler based on Bernoulli trials for partitioning a data sequence.
 * 基于伯努利试验的采样器，用于分割数据序列。
 * @param lb lower bound of the acceptance range
 * @param ub upper bound of the acceptance range
 * @param complement whether to use the complement of the range specified, default to false
 * @tparam T item type
 */
@DeveloperApi
class BernoulliCellSampler[T](lb: Double, ub: Double, complement: Boolean = false)
  extends RandomSampler[T, T] {

  /** epsilon slop to avoid failure from floating point jitter.
    * psilon倾斜以避免浮点抖动失败
    * */
  require(
    lb <= (ub + RandomSampler.roundingEpsilon),
    s"Lower bound ($lb) must be <= upper bound ($ub)")
  require(
    lb >= (0.0 - RandomSampler.roundingEpsilon),
    s"Lower bound ($lb) must be >= 0.0")
  require(
    ub <= (1.0 + RandomSampler.roundingEpsilon),
    s"Upper bound ($ub) must be <= 1.0")

  private val rng: Random = new XORShiftRandom

  override def setSeed(seed: Long): Unit = rng.setSeed(seed)

  override def sample(items: Iterator[T]): Iterator[T] = {
    if (ub - lb <= 0.0) {
      if (complement) items else Iterator.empty
    } else {
      if (complement) {
        items.filter { item => {
          val x = rng.nextDouble()
          (x < lb) || (x >= ub)
        }}
      } else {
        items.filter { item => {
          val x = rng.nextDouble()
          (x >= lb) && (x < ub)
        }}
      }
    }
  }

  /**
   *  Return a sampler that is the complement of the range specified of the current sampler.
    *  返回一个采样器，它是当前采样器指定范围的补码
   */
  def cloneComplement(): BernoulliCellSampler[T] =
    new BernoulliCellSampler[T](lb, ub, !complement)

  override def clone: BernoulliCellSampler[T] = new BernoulliCellSampler[T](lb, ub, complement)
}


/**
 * :: DeveloperApi ::
 * A sampler based on Bernoulli trials.
 * 基于伯努利试验的采样器
 * @param fraction the sampling fraction, aka Bernoulli sampling probability
 * @tparam T item type
 */
@DeveloperApi
class BernoulliSampler[T: ClassTag](fraction: Double) extends RandomSampler[T, T] {

  /** epsilon slop to avoid failure from floating point jitter
    * epsilon倾斜以避免浮点抖动失败
    *  */
  require(
    fraction >= (0.0 - RandomSampler.roundingEpsilon)
      && fraction <= (1.0 + RandomSampler.roundingEpsilon),
    s"Sampling fraction ($fraction) must be on interval [0, 1]")

  private val rng: Random = RandomSampler.newDefaultRNG

  override def setSeed(seed: Long): Unit = rng.setSeed(seed)

  override def sample(items: Iterator[T]): Iterator[T] = {
    if (fraction <= 0.0) {
      Iterator.empty
    } else if (fraction >= 1.0) {
      items
    } else if (fraction <= RandomSampler.defaultMaxGapSamplingFraction) {
      new GapSamplingIterator(items, fraction, rng, RandomSampler.rngEpsilon)
    } else {
      items.filter { _ => rng.nextDouble() <= fraction }
    }
  }

  override def clone: BernoulliSampler[T] = new BernoulliSampler[T](fraction)
}


/**
 * :: DeveloperApi ::
 * A sampler for sampling with replacement, based on values drawn from Poisson distribution.
  * 取样器，根据从泊松分布中抽取的值进行取样
 *
 * @param fraction the sampling fraction (with replacement)
 * @param useGapSamplingIfPossible if true, use gap sampling when sampling ratio is low.
 * @tparam T item type
 */
@DeveloperApi
class PoissonSampler[T: ClassTag](
    fraction: Double,
    useGapSamplingIfPossible: Boolean) extends RandomSampler[T, T] {

  def this(fraction: Double) = this(fraction, useGapSamplingIfPossible = true)

  /** Epsilon slop to avoid failure from floating point jitter.
    * Epsilon倾斜以避免浮点抖动失败
    * */
  require(
    fraction >= (0.0 - RandomSampler.roundingEpsilon),
    s"Sampling fraction ($fraction) must be >= 0")

  // PoissonDistribution throws an exception when fraction <= 0
  // If fraction is <= 0, Iterator.empty is used below, so we can use any placeholder value.
  //当分数<= 0时，泊松分布引发异常如果分数为<= 0,下面使用Iterator.empty，因此我们可以使用任何占位符值。
  private val rng = new PoissonDistribution(if (fraction > 0.0) fraction else 1.0)
  private val rngGap = RandomSampler.newDefaultRNG

  override def setSeed(seed: Long) {
    rng.reseedRandomGenerator(seed)
    rngGap.setSeed(seed)
  }

  override def sample(items: Iterator[T]): Iterator[T] = {
    if (fraction <= 0.0) {
      Iterator.empty
    } else if (useGapSamplingIfPossible &&
               fraction <= RandomSampler.defaultMaxGapSamplingFraction) {
      new GapSamplingReplacementIterator(items, fraction, rngGap, RandomSampler.rngEpsilon)
    } else {
      items.flatMap { item =>
        val count = rng.sample()
        if (count == 0) Iterator.empty else Iterator.fill(count)(item)
      }
    }
  }

  override def clone: PoissonSampler[T] = new PoissonSampler[T](fraction, useGapSamplingIfPossible)
}


private[spark]
class GapSamplingIterator[T: ClassTag](
    var data: Iterator[T],
    f: Double,
    rng: Random = RandomSampler.newDefaultRNG,
    epsilon: Double = RandomSampler.rngEpsilon) extends Iterator[T] {

  require(f > 0.0  &&  f < 1.0, s"Sampling fraction ($f) must reside on open interval (0, 1)")
  require(epsilon > 0.0, s"epsilon ($epsilon) must be > 0")

  /** implement efficient linear-sequence drop until Scala includes fix for jira SI-8835. */
  private val iterDrop: Int => Unit = {
    val arrayClass = Array.empty[T].iterator.getClass
    val arrayBufferClass = ArrayBuffer.empty[T].iterator.getClass
    data.getClass match {
      case `arrayClass` =>
        (n: Int) => { data = data.drop(n) }
      case `arrayBufferClass` =>
        (n: Int) => { data = data.drop(n) }
      case _ =>
        (n: Int) => {
          var j = 0
          while (j < n && data.hasNext) {
            data.next()
            j += 1
          }
        }
    }
  }

  override def hasNext: Boolean = data.hasNext

  override def next(): T = {
    val r = data.next()
    advance()
    r
  }

  private val lnq = math.log1p(-f)

  /** skip elements that won't be sampled, according to geometric dist P(k) = (f)(1-f)^k.
    * 根据几何dist P（k）=（f）（1-f）^ k，跳过不会被采样的元素
    *  */
  private def advance(): Unit = {
    val u = math.max(rng.nextDouble(), epsilon)
    val k = (math.log(u) / lnq).toInt
    iterDrop(k)
  }

  /** advance to first sample as part of object construction.
    * 作为对象构造的一部分，推进到第一个样本
    *  */
  advance()
  // Attempting to invoke this closer to the top with other object initialization
  // was causing it to break in strange ways, so I'm invoking it last, which seems to
  // work reliably.
  //尝试使用其他对象初始化调用这个更接近顶端的方法导致它以奇怪的方式打破，所以我最后调用它，这似乎可靠地工作。
}

private[spark]
class GapSamplingReplacementIterator[T: ClassTag](
    var data: Iterator[T],
    f: Double,
    rng: Random = RandomSampler.newDefaultRNG,
    epsilon: Double = RandomSampler.rngEpsilon) extends Iterator[T] {

  require(f > 0.0, s"Sampling fraction ($f) must be > 0")
  require(epsilon > 0.0, s"epsilon ($epsilon) must be > 0")

  /** implement efficient linear-sequence drop until scala includes fix for jira SI-8835.
    * 实现高效的线性顺序降低，直到scala包括对于jira SI-8835的修复
    * */
  private val iterDrop: Int => Unit = {
    val arrayClass = Array.empty[T].iterator.getClass
    val arrayBufferClass = ArrayBuffer.empty[T].iterator.getClass
    data.getClass match {
      case `arrayClass` =>
        (n: Int) => { data = data.drop(n) }
      case `arrayBufferClass` =>
        (n: Int) => { data = data.drop(n) }
      case _ =>
        (n: Int) => {
          var j = 0
          while (j < n && data.hasNext) {
            data.next()
            j += 1
          }
        }
    }
  }

  /** current sampling value, and its replication factor, as we are sampling with replacement.
    * 当前采样值及其复制因子，因为我们正在用替换取样
    * */
  private var v: T = _
  private var rep: Int = 0

  override def hasNext: Boolean = data.hasNext || rep > 0

  override def next(): T = {
    val r = v
    rep -= 1
    if (rep <= 0) advance()
    r
  }

  /**
   * Skip elements with replication factor zero (i.e. elements that won't be sampled).
   * Samples 'k' from geometric distribution  P(k) = (1-q)(q)^k, where q = e^(-f), that is
   * q is the probabililty of Poisson(0; f)
    * 跳过具有复制因子为零的元素（即不会被采样的元素）。
    * 来自几何分布的样本k'（k）=（1-q）（q）^ k，其中q = e ^（ - f） 那就是q是泊松的可能性（0; f）
   */
  private def advance(): Unit = {
    val u = math.max(rng.nextDouble(), epsilon)
    val k = (math.log(u) / (-f)).toInt
    iterDrop(k)
    // set the value and replication factor for the next value
    //设置下一个值的值和复制因子
    if (data.hasNext) {
      v = data.next()
      rep = poissonGE1
    }
  }

  private val q = math.exp(-f)

  /**
   * Sample from Poisson distribution, conditioned such that the sampled value is >= 1.
   * This is an adaptation from the algorithm for Generating Poisson distributed random variables:
    * 采样从泊松分布，条件使采样值> 1,这是从生成泊松分布随机变量的算法的一个改编：
   * http://en.wikipedia.org/wiki/Poisson_distribution
   */
  private def poissonGE1: Int = {
    // simulate that the standard poisson sampling
    // gave us at least one iteration, for a sample of >= 1
    //模拟标准泊松取样给我们至少一次迭代，样本≥1
    var pp = q + ((1.0 - q) * rng.nextDouble())
    var r = 1

    // now continue with standard poisson sampling algorithm
    pp *= rng.nextDouble()
    while (pp > q) {
      r += 1
      pp *= rng.nextDouble()
    }
    r
  }

  /** advance to first sample as part of object construction.
    * 作为对象构造的一部分，推进到第一个样本
    * */
  advance()
  // Attempting to invoke this closer to the top with other object initialization
  // was causing it to break in strange ways, so I'm invoking it last, which seems to
  // work reliably.
  //尝试使用其他对象初始化调用这个更接近顶端的方法导致它以奇怪的方式打破,所以我最后调用它,这似乎可靠地工作。
}
