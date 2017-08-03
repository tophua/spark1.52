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

import org.apache.spark.annotation.Experimental
import org.apache.spark.{TaskContext, Logging}
import org.apache.spark.partial.BoundedDouble
import org.apache.spark.partial.MeanEvaluator
import org.apache.spark.partial.PartialResult
import org.apache.spark.partial.SumEvaluator
import org.apache.spark.util.StatCounter

/**
 * Extra functions available on RDDs of Doubles through an implicit conversion.
 * 这个扩展类包含了很多数值的聚合方法,如果RDD的数据单元能够隐式变换成Scala的double数据类型
 */
class DoubleRDDFunctions(self: RDD[Double]) extends Logging with Serializable {
  /** Add up the elements in this RDD.
    * 添加此RDD中的元素 */
  def sum(): Double = self.withScope {
    self.fold(0.0)(_ + _)
  }

  /**
   * Return a [[org.apache.spark.util.StatCounter]] object that captures the mean, variance and
   * count of the RDD's elements in one operation.
    * 返回一个[[org.apache.spark.util.StatCounter]]对象,它捕获一个操作中RDD元素的均值,方差和计数
   */
  def stats(): StatCounter = self.withScope {
    self.mapPartitions(nums => Iterator(StatCounter(nums))).reduce((a, b) => a.merge(b))
  }

  /** 
   *  Compute the mean(平均数) of this RDD's elements.
   *  计算RDD元素平均数
   *   */
  def mean(): Double = self.withScope {
    stats().mean
  }

  /** 
   *  Compute the variance of this RDD's elements.
   *  计算RDD元素方差
   *  */
  def variance(): Double = self.withScope {
    stats().variance
  }

  /** 
   *  Compute the standard deviation  of this RDD's elements.
   *  计算RDD元素标准差
   *  */
  def stdev(): Double = self.withScope {
    stats().stdev
  }

  /**
   * Compute the sample standard deviation of this RDD's elements (which corrects for bias in
   * estimating the standard deviation by dividing by N-1 instead of N).
    * 计算该RDD元素的样本标准偏差(其通过除以N-1而不是N来校正标准偏差中的偏差)
   */
  def sampleStdev(): Double = self.withScope {
    stats().sampleStdev
  }

  /**
   * Compute the sample variance of this RDD's elements (which corrects for bias in
   * estimating the variance by dividing by N-1 instead of N).
   *  计算这个RDD元素的样本方差（通过用N-1而不是N来估计方差来校正偏差）
   */
  def sampleVariance(): Double = self.withScope {
    stats().sampleVariance
  }

  /**
   * :: Experimental ::
   * Approximate operation to return the mean within a timeout.
    * 在超时内返回平均值的近似操作
   */
  @Experimental
  def meanApprox(
      timeout: Long,
      confidence: Double = 0.95): PartialResult[BoundedDouble] = self.withScope {
    val processPartition = (ctx: TaskContext, ns: Iterator[Double]) => StatCounter(ns)
    val evaluator = new MeanEvaluator(self.partitions.length, confidence)
    self.context.runApproximateJob(self, processPartition, evaluator, timeout)
  }

  /**
   * :: Experimental ::
   * Approximate operation to return the sum within a timeout.
    * 在超时内返回总和的近似操作
   */
  @Experimental
  def sumApprox(
      timeout: Long,
      confidence: Double = 0.95): PartialResult[BoundedDouble] = self.withScope {
    val processPartition = (ctx: TaskContext, ns: Iterator[Double]) => StatCounter(ns)
    val evaluator = new SumEvaluator(self.partitions.length, confidence)
    self.context.runApproximateJob(self, processPartition, evaluator, timeout)
  }

  /**
   * Compute a histogram of the data using bucketCount number of buckets evenly
   *  spaced between the minimum and maximum of the RDD. For example if the min
   *  value is 0 and the max is 100 and there are two buckets the resulting
   *  buckets will be [0, 50) [50, 100]. bucketCount must be at least 1
   * If the RDD contains infinity, NaN throws an exception
   * If the elements in RDD do not vary (max == min) always returns a single bucket.
    * 使用bucketCount数量的数据块计算数据的直方图，均匀间隔在RDD的最小值和最大值之间。
    * 例如，如果最小值为0，最大值为100，并且有两个桶，则生成的桶将为[0,50] [50,100]。
    * bucketCount必须至少为1如果RDD包含无穷大，则NaN将抛出异常如果RDD中的元素不变（max == min），则始终返回单个存储桶。
   */
  def histogram(bucketCount: Int): Pair[Array[Double], Array[Long]] = self.withScope {
    // Scala's built-in range has issues. See #SI-8782
    //Scala的内置范围有问题。 见＃SI-8782
    def customRange(min: Double, max: Double, steps: Int): IndexedSeq[Double] = {
      val span = max - min
      Range.Int(0, steps, 1).map(s => min + (s * span) / steps) :+ max
    }
    // Compute the minimum and the maximum 计算最小值和最大值
    val (max: Double, min: Double) = self.mapPartitions { items =>
      Iterator(items.foldRight(Double.NegativeInfinity,
        Double.PositiveInfinity)((e: Double, x: Pair[Double, Double]) =>
        (x._1.max(e), x._2.min(e))))
    }.reduce { (maxmin1, maxmin2) =>
      (maxmin1._1.max(maxmin2._1), maxmin1._2.min(maxmin2._2))
    }
    if (min.isNaN || max.isNaN || max.isInfinity || min.isInfinity ) {
      throw new UnsupportedOperationException(
        "Histogram on either an empty RDD or RDD containing +/-infinity or NaN")
    }
    val range = if (min != max) {
      // Range.Double.inclusive(min, max, increment)
      // The above code doesn't always work. See Scala bug #SI-8782.
      // https://issues.scala-lang.org/browse/SI-8782
      customRange(min, max, bucketCount)
    } else {
      List(min, min)
    }
    val buckets = range.toArray
    (buckets, histogram(buckets, true))
  }

  /**
   * 针对数据类型为Double的RDD统计直方图,
   * 可以输入分桶数进行平均分桶,也可以输入自定义的分桶区间。
   * 平均分桶情况下会输出两个数组,第一个是每个分桶边界值,第二个是每个分桶的统计数。
   * 自定义分桶情况下只输出一个数组,即每个分桶的统计数
   * val a = sc.parallelize(List(1.1, 1.2, 1.3, 2.0, 2.1, 7.4, 7.5, 7.6, 8.8, 9.0), 3)
	 * a.histogram(5)
	 * val a = sc.parallelize(List(1.1, 1.2, 1.3, 2.0, 2.1, 7.4, 7.5, 7.6, 8.8, 9.0), 3)
   * a.histogram(Array(0.0, 3.0, 8.0))
   * Compute a histogram using the provided buckets. The buckets are all open
   * to the right except for the last which is closed
   *  e.g. for the array
   *  [1, 10, 20, 50] the buckets are [1, 10) [10, 20) [20, 50]
   *  e.g 1<=x<10 , 10<=x<20, 20<=x<=50
   *  And on the input of 1 and 50 we would have a histogram of 1, 0, 1
   *
   * Note: if your histogram is evenly spaced (e.g. [0, 10, 20, 30]) this can be switched
   * from an O(log n) inseration to O(1) per element. (where n = # buckets) if you set evenBuckets
   * to true.
   * buckets must be sorted and not contain any duplicates.
   * buckets array must be at least two elements
   * All NaN entries are treated the same. If you have a NaN bucket it must be
   * the maximum value of the last position and all NaN entries will be counted
   * in that bucket.
   */
  def histogram(
      buckets: Array[Double],
      evenBuckets: Boolean = false): Array[Long] = self.withScope {
    if (buckets.length < 2) {
      throw new IllegalArgumentException("buckets array must have at least two elements")
    }
    // The histogramPartition function computes the partail histogram for a given
    // partition. The provided bucketFunction determines which bucket in the array
    // to increment or returns None if there is no bucket. This is done so we can
    // specialize for uniformly distributed buckets and save the O(log n) binary
    // search cost.
    def histogramPartition(bucketFunction: (Double) => Option[Int])(iter: Iterator[Double]):
        Iterator[Array[Long]] = {
      val counters = new Array[Long](buckets.length - 1)
      while (iter.hasNext) {
        bucketFunction(iter.next()) match {
          case Some(x: Int) => {counters(x) += 1}
          case _ => {}
        }
      }
      Iterator(counters)
    }
    // Merge the counters.
    def mergeCounters(a1: Array[Long], a2: Array[Long]): Array[Long] = {
      a1.indices.foreach(i => a1(i) += a2(i))
      a1
    }
    // Basic bucket function. This works using Java's built in Array
    // binary search. Takes log(size(buckets))
    //基本桶功能,这可以使用Java内置的Array二进制搜索, 记录日志（大小（桶））
    def basicBucketFunction(e: Double): Option[Int] = {
      val location = java.util.Arrays.binarySearch(buckets, e)
      if (location < 0) {
        // If the location is less than 0 then the insertion point in the array
        // to keep it sorted is -location-1
        //如果位置小于0,则数组中的插入点保持排序是-location-1
        val insertionPoint = -location-1
        // If we have to insert before the first element or after the last one
        // its out of bounds.
        //如果我们必须在第一个元素之前插入,或者在最后一个元素之后插入它的边界。
        // We do this rather than buckets.lengthCompare(insertionPoint)
        // because Array[Double] fails to override it (for now).
        //我们这样做而不是buckets.lengthCompare(insertionPoint),因为Array [Double]无法覆盖它(现在)
        if (insertionPoint > 0 && insertionPoint < buckets.length) {
          Some(insertionPoint-1)
        } else {
          None
        }
      } else if (location < buckets.length - 1) {
        // Exact match, just insert here
        //完全匹配，只需插入
        Some(location)
      } else {
        // Exact match to the last element
        //与最后一个元素完全匹配
        Some(location - 1)
      }
    }
    // Determine the bucket function in constant time. Requires that buckets are evenly spaced
    //确定桶功能在恒定时间,需要桶分配均匀
    def fastBucketFunction(min: Double, max: Double, count: Int)(e: Double): Option[Int] = {
      // If our input is not a number unless the increment is also NaN then we fail fast
      //如果我们的输入不是数字,除非增量也是NaN,那么我们失败快
      if (e.isNaN || e < min || e > max) {
        None
      } else {
        // Compute ratio of e's distance along range to total range first, for better precision
        //为了更好的精度,首先计算E的距离与距离的距离。
        val bucketNumber = (((e - min) / (max - min)) * count).toInt
        // should be less than count, but will equal count if e == max, in which case
        // it's part of the last end-range-inclusive bucket, so return count-1
        //应小于计数，但会等计数如果E = =最大,在这种情况下,这是最后的结束范围包括斗部分，所以返回count-1
        Some(math.min(bucketNumber, count - 1))
      }
    }
    // Decide which bucket function to pass to histogramPartition. We decide here
    // rather than having a general function so that the decision need only be made
    // once rather than once per shard
    //决定哪个bucket函数传递给histogramPartition,我们在这里决定而不是一般功能,所以决定只需要一次,而不是每个分片一次
    val bucketFunction = if (evenBuckets) {
      fastBucketFunction(buckets.head, buckets.last, buckets.length - 1) _
    } else {
      basicBucketFunction _
    }
    if (self.partitions.length == 0) {
      new Array[Long](buckets.length - 1)
    } else {
      // reduce() requires a non-empty RDD. This works because the mapPartitions will make
      // non-empty partitions out of empty ones. But it doesn't handle the no-partitions case,
      // which is below
      //reduce（）需要一个非空的RDD,这样做是因为mapPartitions会使空的分区变为空的,但是它没有处理没有分区的情况，这在下面
      self.mapPartitions(histogramPartition(bucketFunction)).reduce(mergeCounters)
    }
  }

}
