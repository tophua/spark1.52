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

package org.apache.spark.util.collection

import scala.collection.mutable

import org.apache.spark.util.SizeEstimator

/**
 * A general interface for collections to keep track of their estimated sizes in bytes.
 * 集合跟踪估计字节大小
 * We sample with a slow exponential back-off using the SizeEstimator to amortize the time,
 * as each call to SizeEstimator is somewhat expensive (order of a few milliseconds).
 */
private[spark] trait SizeTracker {

  import SizeTracker._

  /**
   * Controls the base of the exponential which governs the rate of sampling.
   * 控制指数的基础,控制采样率,将意味着我们的样本1,2,4,8,18
   * E.g., a value of 2 would mean we sample at 1, 2, 4, 8, ... elements.
   */
  private val SAMPLE_GROWTH_RATE = 1.1

  /**
   *  Samples taken since last resetSamples(). Only the last two are kept for extrapolation.
   *  自上次以来所采取的样品,只有最后两个保持论
   */
  private val samples = new mutable.Queue[Sample]

  /**
   *  The average number of bytes per update between our last two samples.
   *  最后两个样本之间的平均每更新的字节数
   */
  private var bytesPerUpdate: Double = _

  /**
   *  Total number of insertions and updates into the map since the last resetSamples().
   *  自上次以来的Map的插入和更新的总数
   */
  private var numUpdates: Long = _

  /**
   *  The value of 'numUpdates' at which we will take our next sample.
   *  将把我们的下一个样本的value
   */
  private var nextSampleNum: Long = _

  resetSamples()

  /**
   * Reset samples collected so far.
   * 复位样本收集
   * This should be called after the collection undergoes a dramatic change in size.
   */
  protected def resetSamples(): Unit = {
    numUpdates = 1 //采样编号
    nextSampleNum = 1 //下一个样本的值
    samples.clear()
    takeSample()
  }

  /**
   * Callback to be invoked after every update.
   * 每次更新AppendOnlyMap的缓存后进行采样,采样前提是已经达到设定的采样间隔
   */
  protected def afterUpdate(): Unit = {
    numUpdates += 1 //当前编号
    if (nextSampleNum == numUpdates) {
      takeSample()
    }
  }

  /**
   * Take a new sample of the current collection's size.
   * 取当前集合的大小的新样本
   */
  private def takeSample(): Unit = {
    //将AppendOnlyMap继承所占的内存估算并且与当前编号numUpdates一起作为样本
    samples.enqueue(Sample(SizeEstimator.estimate(this), numUpdates)) //estimate推测AppendOnlyMap大小
    // Only use the last two samples to extrapolate
    //如果当前采样数量大于2,则使sample执行一次出队操作,保证样本总数等于2
    if (samples.size > 2) {
      samples.dequeue()
    }
    val bytesDelta = samples.toList.reverse match {
      case latest :: previous :: tail =>
        //计算每次更新增加的大小    
        //公式:本次采集大小-上次采样大小/本次采集编号-上次采样编号
        //两个Sample里大小的差值除以它们update次数的差值
        (latest.size - previous.size).toDouble / (latest.numUpdates - previous.numUpdates)
      // If fewer than 2 samples, assume no change
      //如果样本数小于2,则0
      case _ => 0
    }

    bytesPerUpdate = math.max(0, bytesDelta)
    //
    nextSampleNum = math.ceil(numUpdates * SAMPLE_GROWTH_RATE).toLong //计算下次采样的间隔
  }

  /**
   * Estimate the current size of the collection in bytes. O(1) time.
   * 估计当前集合中对象占用内存的大小
   * 对AppendOnlyMap和SizeTrackingPairBuffer在内存中的容量进行限制以防内存溢出进发挥其作用
   */
  def estimateSize(): Long = {
    assert(samples.nonEmpty) //不能为空
    //bytePerUpdate作为最近平均每次更新,
    //用当前的update次数减去最后一个Sample的update次数,然后乘以bytePerUpdate,结果加上最后一个Sample记录的大小
    val extrapolatedDelta = bytesPerUpdate * (numUpdates - samples.last.numUpdates)
    (samples.last.size + extrapolatedDelta).toLong
  }
}

private object SizeTracker {
  case class Sample(size: Long, numUpdates: Long)
}
