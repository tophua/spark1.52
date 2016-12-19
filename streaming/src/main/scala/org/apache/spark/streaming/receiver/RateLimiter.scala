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

package org.apache.spark.streaming.receiver

import com.google.common.util.concurrent.{RateLimiter => GuavaRateLimiter}

import org.apache.spark.{Logging, SparkConf}

/** 
 *  Provides waitToPush() method to limit the rate at which receivers consume data.
  * 提供限制在接收器消费数据的速率waittopush()方法
  * waitToPush method will block the thread if too many messages have been pushed too quickly,
  * waitToPush方法	如果太多的消息被推得太快,就会阻塞线程,只在新消息被推时返回,它假定只有一个消息被推一次
  * and only return when a new message has been pushed. It assumes that only one message is
  * pushed at a time.
  *
  * The spark configuration spark.streaming.receiver.maxRate gives the maximum number of messages
  * per second that each receiver will accept. 给出每秒接收到的消息的最大数目
  *
  * @param conf spark configuration
  */
private[receiver] abstract class RateLimiter(conf: SparkConf) extends Logging {

  // treated as an upper limit 接受的上限
  private val maxRateLimit = conf.getLong("spark.streaming.receiver.maxRate", Long.MaxValue)
  private lazy val rateLimiter = GuavaRateLimiter.create(maxRateLimit.toDouble)

  def waitToPush() {
    rateLimiter.acquire()
  }

  /**
   * Return the current rate limit. If no limit has been set so far, it returns {{{Long.MaxValue}}}.
   * 返回当前限制速率
   */
  def getCurrentLimit: Long = rateLimiter.getRate.toLong

  /**
   * Set the rate limit to `newRate`. The new rate will not exceed the maximum rate configured by
   * {{{spark.streaming.receiver.maxRate}}}, even if `newRate` is higher than that.
   * 每秒事件的新速率,如果没有效果,设置0或者为负数
   * @param newRate A new rate in events per second. It has no effect if it's 0 or negative.
   */
  private[receiver] def updateRate(newRate: Long): Unit =
    if (newRate > 0) {
      if (maxRateLimit > 0) {
        rateLimiter.setRate(newRate.min(maxRateLimit))
      } else {
        rateLimiter.setRate(newRate)
      }
    }
}
