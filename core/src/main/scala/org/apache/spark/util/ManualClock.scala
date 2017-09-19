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

package org.apache.spark.util

/**
 * A `Clock` whose time can be manually set and modified. Its reported time does not change
 * as time elapses, but only as its time is modified by callers. This is mainly useful for
 * testing.
  * “时钟”,其时间可以手动设置和修改,其报告的时间并不随着时间的流逝而变化,而只是随着时间被呼叫者修改,这主要用于测试。
 *
 * @param time initial time (in milliseconds since the epoch) 初始时间（从时代开始以毫秒计）
 */
private[spark] class ManualClock(private var time: Long) extends Clock {

  /**
   * @return `ManualClock` with initial time 0
   */
  def this() = this(0L)

  def getTimeMillis(): Long =
    synchronized {
      time
    }

  /**
   * @param timeToSet new time (in milliseconds) that the clock should represent
    *                  时钟应该代表的新时间（以毫秒为单位）
   */
  def setTime(timeToSet: Long): Unit = synchronized {
    time = timeToSet
    notifyAll()
  }

  /**
   * @param timeToAdd time (in milliseconds) to add to the clock's time
    *                  时间（以毫秒为单位）添加到时钟的时间
   */
  def advance(timeToAdd: Long): Unit = synchronized {
    time += timeToAdd
    notifyAll()
  }

  /**
   * @param targetTime block until the clock time is set or advanced to at least this time
    *                   阻塞直到时钟时间被设置或提前至少这个时间
   * @return current time reported by the clock when waiting finishes 等待完成时钟的当前时间
   */
  def waitTillTime(targetTime: Long): Long = synchronized {
    while (time < targetTime) {
      wait(10)
    }
    getTimeMillis()
  }
}
