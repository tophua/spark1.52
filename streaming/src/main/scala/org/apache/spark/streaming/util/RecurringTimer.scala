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

package org.apache.spark.streaming.util

import org.apache.spark.Logging
import org.apache.spark.util.{Clock, SystemClock}

private[streaming]
class RecurringTimer(clock: Clock, period: Long, callback: (Long) => Unit, name: String)
  extends Logging {

  private val thread = new Thread("RecurringTimer - " + name) {
    setDaemon(true)
    override def run() { loop }
  }

  @volatile private var prevTime = -1L
  @volatile private var nextTime = -1L
  @volatile private var stopped = false

  /**
   * Get the time when this timer will fire if it is started right now.
   * 获取时间,当这个计时器将火,如果它是现在开始
   * The time will be a multiple of this timer's period and more than
   * current system time.
   * 时间将是这个定时器周期的倍数和超过当前系统时间的倍数
   */
  def getStartTime(): Long = {
    (math.floor(clock.getTimeMillis().toDouble / period) + 1).toLong * period
  }

  /**
   * Get the time when the timer will fire if it is restarted right now.
   * 得到的时间,当定时器将火,如果它现在重新启动
   * This time depends on when the timer was started the first time, and was stopped
   * 这个时间取决于当定时器是第一次启动的时候,
   * for whatever reason. The time must be a multiple of this timer's period and
   * 并被停止的任何原因,时间必须是这个定时器周期的倍数,超过当前时间
   * more than current time.
   */
  def getRestartTime(originalStartTime: Long): Long = {
    val gap = clock.getTimeMillis() - originalStartTime
    (math.floor(gap.toDouble / period).toLong + 1) * period + originalStartTime
  }

  /**
   * Start at the given start time.
   * 在给定的开始时间
   */
  def start(startTime: Long): Long = synchronized {
    nextTime = startTime
    thread.start()
    logInfo("Started timer for " + name + " at time " + nextTime)
    nextTime
  }

  /**
   * Start at the earliest time it can start based on the period.
   * 从最早开始它可以根据时间开始
   */
  def start(): Long = {
    start(getStartTime())
  }

  /**
   * Stop the timer, and return the last time the callback was made.
   * 停止计时器,并返回上一次回调的最后一次
   * - interruptTimer = true will interrupt the callback 将中断回调
   * if it is in progress (not guaranteed to give correct time in this case).
   * 如果它正在进行中 (不保证在这种情况下给予正确的时间)
   * - interruptTimer = false guarantees that there will be at least one callback after `stop` has
   * been called.
   */
  def stop(interruptTimer: Boolean): Long = synchronized {
    if (!stopped) {
      stopped = true
      if (interruptTimer) {
        thread.interrupt()
      }
      thread.join()
      logInfo("Stopped timer for " + name + " after time " + prevTime)
    }
    prevTime
  }

  private def triggerActionForNextInterval(): Unit = {
    clock.waitTillTime(nextTime)
    callback(nextTime)
    prevTime = nextTime
    nextTime += period
    logDebug("Callback for " + name + " called at time " + prevTime)
  }

  /**
   * Repeatedly call the callback every interval.
   * 反复调用回调的每一个间隔
   */
  private def loop() {
    try {
      while (!stopped) {
        triggerActionForNextInterval()
      }
      triggerActionForNextInterval()
    } catch {
      case e: InterruptedException =>
    }
  }
}

private[streaming]
object RecurringTimer extends Logging {

  def main(args: Array[String]) {
    var lastRecurTime = 0L
    val period = 1000

    def onRecur(time: Long) {
      val currentTime = System.currentTimeMillis()
      logInfo("" + currentTime + ": " + (currentTime - lastRecurTime))
      lastRecurTime = currentTime
    }
    val timer = new  RecurringTimer(new SystemClock(), period, onRecur, "Test")
    timer.start()
    Thread.sleep(30 * 1000)
    timer.stop(true)
  }
}

