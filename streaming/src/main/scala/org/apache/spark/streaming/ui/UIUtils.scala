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

package org.apache.spark.streaming.ui

import java.text.SimpleDateFormat
import java.util.TimeZone
import java.util.concurrent.TimeUnit

private[streaming] object UIUtils {

  /**
   * Return the short string for a `TimeUnit`.
   * 返回一个短的时间单位的字符串
   */
  def shortTimeUnitString(unit: TimeUnit): String = unit match {
    case TimeUnit.NANOSECONDS => "ns"
    case TimeUnit.MICROSECONDS => "us"
    case TimeUnit.MILLISECONDS => "ms"
    case TimeUnit.SECONDS => "sec"
    case TimeUnit.MINUTES => "min"
    case TimeUnit.HOURS => "hrs"
    case TimeUnit.DAYS => "days"
  }

  /**
   * Find the best `TimeUnit` for converting milliseconds to a friendly string. Return the value
   * after converting, also with its TimeUnit.
   * 寻找最佳的“时间单位”转换为一个友好的字符串毫秒,返回一个元组(转换后的值,一个时间单位)
   * 
   */
  def normalizeDuration(milliseconds: Long): (Double, TimeUnit) = {
    if (milliseconds < 1000) {
      return (milliseconds, TimeUnit.MILLISECONDS)
    }
    val seconds = milliseconds.toDouble / 1000
    if (seconds < 60) {
      return (seconds, TimeUnit.SECONDS)
    }
    val minutes = seconds / 60
    if (minutes < 60) {
      return (minutes, TimeUnit.MINUTES)
    }
    val hours = minutes / 60
    if (hours < 24) {
      return (hours, TimeUnit.HOURS)
    }
    val days = hours / 24
    (days, TimeUnit.DAYS)
  }

  /**
   * Convert `milliseconds` to the specified `unit`. We cannot use `TimeUnit.convert` because it
   * will discard the fractional part.
   */
  def convertToTimeUnit(milliseconds: Long, unit: TimeUnit): Double = unit match {
    case TimeUnit.NANOSECONDS => milliseconds * 1000 * 1000
    case TimeUnit.MICROSECONDS => milliseconds * 1000
    case TimeUnit.MILLISECONDS => milliseconds
    case TimeUnit.SECONDS => milliseconds / 1000.0
    case TimeUnit.MINUTES => milliseconds / 1000.0 / 60.0
    case TimeUnit.HOURS => milliseconds / 1000.0 / 60.0 / 60.0
    case TimeUnit.DAYS => milliseconds / 1000.0 / 60.0 / 60.0 / 24.0
  }

  // SimpleDateFormat is not thread-safe. Don't expose it to avoid improper use.
  //不是不是线程安全的,不要暴露它,以避免不正确的使用
  private val batchTimeFormat = new ThreadLocal[SimpleDateFormat]() {
    override def initialValue(): SimpleDateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
  }

  private val batchTimeFormatWithMilliseconds = new ThreadLocal[SimpleDateFormat]() {
    override def initialValue(): SimpleDateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS")
  }

  /**
   * If `batchInterval` is less than 1 second, format `batchTime` with milliseconds. Otherwise,
   * format `batchTime` without milliseconds.
   *
   * @param batchTime the batch time to be formatted 
   * 	格式化的批处理时间,如果小于1000格式化时间显示毫秒,如果大于1000格式日期不显示毫秒
   * @param batchInterval the batch interval 间隔区间
   * @param showYYYYMMSS if showing the `yyyy/MM/dd` part. If it's false, the return value wll be
   * 										 如果显示` YYYY/MM/DD `部分,如果它是false,
   *                     only `HH:mm:ss` or `HH:mm:ss.SSS` depending on `batchInterval`
   *                     返回值就只有` HH:mm:ss`或`HH:mm:ss.SSS`取决于` batchinterval `
   * @param timezone only for test 只有测试
   */
  def formatBatchTime(
      batchTime: Long,
      batchInterval: Long,
      showYYYYMMSS: Boolean = true,
      timezone: TimeZone = null): String = {
    val oldTimezones =
      (batchTimeFormat.get.getTimeZone, batchTimeFormatWithMilliseconds.get.getTimeZone)
    if (timezone != null) {
      batchTimeFormat.get.setTimeZone(timezone)
      batchTimeFormatWithMilliseconds.get.setTimeZone(timezone)
    }
    try {
      val formattedBatchTime =
        if (batchInterval < 1000) {
          batchTimeFormatWithMilliseconds.get.format(batchTime)
        } else {
          //如果batchinterval > = 1秒,不显示毫秒
          // If batchInterval >= 1 second, don't show milliseconds
          batchTimeFormat.get.format(batchTime)
        }
      if (showYYYYMMSS) {
        formattedBatchTime
      } else {
        formattedBatchTime.substring(formattedBatchTime.indexOf(' ') + 1)
      }
    } finally {
      if (timezone != null) {
        batchTimeFormat.get.setTimeZone(oldTimezones._1)
        batchTimeFormatWithMilliseconds.get.setTimeZone(oldTimezones._2)
      }
    }
  }
}
