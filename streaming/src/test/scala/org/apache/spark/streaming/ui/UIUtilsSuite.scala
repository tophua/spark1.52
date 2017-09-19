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

import java.util.TimeZone
import java.util.concurrent.TimeUnit
import org.scalatest.Matchers
import org.apache.spark.SparkFunSuite
import java.util.Date
import java.text.SimpleDateFormat

class UIUtilsSuite extends SparkFunSuite with Matchers{

  test("shortTimeUnitString") {//短时间单位字符串
    //纳秒
    assert("ns" === UIUtils.shortTimeUnitString(TimeUnit.NANOSECONDS))
    //毫秒
    assert("us" === UIUtils.shortTimeUnitString(TimeUnit.MICROSECONDS))
    //秒
    assert("ms" === UIUtils.shortTimeUnitString(TimeUnit.MILLISECONDS))
    //分
    assert("sec" === UIUtils.shortTimeUnitString(TimeUnit.SECONDS))
    //分
    assert("min" === UIUtils.shortTimeUnitString(TimeUnit.MINUTES))
    //小时
    assert("hrs" === UIUtils.shortTimeUnitString(TimeUnit.HOURS))
    //天
    assert("days" === UIUtils.shortTimeUnitString(TimeUnit.DAYS))
  }

  test("normalizeDuration") {//验证正常时间
    
    verifyNormalizedTime(900, TimeUnit.MILLISECONDS, 900)//毫秒
    verifyNormalizedTime(1.0, TimeUnit.SECONDS, 1000)//秒
    verifyNormalizedTime(1.0, TimeUnit.MINUTES, 60 * 1000)//分
    verifyNormalizedTime(1.0, TimeUnit.HOURS, 60 * 60 * 1000)//小时
    verifyNormalizedTime(1.0, TimeUnit.DAYS, 24 * 60 * 60 * 1000)//天
  }
/**
 * 验证输入数据是否指定单位
 * @param expectedTime 期望的时间
 * @param expectedUnit 期望的单位
 * @paraminput				输入的数据
 */
  private def verifyNormalizedTime(
      expectedTime: Double, expectedUnit: TimeUnit, input: Long): Unit = {
    val (time, unit) = UIUtils.normalizeDuration(input)//正常时间
   
    time should be (expectedTime +- 1E-6)
    unit should be (expectedUnit)
  }

  test("convertToTimeUnit") {//时间转换
    verifyConvertToTimeUnit(60.0 * 1000 * 1000 * 1000, 60 * 1000, TimeUnit.NANOSECONDS)
    verifyConvertToTimeUnit(60.0 * 1000 * 1000, 60 * 1000, TimeUnit.MICROSECONDS)
    verifyConvertToTimeUnit(60 * 1000, 60 * 1000, TimeUnit.MILLISECONDS)
    verifyConvertToTimeUnit(60, 60 * 1000, TimeUnit.SECONDS)
    verifyConvertToTimeUnit(1, 60 * 1000, TimeUnit.MINUTES)
    verifyConvertToTimeUnit(1.0 / 60, 60 * 1000, TimeUnit.HOURS)
    verifyConvertToTimeUnit(1.0 / 60 / 24, 60 * 1000, TimeUnit.DAYS)
  }

  private def verifyConvertToTimeUnit(
      expectedTime: Double, milliseconds: Long, unit: TimeUnit): Unit = {
    val convertedTime = UIUtils.convertToTimeUnit(milliseconds, unit)
    convertedTime should be (expectedTime +- 1E-6)
  }

  test("formatBatchTime") {//批量时间格式化
    //获得默认时区
    val tzForTest = TimeZone.getTimeZone("America/Los_Angeles")
    //批量时间
    val batchTime = 1431637480452L // Thu May 14 14:04:40 PDT 2015 2015/5/14 14:04:40
    new Date(1431637480452L).toString()
   
    
    val form=new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    form.setTimeZone(tzForTest)     
    println(UIUtils.formatBatchTime(batchTime,2000,timezone = tzForTest)+"\t"+form.format(new Date(1431637480452L)))
    //2015/05/14 14:04:40,大于或者1000不显示毫秒
    assert("2015/05/14 14:04:40" === UIUtils.formatBatchTime(batchTime, 1000, timezone = tzForTest))
    //大于或者1000不显示毫秒
    assert("2015/05/14 14:04:40.452" ===
      UIUtils.formatBatchTime(batchTime, 999, timezone = tzForTest))
      //如果false格式化时间格式
    assert("14:04:40" === UIUtils.formatBatchTime(batchTime, 1000, false, timezone = tzForTest))
    
    assert("14:04:40.452" === UIUtils.formatBatchTime(batchTime, 999, false, timezone = tzForTest))
  }
}
