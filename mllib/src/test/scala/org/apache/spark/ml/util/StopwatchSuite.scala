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

package org.apache.spark.ml.util

import java.util.Random

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.util.MLlibTestSparkContext
/**
 * 秒表测试套件
 */
class StopwatchSuite extends SparkFunSuite with MLlibTestSparkContext {

  import StopwatchSuite._
/**
 * 在驱动器秒表的测试
 */
  private def testStopwatchOnDriver(sw: Stopwatch): Unit = {
    assert(sw.name === "sw")
    assert(sw.elapsed() === 0L)
    assert(!sw.isRunning)
    intercept[AssertionError] {
      sw.stop()
    }
    val duration = checkStopwatch(sw)
    val elapsed = sw.elapsed()//消逝
    assert(elapsed === duration)
    val duration2 = checkStopwatch(sw)
    val elapsed2 = sw.elapsed()
    assert(elapsed2 === duration + duration2)
    assert(sw.toString === s"sw: ${elapsed2}ms")
    sw.start()
    assert(sw.isRunning)
    intercept[AssertionError] {
      sw.start()
    }
  }

  test("LocalStopwatch") {//当地的秒表
    val sw = new LocalStopwatch("sw")
    testStopwatchOnDriver(sw)
  }

  test("DistributedStopwatch on driver") {//分布式驱动程序
    val sw = new DistributedStopwatch(sc, "sw")
    testStopwatchOnDriver(sw)
  }

  test("DistributedStopwatch on executors") {//分布式执行者
    val sw = new DistributedStopwatch(sc, "sw")
    val rdd = sc.parallelize(0 until 4, 4)
    val acc = sc.accumulator(0L)
    rdd.foreach { i =>
      acc += checkStopwatch(sw)
    }
    assert(!sw.isRunning)
    val elapsed = sw.elapsed()
    assert(elapsed === acc.value)
  }

  test("MultiStopwatch") {//多秒表
    val sw = new MultiStopwatch(sc)
      .addLocal("local")
      .addDistributed("spark")
    assert(sw("local").name === "local")
    assert(sw("spark").name === "spark")
    intercept[NoSuchElementException] {
      sw("some")
    }
    assert(sw.toString === "{\n  local: 0ms,\n  spark: 0ms\n}")
    val localDuration = checkStopwatch(sw("local"))
    val sparkDuration = checkStopwatch(sw("spark"))
    val localElapsed = sw("local").elapsed()
    val sparkElapsed = sw("spark").elapsed()
    assert(localElapsed === localDuration)
    assert(sparkElapsed === sparkDuration)
    assert(sw.toString ===
      s"{\n  local: ${localElapsed}ms,\n  spark: ${sparkElapsed}ms\n}")
    val rdd = sc.parallelize(0 until 4, 4)
    val acc = sc.accumulator(0L)
    rdd.foreach { i =>
      sw("local").start()
      val duration = checkStopwatch(sw("spark"))
      sw("local").stop()
      acc += duration
    }
    val localElapsed2 = sw("local").elapsed()
    assert(localElapsed2 === localElapsed)
    val sparkElapsed2 = sw("spark").elapsed()
    assert(sparkElapsed2 === sparkElapsed + acc.value)
  }
}
/**
 * 秒表测试套件
 */
private object StopwatchSuite extends SparkFunSuite {

  /**
   * Checks the input stopwatch on a task that takes a random time (<10ms) to finish. Validates and
   * 检查需要一个随机时间输入秒表(小于10ms)完成
   * returns the duration reported by the stopwatch.
   * 验证并返回由计时表报告的持续时间
   */
  def checkStopwatch(sw: Stopwatch): Long = {
    val ubStart = now
    sw.start()
    val lbStart = now
    Thread.sleep(new Random().nextInt(10))
    val lb = now - lbStart
    val duration = sw.stop()
    val ub = now - ubStart
    assert(duration >= lb && duration <= ub)
    duration
  }

  /** 
   *  The current time in milliseconds. 
   *  当前时间以毫秒为单位
   *  */
  private def now: Long = System.currentTimeMillis()
}
