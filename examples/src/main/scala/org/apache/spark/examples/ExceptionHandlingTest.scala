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

package org.apache.spark.examples

import org.apache.spark.{SparkConf, SparkContext}
/**
  * 测试异常处理
  */
object ExceptionHandlingTest {//异常处理测试
def main(args: Array[String]) {
  val sparkConf = new SparkConf().setAppName("ExceptionHandlingTest").setMaster("local")
  val sc = new SparkContext(sparkConf)
  println("sc.defaultParallelism:"+sc.defaultParallelism)
  sc.parallelize(0 until sc.defaultParallelism).foreach { i =>
    val rd=math.random
    println(rd)
    if (rd > 0.75) {
      //测试异常处理
      throw new Exception("Testing exception handling")
    }
  }

  sc.stop()
}
}