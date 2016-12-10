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

import org.apache.spark.SparkConf
import org.apache.spark.SparkFunSuite

/** 
 *  Testsuite for testing the network receiver behavior
 *  测试网络接收行为
 * */
class RateLimiterSuite extends SparkFunSuite {
//即使没有maxrate速率限制器初始化设置
  test("rate limiter initializes even without a maxRate set") {
    val conf = new SparkConf()
    val rateLimiter = new RateLimiter(conf){}
    rateLimiter.updateRate(105)//速率限制
    //获得当前限制
    assert(rateLimiter.getCurrentLimit == 105)
  }

  test("rate limiter updates when below maxRate") {//速率限制器更新时低于maxrate
    val conf = new SparkConf().set("spark.streaming.receiver.maxRate", "110")
    val rateLimiter = new RateLimiter(conf){}
    rateLimiter.updateRate(105)
    assert(rateLimiter.getCurrentLimit == 105)
  }

  test("rate limiter stays below maxRate despite large updates") {//速率限制器下maxrate尽最大更新
    val conf = new SparkConf().set("spark.streaming.receiver.maxRate", "100")
    val rateLimiter = new RateLimiter(conf){}
    rateLimiter.updateRate(105)
    assert(rateLimiter.getCurrentLimit === 100)
  }
}
