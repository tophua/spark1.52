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

package org.apache.spark.mllib.util

import org.scalatest.{BeforeAndAfterAll, Suite}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

trait MLlibTestSparkContext extends BeforeAndAfterAll { 
  //自身类型（self type）的声明来定义特质时（this: ClassName =>）,只能被混入给定类型的子类当中
  self: Suite =>
  //transient是Java语言的关键字，用来表示一个域不是该对象串行化的一部分。
  //当一个对象被串行化的时候，transient型变量的值不包括在串行化的表示中，然而非transient型的变量是被包括进去的。
  //这样对象的size就会小一些，相对占用的网络资源就会小。 
  @transient var sc: SparkContext = _
  @transient var sqlContext: SQLContext = _

  override def beforeAll() {
    super.beforeAll()
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("MLlibUnitTest")
    sc = new SparkContext(conf)
    sqlContext = new SQLContext(sc)
  }

  override def afterAll() {
    sqlContext = null
    if (sc != null) {
      sc.stop()
    }
    sc = null
    super.afterAll()
  }
}
