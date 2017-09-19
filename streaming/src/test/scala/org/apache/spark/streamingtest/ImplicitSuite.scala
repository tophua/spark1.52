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

package org.apache.spark.streamingtest

/**
 * A test suite to make sure all `implicit` functions work correctly.
 * 一个测试套件,以确保所有的隐式功能工作的正确
 * As `implicit` is a compiler feature, we don't need to run this class.
 * 作为隐式是一个编译器功能,我们不需要运行这个类
 * What we need to do is making the compiler happy.
 * 我们所需要做的是让编译器快乐
 */
class ImplicitSuite {

  // We only want to test if `implicit` works well with the compiler,
  //我们只想测试,如果“隐式”的作品以及与编译器
  // so we don't need a real DStream.
  //所以我们不需要一个真正的dstream
  def mockDStream[T]: org.apache.spark.streaming.dstream.DStream[T] = null

  def testToPairDStreamFunctions(): Unit = {
    val dstream: org.apache.spark.streaming.dstream.DStream[(Int, Int)] = mockDStream
    //groupByKey也是对每个key进行操作,但只生成一个sequence
    dstream.groupByKey()
  }
}
