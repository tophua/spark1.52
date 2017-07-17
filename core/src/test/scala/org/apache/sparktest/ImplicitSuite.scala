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

package org.apache.sparktest

/**
 * A test suite to make sure all implicit functions work correctly.
 * Please don't import org.apache.spark.SparkContext._ in this class.
 * 一个测试套件，以确保所有“隐式”功能正常工作,请不要在此类中导入org.apache.spark.SparkContext._。
 * As implicit is a compiler feature, we don't need to run this class.
 * What we need to do is making the compiler happy.
  * 由于implicit是一个编译器功能，我们不需要运行这个类,我们需要做的是使编译器变得快乐。
 */
class ImplicitSuite {

  // We only want to test if `implict` works well with the compiler, so we don't need a real
  // SparkContext.
  //我们只想测试“implicit”是否与编译器配合使用，所以我们不需要真正的SparkContext
  def mockSparkContext[T]: org.apache.spark.SparkContext = null

  // We only want to test if `implict` works well with the compiler, so we don't need a real RDD.
  // 我们只想测试“implicit”是否与编译器配合良好,所以我们不需要真正的RDD。
  def mockRDD[T]: org.apache.spark.rdd.RDD[T] = null

  def testRddToPairRDDFunctions(): Unit = {
    val rdd: org.apache.spark.rdd.RDD[(Int, Int)] = mockRDD
    rdd.groupByKey()
  }

  def testRddToAsyncRDDActions(): Unit = {
    val rdd: org.apache.spark.rdd.RDD[Int] = mockRDD
    rdd.countAsync()
  }

  def testRddToSequenceFileRDDFunctions(): Unit = {
    val rdd: org.apache.spark.rdd.RDD[(Int, Int)] = mockRDD
    rdd.saveAsSequenceFile("/a/test/path")
  }

  def testRddToSequenceFileRDDFunctionsWithWritable(): Unit = {
    val rdd: org.apache.spark.rdd.RDD[(org.apache.hadoop.io.IntWritable, org.apache.hadoop.io.Text)]
      = mockRDD
    rdd.saveAsSequenceFile("/a/test/path")
  }

  def testRddToSequenceFileRDDFunctionsWithBytesArray(): Unit = {
    val rdd: org.apache.spark.rdd.RDD[(Int, Array[Byte])] = mockRDD
    rdd.saveAsSequenceFile("/a/test/path")
  }

  def testRddToOrderedRDDFunctions(): Unit = {
    val rdd: org.apache.spark.rdd.RDD[(Int, Int)] = mockRDD
    rdd.sortByKey()
  }

  def testDoubleRDDToDoubleRDDFunctions(): Unit = {
    val rdd: org.apache.spark.rdd.RDD[Double] = mockRDD
    rdd.stats()
  }

  def testNumericRDDToDoubleRDDFunctions(): Unit = {
    val rdd: org.apache.spark.rdd.RDD[Int] = mockRDD
    rdd.stats()
  }

  def testDoubleAccumulatorParam(): Unit = {
    val sc = mockSparkContext
    sc.accumulator(123.4)
  }

  def testIntAccumulatorParam(): Unit = {
    val sc = mockSparkContext
    sc.accumulator(123)
  }

  def testLongAccumulatorParam(): Unit = {
    val sc = mockSparkContext
    sc.accumulator(123L)
  }

  def testFloatAccumulatorParam(): Unit = {
    val sc = mockSparkContext
    sc.accumulator(123F)
  }

  def testIntWritableConverter(): Unit = {
    val sc = mockSparkContext
    sc.sequenceFile[Int, Int]("/a/test/path")
  }

  def testLongWritableConverter(): Unit = {
    val sc = mockSparkContext
    sc.sequenceFile[Long, Long]("/a/test/path")
  }

  def testDoubleWritableConverter(): Unit = {
    val sc = mockSparkContext
    sc.sequenceFile[Double, Double]("/a/test/path")
  }

  def testFloatWritableConverter(): Unit = {
    val sc = mockSparkContext
    sc.sequenceFile[Float, Float]("/a/test/path")
  }

  def testBooleanWritableConverter(): Unit = {
    val sc = mockSparkContext
    sc.sequenceFile[Boolean, Boolean]("/a/test/path")
  }

  def testBytesWritableConverter(): Unit = {
    val sc = mockSparkContext
    sc.sequenceFile[Array[Byte], Array[Byte]]("/a/test/path")
  }

  def testStringWritableConverter(): Unit = {
    val sc = mockSparkContext
    sc.sequenceFile[String, String]("/a/test/path")
  }

  def testWritableWritableConverter(): Unit = {
    val sc = mockSparkContext
    sc.sequenceFile[org.apache.hadoop.io.Text, org.apache.hadoop.io.Text]("/a/test/path")
  }
}
