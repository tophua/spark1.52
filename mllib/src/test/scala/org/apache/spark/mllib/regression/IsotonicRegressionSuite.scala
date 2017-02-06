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

package org.apache.spark.mllib.regression

import org.scalatest.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.util.Utils
/**
 * 保序回归:从该序列的首元素往后观察,一旦出现乱序现象停止该轮观察,从该乱序元素开始逐个吸收元素组成一个序列,
 * 					直到该序列所有元素的平均值小于或等于下一个待吸收的元素.
 * http://blog.csdn.net/fsz521/article/details/7706250 具体规则
 */
class IsotonicRegressionSuite extends SparkFunSuite with MLlibTestSparkContext with Matchers {

  private def round(d: Double) = {
    math.round(d * 100).toDouble / 100
  }

  private def generateIsotonicInput(labels: Seq[Double]): Seq[(Double, Double, Double)] = {
    Seq.tabulate(labels.size)(i => (labels(i), i.toDouble, 1d))
  }

  private def generateIsotonicInput(
      labels: Seq[Double],
      weights: Seq[Double]): Seq[(Double, Double, Double)] = {
    Seq.tabulate(labels.size)(i => (labels(i), i.toDouble, weights(i)))
  }

  private def runIsotonicRegression(
      labels: Seq[Double],
      weights: Seq[Double],
      isotonic: Boolean): IsotonicRegressionModel = {
    println("labels:"+labels)
    println("weights:"+weights)
    /**
     * labels:List(1.0, 2.0, 3.0, 1.0, 6.0, 17.0, 16.0, 17.0, 18.0)
		 * weights:WrappedArray(1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)
     */
    val trainRDD = sc.parallelize(generateIsotonicInput(labels, weights)).cache()
    //isotonic,默认值是true,此参数指定保序回归是保序的(单调增加)还是不保序的(单调减少)
    new IsotonicRegression().setIsotonic(isotonic).run(trainRDD)
  }

  private def runIsotonicRegression(
      labels: Seq[Double],
      isotonic: Boolean): IsotonicRegressionModel = {
    runIsotonicRegression(labels, Array.fill(labels.size)(1d), isotonic)
  }

  test("increasing isotonic regression") {//增加保序回归
    /*
     The following result could be re-produced with sklearn.
		下面的结果可以重新产生sklearn
     > from sklearn.isotonic import IsotonicRegression
     > x = range(9)
     > y = [1, 2, 3, 1, 6, 17, 16, 17, 18]
     > ir = IsotonicRegression(x, y)
     > print ir.predict(x)
     array([  1. ,   2. ,   2. ,   2. ,   6. ,  16.5,  16.5,  17. ,  18. ])
     */
    val model = runIsotonicRegression(Seq(1, 2, 3, 1, 6, 17, 16, 17, 18), true)
   //返回一个包含给定函数的值超过整数值从0开始范围的二维数组
    assert(Array.tabulate(9)(x => model.predict(x)) === Array(1, 2, 2, 2, 6, 16.5, 16.5, 17, 18))
   //boundaries 边界数据
    assert(model.boundaries === Array(0, 1, 3, 4, 5, 6, 7, 8))
    //predictions对应边界数据组的值
    assert(model.predictions === Array(1, 2, 2, 6, 16.5, 16.5, 17.0, 18.0))
    //序列升序或者降序
    assert(model.isotonic)
  }

  test("model save/load") {//模型保存/加载
    val boundaries = Array(0.0, 1.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0)
    val predictions = Array(1, 2, 2, 6, 16.5, 16.5, 17.0, 18.0)
    val model = new IsotonicRegressionModel(boundaries, predictions, true)

    val tempDir = Utils.createTempDir()
    val path = tempDir.toURI.toString

    // Save model, load it back, and compare.
    //保存模型,加载它回来,并比较
    try {
      model.save(sc, path)
      val sameModel = IsotonicRegressionModel.load(sc, path)
      assert(model.boundaries === sameModel.boundaries)
      assert(model.predictions === sameModel.predictions)
      assert(model.isotonic === model.isotonic)
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }

  test("isotonic regression with size 0") {//保序回归与尺寸0
    val model = runIsotonicRegression(Seq(), true)

    assert(model.predictions === Array())
  }

  test("isotonic regression with size 1") {//保序回归与尺寸1
    val model = runIsotonicRegression(Seq(1), true)

    assert(model.predictions === Array(1.0))
  }
  //保序回归严格递增序列
  test("isotonic regression strictly increasing sequence(递增)") {
    val model = runIsotonicRegression(Seq(1, 2, 3, 4, 5), true)

    assert(model.predictions === Array(1, 2, 3, 4, 5))
  }
  //保序回归严格渐减序列
  test("isotonic regression strictly decreasing sequence(渐减)") {
    val model = runIsotonicRegression(Seq(5, 4, 3, 2, 1), true)

    assert(model.boundaries === Array(0, 4))
    assert(model.predictions === Array(3, 3))
  }
  //保序回归一元违反单调性
  test("isotonic regression with last element violating monotonicity") {
    val model = runIsotonicRegression(Seq(1, 2, 3, 4, 2), true)

    assert(model.boundaries === Array(0, 1, 2, 4))
    assert(model.predictions === Array(1, 2, 3, 3))
  }
  //保序回归一元违反单调性
  test("isotonic regression with first element violating monotonicity") {
    val model = runIsotonicRegression(Seq(4, 2, 3, 4, 5), true)

    assert(model.boundaries === Array(0, 2, 3, 4))
    assert(model.predictions === Array(3, 3, 4, 5))
  }  
  //保序回归与负标签
  test("isotonic regression with negative labels") {
    val model = runIsotonicRegression(Seq(-1, -2, 0, 1, -1), true)

    assert(model.boundaries === Array(0, 1, 2, 4))
    assert(model.predictions === Array(-1.5, -1.5, 0, 0))
  }

  test("isotonic regression with unordered input") {//保序回归无序输入
    val trainRDD = sc.parallelize(generateIsotonicInput(Seq(1, 2, 3, 4, 5)).reverse, 2).cache()

    val model = new IsotonicRegression().run(trainRDD)
    assert(model.predictions === Array(1, 2, 3, 4, 5))
  }

  test("weighted isotonic regression") {//加权保序回归
    val model = runIsotonicRegression(Seq(1, 2, 3, 4, 2), Seq(1, 1, 1, 1, 2), true)

    assert(model.boundaries === Array(0, 1, 2, 4))
    assert(model.predictions === Array(1, 2, 2.75, 2.75))
  }
  //重量低于1加权序回归
  test("weighted isotonic regression with weights lower than 1") {
    val model = runIsotonicRegression(Seq(1, 2, 3, 2, 1), Seq(1, 1, 1, 0.1, 0.1), true)

    assert(model.boundaries === Array(0, 1, 2, 4))
    assert(model.predictions.map(round) === Array(1, 2, 3.3/1.2, 3.3/1.2))
  }
  //负权值的加权序回归
  test("weighted isotonic regression with negative weights") {
    val model = runIsotonicRegression(Seq(1, 2, 3, 2, 1), Seq(-1, 1, -3, 1, -5), true)

    assert(model.boundaries === Array(0.0, 1.0, 4.0))
    assert(model.predictions === Array(1.0, 10.0/6, 10.0/6))
  }
  //零权重加权序回归
  test("weighted isotonic regression with zero weights") {
    val model = runIsotonicRegression(Seq[Double](1, 2, 3, 2, 1), Seq[Double](0, 0, 0, 1, 0), true)

    assert(model.boundaries === Array(0.0, 1.0, 4.0))
    assert(model.predictions === Array(1, 2, 2))
  }
  //保序回归预测
  test("isotonic regression prediction") {
    val model = runIsotonicRegression(Seq(1, 2, 7, 1, 2), true)

    assert(model.predict(-2) === 1)
    assert(model.predict(-1) === 1)
    assert(model.predict(0.5) === 1.5)
    assert(model.predict(0.75) === 1.75)
    assert(model.predict(1) === 2)
    assert(model.predict(2) === 10d/3)
    assert(model.predict(9) === 10d/3)
  }  
  //重复功能的保序回归预测
  test("isotonic regression prediction with duplicate features") {
    val trainRDD = sc.parallelize(
      Seq[(Double, Double, Double)](
        (2, 1, 1), (1, 1, 1), (4, 2, 1), (2, 2, 1), (6, 3, 1), (5, 3, 1)), 2).cache()
    val model = new IsotonicRegression().run(trainRDD)

    assert(model.predict(0) === 1)
    assert(model.predict(1.5) === 2)
    assert(model.predict(2.5) === 4.5)
    assert(model.predict(4) === 6)
  }
  //重复特征逆序回归预测
  test("antitonic regression prediction with duplicate features") {
    val trainRDD = sc.parallelize(
      Seq[(Double, Double, Double)](
        (5, 1, 1), (6, 1, 1), (2, 2, 1), (4, 2, 1), (1, 3, 1), (2, 3, 1)), 2).cache()
    val model = new IsotonicRegression().setIsotonic(false).run(trainRDD)

    assert(model.predict(0) === 6)
    assert(model.predict(1.5) === 4.5)
    assert(model.predict(2.5) === 2)
    assert(model.predict(4) === 1)
  }
  //保序回归法预测
  test("isotonic regression RDD prediction") {
    val model = runIsotonicRegression(Seq(1, 2, 7, 1, 2), true)

    val testRDD = sc.parallelize(List(-2.0, -1.0, 0.5, 0.75, 1.0, 2.0, 9.0), 2).cache()
    val predictions = testRDD.map(x => (x, model.predict(x))).collect().sortBy(_._1).map(_._2)
    assert(predictions === Array(1, 1, 1.5, 1.75, 2, 10.0/3, 10.0/3))
  }
  //逆序回归预测
  test("antitonic regression prediction") {
    val model = runIsotonicRegression(Seq(7, 5, 3, 5, 1), false)

    assert(model.predict(-2) === 7)
    assert(model.predict(-1) === 7)
    assert(model.predict(0.5) === 6)
    assert(model.predict(0.75) === 5.5)
    assert(model.predict(1) === 5)
    assert(model.predict(2) === 4)
    assert(model.predict(9) === 1)
  }
  //模型构建
  test("model construction") {
    val model = new IsotonicRegressionModel(Array(0.0, 1.0), Array(1.0, 2.0), isotonic = true)
    assert(model.predict(-0.5) === 1.0)
    assert(model.predict(0.0) === 1.0)
    assert(model.predict(0.5) ~== 1.5 absTol 1e-14)
    assert(model.predict(1.0) === 2.0)
    assert(model.predict(1.5) === 2.0)

    intercept[IllegalArgumentException] {  
      // different array sizes.不同数组大小
      new IsotonicRegressionModel(Array(0.0, 1.0), Array(1.0), isotonic = true)
    }

    intercept[IllegalArgumentException] {
      // unordered boundaries 无序的界限
      new IsotonicRegressionModel(Array(1.0, 0.0), Array(1.0, 2.0), isotonic = true)
    }

    intercept[IllegalArgumentException] {
      // unordered predictions (isotonic)无序的预测(保序)
      new IsotonicRegressionModel(Array(0.0, 1.0), Array(2.0, 1.0), isotonic = true)
    }

    intercept[IllegalArgumentException] {
      // unordered predictions (antitonic)无序的预测(反序)
      new IsotonicRegressionModel(Array(0.0, 1.0), Array(1.0, 2.0), isotonic = false)
    }
  }
}
