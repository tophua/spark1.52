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

package org.apache.spark.mllib.stat

import breeze.linalg.{ DenseMatrix => BDM, Matrix => BM }

import org.apache.spark.{ Logging, SparkFunSuite }
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.correlation.{
  Correlations,
  PearsonCorrelation,
  SpearmanCorrelation
}
import org.apache.spark.mllib.util.MLlibTestSparkContext
/**
 * 相关性是两个变量之间的统计关系,意为当一个变量变化时会导致另一个变更的变化,
 * 相关性分析是度量两个变量的相关程度,如果一个变更的增加导致另一个变量也增加叫正相关
 * 如果一个变量的增加导致另一个变量的降低叫负相关
 * pearson皮尔森相关性算法用于两个连续的变量
 * spearman 斯皮漫相关性算法用于一个连续值和一个离散值
 */
class CorrelationSuite extends SparkFunSuite with MLlibTestSparkContext with Logging {

  // test input data 测试输入数据
  val xData = Array(1.0, 0.0, -2.0)
  val yData = Array(4.0, 5.0, 3.0)
  val zeros = new Array[Double](3)
  val data = Seq(
    Vectors.dense(1.0, 0.0, 0.0, -2.0),
    Vectors.dense(4.0, 5.0, 0.0, 3.0),
    Vectors.dense(6.0, 7.0, 0.0, 8.0),
    Vectors.dense(9.0, 0.0, 0.0, 1.0))
  /**
   * pearson皮尔森相关性
   * spearman 斯皮漫相关性
   */
  test("corr(x, y) pearson, 1 value in data") {//corr函数是求两个矩阵皮尔森相似度,在1值数据
    //一个数据值无法相关系数
    val x = sc.parallelize(Array(1.0))
    val y = sc.parallelize(Array(4.0))
    intercept[RuntimeException] {
      Statistics.corr(x, y, "pearson")
    }
    intercept[RuntimeException] {
      Statistics.corr(x, y, "spearman")
    }
  }
  //corr函数是求两个矩阵皮尔森相似度,默认皮尔森相似度
  test("corr(x, y) default, pearson") {
    val x = sc.parallelize(xData)
    val y = sc.parallelize(yData)
    val expected = 0.6546537 //期望值
    //pearson皮尔森相关性算法用于两个连续的变量
    val default = Statistics.corr(x, y) //默认皮尔森相关性,default值为 0.6546536707079771    
    val p1 = Statistics.corr(x, y, "pearson") //皮尔森相关性
    assert(approxEqual(expected, default))
    assert(approxEqual(expected, p1))

    // numPartitions >= size for input RDDs
    for (numParts <- List(xData.size, xData.size * 2)) {
      val x1 = sc.parallelize(xData, numParts)
      val y1 = sc.parallelize(yData, numParts)
      val p2 = Statistics.corr(x1, y1)
      assert(approxEqual(expected, p2))
    }

    // RDD of zero variance
    //RDD零方差
    val z = sc.parallelize(zeros)
    assert(Statistics.corr(x, z).isNaN)
  }
  /**
   * spearman 由于利用的等级相关,因而spearman相关性分析也称为spearman等级相关分析或等级差数法
	 * spearman 斯皮漫相关性算法用于一个连续值和一个离散值
   */

  test("corr(x, y) spearman") { //斯皮漫相关性 
    val x = sc.parallelize(xData)
    val y = sc.parallelize(yData)
    val expected = 0.5
    val s1 = Statistics.corr(x, y, "spearman")
    assert(approxEqual(expected, s1))

    // numPartitions >= size for input RDDs
    for (numParts <- List(xData.size, xData.size * 2)) {
      val x1 = sc.parallelize(xData, numParts)
      val y1 = sc.parallelize(yData, numParts)
      val s2 = Statistics.corr(x1, y1, "spearman")
      assert(approxEqual(expected, s2))
    }

    // RDD of zero variance => zero variance in ranks
    //RDD零方差=>零方差的行列
    val z = sc.parallelize(zeros)
    assert(Statistics.corr(x, z, "spearman").isNaN)
  }

  test("corr(X) default, pearson") {//corr函数是求两个矩阵皮尔森相似度,默认皮尔森相似度
    val X = sc.parallelize(data)
    val defaultMat = Statistics.corr(X)
    val pearsonMat = Statistics.corr(X, "pearson") //(皮尔森相关数)
    // scalastyle:off
    val expected = BDM(
      (1.00000000, 0.05564149, Double.NaN, 0.4004714),
      (0.05564149, 1.00000000, Double.NaN, 0.9135959),
      (Double.NaN, Double.NaN, 1.00000000, Double.NaN),
      (0.40047142, 0.91359586, Double.NaN, 1.0000000))
    // scalastyle:on
    assert(matrixApproxEqual(defaultMat.toBreeze, expected))
    assert(matrixApproxEqual(pearsonMat.toBreeze, expected))
  }

  test("corr(X) spearman") {//corr函数是求两个矩阵皮尔森相似度,默认斯皮尔曼相似度
     /**
     *Vectors.dense(1.0, 0.0, 0.0, -2.0),
     *Vectors.dense(4.0, 5.0, 0.0, 3.0),
     *Vectors.dense(6.0, 7.0, 0.0, 8.0),
     *Vectors.dense(9.0, 0.0, 0.0, 1.0)
     */
    val X = sc.parallelize(data)
    val spearmanMat = Statistics.corr(X, "spearman")//默认斯皮尔曼相似度
    // scalastyle:off
    val expected = BDM(
      (1.0000000, 0.1054093, Double.NaN, 0.4000000),
      (0.1054093, 1.0000000, Double.NaN, 0.9486833),
      (Double.NaN, Double.NaN, 1.00000000, Double.NaN),
      (0.4000000, 0.9486833, Double.NaN, 1.0000000))
    // scalastyle:on
    assert(matrixApproxEqual(spearmanMat.toBreeze, expected))
  }

  test("method identification") {//方法识别
    val pearson = PearsonCorrelation //(皮尔森相关数)
    val spearman = SpearmanCorrelation

    assert(Correlations.getCorrelationFromName("pearson") === pearson) //(皮尔森相关数)
    assert(Correlations.getCorrelationFromName("spearman") === spearman)//斯皮尔曼相似度

    // Should throw IllegalArgumentException
    //应该抛出非法参数异常
    try {
      Correlations.getCorrelationFromName("kendall")
      assert(false)
    } catch {
      case ie: IllegalArgumentException =>
    }
  }
  //约等于
  def approxEqual(v1: Double, v2: Double, threshold: Double = 1e-6): Boolean = {
    if (v1.isNaN) {
      v2.isNaN
    } else {
      //求绝对值,threshold
     // println("V1:" + v1 + "\t V2:" + v2 + "\t threshold:" + threshold)
      //println("math.abs(v1 - v2):" + math.abs(v1 - v2))
       //math.abs返回数的绝对值
      val b = math.abs(v1 - v2) <= threshold

      b
    }
  }
  //矩阵近似相等
  def matrixApproxEqual(A: BM[Double], B: BM[Double], threshold: Double = 1e-6): Boolean = {

    for (i <- 0 until A.rows; j <- 0 until A.cols) {
      if (!approxEqual(A(i, j), B(i, j), threshold)) {
        logInfo("i, j = " + i + ", " + j + " actual: " + A(i, j) + " expected:" + B(i, j))
        println("i, j = " + i + ", " + j + " actual: " + A(i, j) + " expected:" + B(i, j))
        return false
      }
    }
    true
  }
}
