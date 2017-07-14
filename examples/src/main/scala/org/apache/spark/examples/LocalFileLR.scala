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

// scalastyle:off println
package org.apache.spark.examples

import java.util.Random

import breeze.linalg.{Vector, DenseVector}

/**
  * Logistic regression based classification.
  * 基于逻辑回归的分类
  * This is an example implementation for learning how to use Spark. For more conventional use,
  * 这是一个学习如何使用Spark的例子实现,为更传统的使用,LogisticRegressionWithSGD(SGD随机梯度下降)
  * please refer to either org.apache.spark.mllib.classification.LogisticRegressionWithSGD or
  * org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS(BFGS是逆秩2拟牛顿法) based on your needs.
  */
object LocalFileLR {
  val D = 10   // Numer of dimensions 维度
  val rand = new Random(42)

  case class DataPoint(x: Vector[Double], y: Double)
  //解析每一行数据,生成DataPoint对像
  def parsePoint(line: String): DataPoint = {
    val nums = line.split(' ').map(_.toDouble)
    DataPoint(new DenseVector(nums.slice(1, D + 1)), nums(0))
  }

  def showWarning() {
    System.err.println(
      """WARN: This is a naive implementation of Logistic Regression and is given as an example!
        |Please use either org.apache.spark.mllib.classification.LogisticRegressionWithSGD or
        |org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS(BFGS是逆秩2拟牛顿法)
        |for more conventional use.
      """.stripMargin)
  }

  def main(args: Array[String]) {

    showWarning()
    //fromFile读取文件,转换成Array[String]
    val lines = scala.io.Source.fromFile(args(0)).getLines().toArray
    //调用parsePoint解析每一行数据
    val points = lines.map(parsePoint _)
    val ITERATIONS = args(1).toInt

    // Initialize w to a random value
    //初始化W到一个随机值,可数组
    var w = DenseVector.fill(D){2 * rand.nextDouble - 1}
    println("Initial w: " + w)

    for (i <- 1 to ITERATIONS) {
      println("On iteration " + i)
      var gradient = DenseVector.zeros[Double](D)
      for (p <- points) {
        val scale = (1 / (1 + math.exp(-p.y * (w.dot(p.x)))) - 1) * p.y
        gradient += p.x * scale
      }
      w -= gradient
    }

    println("Final w: " + w)
  }
}
// scalastyle:on println