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

import scala.math.exp

import breeze.linalg.{Vector, DenseVector}
import org.apache.hadoop.conf.Configuration

import org.apache.spark._
import org.apache.spark.scheduler.InputFormatInfo


/**
  * Logistic regression based classification.
  * 基于逻辑回归的分类
  * This is an example implementation for learning how to use Spark. For more conventional use,
  * 这是一个学习如何使用Spark的例子实现,为更传统的使用
  * please refer to either org.apache.spark.mllib.classification.LogisticRegressionWithSGD(SGD随机梯度下降) or
  * org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS(BFGS是逆秩2拟牛顿法) based on your needs.
  */
object SparkHdfsLR {
  val D = 5   // Numer of dimensions 维度
  val rand = new Random(42)

  case class DataPoint(x: Vector[Double], y: Double)

  def parsePoint(line: String): DataPoint = {
    val tok = new java.util.StringTokenizer(line, " ")
    var y = tok.nextToken.toDouble
    var x = new Array[Double](D)
    var i = 0
    while (i < D) {
      x(i) = tok.nextToken.toDouble; i += 1
    }
    DataPoint(new DenseVector(x), y)
  }

  def showWarning() {
    System.err.println(
      """WARN: This is a naive implementation of Logistic Regression and is given as an example!
        |Please use either org.apache.spark.mllib.classification.LogisticRegressionWithSGD(SGD随机梯度下降) or
        |org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS(BFGS是逆秩2拟牛顿法)
        |for more conventional use.
      """.stripMargin)
  }

  def main(args: Array[String]) {

    /*  if (args.length < 2) {
        System.err.println("Usage: SparkHdfsLR <file> <iters>")
        System.exit(1)
      }
  */
    showWarning()

    val sparkConf = new SparkConf().setAppName("SparkHdfsLR").setMaster("local[2]")
    val inputPath = "D:\\spark\\spark-1.5.0-hadoop2.6\\data\\mllib\\lr_data.txt"//args(0)
    val conf = new Configuration()
    val sc = new SparkContext(sparkConf,
      InputFormatInfo.computePreferredLocations(
        Seq(new InputFormatInfo(conf, classOf[org.apache.hadoop.mapred.TextInputFormat], inputPath))
      ))
    val lines = sc.textFile(inputPath)
    val points = lines.map(parsePoint _).cache()//缓存
    val ITERATIONS = 6 //args(1).toInt 迭代次数

    // Initialize w to a random value
    //初始化W到一个随机值
    var w = DenseVector.fill(D){2 * rand.nextDouble - 1}
    println("Initial w: " + w)

    for (i <- 1 to ITERATIONS) {
      println("On iteration " + i)
      val gradient = points.map { p =>
        //p代表DataPoint Vector
        p.x * (1 / (1 + exp(-p.y * (w.dot(p.x)))) - 1) * p.y
      }.reduce(_ + _)
      w -= gradient
    }

    println("Final w: " + w)
    sc.stop()
  }
}
// scalastyle:on println