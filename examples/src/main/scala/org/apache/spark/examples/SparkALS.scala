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

import org.apache.commons.math3.linear._

import org.apache.spark._

/**
  * Alternating least squares matrix factorization.
  * 交替最小二乘矩阵分解
  * This is an example implementation for learning how to use Spark. For more conventional use,
  * 这是学习如何使用Spark的示例实现,对于更传统的使用,
  * please refer to org.apache.spark.mllib.recommendation.ALS
  */
object SparkALS {

  // Parameters set through command line arguments
  //通过命令行参数设置的参数
  var M = 0 // Number of movies  电影数
  var U = 0 // Number of users   用户数
  var F = 0 // Number of features 特征数
  var ITERATIONS = 0
  val LAMBDA = 0.01 // Regularization coefficient 正则化系数

  def generateR(): RealMatrix = {
    val mh = randomMatrix(M, F)
    val uh = randomMatrix(U, F)
    mh.multiply(uh.transpose())
  }
  //rmse均方根误差说明样本的离散程度
  def rmse(targetR: RealMatrix, ms: Array[RealVector], us: Array[RealVector]): Double = {
    val r = new Array2DRowRealMatrix(M, U)
    for (i <- 0 until M; j <- 0 until U) {
      r.setEntry(i, j, ms(i).dotProduct(us(j)))
    }
    val diffs = r.subtract(targetR)
    var sumSqs = 0.0
    for (i <- 0 until M; j <- 0 until U) {
      val diff = diffs.getEntry(i, j)
      sumSqs += diff * diff
    }
    math.sqrt(sumSqs / (M.toDouble * U.toDouble))
  }

  def update(i: Int, m: RealVector, us: Array[RealVector], R: RealMatrix) : RealVector = {
    val U = us.size
    val F = us(0).getDimension
    var XtX: RealMatrix = new Array2DRowRealMatrix(F, F)
    var Xty: RealVector = new ArrayRealVector(F)
    // For each user that rated the movie
    //为每一个用户评价的电影
    for (j <- 0 until U) {
      val u = us(j)
      // Add u * u^t to XtX
      XtX = XtX.add(u.outerProduct(u))
      // Add u * rating to Xty
      //添加U *评级XTY
      Xty = Xty.add(u.mapMultiply(R.getEntry(i, j)))
    }
    // Add regularization coefs to diagonal terms
    //添加正则coefs对角项
    for (d <- 0 until F) {
      XtX.addToEntry(d, d, LAMBDA * U)
    }
    // Solve it with Cholesky
    //Cholesky解决它
    new CholeskyDecomposition(XtX).getSolver.solve(Xty)
  }

  def showWarning() {
    System.err.println(
      """WARN: This is a naive implementation of ALS and is given as an example!
        |Please use the ALS method found in org.apache.spark.mllib.recommendation
        |for more conventional use.
      """.stripMargin)
  }

  def main(args: Array[String]) {

    var slices = 0
   //Some 使用方式
    //把数组转换成Option方式及Option匹配方式
    val options = (0 to 4).map(i => if (i < args.length) Some(args(i)) else None)
    //Option匹配方式
    options.toArray match {
        //参数的value
      case Array(m, u, f, iters, slices_) =>
        M = m.getOrElse("10").toInt //
        U = u.getOrElse("50").toInt //
        F = f.getOrElse("5").toInt //
        ITERATIONS = iters.getOrElse("5").toInt//迭代次数5
        slices = slices_.getOrElse("2").toInt//分片数2
      case _ =>
        System.err.println("Usage: SparkALS [M] [U] [F] [iters] [slices]")
        System.exit(1)
    }

    showWarning()

    println(s"Running with M=$M, U=$U, F=$F, iters=$ITERATIONS")

    val sparkConf = new SparkConf().setAppName("SparkALS").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val R = generateR()

    // Initialize m and u randomly
    //随机初始化M和U
    var ms = Array.fill(M)(randomVector(F))
    var us = Array.fill(U)(randomVector(F))

    // Iteratively update movies then users
    //迭代更新电影然用户
    val Rc = sc.broadcast(R)
    var msb = sc.broadcast(ms)
    var usb = sc.broadcast(us)
    for (iter <- 1 to ITERATIONS) {
      println(s"Iteration $iter:")
      ms = sc.parallelize(0 until M, slices)
        .map(i => update(i, msb.value(i), usb.value, Rc.value))
        .collect()
      msb = sc.broadcast(ms) // Re-broadcast ms because it was updated
      us = sc.parallelize(0 until U, slices)
        .map(i => update(i, usb.value(i), msb.value, Rc.value.transpose()))
        .collect()
      usb = sc.broadcast(us) // Re-broadcast us because it was updated
      //rmse均方根误差说明样本的离散程度
      println("RMSE(均方根误差) = " + rmse(R, ms, us))
      println()
    }

    sc.stop()
  }

  private def randomVector(n: Int): RealVector =
    new ArrayRealVector(Array.fill(n)(math.random))

  private def randomMatrix(rows: Int, cols: Int): RealMatrix =
    new Array2DRowRealMatrix(Array.fill(rows, cols)(math.random))

}
// scalastyle:on println