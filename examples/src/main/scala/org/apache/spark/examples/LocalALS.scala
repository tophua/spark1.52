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

/**
  * Alternating least squares matrix factorization.
  * 交替最小二乘矩阵分解
  * This is an example implementation for learning how to use Spark. For more conventional use,
  * 这是一个学习如何使用Spark的例子实现,更多的常规使用
  * please refer to org.apache.spark.mllib.recommendation.ALS
  */
object LocalALS {

  // Parameters set through command line arguments
  //通过命令行参数设置的参数
  var M = 0 // Number of movies 电影数
  var U = 0 // Number of users  用户数
  var F = 0 // Number of features 特征数
  var ITERATIONS = 0 //迭代次数
  val LAMBDA = 0.01 // Regularization coefficient 正则化系数

  def generateR(): RealMatrix = {
    /**
      * Running with M=10, U=15, F=5, iters=5
      * Array2DRowRealMatrix{
      * {0.3346931778,0.4137105472,0.0775105307,0.6437870131,0.9366413242},
      * {0.0254047911,0.2934716021,0.6934367432,0.6470339077,0.0065122415},
      * {0.7911883829,0.771389478,0.6633419584,0.2108281304,0.4653953243},
      * {0.0116078174,0.7168143725,0.5784344438,0.842781445,0.8690837005},
      * {0.1032660647,0.8737876095,0.2731679305,0.29573928,0.6466522409},
      * {0.2984638302,0.0832722213,0.2090916925,0.8779297198,0.8443151732},
      * {0.9252667486,0.0886728305,0.6814333808,0.5740787662,0.7876919509},
      * {0.7471098667,0.4119719644,0.3690202518,0.6528854508,0.0659361356},
      * {0.0180739024,0.066982024,0.5829397236,0.7606157261,0.0202911592},
      * {0.3292455703,0.095303284,0.0449297776,0.2881946408,0.0209063863}}
      */
    val mh = randomMatrix(M, F)//10行,5列
    /**
      * Running with M=10, U=15, F=5, iters=5
      * Array2DRowRealMatrix{
      * {0.0494910255,0.4017862365,0.4356719302,0.3669989977,0.9079717037},
      * {0.5356786728,0.3113646898,0.6553990158,0.1571514636,0.981434355},
      * {0.4995591297,0.7975672806,0.8784744369,0.7745551581,0.966569987},
      * {0.6294914284,0.9450123316,0.050369834,0.9886350041,0.4377183739},
      * {0.0855174541,0.4323791888,0.2795630637,0.8291606295,0.1057639558},
      * {0.594590678,0.7998559703,0.7283983096,0.8861048851,0.4383024213},
      * {0.0737506968,0.982488169,0.3845232206,0.9107111695,0.9666048273},
      * {0.7808289531,0.7660572005,0.6108821077,0.7822751062,0.8564232487},
      * {0.9523315915,0.9780333576,0.2286152001,0.224912862,0.3390280741},
      * {0.0377513149,0.268945459,0.0390559442,0.1644797165,0.3936674695},
      * {0.1283241396,0.9888515439,0.0291757933,0.3952913592,0.7556757173},
      * {0.5613070703,0.3638149307,0.1871081164,0.856309131,0.3888833832},
      * {0.2226459841,0.6497133329,0.2040848086,0.8619406097,0.6391880058},
      * {0.0806722842,0.3184658737,0.1580474341,0.9230214049,0.8309540251},
      * {0.7489161181,0.8498054219,0.6991951628,0.0122053458,0.1475830496}}
      */
    val uh = randomMatrix(U, F)//15行,5列
    /*println(mh.toString())
      println(uh.toString())
      println(mh.multiply(uh.transpose()).toString())*/
    //相乘multiply
    /**
      * Array2DRowRealMatrix{
      {1.3032696823,1.3793272242,1.9692299928,1.6520077415,0.8620367573,1.5673678117,1.9526202209,1.9313938233,1.2034253715,0.6015430528,1.4165895065,1.2684075152,1.512726143,1.5435385662,0.8025155959},
      {0.6646548107,0.6675369212,1.3633789833,0.9707855726,0.8601066694,1.3311328542,1.1524042988,1.1799966835,0.617483062,0.2159573571,0.5743795175,0.807370428,0.8997159401,0.8077435705,0.7621258018},
      {1.1380293692,1.5886467643,2.2063445101,1.6725754529,0.810671519,1.9614106114,1.7131590169,2.1774359682,1.8647684263,0.4811254964,1.3186982564,1.2103778685,1.2919113981,0.9956486878,1.7831278733},
      {1.6389916362,1.5939070487,2.5784572897,1.9274582314,1.2633561167,2.1292950723,2.5351301759,2.3151302141,1.3285579249,0.6965639185,1.7170785176,1.435186757,1.8682934017,1.8207113368,1.1608330488},
      {1.1708761344,1.1875406149,1.8425639898,1.4799343637,0.7766141588,1.5047655982,1.8655315884,1.7020353139,1.3011361381,0.5527775352,1.4908302246,0.9316894448,1.3146947111,1.1400877025,1.1099292741},
      {1.2281379082,1.2894550851,1.8952919361,1.5146299294,0.9372262283,1.5443748282,1.7998860228,1.834444567,0.8971853168,0.5186104104,1.1118109013,1.3170676647,1.4596266935,1.5955785402,0.5758075023},
      {1.3041896538,1.8331512794,2.3375848736,1.612909695,0.8672832746,1.9713766873,1.7015928191,2.3303642142,1.5198391535,0.4899052254,1.04846774,1.477030184,1.4009940962,1.3950042363,1.3680135675},
      {0.6627481154,0.9376537474,1.5954034553,1.552532201,0.8935040475,1.6499614803,1.2600785693,1.6915922488,1.367978827,0.2867581288,0.8219241163,1.2229995268,1.1142101876,0.9072099183,1.1853355539},
      {0.5793464333,0.5520420776,1.1833009586,0.8648919271,0.8262943699,1.1718138297,1.0036106389,1.0339207034,0.393943416,0.1745579144,0.4015606852,0.8027999093,0.8350877861,0.8338472959,0.490324368},
      {0.1989104431,0.3012992949,0.5233882038,0.5936535677,0.3230948978,0.5692560204,0.4178632743,0.6008912466,0.4889389122,0.0954479505,0.2675211157,0.4828011988,0.4061642791,0.3473949399,0.3655841885}}
      */
    mh.multiply(uh.transpose())//transpose转置矩阵
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

  def updateMovie(i: Int, m: RealVector, us: Array[RealVector], R: RealMatrix) : RealVector = {
    var XtX: RealMatrix = new Array2DRowRealMatrix(F, F)
    var Xty: RealVector = new ArrayRealVector(F)
    // For each user that rated the movie
    //为每一个用户评价的电影
    for (j <- 0 until U) {
      val u = us(j)
      // Add u * u^t to XtX
      XtX = XtX.add(u.outerProduct(u))
      // Add u * rating to Xty
      Xty = Xty.add(u.mapMultiply(R.getEntry(i, j)))
    }
    // Add regularization coefficients to diagonal terms
    //向对角项的正则化系数
    for (d <- 0 until F) {
      XtX.addToEntry(d, d, LAMBDA * U)
    }
    // Solve it with Cholesky
    //Cholesky解决它
    new CholeskyDecomposition(XtX).getSolver.solve(Xty)
  }

  def updateUser(j: Int, u: RealVector, ms: Array[RealVector], R: RealMatrix) : RealVector = {
    var XtX: RealMatrix = new Array2DRowRealMatrix(F, F)
    var Xty: RealVector = new ArrayRealVector(F)
    // For each movie that the user rated
    //对于每一部电影的用户评分
    for (i <- 0 until M) {
      val m = ms(i)
      // Add m * m^t to XtX
      XtX = XtX.add(m.outerProduct(m))
      // Add m * rating to Xty
      Xty = Xty.add(m.mapMultiply(R.getEntry(i, j)))
    }
    // Add regularization coefficients to diagonal terms
    for (d <- 0 until F) {
      XtX.addToEntry(d, d, LAMBDA * M)
    }
    // Solve it with Cholesky
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

    M = 10
    U = 15
    F = 5
    ITERATIONS = 5

    /*args match {
      case Array(m, u, f, iters) => {
        M = m.toInt
        U = u.toInt
        F = f.toInt
        ITERATIONS = iters.toInt
      }
      case _ => {
        System.err.println("Usage: LocalALS <M> <U> <F> <iters>")
        System.exit(1)
      }
    }*/

    showWarning()
    //Running with M=10, U=15, F=5, iters=5
    println(s"Running with M=$M, U=$U, F=$F, iters=$ITERATIONS")

    val R = generateR()

    // Initialize m and u randomly
    var ms = Array.fill(M)(randomVector(F))
    var us = Array.fill(U)(randomVector(F))

    // Iteratively update movies then users
    //迭代更新电影然后用户
    for (iter <- 1 to ITERATIONS) {//迭代次数计算
      println(s"Iteration $iter:")
      ms = (0 until M).map(i => updateMovie(i, ms(i), us, R)).toArray
      us = (0 until U).map(j => updateUser(j, us(j), ms, R)).toArray
      //rmse均方根误差说明样本的离散程度
      println("RMSE = " + rmse(R, ms, us))
      println()
    }
  }

  private def randomVector(n: Int): RealVector =
  //org.apache.commons.math3.linear.ArrayRealVector
    new ArrayRealVector(Array.fill(n)(math.random))

  private def randomMatrix(rows: Int, cols: Int): RealMatrix =
  //org.apache.commons.math3.linear.Array2DRowRealMatrix
    new Array2DRowRealMatrix(Array.fill(rows, cols)(math.random))

}
// scalastyle:on println