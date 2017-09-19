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

import scala.collection.JavaConversions._
import scala.util.Random

import com.github.fommil.netlib.BLAS.{getInstance => blas}

import org.apache.spark.SparkContext
import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
 * :: DeveloperApi ::
 * Generate sample data used for Linear Data. This class generates
 * uniformly random values for every feature and adds Gaussian noise with mean `eps` to the
 * response variable `Y`.
 * 生成用于线性数据的样本数据,这个类产生均匀的随机值,并增加了高斯噪声的均值“EPS”的响应变量
 */
@DeveloperApi
@Since("0.8.0")
object LinearDataGenerator {

  /**
   * Return a Java List of synthetic data randomly generated according to a multi
   * collinear model.
   * 用于生成线性回归的训练样本数据
   * @param intercept Data intercept 数据拦截(截取)
   * @param weights  Weights to be applied.应用的权重
   * @param nPoints Number of points in sample.样本点数
   * @param seed Random seed 随机种子
   * @return Java List of input.
   */
  @Since("0.8.0")
  def generateLinearInputAsList(
      intercept: Double,
      weights: Array[Double],
      nPoints: Int,
      seed: Int,
      eps: Double): java.util.List[LabeledPoint] = {
    seqAsJavaList(generateLinearInput(intercept, weights, nPoints, seed, eps))
  }

  /**
   * For compatibility(兼容性), the generated data without specifying the mean and variance
   * will have zero mean and variance of (1.0/3.0) since the original output range is
   * [-1, 1] with uniform distribution, and the variance of uniform distribution
   * is (b - a)^2^ / 12 which will be (1.0/3.0)
   * 没有指定的平均值和方差的生成的数据将有零均值和方差(1/3),因为原来的输出范围是[1,1]均匀分布,
   * 而均匀分布的方差为(b - a)^2^ / 12这将是(1.0/3.0)
   * @param intercept Data intercept 数据拦截(截取)
   * @param weights  Weights to be applied. 应用的权重
   * @param nPoints Number of points in sample.样本点数
   * @param seed Random seed 随机种子
   * @param eps Epsilon scaling factor. 乘以ε作为比例系数
   * @return Seq of input.
   */
  @Since("0.8.0")
  def generateLinearInput(
      intercept: Double,
      weights: Array[Double],
      nPoints: Int,
      seed: Int,
      eps: Double = 0.1): Seq[LabeledPoint] = {
    generateLinearInput(intercept, weights,
      Array.fill[Double](weights.length)(0.0),
      Array.fill[Double](weights.length)(1.0 / 3.0),
      nPoints, seed, eps)}

  /**
   *
   * @param intercept Data intercept 数据拦截(截取)
   * @param weights  Weights to be applied.应用的权重
   * @param xMean the mean of the generated features. Lots of time, if the features are not properly
   *              standardized, the algorithm with poor implementation will have difficulty
   *              to converge.
   *              生成特征的平均值
   * @param xVariance the variance of the generated features. 生成特征的方差
   * @param nPoints Number of points in sample.样本点数
   * @param seed Random seed 随机种子
   * @param eps Epsilon scaling factor. 乘以ε作为比例系数
   * @return Seq of input. 序列输入
   */
  @Since("0.8.0")
  def generateLinearInput(
      intercept: Double,
      weights: Array[Double],
      xMean: Array[Double],
      xVariance: Array[Double],
      nPoints: Int,
      seed: Int,
      eps: Double): Seq[LabeledPoint] = {

    val rnd = new Random(seed)
    val x = Array.fill[Array[Double]](nPoints)(
      Array.fill[Double](weights.length)(rnd.nextDouble()))

    x.foreach { v =>
      var i = 0
      val len = v.length
      while (i < len) {
        v(i) = (v(i) - 0.5) * math.sqrt(12.0 * xVariance(i)) + xMean(i)
        i += 1
      }
    }

    val y = x.map { xi =>
      blas.ddot(weights.length, xi, 1, weights, 1) + intercept + eps * rnd.nextGaussian()
    }
    y.zip(x).map(p => LabeledPoint(p._1, Vectors.dense(p._2)))
  }

  /**
   * Generate an RDD containing sample data for Linear Regression models - including Ridge, Lasso,
   * and uregularized variants.
   * 生成一个RDD线性回归模型包含示例数据,包括Ridge,Lasso
   * @param sc SparkContext to be used for generating the RDD. sparkcontext用于生成RDD
   * @param nexamples Number of examples that will be contained in the RDD.将包含在RDD数的例子
   * @param nfeatures Number of features to generate for each example.为每个示例生成的功能的数目
   * @param eps Epsilon factor by which examples are scaled.
   * @param nparts Number of partitions in the RDD. Default value is 2.在RDD的分区数,默认值为2
   *
   * @return RDD of LabeledPoint containing sample data.RDD的标记点包含示例数据
   */
  @Since("0.8.0")
  def generateLinearRDD(
      sc: SparkContext,
      nexamples: Int,
      nfeatures: Int,
      eps: Double,
      nparts: Int = 2,
      intercept: Double = 0.0) : RDD[LabeledPoint] = {
    val random = new Random(42)
    // Random values distributed uniformly in [-0.5, 0.5]
    //均匀分布的随机值 [-0.5, 0.5]
    val w = Array.fill(nfeatures)(random.nextDouble() - 0.5)

    val data: RDD[LabeledPoint] = sc.parallelize(0 until nparts, nparts).flatMap { p =>
      val seed = 42 + p
      val examplesInPartition = nexamples / nparts
      generateLinearInput(intercept, w.toArray, examplesInPartition, seed, eps)
    }
    data
  }

  @Since("0.8.0")
  def main(args: Array[String]) {
    if (args.length < 2) {
      // scalastyle:off println
      println("Usage: LinearDataGenerator " +
        "<master> <output_dir> [num_examples] [num_features] [num_partitions]")
      // scalastyle:on println
      System.exit(1)
    }

    val sparkMaster: String = args(0)
    val outputPath: String = args(1)
    val nexamples: Int = if (args.length > 2) args(2).toInt else 1000
    val nfeatures: Int = if (args.length > 3) args(3).toInt else 100
    val parts: Int = if (args.length > 4) args(4).toInt else 2
    val eps = 10

    val sc = new SparkContext(sparkMaster, "LinearDataGenerator")
    val data = generateLinearRDD(sc, nexamples, nfeatures, eps, nparts = parts)

    data.saveAsTextFile(outputPath)

    sc.stop()
  }
}
