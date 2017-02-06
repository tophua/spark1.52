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

package org.apache.spark.mllib.optimization

import scala.util.Random

import org.scalatest.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.{LocalClusterSparkContext, MLlibTestSparkContext}
import org.apache.spark.mllib.util.TestingUtils._
/**
 * 有限内存BFGS
 */
class LBFGSSuite extends SparkFunSuite with MLlibTestSparkContext with Matchers {

  val nPoints = 10000
  val A = 2.0
  val B = -1.5

  val initialB = -1.0
  val initialWeights = Array(initialB)

  val gradient = new LogisticGradient()
  val numCorrections = 10
  val miniBatchFrac = 1.0

  val simpleUpdater = new SimpleUpdater()
  val squaredL2Updater = new SquaredL2Updater()

  // Add an extra variable consisting of all 1.0's for the intercept.
  //添加一个额外的变量组成的所有1个的拦截
  val testData = GradientDescentSuite.generateGDInput(A, B, nPoints, 42)
  //LabeledPoint标记点是局部向量,向量可以是密集型或者稀疏型,每个向量会关联了一个标签(label)
  val data = testData.map { case LabeledPoint(label, features) =>
    label -> Vectors.dense(1.0 +: features.toArray)
  }

  lazy val dataRDD = sc.parallelize(data, 2).cache()
  //损失应该减少比赛LBFGS梯度下降的结果
  test("LBFGS loss should be decreasing and match the result of Gradient Descent.") {
    val regParam = 0//正则化参数>=0

    val initialWeightsWithIntercept = Vectors.dense(1.0 +: initialWeights.toArray)
    val convergenceTol = 1e-12
    val numIterations = 10

    val (_, loss) = LBFGS.runLBFGS(
      dataRDD,
      gradient,
      simpleUpdater,
      numCorrections,
      convergenceTol,
      numIterations,
      regParam,//正则化参数>=0
      initialWeightsWithIntercept)

    // Since the cost function is convex, the loss is guaranteed to be monotonically decreasing
    // with L-BFGS optimizer.
    // (SGD doesn't guarantee this, and the loss will be fluctuating in the optimization process.)
    assert((loss, loss.tail).zipped.forall(_ > _), "loss should be monotonically decreasing.")

    val stepSize = 1.0//每次迭代优化步长
    // Well, GD converges slower, so it requires more iterations!
    val numGDIterations = 50//(SGD随机梯度下降)
    val (_, lossGD) = GradientDescent.runMiniBatchSGD(
      dataRDD,
      gradient,
      simpleUpdater,
      stepSize,//每次迭代优化步长
      numGDIterations,
      regParam,//正则化参数>=0
      miniBatchFrac,
      initialWeightsWithIntercept)

    // GD converges a way slower than L-BFGS. To achieve 1% difference,
    // it requires 90 iterations in GD. No matter how hard we increase
    // the number of iterations in GD here, the lossGD will be always
    // larger than lossLBFGS. This is based on observation, no theoretically guaranteed
     //math.abs返回数的绝对值
    assert(math.abs((lossGD.last - loss.last) / loss.last) < 0.02,
      "LBFGS should match GD result within 2% difference.")
  }

  test("LBFGS and Gradient Descent with L2 regularization should get the same result.") {
    val regParam = 0.2//正则化参数>=0

    // Prepare another non-zero weights to compare the loss in the first iteration.
    //准备另一个非零的权重,以比较第一次迭代中的损失
    val initialWeightsWithIntercept = Vectors.dense(0.3, 0.12)
    val convergenceTol = 1e-12
    val numIterations = 10
     //基于lbfgs优化损失函数,支持多分类(BFGS是逆秩2拟牛顿法)
    val (weightLBFGS, lossLBFGS) = LBFGS.runLBFGS(
      dataRDD,
      gradient,
      squaredL2Updater,
      numCorrections,
      convergenceTol,
      numIterations,
      regParam,//正则化参数>=0
      initialWeightsWithIntercept)

    val numGDIterations = 50
    val stepSize = 1.0//每次迭代优化步长
    val (weightGD, lossGD) = GradientDescent.runMiniBatchSGD(//(SGD随机梯度下降)
      dataRDD,
      gradient,
      squaredL2Updater,
      stepSize,//每次迭代优化步长
      numGDIterations,
      regParam,//正则化参数>=0
      miniBatchFrac,
      initialWeightsWithIntercept,
      convergenceTol)

    assert(lossGD(0) ~= lossLBFGS(0) absTol 1E-5,
      "The first losses of LBFGS and GD should be the same.")

    // The 2% difference here is based on observation, but is not theoretically guaranteed.
    assert(lossGD.last ~= lossLBFGS.last relTol 0.02,
      "The last losses of LBFGS and GD should be within 2% difference.")

    assert(
      (weightLBFGS(0) ~= weightGD(0) relTol 0.02) && (weightLBFGS(1) ~= weightGD(1) relTol 0.02),
      "The weight differences between LBFGS and GD should be within 2%.")
  }

  test("The convergence criteria should work as we expect.") {//收敛准则应该像我们期望的那样工作
    val regParam = 0.0//正则化参数>=0

    /**
     * For the first run, we set the convergenceTol to 0.0, so that the algorithm will
     * run up to the numIterations which is 8 here.
     */
    val initialWeightsWithIntercept = Vectors.dense(0.0, 0.0)
    val numIterations = 8
    var convergenceTol = 0.0

    val (_, lossLBFGS1) = LBFGS.runLBFGS(
      dataRDD,
      gradient,
      squaredL2Updater,
      numCorrections,
      convergenceTol,
      numIterations,
      regParam,//正则化参数>=0
      initialWeightsWithIntercept)

    // Note that the first loss is computed with initial weights,
    // so the total numbers of loss will be numbers of iterations + 1
    assert(lossLBFGS1.length == 9)

    convergenceTol = 0.1
    val (_, lossLBFGS2) = LBFGS.runLBFGS(
      dataRDD,
      gradient,
      squaredL2Updater,
      numCorrections,
      convergenceTol,
      numIterations,
      regParam,//正则化参数>=0
      initialWeightsWithIntercept)

    // Based on observation, lossLBFGS2 runs 3 iterations, no theoretically guaranteed.
    assert(lossLBFGS2.length == 4)
    assert((lossLBFGS2(2) - lossLBFGS2(3)) / lossLBFGS2(2) < convergenceTol)

    convergenceTol = 0.01
    val (_, lossLBFGS3) = LBFGS.runLBFGS(
      dataRDD,
      gradient,
      squaredL2Updater,
      numCorrections,
      convergenceTol,
      numIterations,
      regParam,//正则化参数>=0
      initialWeightsWithIntercept)

    // With smaller convergenceTol, it takes more steps.
    assert(lossLBFGS3.length > lossLBFGS2.length)

    // Based on observation, lossLBFGS2 runs 5 iterations, no theoretically guaranteed.
    assert(lossLBFGS3.length == 6)
    assert((lossLBFGS3(4) - lossLBFGS3(5)) / lossLBFGS3(4) < convergenceTol)
  }

  test("Optimize via class LBFGS.") {
    val regParam = 0.2//正则化参数>=0

    // Prepare another non-zero weights to compare the loss in the first iteration.
    val initialWeightsWithIntercept = Vectors.dense(0.3, 0.12)
    val convergenceTol = 1e-12
    val numIterations = 10

    val lbfgsOptimizer = new LBFGS(gradient, squaredL2Updater)
      .setNumCorrections(numCorrections)
      .setConvergenceTol(convergenceTol)
      .setNumIterations(numIterations)
      .setRegParam(regParam)//正则化参数>=0
     //基于lbfgs优化损失函数,支持多分类(BFGS是逆秩2拟牛顿法)
    val weightLBFGS = lbfgsOptimizer.optimize(dataRDD, initialWeightsWithIntercept)

    val numGDIterations = 50
    val stepSize = 1.0//每次迭代优化步长
    val (weightGD, _) = GradientDescent.runMiniBatchSGD(//(SGD随机梯度下降)
      dataRDD,
      gradient,
      squaredL2Updater,
      stepSize,//每次迭代优化步长
      numGDIterations,
      regParam,//正则化参数>=0
      miniBatchFrac,
      initialWeightsWithIntercept,
      convergenceTol)

    // for class LBFGS and the optimize method, we only look at the weights
    assert(
      (weightLBFGS(0) ~= weightGD(0) relTol 0.02) && (weightLBFGS(1) ~= weightGD(1) relTol 0.02),
      "The weight differences between LBFGS and GD should be within 2%.")
  }
}

class LBFGSClusterSuite extends SparkFunSuite with LocalClusterSparkContext {

  test("task size should be small") {
    val m = 10
    val n = 200000
    val examples = sc.parallelize(0 until m, 2).mapPartitionsWithIndex { (idx, iter) =>
      val random = new Random(idx)
      iter.map(i => (1.0, Vectors.dense(Array.fill(n)(random.nextDouble))))
    }.cache()
    //基于lbfgs优化损失函数,支持多分类(BFGS是逆秩2拟牛顿法)
    val lbfgs = new LBFGS(new LogisticGradient, new SquaredL2Updater)
      .setNumCorrections(1)
      .setConvergenceTol(1e-12)
      .setNumIterations(1)
      .setRegParam(1.0)//正则化参数>=0
    val random = new Random(0)
    // If we serialize data directly in the task closure, the size of the serialized task would be
    // greater than 1MB and hence Spark would throw an error.
    val weights = lbfgs.optimize(examples, Vectors.dense(Array.fill(n)(random.nextDouble)))
  }
}
