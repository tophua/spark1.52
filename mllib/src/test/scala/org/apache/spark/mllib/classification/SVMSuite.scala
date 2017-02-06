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

package org.apache.spark.mllib.classification

import scala.collection.JavaConversions._
import scala.util.Random

import org.jblas.DoubleMatrix

import org.apache.spark.{ SparkException, SparkFunSuite }
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.util.{ LocalClusterSparkContext, MLlibTestSparkContext }
import org.apache.spark.util.Utils
/**
 * 线性支持向量机(SVM):对于大规模的分类任务来说,线性支持向量机是标准方法.
 * 
 */
object SVMSuite {

  def generateSVMInputAsList(
    intercept: Double,
    weights: Array[Double],
    nPoints: Int,
    seed: Int): java.util.List[LabeledPoint] = {
    seqAsJavaList(generateSVMInput(intercept, weights, nPoints, seed))
  }

  // Generate noisy input of the form Y = signum(x.dot(weights) + intercept + noise)
  //生成表格的y=signum(x.dot(权重)+intercept+噪声）
  def generateSVMInput(
    intercept: Double,
    weights: Array[Double],
    nPoints: Int,
    seed: Int): Seq[LabeledPoint] = {
    val rnd = new Random(seed)
    val weightsMat = new DoubleMatrix(1, weights.length, weights: _*)
    val x = Array.fill[Array[Double]](nPoints)(
      Array.fill[Double](weights.length)(rnd.nextDouble() * 2.0 - 1.0))
    val y = x.map { xi =>
      val yD = new DoubleMatrix(1, xi.length, xi: _*).dot(weightsMat) +
        intercept + 0.01 * rnd.nextGaussian()
      if (yD < 0) 0.0 else 1.0
    }
    //LabeledPoint标记点是局部向量,向量可以是密集型或者稀疏型,每个向量会关联了一个标签(label)
    y.zip(x).map(p => LabeledPoint(p._1, Vectors.dense(p._2)))
  }

  /** 
   *  Binary labels, 3 features
   *  二进制标签,3个特征 
   *  */
  private val binaryModel = new SVMModel(weights = Vectors.dense(0.1, 0.2, 0.3), intercept = 0.5)

}
/**
 * Llib提供两种线性方法用于分类：线性支持向量机(SVM)和逻辑回归。SVM只支持二分类,而逻辑回归既支持二分类又支持多分类
 */
class SVMSuite extends SparkFunSuite with MLlibTestSparkContext {

  def validatePrediction(predictions: Seq[Double], input: Seq[LabeledPoint]) {
    val numOffPredictions = predictions.zip(input).count {
      case (prediction, expected) =>
        prediction != expected.label
    }
    // At least 80% of the predictions should be on.
    //应该至少有80%的预测
    assert(numOffPredictions < input.length / 5)
  }

  test("SVM with threshold") {//支持向量机的门槛
    val nPoints = 10000

    // NOTE: Intercept should be small for generating equal 0s and 1s
    //拦截应该小产生等于0和1
    val A = 0.01
    val B = -1.5
    val C = 1.0

    val testData = SVMSuite.generateSVMInput(A, Array[Double](B, C), nPoints, 42)

    val testRDD = sc.parallelize(testData, 2)
    testRDD.cache()
   //(SGD随机梯度下降)
    val svm = new SVMWithSGD().setIntercept(true)
    //正则化参数>=0 //每次迭代优化步长
    svm.optimizer.setStepSize(1.0).setRegParam(1.0).setNumIterations(100)

    val model = svm.run(testRDD)

    val validationData = SVMSuite.generateSVMInput(A, Array[Double](B, C), nPoints, 17)
    val validationRDD = sc.parallelize(validationData, 2)

    // Test prediction on RDD.
    //在RDD试验预测
    var predictions = model.predict(validationRDD.map(_.features)).collect()
    assert(predictions.count(_ == 0.0) != predictions.length)

    // High threshold makes all the predictions 0.0
    //高门槛使所有的预测0.0
    //在二进制分类中设置阈值,范围为[0,1],如果类标签1的估计概率>Threshold,则预测1,否则0
    model.setThreshold(10000.0)
    predictions = model.predict(validationRDD.map(_.features)).collect()
    assert(predictions.count(_ == 0.0) == predictions.length)

    // Low threshold makes all the predictions 1.0
    //低门槛使所有的预测1
    model.setThreshold(-10000.0)
    predictions = model.predict(validationRDD.map(_.features)).collect()
    assert(predictions.count(_ == 1.0) == predictions.length)
  }

  test("SVM using local random SGD") {//支持向量机的局部随机梯度下降
    val nPoints = 10000

    // NOTE: Intercept should be small for generating equal 0s and 1s
    //拦截应该小产生等于0和1
    val A = 0.01
    val B = -1.5
    val C = 1.0

    val testData = SVMSuite.generateSVMInput(A, Array[Double](B, C), nPoints, 42)

    val testRDD = sc.parallelize(testData, 2)
    testRDD.cache()
   //(SGD随机梯度下降)
    val svm = new SVMWithSGD().setIntercept(true)
    //正则化参数>=0 //每次迭代优化步长
    svm.optimizer.setStepSize(1.0).setRegParam(1.0).setNumIterations(100)

    val model = svm.run(testRDD)

    val validationData = SVMSuite.generateSVMInput(A, Array[Double](B, C), nPoints, 17)
    val validationRDD = sc.parallelize(validationData, 2)

    // Test prediction on RDD.
    //在RDD测试预测
    validatePrediction(model.predict(validationRDD.map(_.features)).collect(), validationData)

    // Test prediction on Array.
    //在数组测试预测
    validatePrediction(validationData.map(row => model.predict(row.features)), validationData)
  }

  test("SVM local random SGD with initial weights") {//SVM的局部随机SGD与初始权值
    val nPoints = 10000

    // NOTE: Intercept should be small for generating equal 0s and 1s
    //拦截应该小产生等于0和1
    val A = 0.01
    val B = -1.5
    val C = 1.0

    val testData = SVMSuite.generateSVMInput(A, Array[Double](B, C), nPoints, 42)

    val initialB = -1.0
    val initialC = -1.0
    val initialWeights = Vectors.dense(initialB, initialC)

    val testRDD = sc.parallelize(testData, 2)
    testRDD.cache()

    val svm = new SVMWithSGD().setIntercept(true)
    //正则化参数>=0 每次迭代优化步长
    svm.optimizer.setStepSize(1.0).setRegParam(1.0).setNumIterations(100)

    val model = svm.run(testRDD, initialWeights)

    val validationData = SVMSuite.generateSVMInput(A, Array[Double](B, C), nPoints, 17)
    val validationRDD = sc.parallelize(validationData, 2)

    // Test prediction on RDD.
    //在RDD测试预测
    validatePrediction(model.predict(validationRDD.map(_.features)).collect(), validationData)

    // Test prediction on Array.
    //在数组测试预测
    validatePrediction(validationData.map(row => model.predict(row.features)), validationData)
  }

  test("SVM with invalid labels") {//具有无效标签的支持向量机
    val nPoints = 10000

    // NOTE: Intercept should be small for generating equal 0s and 1s
    //拦截应该小产生等于0和1
    val A = 0.01
    val B = -1.5
    val C = 1.0

    val testData = SVMSuite.generateSVMInput(A, Array[Double](B, C), nPoints, 42)
    val testRDD = sc.parallelize(testData, 2)

    val testRDDInvalid = testRDD.map { lp =>
      if (lp.label == 0.0) {
        LabeledPoint(-1.0, lp.features)
      } else {
        lp
      }
    }

    intercept[SparkException] {
    //(SGD随机梯度下降)
      SVMWithSGD.train(testRDDInvalid, 100)
    }

    // Turning off data validation should not throw an exception
    //关闭数据验证不应该抛出异常
    new SVMWithSGD().setValidateData(false).run(testRDDInvalid)
  }

  test("model save/load") {//模型保存/加载
    // NOTE: This will need to be generalized once there are multiple model format versions.
    //这将需要被推广一次有多个模型格式版本。
    val model = SVMSuite.binaryModel

    model.clearThreshold()
    assert(model.getThreshold.isEmpty)

    val tempDir = Utils.createTempDir()
    val path = tempDir.toURI.toString

    // Save model, load it back, and compare.
    //保存模型,加载它回来,并比较
    try {
      model.save(sc, path)
      val sameModel = SVMModel.load(sc, path)
      assert(model.weights == sameModel.weights)
      assert(model.intercept == sameModel.intercept)
      assert(sameModel.getThreshold.isEmpty)
    } finally {
      Utils.deleteRecursively(tempDir)
    }

    // Save model with threshold.
    //阈值保存模型
    try {
      model.setThreshold(0.7)
      model.save(sc, path)
      val sameModel2 = SVMModel.load(sc, path)
      //在二进制分类中设置阈值,范围为[0,1],如果类标签1的估计概率>Threshold,则预测1,否则0
      assert(model.getThreshold.get == sameModel2.getThreshold.get)
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }
}

class SVMClusterSuite extends SparkFunSuite with LocalClusterSparkContext {
  //在训练和预测中,任务的大小应该是小的
  test("task size should be small in both training and prediction") {
    val m = 4
    val n = 200000
    val points = sc.parallelize(0 until m, 2).mapPartitionsWithIndex { (idx, iter) =>
      val random = new Random(idx)
      //LabeledPoint标记点是局部向量,向量可以是密集型或者稀疏型,每个向量会关联了一个标签(label)
      iter.map(i => LabeledPoint(1.0, Vectors.dense(Array.fill(n)(random.nextDouble()))))
    }.cache()
    // If we serialize data directly in the task closure, the size of the serialized task would be
    // greater than 1MB and hence Spark would throw an error.
    //如果我们将数据直接在任务结束,该系列任务的规模将大于1MB,因此Spark会抛出一个错误。
    //(SGD随机梯度下降)
    val model = SVMWithSGD.train(points, 2)
    val predictions = model.predict(points.map(_.features))
  }
}
