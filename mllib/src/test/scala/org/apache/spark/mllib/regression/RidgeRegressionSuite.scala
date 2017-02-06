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

import scala.util.Random

import org.jblas.DoubleMatrix

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.{LocalClusterSparkContext, LinearDataGenerator,
  MLlibTestSparkContext}
import org.apache.spark.util.Utils

private object RidgeRegressionSuite {

  /** 
   *  3 features 
   *  3特点
   *  */
  val model = new RidgeRegressionModel(weights = Vectors.dense(0.1, 0.2, 0.3), intercept = 0.5)
}
/**
 * 岭回归数值计算方法的“稳定性”是指在计算过程中舍入误差是可以控制的
 * 岭回归而不会把预测因子系数设为0,但它会让他们近似于0.
 */
class RidgeRegressionSuite extends SparkFunSuite with MLlibTestSparkContext {

  def predictionError(predictions: Seq[Double], input: Seq[LabeledPoint]): Double = {
    predictions.zip(input).map { case (prediction, expected) =>
      (prediction - expected.label) * (prediction - expected.label)
    }.reduceLeft(_ + _) / predictions.size
  }

  test("ridge regression can help avoid overfitting") {//岭回归分析可以帮助避免过拟合

    // For small number of examples and large variance of error distribution,
    //少量的例子和大方差的误差分布
    // ridge regression should give smaller generalization error that linear regression.
    //岭回归应该给出较小的泛化误差,线性回归

    val numExamples = 50
    val numFeatures = 20

    org.jblas.util.Random.seed(42)
    // Pick weights as random values distributed uniformly in [-0.5, 0.5]
    //选择权重为随机值分布的均匀分布在[ 0.5,0.5 ]
    val w = DoubleMatrix.rand(numFeatures, 1).subi(0.5)

    // Use half of data for training and other half for validation
    //使用一半的数据进行培训和其他一半的验证
    val data = LinearDataGenerator.generateLinearInput(3.0, w.toArray, 2 * numExamples, 42, 10.0)
    val testData = data.take(numExamples)
    val validationData = data.takeRight(numExamples)

    val testRDD = sc.parallelize(testData, 2).cache()
    val validationRDD = sc.parallelize(validationData, 2).cache()

    // First run without regularization.
    //无正则化第一次运行,(SGD随机梯度下降)
    val linearReg = new LinearRegressionWithSGD()
    //每次迭代优化步长
    linearReg.optimizer.setNumIterations(200).setStepSize(1.0)
    val linearModel = linearReg.run(testRDD)
    val linearErr = predictionError(
        linearModel.predict(validationRDD.map(_.features)).collect(), validationData)
    //(SGD随机梯度下降)
    val ridgeReg = new RidgeRegressionWithSGD()
    ridgeReg.optimizer.setNumIterations(200)
                      .setRegParam(0.1)//正则化参数>=0
                      .setStepSize(1.0)//每次迭代优化步长
    val ridgeModel = ridgeReg.run(testRDD)
    val ridgeErr = predictionError(
        ridgeModel.predict(validationRDD.map(_.features)).collect(), validationData)

    // Ridge validation error should be lower than linear regression.
    //岭验证误差应低于线性回归
    assert(ridgeErr < linearErr,
      "ridgeError (" + ridgeErr + ") was not less than linearError(" + linearErr + ")")
  }

  test("model save/load") {
    val model = RidgeRegressionSuite.model

    val tempDir = Utils.createTempDir()
    val path = tempDir.toURI.toString

    // Save model, load it back, and compare.
    //保存模型,加载它回来,并比较
    try {
      model.save(sc, path)
      val sameModel = RidgeRegressionModel.load(sc, path)
      assert(model.weights == sameModel.weights)
      assert(model.intercept == sameModel.intercept)
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }
}

class RidgeRegressionClusterSuite extends SparkFunSuite with LocalClusterSparkContext {
   //在训练和预测中,任务的大小应该是小
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
    //如果我们将数据直接在任务结束,该系列任务的规模将大于1MB,因此Spark会抛出一个错误
    //(SGD随机梯度下降)
    val model = RidgeRegressionWithSGD.train(points, 2)
    val predictions = model.predict(points.map(_.features))
  }
}
