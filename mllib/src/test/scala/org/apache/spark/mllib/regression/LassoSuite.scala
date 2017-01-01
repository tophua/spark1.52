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

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.{LocalClusterSparkContext, LinearDataGenerator,
  MLlibTestSparkContext}
import org.apache.spark.util.Utils

private object LassoSuite {

  /** 3 features 三个特征 */
  val model = new LassoModel(weights = Vectors.dense(0.1, 0.2, 0.3), intercept = 0.5)
}
/**
 * Lasso(套索)算法是线性回归中一种收缩和选择方法,它可最小化通常的平方差之和
 * Lasso(套索)算法主要特征:做任意它认为没有用的预测因子,它会把它们的相关系数设为0从而把它们从方程式中删除
 */
class LassoSuite extends SparkFunSuite with MLlibTestSparkContext {

  def validatePrediction(predictions: Seq[Double], input: Seq[LabeledPoint]) {
    val numOffPredictions = predictions.zip(input).count { case (prediction, expected) =>
      // A prediction is off if the prediction is more than 0.5 away from expected value.
      //预测是关闭的,如果预测是超过0.5,从预期值
      math.abs(prediction - expected.label) > 0.5
    }
    // At least 80% of the predictions should be on.
    //至少有80%的预测应该
    assert(numOffPredictions < input.length / 5)
  }
//本地岭回归随机梯度
  test("Lasso local random SGD") {
    val nPoints = 1000

    val A = 2.0
    val B = -1.5
    val C = 1.0e-2

    val testData = LinearDataGenerator.generateLinearInput(A, Array[Double](B, C), nPoints, 42)
      .map { case LabeledPoint(label, features) =>
      LabeledPoint(label, Vectors.dense(1.0 +: features.toArray))
    }
    val testRDD = sc.parallelize(testData, 2).cache()

    val ls = new LassoWithSGD()
    ls.optimizer.setStepSize(1.0).setRegParam(0.01).setNumIterations(40)

    val model = ls.run(testRDD)
    val weight0 = model.weights(0)
    val weight1 = model.weights(1)
    val weight2 = model.weights(2)
    assert(weight0 >= 1.9 && weight0 <= 2.1, weight0 + " not in [1.9, 2.1]")
    assert(weight1 >= -1.60 && weight1 <= -1.40, weight1 + " not in [-1.6, -1.4]")
    assert(weight2 >= -1.0e-3 && weight2 <= 1.0e-3, weight2 + " not in [-0.001, 0.001]")

    val validationData = LinearDataGenerator
      .generateLinearInput(A, Array[Double](B, C), nPoints, 17)
      .map { case LabeledPoint(label, features) =>
      LabeledPoint(label, Vectors.dense(1.0 +: features.toArray))
    }
    val validationRDD = sc.parallelize(validationData, 2)

    // Test prediction on RDD.
    //测试在RDD上预测
    validatePrediction(model.predict(validationRDD.map(_.features)).collect(), validationData)

    // Test prediction on Array.
    //测试在数组上预测
    validatePrediction(validationData.map(row => model.predict(row.features)), validationData)
  }
  //岭回归局部随机SGD与初始权值
  test("Lasso local random SGD with initial weights") {
    val nPoints = 1000

    val A = 2.0
    val B = -1.5
    val C = 1.0e-2

    val testData = LinearDataGenerator.generateLinearInput(A, Array[Double](B, C), nPoints, 42)
      .map { case LabeledPoint(label, features) =>
      LabeledPoint(label, Vectors.dense(1.0 +: features.toArray))
    }

    val initialA = -1.0
    val initialB = -1.0
    val initialC = -1.0
    val initialWeights = Vectors.dense(initialA, initialB, initialC)

    val testRDD = sc.parallelize(testData, 2).cache()

    val ls = new LassoWithSGD()
    ls.optimizer.setStepSize(1.0).setRegParam(0.01).setNumIterations(40).setConvergenceTol(0.0005)

    val model = ls.run(testRDD, initialWeights)
    val weight0 = model.weights(0)
    val weight1 = model.weights(1)
    val weight2 = model.weights(2)
    assert(weight0 >= 1.9 && weight0 <= 2.1, weight0 + " not in [1.9, 2.1]")
    assert(weight1 >= -1.60 && weight1 <= -1.40, weight1 + " not in [-1.6, -1.4]")
    assert(weight2 >= -1.0e-3 && weight2 <= 1.0e-3, weight2 + " not in [-0.001, 0.001]")

    val validationData = LinearDataGenerator
      .generateLinearInput(A, Array[Double](B, C), nPoints, 17)
      .map { case LabeledPoint(label, features) =>
      LabeledPoint(label, Vectors.dense(1.0 +: features.toArray))
    }
    val validationRDD = sc.parallelize(validationData, 2)

    // Test prediction on RDD,测试在RDD预测
    validatePrediction(model.predict(validationRDD.map(_.features)).collect(), validationData)

    // Test prediction on Array.测试数组的RDD预测
    validatePrediction(validationData.map(row => model.predict(row.features)), validationData)
  }

  test("model save/load") {//模型保存/加载
    val model = LassoSuite.model

    val tempDir = Utils.createTempDir()
    val path = tempDir.toURI.toString

    // Save model, load it back, and compare.
    //保存模型,加载它回来,并比较
    try {
      model.save(sc, path)
      val sameModel = LassoModel.load(sc, path)
      assert(model.weights == sameModel.weights)
      assert(model.intercept == sameModel.intercept)
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }
}

class LassoClusterSuite extends SparkFunSuite with LocalClusterSparkContext {
  //在训练和预测中,任务的大小应该是小的
  test("task size should be small in both training and prediction") {
    val m = 4
    val n = 200000
    val points = sc.parallelize(0 until m, 2).mapPartitionsWithIndex { (idx, iter) =>
      val random = new Random(idx)
      iter.map(i => LabeledPoint(1.0, Vectors.dense(Array.fill(n)(random.nextDouble()))))
    }.cache()
    // If we serialize data directly in the task closure, the size of the serialized task would be
    //如果我们将数据直接在任务结束,该系列任务的规模将大于1MB,因此Spark会抛出一个错误
    // greater than 1MB and hence Spark would throw an error.
    val model = LassoWithSGD.train(points, 2)
    val predictions = model.predict(points.map(_.features))
  }
}
