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
/**
 * 逻辑回归 和线性回归:都可以做预测,
 * 逻辑回归用在二值预测,比如预测一个客户是否会流失,只有0-不流失,1-流失；
 * 线性回归用来进行连续值预测,比如预测投入一定的营销费用时会带来多少收益
 */
private object LinearRegressionSuite {

  /** 3 features */
  /**
   * weights:每一个特征的权重计算
   * intercept:此模型计算的拦截
   */
  val model = new LinearRegressionModel(weights = Vectors.dense(0.1, 0.2, 0.3), intercept = 0.5)
}

class LinearRegressionSuite extends SparkFunSuite with MLlibTestSparkContext {

  def validatePrediction(predictions: Seq[Double], input: Seq[LabeledPoint]) {
    val numOffPredictions = predictions.zip(input).count { case (prediction, expected) =>
      // A prediction is off if the prediction is more than 0.5 away from expected value.
      //预测是关闭的,如果预测是超过0.5,从预期值
      math.abs(prediction - expected.label) > 0.5
    }
    // At least 80% of the predictions should be on.
    //应该至少有80%的预测
    assert(numOffPredictions < input.length / 5)
  }

  // Test if we can correctly learn Y = 3 + 10*X1 + 10*X2
  //测试 如果我们能正确学习  y = 3 + 10 + 10 * * X1 X2
  test("linear regression") {
    val testRDD = sc.parallelize(LinearDataGenerator.generateLinearInput(
      3.0, Array(10.0, 10.0), 100, 42), 2).cache()
      println(testRDD.collect().toVector)
      /**
       * Vector((11.214024772532593,[0.4551273600657362,0.36644694351969087]), 
       *        (-5.415689635517044,[-0.38256108933468047,-0.4458430198517267]), 
       *        (14.375642473813445,[0.33109790358914726,0.8067445293443565]), 
       *        (-3.912182009031524,[-0.2624341731773887,-0.44850386111659524]), 
       *        (8.068693902377857,[-0.07269284838169332,0.5658035575800715]), 
       *        (9.989306160850216,[0.8386555657374337,-0.1270180511534269]), 
       *        (5.758664490318773,[0.499812362510895,-0.22686625128130267]), 
       *        (6.979381010458447,[0.22684501241708888,0.1726291966578266]), 
       *        (6.099186673441009,[-0.6778773666126594,0.9993906921393696]), 
       *        (10.465315437042944,[0.1789490173139363,0.5584053824232391]), 
       *        (-5.245508439024772,[0.03495894704368174,-0.8505720014852347]))
       */
    val linReg = new LinearRegressionWithSGD().setIntercept(true)//是否给数据加上一个干扰特征或者偏差特征--也就是一个值始终未1的特征
    //每次迭代优化步长
    linReg.optimizer.setNumIterations(1000).setStepSize(1.0)
    //运行模型
    val model = linReg.run(testRDD)
    //
    assert(model.intercept >= 2.5 && model.intercept <= 3.5)//3.062070032411828
    //权重
    val weights = model.weights //[9.735526941802604,9.700906954001237]
    assert(weights.size === 2)
    assert(weights(0) >= 9.0 && weights(0) <= 11.0)
    assert(weights(1) >= 9.0 && weights(1) <= 11.0)

    val validationData = LinearDataGenerator.generateLinearInput(
      3.0, Array(10.0, 10.0), 100, 17)  
    val validationRDD = sc.parallelize(validationData, 2).cache()
    // Test prediction on RDD. 测试在RDD预测
    validatePrediction(model.predict(validationRDD.map(_.features)).collect(), validationData)
    // Test prediction on Array.测试在数据上预测
    validatePrediction(validationData.map(row => model.predict(row.features)), validationData)
  }

  // Test if we can correctly learn Y = 10*X1 + 10*X2
  //测试如果能正常的学习Y = 10*X1 + 10*X2
  test("linear regression without intercept") {//无拦截的线性回归
    val testRDD = sc.parallelize(LinearDataGenerator.generateLinearInput(
      0.0, Array(10.0, 10.0), 100, 42), 2).cache()
    val linReg = new LinearRegressionWithSGD().setIntercept(false)
    //每次迭代优化步长
    linReg.optimizer.setNumIterations(1000).setStepSize(1.0)

    val model = linReg.run(testRDD)

    assert(model.intercept === 0.0)

    val weights = model.weights
    assert(weights.size === 2)
    assert(weights(0) >= 9.0 && weights(0) <= 11.0)
    assert(weights(1) >= 9.0 && weights(1) <= 11.0)

    val validationData = LinearDataGenerator.generateLinearInput(
      0.0, Array(10.0, 10.0), 100, 17)
    val validationRDD = sc.parallelize(validationData, 2).cache()

    // Test prediction on RDD.
    //测试预测在RDD上
    validatePrediction(model.predict(validationRDD.map(_.features)).collect(), validationData)

    // Test prediction on Array.
     //测试预测在数组上
    validatePrediction(validationData.map(row => model.predict(row.features)), validationData)
  }

  // Test if we can correctly learn Y = 10*X1 + 10*X10000
  //测试 如果我们能正确学习  y = Y = 10*X1 + 10*X10000
  test("sparse linear regression without intercept") {//无拦截的稀疏线性回归
    val denseRDD = sc.parallelize(
      LinearDataGenerator.generateLinearInput(0.0, Array(10.0, 10.0), 100, 42), 2)
    val sparseRDD = denseRDD.map { case LabeledPoint(label, v) =>
      val sv = Vectors.sparse(10000, Seq((0, v(0)), (9999, v(1))))
      LabeledPoint(label, sv)
    }.cache()
    val linReg = new LinearRegressionWithSGD().setIntercept(false)
    //每次迭代优化步长
    linReg.optimizer.setNumIterations(1000).setStepSize(1.0)

    val model = linReg.run(sparseRDD)

    assert(model.intercept === 0.0)

    val weights = model.weights
    assert(weights.size === 10000)
    assert(weights(0) >= 9.0 && weights(0) <= 11.0)
    assert(weights(9999) >= 9.0 && weights(9999) <= 11.0)

    val validationData = LinearDataGenerator.generateLinearInput(0.0, Array(10.0, 10.0), 100, 17)
    val sparseValidationData = validationData.map { case LabeledPoint(label, v) =>
      val sv = Vectors.sparse(10000, Seq((0, v(0)), (9999, v(1))))
      LabeledPoint(label, sv)
    }
    val sparseValidationRDD = sc.parallelize(sparseValidationData, 2)

      // Test prediction on RDD.
    //测试预测在RDD上
    validatePrediction(
      model.predict(sparseValidationRDD.map(_.features)).collect(), sparseValidationData)

    // Test prediction on Array.
    //测试预测在数组上
    validatePrediction(
      sparseValidationData.map(row => model.predict(row.features)), sparseValidationData)
  }

  test("model save/load") {//模型保存/加载
    val model = LinearRegressionSuite.model

    val tempDir = Utils.createTempDir()
    val path = tempDir.toURI.toString

    // Save model, load it back, and compare.
    //保存模型,加载它回来,并比较
    try {
      model.save(sc, path)
      val sameModel = LinearRegressionModel.load(sc, path)
      assert(model.weights == sameModel.weights)
      assert(model.intercept == sameModel.intercept)
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }
}

class LinearRegressionClusterSuite extends SparkFunSuite with LocalClusterSparkContext {
  //在训练和预测中，任务的大小应该是小的
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
    val model = LinearRegressionWithSGD.train(points, 2)
    val predictions = model.predict(points.map(_.features))
  }
}
