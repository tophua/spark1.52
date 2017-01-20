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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.LinearDataGenerator
import org.apache.spark.streaming.{StreamingContext, TestSuiteBase}
import org.apache.spark.streaming.dstream.DStream
/**
 * 流式器学习线性回归套件
 */
class StreamingLinearRegressionSuite extends SparkFunSuite with TestSuiteBase {

  // use longer wait time to ensure job completion
  //使用更长的等待时间,以确保工作完成
  override def maxWaitTimeMillis: Int = 20000

  var ssc: StreamingContext = _

  override def afterFunction() {
    super.afterFunction()
    if (ssc != null) {
      ssc.stop()
    }
  }

  // Assert that two values are equal within tolerance epsilon
  //断言两值在公差允许的范围内,ε是平等
  def assertEqual(v1: Double, v2: Double, epsilon: Double) {
    def errorMessage = v1.toString + " did not equal " + v2.toString
    //math.abs绝对值,epsilon一定无限大小
    assert(math.abs(v1-v2) <= epsilon, errorMessage)
  }

  // Assert that model predictions are correct
  //断言模型预测是正确的
  def validatePrediction(predictions: Seq[Double], input: Seq[LabeledPoint]) {
    val numOffPredictions = predictions.zip(input).count { case (prediction, expected) =>
      // A prediction is off if the prediction is more than 0.5 away from expected value.
      //预测是关闭的,如果预测是超过0.5,从预期值
       //math.abs返回数的绝对值
      math.abs(prediction - expected.label) > 0.5
    }
    // At least 80% of the predictions should be on.
    //应该至少有80%的预测
    assert(numOffPredictions < input.length / 5)
  }

  // Test if we can accurately learn Y = 10*X1 + 10*X2 on streaming data
  test("parameter accuracy") {//参数的精度
    // create model,(SGD随机梯度下降)
    val model = new StreamingLinearRegressionWithSGD()
     //initialWeights–初始取值,默认是0向量
      .setInitialWeights(Vectors.dense(0.0, 0.0))
      .setStepSize(0.2)//每次迭代优化步长
      .setNumIterations(25)
      .setConvergenceTol(0.0001)

    // generate sequence of simulated data
      //生成模拟数据序列
    val numBatches = 10
    val input = (0 until numBatches).map { i =>
      LinearDataGenerator.generateLinearInput(0.0, Array(10.0, 10.0), 100, 42 * (i + 1))
    }

    // apply model training to input stream
    //将模型训练应用于输入流
    ssc = setupStreams(input, (inputDStream: DStream[LabeledPoint]) => {
      model.trainOn(inputDStream)
      inputDStream.count()
    })
    runStreams(ssc, numBatches, numBatches)

    // check accuracy of final parameter estimates
    //检查最终参数估计的准确性
    assertEqual(model.latestModel().intercept, 0.0, 0.1)
    assertEqual(model.latestModel().weights(0), 10.0, 0.1)
    assertEqual(model.latestModel().weights(1), 10.0, 0.1)

    // check accuracy of predictions
    //检查预测的准确性
    val validationData = LinearDataGenerator.generateLinearInput(0.0, Array(10.0, 10.0), 100, 17)
    validatePrediction(validationData.map(row => model.latestModel().predict(row.features)),
      validationData)
  }

  // Test that parameter estimates improve when learning Y = 10*X1 on streaming data
  //测试参数估计提高学习y = 10×x1数据流的时
  test("parameter convergence") {//参数收敛
    // create model (SGD随机梯度下降)
    val model = new StreamingLinearRegressionWithSGD()
     //initialWeights–初始取值,默认是0向量
      .setInitialWeights(Vectors.dense(0.0)).setStepSize(0.2).setNumIterations(25)

    // generate sequence of simulated data
    //生成模拟数据序列
    val numBatches = 10
    val input = (0 until numBatches).map { i =>
      LinearDataGenerator.generateLinearInput(0.0, Array(10.0), 100, 42 * (i + 1))
    }

    // create buffer to store intermediate fits
    //创建缓冲区以存储中间安装
    val history = new ArrayBuffer[Double](numBatches)

    // apply model training to input stream, storing the intermediate results
    //将模型训练应用到输入流中,存储中间结果
    // (we add a count to ensure the result is a DStream)
    ssc = setupStreams(input, (inputDStream: DStream[LabeledPoint]) => {
      model.trainOn(inputDStream)
       //math.abs返回数的绝对值
      inputDStream.foreachRDD(x => history.append(math.abs(model.latestModel().weights(0) - 10.0)))
      inputDStream.count()
    })
    runStreams(ssc, numBatches, numBatches)

    // compute change in error
    //计算误差的变化
    val deltas = history.drop(1).zip(history.dropRight(1))
    // check error stability (it always either shrinks, or increases with small tol)
    //检查错误的稳定性(它总是要么收缩，或增加小公差)
    assert(deltas.forall(x => (x._1 - x._2) <= 0.1))
    // check that error shrunk on at least 2 batches
    //检查至少2个批次的错误缩水
    assert(deltas.map(x => if ((x._1 - x._2) < 0) 1 else 0).sum > 1)
  }

  // Test predictions on a stream 在流上测试预测
  test("predictions") {//预测
    // create model initialized with true weights
    //创建真正的权重初始化模型(SGD随机梯度下降)
    val model = new StreamingLinearRegressionWithSGD()
     //initialWeights–初始取值,默认是0向量
      .setInitialWeights(Vectors.dense(10.0, 10.0))
      .setStepSize(0.2)//每次迭代优化步长
      .setNumIterations(25)

    // generate sequence of simulated data for testing
    //生成测试的模拟数据序列
    val numBatches = 10
    val nPoints = 100
    val testInput = (0 until numBatches).map { i =>
      LinearDataGenerator.generateLinearInput(0.0, Array(10.0, 10.0), nPoints, 42 * (i + 1))
    }

    // apply model predictions to test stream
    //将模型预测应用到测试流
    ssc = setupStreams(testInput, (inputDStream: DStream[LabeledPoint]) => {
      model.predictOnValues(inputDStream.map(x => (x.label, x.features)))
    })
    // collect the output as (true, estimated) tuples
    //收集输出(真的,估计)的元组
    val output: Seq[Seq[(Double, Double)]] = runStreams(ssc, numBatches, numBatches)

    // compute the mean absolute error and check that it's always less than 0.1
    //计算平均绝对误差,并检查它总是小于0.1
    val errors = output.map(batch => batch.map(p => math.abs(p._1 - p._2)).sum / nPoints)
    assert(errors.forall(x => x <= 0.1))
  }

  // Test training combined with prediction 测试训练结合预测
  test("training and prediction") {
    // create model initialized with zero weights
    //创建具有零权重的初始化模型(SGD随机梯度下降)
    val model = new StreamingLinearRegressionWithSGD()
     //initialWeights–初始取值,默认是0向量
      .setInitialWeights(Vectors.dense(0.0, 0.0))
      .setStepSize(0.2)//每次迭代优化步长
      .setNumIterations(25)

    // generate sequence of simulated data for testing
    //生成测试的模拟数据序列
    val numBatches = 10
    val nPoints = 100
    val testInput = (0 until numBatches).map { i =>
      LinearDataGenerator.generateLinearInput(0.0, Array(10.0, 10.0), nPoints, 42 * (i + 1))
    }

    // train and predict 训练和预测
    ssc = setupStreams(testInput, (inputDStream: DStream[LabeledPoint]) => {
      model.trainOn(inputDStream)
      model.predictOnValues(inputDStream.map(x => {
        /**
         *  5.117672680666696||||[-0.43560342773870375,0.9349906440170221]
            8.214024772532593||||[0.4551273600657362,0.36644694351969087]
            4.961775958770297||||[0.8090021580031235,-0.3121157071110545]
            -3.5835907407691754||||[-0.9718883630945336,0.6191882496201251]
            7.238079296773607||||[0.0429886073795116,0.670311110015402]
            -8.415689635517044||||[-0.38256108933468047,-0.4458430198517267]
            5.528566011825288||||[0.16692329718223786,0.37649213869502973]
            11.375642473813445||||[0.33109790358914726,0.8067445293443565]
         */
         println(x.label+"||||"+x.features)
        (x.label, x.features)
       
       
      }
      )
          )
    })

    val output: Seq[Seq[(Double, Double)]] = runStreams(ssc, numBatches, numBatches)

    // assert that prediction error improves, ensuring that the updated model is being used
    //断言预测错误提高,确保正在使用的更新模型
    val error = output.map(batch => batch.map(p => math.abs(p._1 - p._2)).sum / nPoints).toList
    assert((error.head - error.last) > 2)
  }

  // Test empty RDDs in a stream 测试空RDDSr的流
  test("handling empty RDDs in a stream") {
    //(SGD随机梯度下降)
    val model = new StreamingLinearRegressionWithSGD()
     //initialWeights–初始取值,默认是0向量
      .setInitialWeights(Vectors.dense(0.0, 0.0))
      .setStepSize(0.2)//每次迭代优化步长
      .setNumIterations(25)
    val numBatches = 10
    val nPoints = 100
    val emptyInput = Seq.empty[Seq[LabeledPoint]]
    ssc = setupStreams(emptyInput,
      (inputDStream: DStream[LabeledPoint]) => {
        model.trainOn(inputDStream)
        model.predictOnValues(inputDStream.map(x => (x.label, x.features)))
      }
    )
    val output: Seq[Seq[(Double, Double)]] = runStreams(ssc, numBatches, numBatches)
  }
}
