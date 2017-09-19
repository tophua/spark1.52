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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{StreamingContext, TestSuiteBase}
/**
 * 用时间间隔的数据作为特征训练流式模型,这里的场景非常简单,只是为了说明问题。
 * 实际的产品模型需要结合前面讨论的按天和周末的模型来提高预测的准确性
 */
class StreamingLogisticRegressionSuite extends SparkFunSuite with TestSuiteBase {

  // use longer wait time to ensure job completion
  //使用更长的等待时间,以确保工作完成
  override def maxWaitTimeMillis: Int = 30000

  var ssc: StreamingContext = _

  override def afterFunction() {
    super.afterFunction()
    if (ssc != null) {
      ssc.stop()
    }
  }

  // Test if we can accurately(精确地) learn B for Y = logistic(BX) on streaming data
  test("parameter accuracy") {//参数的精度

    val nPoints = 100
    val B = 1.5

    // create model  
    //逻辑回归基于梯度下降,仅支持2分类,(SGD随机梯度下降)
    val model = new StreamingLogisticRegressionWithSGD()
    //初始化权重
      .setInitialWeights(Vectors.dense(0.0))
      .setStepSize(0.2)//每次迭代优化步长
      .setNumIterations(25)

    // generate sequence of simulated data
    //生成模拟数据序列
    val numBatches = 20
    val input = (0 until numBatches).map { i =>
      LogisticRegressionSuite.generateLogisticInput(0.0, B, nPoints, 42 * (i + 1))
    }

    // apply model training to input stream
    //将模型训练应用于输入流
    //LabeledPoint标记点是局部向量,向量可以是密集型或者稀疏型,每个向量会关联了一个标签(label)
    ssc = setupStreams(input, (inputDStream: DStream[LabeledPoint]) => {
      model.trainOn(inputDStream)
      inputDStream.count()
    })
    runStreams(ssc, numBatches, numBatches)

    // check accuracy of final parameter estimates
    //检查最终参数估计的准确性
    assert(model.latestModel().weights(0) ~== B relTol 0.1)

  }

  // Test that parameter estimates improve when learning Y = logistic(BX) on streaming data
  //测试时参数估计提高学习
  test("parameter convergence") {//参数收敛

    val B = 1.5
    val nPoints = 100

    // create model
    //逻辑回归基于梯度下降,仅支持2分类,SGD随机梯度下降
    val model = new StreamingLogisticRegressionWithSGD()
      //initialWeights初始取值,默认是0向量
      .setInitialWeights(Vectors.dense(0.0))
      //SGD步长,默认为1.0
      .setStepSize(0.2)//每次迭代优化步长
      //iterations迭代次数,默认100
      .setNumIterations(25)

    // generate sequence of simulated data
    //生成模拟数据序列
    val numBatches = 20
    val input = (0 until numBatches).map { i =>
      LogisticRegressionSuite.generateLogisticInput(0.0, B, nPoints, 42 * (i + 1))
    }

    // create buffer to store intermediate fits
    //创建缓冲区存储中间数据
    val history = new ArrayBuffer[Double](numBatches)

    // apply model training to input stream, storing the intermediate results
    //将模型训练应用到输入流中,存储中间结果
    // (we add a count to ensure the result is a DStream)    
    ssc = setupStreams(input, (inputDStream: DStream[LabeledPoint]) => {
      model.trainOn(inputDStream)
      //math.abs返回数的绝对值
      inputDStream.foreachRDD(x => history.append(math.abs(model.latestModel().weights(0) - B)))
      inputDStream.count()
    })
    runStreams(ssc, numBatches, numBatches)

    // compute change in error
    //计算误差的变化
    val deltas = history.drop(1).zip(history.dropRight(1))
    // check error stability (it always either shrinks, or increases with small tol)
    //检查错误的稳定性（它总是要么收缩,或增加小公差）
    assert(deltas.forall(x => (x._1 - x._2) <= 0.1))
    // check that error shrunk on at least 2 batches
    //检查至少2个批次的错误缩水
    assert(deltas.map(x => if ((x._1 - x._2) < 0) 1 else 0).sum > 1)
  }

  // Test predictions on a stream
  //测试在流上预测
  test("predictions") {

    val B = 1.5
    val nPoints = 100

    // create model initialized with true weights
    //逻辑回归基于梯度下降,仅支持2分类,(SGD随机梯度下降)
    val model = new StreamingLogisticRegressionWithSGD()
      //initialWeights–初始取值,默认是0向量
      .setInitialWeights(Vectors.dense(1.5))
      .setStepSize(0.2)//每次迭代优化步长
      //iterations迭代次数,默认100
      .setNumIterations(25)

    // generate sequence of simulated data for testing
    //测试生成序列的模拟数据
    val numBatches = 10
    val testInput = (0 until numBatches).map { i =>
      LogisticRegressionSuite.generateLogisticInput(0.0, B, nPoints, 42 * (i + 1))
    }

    // apply model predictions to test stream
    //将模型预测应用到测试流
    ssc = setupStreams(testInput, (inputDStream: DStream[LabeledPoint]) => {
      model.predictOnValues(inputDStream.map(x => (x.label, x.features)))
    })

    // collect the output as (true, estimated) tuples
    //收集输出(真的,估计)的元组
    val output: Seq[Seq[(Double, Double)]] = runStreams(ssc, numBatches, numBatches)

    // check that at least 60% of predictions are correct on all batches
    //所有批次检查至少有60%的预测是正确
    //math.abs返回数的绝对值
    val errors = output.map(batch => batch.map(p => math.abs(p._1 - p._2)).sum / nPoints)

    assert(errors.forall(x => x <= 0.4))
  }

  // Test training combined with prediction
  // 测试训练合并预测
  test("training and prediction") {
    // create model initialized with zero weights
   //逻辑回归基于梯度下降,仅支持2分类,(SGD随机梯度下降)
    val model = new StreamingLogisticRegressionWithSGD()
    //initialWeights–初始取值,默认是0向量
      .setInitialWeights(Vectors.dense(-0.1))
      .setStepSize(0.01)//每次迭代优化步长
      .setNumIterations(10)

    // generate sequence of simulated data for testing
    //生成测试的模拟数据序列
    val numBatches = 10
    val nPoints = 100
    val testInput = (0 until numBatches).map { i =>
      LogisticRegressionSuite.generateLogisticInput(0.0, 5.0, nPoints, 42 * (i + 1))
    }

    // train and predict 训练和预测
    //LabeledPoint标记点是局部向量,向量可以是密集型或者稀疏型,每个向量会关联了一个标签(label)
    ssc = setupStreams(testInput, (inputDStream: DStream[LabeledPoint]) => {
      model.trainOn(inputDStream)
      model.predictOnValues(inputDStream.map(x => (x.label, x.features)))
    })

    val output: Seq[Seq[(Double, Double)]] = runStreams(ssc, numBatches, numBatches)

    // assert that prediction error improves, ensuring that the updated model is being used
    //断言,预测误差的改善,确保更新后的模型被使用,math.abs返回数的绝对值
    val error = output.map(batch => batch.map(p => math.abs(p._1 - p._2)).sum / nPoints).toList
    assert(error.head > 0.8 & error.last < 0.2)
  }

  // Test empty RDDs in a stream
  //测试空的RDDS流
  test("handling empty RDDs in a stream") {//处理空的RDDS流
  //逻辑回归基于梯度下降,仅支持2分类,(SGD随机梯度下降)
    val model = new StreamingLogisticRegressionWithSGD()
    //initialWeights–初始取值,默认是0向量
      .setInitialWeights(Vectors.dense(-0.1))
      .setStepSize(0.01)//每次迭代优化步长
      .setNumIterations(10)
    val numBatches = 10
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
