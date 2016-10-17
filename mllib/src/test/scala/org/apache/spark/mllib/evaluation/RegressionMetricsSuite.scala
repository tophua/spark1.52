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

package org.apache.spark.mllib.evaluation

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
/**
 * 线性回归评估指标测试
 */
class RegressionMetricsSuite extends SparkFunSuite with MLlibTestSparkContext {
  //无偏(包括拦截项)的预测回归度量
  test("regression metrics for unbiased (includes intercept term) predictor") {
    /* Verify results in R:
       preds = c(2.25, -0.25, 1.75, 7.75) //向量
       obs = c(3.0, -0.5, 2.0, 7.0) //向量

       SStot = sum((obs - mean(obs))^2)
       SSreg = sum((preds - mean(obs))^2)//除以矩阵长度为4
       SSerr = sum((obs - preds)^2)

       explainedVariance = SSreg / length(obs)
       explainedVariance //总体方差
       > [1] 8.796875
       //平均绝对误差
       meanAbsoluteError = mean(abs(preds - obs))
       meanAbsoluteError
       > [1] 0.5
       //均方误差
       meanSquaredError = mean((preds - obs)^2)
       meanSquaredError
       > [1] 0.3125
       rmse = sqrt(meanSquaredError) //sqrt平方根
       rmse
       > [1] 0.559017
       r2 = 1 - SSerr / SStot
       r2
       > [1] 0.9571734
     */
    val predictionAndObservations = sc.parallelize(Seq((2.25, 3.0), (-0.25, -0.5), (1.75, 2.0), (7.75, 7.0)), 2)
    //实例化一个RegressionMetrics对象需要一个键值对类型的RDD,其每一条记录对应每个数据点上相应的预测值与实际值
    val metrics = new RegressionMetrics(predictionAndObservations)
    //总体方差
    assert(metrics.explainedVariance ~== 8.79687 absTol 1E-5,
      "explained variance regression score mismatch")
      //这个指标用于评判预测值与实际值之间的差异度,
      //平均绝对误差
    assert(metrics.meanAbsoluteError ~== 0.5 absTol 1E-5, "mean absolute error mismatch")
    //meanSquaredError 均方差,它的定义为预测值的评级与真实评级的差的平方的和与总数目的商
    assert(metrics.meanSquaredError ~== 0.3125 absTol 1E-5, "mean squared error mismatch")
    //均方根误差,均方差再开平方根
    assert(metrics.rootMeanSquaredError ~== 0.55901 absTol 1E-5,
      "root mean squared error mismatch")//均方根误差不匹配
     //R2平方系统也称判定系数,用来评估模型拟合数据的好坏
    assert(metrics.r2 ~== 0.95717 absTol 1E-5, "r2 score mismatch")
  }
 //有偏(包括拦截项)的预测回归度量
  test("regression metrics for biased (no intercept term) predictor") {
    /* Verify results in R:
       preds = c(2.5, 0.0, 2.0, 8.0)
       obs = c(3.0, -0.5, 2.0, 7.0)

       SStot = sum((obs - mean(obs))^2)
       SSreg = sum((preds - mean(obs))^2)
       SSerr = sum((obs - preds)^2)

       explainedVariance = SSreg / length(obs) //解释方差
       explainedVariance
       > [1] 8.859375
       meanAbsoluteError = mean(abs(preds - obs))//平均绝对误差
       meanAbsoluteError
       > [1] 0.5
       meanSquaredError = mean((preds - obs)^2)//均方误差
       meanSquaredError
       > [1] 0.375
       rmse = sqrt(meanSquaredError)//平方根
       rmse
       > [1] 0.6123724
       r2 = 1 - SSerr / SStot
       r2
       > [1] 0.9486081
     */
    val predictionAndObservations = sc.parallelize(//预测和观察
      Seq((2.5, 3.0), (0.0, -0.5), (2.0, 2.0), (8.0, 7.0)), 2)
    val metrics = new RegressionMetrics(predictionAndObservations)
    assert(metrics.explainedVariance ~== 8.85937 absTol 1E-5,
      "explained variance regression score mismatch")//解释方差回归分数不匹配
    assert(metrics.meanAbsoluteError ~== 0.5 absTol 1E-5, "mean absolute error mismatch")//均绝对误差不匹配
    //均方误差不匹配
    assert(metrics.meanSquaredError ~== 0.375 absTol 1E-5, "mean squared error mismatch")
    assert(metrics.rootMeanSquaredError ~== 0.61237 absTol 1E-5,//均方根误差,均方差再开平方根
      "root mean squared error mismatch")//均方根误差不匹配
      //R2平方系统也称判定系数,用来评估模型拟合数据的好坏
    assert(metrics.r2 ~== 0.94860 absTol 1E-5, "r2 score mismatch")
  }

  test("regression metrics with complete fitting") {//完全拟合的回归度量
    val predictionAndObservations = sc.parallelize(
      Seq((3.0, 3.0), (0.0, 0.0), (2.0, 2.0), (8.0, 8.0)), 2)
      //回归模型评估
    val metrics = new RegressionMetrics(predictionAndObservations)
    //总体方差
    assert(metrics.explainedVariance ~== 8.6875 absTol 1E-5,
      "explained variance regression score mismatch") //解释方差回归分数不匹配
      //平均绝对误差
    assert(metrics.meanAbsoluteError ~== 0.0 absTol 1E-5, "mean absolute error mismatch")//平均绝对误差不匹配
    //平均误差
    assert(metrics.meanSquaredError ~== 0.0 absTol 1E-5, "mean squared error mismatch")//均方误差不匹配
    //平均根误差
    assert(metrics.rootMeanSquaredError ~== 0.0 absTol 1E-5,
      "root mean squared error mismatch")//平均根误差不匹配
     //R2平方系统也称判定系数,用来评估模型拟合数据的好坏
    assert(metrics.r2 ~== 1.0 absTol 1E-5, "r2 score mismatch")
  }
}
