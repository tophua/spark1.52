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

package org.apache.spark.ml.evaluation

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.util.{LinearDataGenerator, MLlibTestSparkContext}
import org.apache.spark.mllib.util.TestingUtils._

class RegressionEvaluatorSuite extends SparkFunSuite with MLlibTestSparkContext {

  test("params") {
    ParamsSuite.checkParams(new RegressionEvaluator)
  }

  test("Regression Evaluator: default params") {//评估回归:默认参数
    /**
     * Here is the instruction describing how to export the test data into CSV format
     * so we can validate the metrics compared with R's mmetric package.
     * 这里是指令描述如何导出到CSV格式的测试数据,所以我们可以验证指标R的对称方案相比
     * import org.apache.spark.mllib.util.LinearDataGenerator
     * val data = sc.parallelize(LinearDataGenerator.generateLinearInput(6.3,
     *   Array(4.7, 7.2), Array(0.9, -1.3), Array(0.7, 1.2), 100, 42, 0.1))
     * data.map(x=> x.label + ", " + x.features(0) + ", " + x.features(1))
     *   .saveAsTextFile("path")
     */
    /**
     * ArraySeq((9.27417626363617,[1.5595422042211196,-0.6047158101224187]), 
     * 				  (-7.657930975818883,[0.3456163126018512,-2.1459276529714737]), 
     * 					(14.443277316316447,[1.379806446078384,0.23069012156522706]), 
     * 					(-6.547255888143898,[0.5196967525411738,-2.1509762443049625])
     * 					(6.7118847606735486,[1.628279808680599,-1.030420831884505]))
     */
    val generateLinear=LinearDataGenerator.generateLinearInput(
        6.3, Array(4.7, 7.2), Array(0.9, -1.3), Array(0.7, 1.2), 100, 42, 0.1)
        
    val dataset = sqlContext.createDataFrame(
      sc.parallelize(generateLinear,2))

    /**
     * Using the following R code to load the data, train the model and evaluate metrics.
     * 使用下面的R代码加载数据,训练模型和评估指标
     * > library("glmnet")
     * > library("rminer")
     * > data <- read.csv("path", header=FALSE, stringsAsFactors=FALSE)
     * > features <- as.matrix(data.frame(as.numeric(data$V2), as.numeric(data$V3)))
     * > label <- as.numeric(data$V1)
     * > model <- glmnet(features, label, family="gaussian", alpha = 0, lambda = 0)
     * > rmse <- mmetric(label, predict(model, features), metric='RMSE')
     * > mae <- mmetric(label, predict(model, features), metric='MAE')
     * > r2 <- mmetric(label, predict(model, features), metric='R2')
     */
    val trainer = new LinearRegression
    val model = trainer.fit(dataset) //转换
    //Prediction 预测
    val predictions = model.transform(dataset)
    predictions.collect()

    // default = rmse
    //默认rmse均方根误差说明样本的离散程度
    val evaluator = new RegressionEvaluator()
    println("==MetricName="+evaluator.getMetricName+"=LabelCol="+evaluator.getLabelCol+"=PredictionCol="+evaluator.getPredictionCol)
    //==MetricName=rmse=LabelCol=label=PredictionCol=prediction,默认rmse均方根误差说明样本的离散程度
    assert(evaluator.evaluate(predictions) ~== 0.1019382 absTol 0.001)

    // r2 score 评分
    //R2平方系统也称判定系数,用来评估模型拟合数据的好坏
    evaluator.setMetricName("r2")
    assert(evaluator.evaluate(predictions) ~== 0.9998196 absTol 0.001)

    //MAE平均绝对误差是所有单个观测值与算术平均值的偏差的绝对值的平均
    evaluator.setMetricName("mae")
    assert(evaluator.evaluate(predictions) ~== 0.08036075 absTol 0.001)
  }
}
