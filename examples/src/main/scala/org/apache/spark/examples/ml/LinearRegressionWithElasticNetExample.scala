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

// scalastyle:off println
package org.apache.spark.examples.ml

// $example on$
import org.apache.spark.ml.regression.LinearRegression
// $example off$
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SQLContext, DataFrame}
/**
 * 线性回归网络神经例子
 */
object LinearRegressionWithElasticNetExample {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LinearRegressionWithElasticNetExample").setMaster("local[4]")
    val sc = new SparkContext(conf)
  
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    // $example on$
    // Load training data
   /**
   *  libSVM的数据格式
   *  <label> <index1>:<value1> <index2>:<value2> ...
   *  其中<label>是训练数据集的目标值,对于分类,它是标识某类的整数(支持多个类);对于回归,是任意实数
   *  <index>是以1开始的整数,可以是不连续
   *  <value>为实数,也就是我们常说的自变量
   */
    //val training = sqlContext.read.format("libsvm")
     // .load("../data/mllib/sample_linear_regression_data.txt")
    import org.apache.spark.mllib.util.MLUtils
      val dataSVM=MLUtils.loadLibSVMFile(sc, "../data/mllib/sample_linear_regression_data.txt")
      val training = sqlContext.createDataFrame(dataSVM)
      
    val lr = new LinearRegression()
      .setMaxIter(10)//设置最大迭代次数
      .setRegParam(0.3)//设置正则化参数
      .setElasticNetParam(0.8)//ElasticNetParam=0.0为L2正则化 1.0为L1正则化

    // Fit the model
    //fit()方法将DataFrame转化为一个Transformer的算法
    val lrModel = lr.fit(training)

    // Print the coefficients and intercept for linear regression
    //println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    // Summarize the model over the training set and print out some metrics
    // 总结模型在训练集和打印出一些指标
    val trainingSummary = lrModel.summary
    //numIterations: 7
    println(s"numIterations: ${trainingSummary.totalIterations}")
    /**objectiveHistory: 
     * List(0.4999999999999999, 0.49676713143380885, 0.4936412643073059, 0.4936402529428255, 
     *			0.4936402206251438, 0.49364021983363127, 0.4936402198299474)
     */
    println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")
    /**
     *+-------------------+
      |          residuals|
      +-------------------+
      | -9.888202329181972|
      | 0.5528444687772243|
      | -5.204451996117621|
      |-20.566894539277463|
      | -9.449046287343903|
      +-------------------+
     */
    trainingSummary.residuals.show()
    //rmse均方根误差说明样本的离散程度
    //RMSE: 10.189126225286143
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    //R2平方系统也称判定系数,用来评估模型拟合数据的好坏
    //r2: 0.02285205756871944
    println(s"r2: ${trainingSummary.r2}")
    // $example off$

    sc.stop()
  }
}
// scalastyle:on println
