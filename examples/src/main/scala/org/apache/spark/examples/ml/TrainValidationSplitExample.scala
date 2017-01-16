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

package org.apache.spark.examples.ml

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * A simple example demonstrating model selection using TrainValidationSplit.
 * 一个简单的例子演示模型选择使用训练验证分裂
 * 数据量小的时候可以用CrossValidator进行交叉验证,数据量大的时候可以直接用trainValidationSplit
 * The example is based on [[SimpleParamsExample]] using linear regression.
 * Run with
 * {{{
 * bin/run-example ml.TrainValidationSplitExample
 * }}}
 * Spark还提供训练验证分裂用以超参数调整。和交叉验证评估K次不同,训练验证分裂只对每组参数评估一次
 * 与交叉验证相同,确定最佳参数表后,训练验证分裂最后使用最佳参数表基于全部数据来重新拟合估计器
 */
object TrainValidationSplitExample {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TrainValidationSplitExample").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // Prepare training and test data.
    // 准备培训和测试数据
    val data = MLUtils.loadLibSVMFile(sc, "../data/mllib/sample_libsvm_data.txt").toDF()
   // 将数据随机分配为两份,一份用于训练,一份用于测试
    val Array(training, test) = data.randomSplit(Array(0.9, 0.1), seed = 12345)
   //线性回归
    val lr = new LinearRegression()

    // We use a ParamGridBuilder to construct a grid of parameters to search over.
    //我们用一个paramgridbuilder构建网格参数搜索
    // TrainValidationSplit will try all combinations of values and determine best model using
    // the evaluator.
    //trainvalidationsplit将尝试所有的组合和使用评估值确定最佳模型
    //ParamGridBuilder构建待选参数(如:logistic regression的regParam)
    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.1, 0.01))
      //是否训练拦截对象
      .addGrid(lr.fitIntercept, Array(true, false))
      //弹性网络混合参数,0.0为L2正则化 1.0为L1正则化
      .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
      .build()

    // In this case the estimator is simply the linear regression.
    //在这种情况下,估计是简单的线性回归
    // A TrainValidationSplit requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
    //一个trainvalidationsplit需要估计,一套估计parammaps,和评估
    val trainValidationSplit = new TrainValidationSplit()//来做模型超参数优化
      .setEstimator(lr)//评估器或适配器
      .setEvaluator(new RegressionEvaluator)//评估器或适配器
      .setEstimatorParamMaps(paramGrid)

    // 80% of the data will be used for training and the remaining 20% for validation.
    //80%的数据将用于训练和剩余的20%进行验证
    trainValidationSplit.setTrainRatio(0.8)//

    // Run train validation split, and choose the best set of parameters.
    //运行训练验证拆分,并选择最佳的参数集
    //fit()方法将DataFrame转化为一个Transformer的算法
    val model = trainValidationSplit.fit(training)

    // Make predictions on test data. model is the model with combination of parameters
    //对测试数据进行预测,模型是最优的参数组合的模型
    // that performed best.
    //transform()方法将DataFrame转化为另外一个DataFrame的算法
    /**
      +--------------------+-----+--------------------+
      |            features|label|          prediction|
      +--------------------+-----+--------------------+
      |(692,[154,155,156...|  1.0|  1.0638236485622758|
      |(692,[153,154,155...|  0.0| 0.07337831421003782|
      |(692,[156,157,158...|  1.0|  0.9377417246384833|
      |(692,[126,127,128...|  0.0|6.372102815044061E-4|
      |(692,[127,128,154...|  1.0|  0.6554640392935814|
      |(692,[124,125,126...|  0.0|-0.03226781932439382|
      +--------------------+-----+--------------------+*/
    model.transform(test)
      .select("features", "label", "prediction")
      .show(5)

    sc.stop()
  }
}
