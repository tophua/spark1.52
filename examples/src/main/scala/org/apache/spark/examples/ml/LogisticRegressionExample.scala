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

import scala.collection.mutable
import scala.language.reflectiveCalls

import scopt.OptionParser

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.examples.mllib.AbstractParams
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.DataFrame

/**
 * An example runner for logistic regression with elastic-net (mixing L1/L2) regularization.
 * 运行逻辑回归的例子,弹性网络（混合L1或L2）正则化
 * Run with
 * {{{
 * bin/run-example ml.LogisticRegressionExample [options]
 * }}} 
 * A synthetic dataset can be found at `data/mllib/sample_libsvm_data.txt` which can be
 * trained by
 * {{{
 * bin/run-example ml.LogisticRegressionExample --regParam 0.3 --elasticNetParam 0.8 \
 *   data/mllib/sample_libsvm_data.txt
 * }}}
 * If you use it as a template to create your own app, please use `spark-submit` to submit your app.
 */
object LogisticRegressionExample {

  case class Params(
      input: String = null,
      testInput: String = "",
      /**
 *  libSVM的数据格式
 *  <label> <index1>:<value1> <index2>:<value2> ...
 *  其中<label>是训练数据集的目标值,对于分类,它是标识某类的整数(支持多个类);对于回归,是任意实数
 *  <index>是以1开始的整数,可以是不连续
 *  <value>为实数,也就是我们常说的自变量
 */
      dataFormat: String = "libsvm",
      regParam: Double = 0.0,//正则化参数(>=0)
      elasticNetParam: Double = 0.0,//弹性网络混合参数,范围[0,1]
      maxIter: Int = 100,//最多迭代次数(>=0)
      fitIntercept: Boolean = true,//是否训练拦截对象
      tol: Double = 1E-6,//迭代算法的收敛性
      fracTest: Double = 0.2) extends AbstractParams[Params]

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("LogisticRegressionExample") {
      head("LogisticRegressionExample: an example Logistic Regression with Elastic-Net app.")
      opt[Double]("regParam")
        .text(s"regularization parameter, default: ${defaultParams.regParam}")
        .action((x, c) => c.copy(regParam = x))
      opt[Double]("elasticNetParam")
        .text(s"ElasticNet mixing parameter. For alpha = 0, the penalty is an L2 penalty. " +
        s"For alpha = 1, it is an L1 penalty. For 0 < alpha < 1, the penalty is a combination of " +
        s"L1 and L2, default: ${defaultParams.elasticNetParam}")
        .action((x, c) => c.copy(elasticNetParam = x))
      opt[Int]("maxIter")
        .text(s"maximum number of iterations, default: ${defaultParams.maxIter}")
        .action((x, c) => c.copy(maxIter = x))
      opt[Boolean]("fitIntercept")
        .text(s"whether to fit an intercept term, default: ${defaultParams.fitIntercept}")
        .action((x, c) => c.copy(fitIntercept = x))
      opt[Double]("tol")
        .text(s"the convergence tolerance of iterations, Smaller value will lead " +
        s"to higher accuracy with the cost of more iterations, default: ${defaultParams.tol}")
        .action((x, c) => c.copy(tol = x))
      opt[Double]("fracTest")
        .text(s"fraction of data to hold out for testing.  If given option testInput, " +
        s"this option is ignored. default: ${defaultParams.fracTest}")
        .action((x, c) => c.copy(fracTest = x))
      opt[String]("testInput")
        .text(s"input path to test dataset.  If given, option fracTest is ignored." +
        s" default: ${defaultParams.testInput}")
        .action((x, c) => c.copy(testInput = x))
      opt[String]("dataFormat")
      /**
 *  libSVM的数据格式
 *  <label> <index1>:<value1> <index2>:<value2> ...
 *  其中<label>是训练数据集的目标值,对于分类,它是标识某类的整数(支持多个类);对于回归,是任意实数
 *  <index>是以1开始的整数,可以是不连续
 *  <value>为实数,也就是我们常说的自变量
 */
        .text("data format: libsvm (default), dense (deprecated in Spark v1.1)")
        .action((x, c) => c.copy(dataFormat = x))
      arg[String]("<input>")
        .text("input path to labeled examples")
        .required()
        .action((x, c) => c.copy(input = x))
      checkConfig { params =>
        if (params.fracTest < 0 || params.fracTest >= 1) {
          failure(s"fracTest ${params.fracTest} value incorrect; should be in [0,1).")
        } else {
          success
        }
      }
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    }.getOrElse {
      sys.exit(1)
    }
  }

  def run(params: Params) {
    val conf = new SparkConf().setAppName(s"LogisticRegressionExample with $params")
    val sc = new SparkContext(conf)

    println(s"LogisticRegressionExample with parameters:\n$params")

    // Load training and test data and cache it.
    //加载训练和测试数据并将其缓存
    val (training: DataFrame, test: DataFrame) = DecisionTreeExample.loadDatasets(sc, params.input,
      params.dataFormat, params.testInput, "classification", params.fracTest)

    // Set up Pipeline
    //建立管道
     //将特征转换,特征聚合,模型等组成一个管道,并调用它的fit方法拟合出模型
     //一个 Pipeline 在结构上会包含一个或多个 PipelineStage,每一个 PipelineStage 都会完成一个任务
    val stages = new mutable.ArrayBuffer[PipelineStage]()

    val labelIndexer = new StringIndexer()
      .setInputCol("labelString")
      .setOutputCol("indexedLabel")
    stages += labelIndexer

    val lor = new LogisticRegression()
      .setFeaturesCol("features")//特征列名
      .setLabelCol("indexedLabel")//标签列名
      .setRegParam(params.regParam)//正则化参数(>=0)
      .setElasticNetParam(params.elasticNetParam)//弹性网络混合参数,范围[0,1]
      .setMaxIter(params.maxIter)//最多迭代次数(>=0)
      .setTol(params.tol)//迭代算法的收敛性
      .setFitIntercept(params.fitIntercept)//是否训练拦截对象

    stages += lor
     //PipeLine:将多个DataFrame和Estimator算法串成一个特定的ML Wolkflow
     //一个 Pipeline在结构上会包含一个或多个 PipelineStage,每一个 PipelineStage 都会完成一个任务
    val pipeline = new Pipeline().setStages(stages.toArray)

    // Fit the Pipeline 安装管道
     // 系统计时器的当前值,以毫微秒为单位
    val startTime = System.nanoTime()
    //fit()方法将DataFrame转化为一个Transformer的算法
    val pipelineModel = pipeline.fit(training)
    //1e9就为1*(10的九次方),也就是十亿
    val elapsedTime = (System.nanoTime() - startTime) / 1e9
    println(s"Training time: $elapsedTime seconds")

    val lorModel = pipelineModel.stages.last.asInstanceOf[LogisticRegressionModel]
    // Print the weights and intercept for logistic regression.
    //打印逻辑回归的权重和截取
    println(s"Weights: ${lorModel.weights} Intercept: ${lorModel.intercept}")

    println("Training data results:")
    DecisionTreeExample.evaluateClassificationModel(pipelineModel, training, "indexedLabel")
    println("Test data results:")
    DecisionTreeExample.evaluateClassificationModel(pipelineModel, test, "indexedLabel")

    sc.stop()
  }
}
// scalastyle:on println
