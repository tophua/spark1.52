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

package org.apache.spark.ml.classification

import scala.annotation.varargs
import scala.reflect.runtime.universe

import org.apache.spark.SparkFunSuite
import org.apache.spark.annotation.Experimental
import org.apache.spark.annotation.Since
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.MLTestingUtils
import org.apache.spark.mllib.classification.LogisticRegressionSuite.generateLogisticInput
import org.apache.spark.mllib.classification.LogisticRegressionSuite.generateMultinomialLogisticInput
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils.DoubleWithAlmostEquals
import org.apache.spark.mllib.util.TestingUtils.VectorWithAlmostEquals
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.SQLUserDefinedType

class LogisticRegressionSuite extends SparkFunSuite with MLlibTestSparkContext {

  @transient var dataset: DataFrame = _
  @transient var binaryDataset: DataFrame = _
  private val eps: Double = 1e-5

  override def beforeAll(): Unit = {
    super.beforeAll()

     dataset = sqlContext.createDataFrame(generateLogisticInput(1.0, 1.0, nPoints = 100, seed = 42))

    /*
       Here is the instruction describing how to export the test data into CSV format
                 这里是指令描述如何测试数据导出为CSV格式
       so we can validate the training accuracy compared with R's glmnet package.
			 所以我们可以验证训练精度与R的glmnet包相比
       import org.apache.spark.mllib.classification.LogisticRegressionSuite
       val nPoints = 10000
       val weights = Array(-0.57997, 0.912083, -0.371077, -0.819866, 2.688191)
       val xMean = Array(5.843, 3.057, 3.758, 1.199)
       val xVariance = Array(0.6856, 0.1899, 3.116, 0.581)
       val data = sc.parallelize(LogisticRegressionSuite.generateMultinomialLogisticInput(
         weights, xMean, xVariance, true, nPoints, 42), 1)
       data.map(x=> x.label + ", " + x.features(0) + ", " + x.features(1) + ", "
         + x.features(2) + ", " + x.features(3)).saveAsTextFile("path")
     */
    binaryDataset = {
      val nPoints = 10000
      val weights = Array(-0.57997, 0.912083, -0.371077, -0.819866, 2.688191)
      val xMean = Array(5.843, 3.057, 3.758, 1.199)
      val xVariance = Array(0.6856, 0.1899, 3.116, 0.581)

      val testData = generateMultinomialLogisticInput(weights, xMean, xVariance, true, nPoints, 42)

      sqlContext.createDataFrame(
        generateMultinomialLogisticInput(weights, xMean, xVariance, true, nPoints, 42))
    }
  }

  test("params") {//参数
    ParamsSuite.checkParams(new LogisticRegression)
    val model = new LogisticRegressionModel("logReg", Vectors.dense(0.0), 0.0)
    ParamsSuite.checkParams(model)
  }

  test("logistic regression: default params") {//逻辑回归:默认参数
    val lr = new LogisticRegression
    //标签列名
    assert(lr.getLabelCol === "label")
    //特征值
    assert(lr.getFeaturesCol === "features")
    //Prediction 算法预测结果的存储列的名称, 默认是”prediction”
    assert(lr.getPredictionCol === "prediction")
    //原始的算法预测结果的存储列的名称
    assert(lr.getRawPredictionCol === "rawPrediction")
    //类别预测结果的条件概率值存储列的名称
    assert(lr.getProbabilityCol === "probability")
    assert(lr.getFitIntercept)//是否训练拦截对象
    assert(lr.getStandardization)//标准化
    //fit()方法将DataFrame转化为一个Transformer的算法
    val model = lr.fit(dataset)
     /**
      * dd: org.apache.spark.sql.DataFrame = 
      * [label: double, features: vector, rawPrediction: vector, probability: vector, prediction: double]
      */
      //transform()方法将DataFrame转化为另外一个DataFrame的算法
    val dd=model.transform(dataset).select("label", "features","rawPrediction", "probability", "prediction").collect().foreach { 
       case Row(label: Double,features:Vector,rawPrediction:Vector, probability: Vector,prediction: Double) =>
        //文档ID,text文本,probability概率,prediction 预测分类
       /**
      label=1.0, features=[1.1419053154730547],rawPrediction=[-2.7045709478814164,2.7045709478814164],probability=[0.06270417307424182,0.9372958269257582],prediction=1.0
      label=0.0, features=[0.9194079489827879],rawPrediction=[-2.3705881157542494,2.3705881157542494],probability=[0.08544317131357694,0.914556828686423],prediction=1.0
      label=0.0, features=[-0.9498666368908959],rawPrediction=[0.4353130509040084,-0.4353130509040084],probability=[0.6071416596853667,0.3928583403146333],prediction=0.0
      label=1.0, features=[-0.19094451307087512],rawPrediction=[-0.7038777821185604,0.7038777821185604],probability=[0.3309530350546704,0.6690469649453297],prediction=1.0
       label=1.0, features=[0.14595005405372477],rawPrediction=[-1.2095781570551412,1.2095781570551412],probability=[0.22977569971269843,0.7702243002873016],prediction=1.0
      label=0.0, features=[-0.9946630124621867],rawPrediction=[0.5025552867036236,-0.5025552867036236],probability=[0.6230596448869219,0.3769403551130781],prediction=0.0
 			*/
        println(s"label=$label,prediction=$prediction,features=$features,rawPrediction=$rawPrediction,probability=$probability")
      }
    //在二进制分类中设置阈值,范围为[0，1],如果类标签1的估计概率>Threshold,则预测1,否则0
    assert(model.getThreshold === 0.5)
    //特征列名
    assert(model.getFeaturesCol === "features")//特征
    //Prediction 算法预测结果的存储列的名称, 默认是”prediction”
    assert(model.getPredictionCol === "prediction")
    //算法预测结果的存储列的名称, 默认是”prediction”
    assert(model.getRawPredictionCol === "rawPrediction")//原预测
    //可能性列名
    assert(model.getProbabilityCol === "probability")//可能性
    //拦截值
    assert(model.intercept !== 0.0)//拦截
    assert(model.hasParent)
  }

  test("setThreshold, getThreshold") {//设置和获得阈值
    val lr = new LogisticRegression
    //在二进制分类中设置阈值,范围为[0，1],如果类标签1的估计概率>Threshold,则预测1,否则0
    assert(lr.getThreshold === 0.5, "LogisticRegression.threshold should default to 0.5")
    //逻辑回归不应有默认设置的阈值
    withClue("LogisticRegression should not have thresholds set by default.") {
      //注意:异常类型可能在特征可能发生变化
      intercept[java.util.NoSuchElementException] { // Note: The exception type may change in future
        lr.getThresholds
      }
    }
    // Set via threshold. 设置阈值
    // Intuition: Large threshold or large thresholds(1) makes class 0 more likely.
    //凭直觉:大的阈值或大阈值1可能0适合
    lr.setThreshold(1.0)
    //在二进制分类中设置阈值,范围为[0，1],如果类标签1的估计概率>Threshold,则预测1,否则0
    assert(lr.getThresholds === Array(0.0, 1.0))
    lr.setThreshold(0.0)
    //在二进制分类中设置阈值,范围为[0，1],如果类标签1的估计概率>Threshold,则预测1,否则0
    assert(lr.getThresholds === Array(1.0, 0.0))
    lr.setThreshold(0.5)
    assert(lr.getThresholds === Array(0.5, 0.5))
    // Set via thresholds
    //通过设置阈值
    val lr2 = new LogisticRegression
    lr2.setThresholds(Array(0.3, 0.7))
    val expectedThreshold = 1.0 / (1.0 + 0.3 / 0.7)
    assert(lr2.getThreshold ~== expectedThreshold relTol 1E-7)
    // thresholds and threshold must be consistent
    //阈值和阈值必须是一致的
    lr2.setThresholds(Array(0.1, 0.2, 0.3))
    withClue("getThreshold should throw error if thresholds has length != 2.") {
      intercept[IllegalArgumentException] {
        lr2.getThreshold
      }
    }
    // thresholds and threshold must be consistent: values
    //阈值和阈值必须是一致的,阈值不匹配
    withClue("fit with ParamMap should throw error if threshold, thresholds do not match.") {
      intercept[IllegalArgumentException] {
      //fit()方法将DataFrame转化为一个Transformer的算法
        val lr2model = lr2.fit(dataset,
          lr2.thresholds -> Array(0.3, 0.7), lr2.threshold -> (expectedThreshold / 2.0))
        lr2model.getThreshold
      }
    }
  }
  //逻辑回归模型不适合拦截时,fitintercept关闭
  test("logistic regression doesn't fit intercept when fitIntercept is off") {
    val lr = new LogisticRegression
    lr.setFitIntercept(false)//是否训练拦截对象
    //fit()方法将DataFrame转化为一个Transformer的算法
    val model = lr.fit(dataset)
    //默认拦截0.0
    assert(model.intercept === 0.0)

    // copied model must have the same parent.
    //复制的模型必须有相同的父类
    MLTestingUtils.checkCopy(model)
  }

  test("logistic regression with setters") {//逻辑回归设置
    // Set params, train, and check as many params as we can.
    //设置参数,训练并检查许多参数
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(1.0)//正则化参数>=0
       //在二进制分类中设置阈值,范围为[0，1],如果类标签1的估计概率>Threshold,则预测1,否则0
      .setThreshold(0.6)
      //类别条件概率预测结果列名
      .setProbabilityCol("myProbability")
     //fit()方法将DataFrame转化为一个Transformer的算法
    val model = lr.fit(dataset)
    val parent = model.parent.asInstanceOf[LogisticRegression]
    assert(parent.getMaxIter === 10)
    assert(parent.getRegParam === 1.0)//正则化参数>=0
    //在二进制分类中设置阈值,范围为[0，1],如果类标签1的估计概率>Threshold,则预测1,否则0
    assert(parent.getThreshold === 0.6)
    assert(model.getThreshold === 0.6)

    // Modify model params, and check that the params worked.
    //修改模型参数,并检查工作的参数
    model.setThreshold(1.0)
    //transform()方法将DataFrame转化为另外一个DataFrame的算法
    val predAllZero = model.transform(dataset)
      .select("prediction", "myProbability")
      .collect()
      .map { case Row(pred: Double, prob: Vector) => pred }
    assert(predAllZero.forall(_ === 0),
      s"With threshold=1.0, expected predictions to be all 0, but only" +
      s" ${predAllZero.count(_ === 0)} of ${dataset.count()} were 0.")
    // Call transform with params, and check that the params worked.
      //调用变换参数,并检查工作的参数
    val predNotAllZero =
      model.transform(dataset, model.threshold -> 0.0,
        model.probabilityCol -> "myProb") //类别条件概率预测结果列名
        .select("prediction", "myProb")
        .collect()
        .map { case Row(pred: Double, prob: Vector) => pred }
    assert(predNotAllZero.exists(_ !== 0.0))

    // Call fit() with new params, and check as many params as we can.
    //新的参数调用fit(),我们可以检查的参数
    lr.setThresholds(Array(0.6, 0.4))
    //fit()方法将DataFrame转化为一个Transformer的算法
    val model2 = lr.fit(dataset, lr.maxIter -> 5, lr.regParam -> 0.1,
      lr.probabilityCol -> "theProb") //类别条件概率预测结果列名
    val parent2 = model2.parent.asInstanceOf[LogisticRegression]
    assert(parent2.getMaxIter === 5)//最大迭代数
    assert(parent2.getRegParam === 0.1)//正则化参数>=0
    //在二进制分类中设置阈值,范围为[0，1],如果类标签1的估计概率>Threshold,则预测1,否则0
    assert(parent2.getThreshold === 0.4)//获得阈值
    assert(model2.getThreshold === 0.4)
     //类别条件概率预测结果列名
    assert(model2.getProbabilityCol === "theProb")
  }

  test("logistic regression: Predictor, Classifier methods") {//逻辑回归:预测,分类方法
    val sqlContext = this.sqlContext
    val lr = new LogisticRegression
    //fit()方法将DataFrame转化为一个Transformer的算法
    val model = lr.fit(dataset)
    //默认分类数
    assert(model.numClasses === 2)
    //在二进制分类中设置阈值,范围为[0，1],如果类标签1的估计概率>Threshold,则预测1,否则0
    val threshold = model.getThreshold
    //transform()方法将DataFrame转化为另外一个DataFrame的算法
    val results = model.transform(dataset)

    // Compare rawPrediction with probability
    //用概率比较原始预测
    results.select("rawPrediction", "probability").collect().foreach {
      case Row(raw: Vector, prob: Vector) =>
        assert(raw.size === 2)
        assert(prob.size === 2)
        val probFromRaw1 = 1.0 / (1.0 + math.exp(-raw(1)))
        assert(prob(1) ~== probFromRaw1 relTol eps)
        assert(prob(0) ~== 1.0 - probFromRaw1 relTol eps)
    }

    // Compare prediction with probability
    //用概率比较预测
    results.select("prediction", "probability").collect().foreach {
      //prediction 预测,probability 概率
      case Row(pred: Double, prob: Vector) =>
        
        val predFromProb = prob.toArray.zipWithIndex.maxBy(_._1)._2
        assert(pred == predFromProb)
    }
  }

  test("MultiClassSummarizer") {//多类总结
    val summarizer1 = (new MultiClassSummarizer)
      .add(0.0).add(3.0).add(4.0).add(3.0).add(6.0)
    assert(summarizer1.histogram.zip(Array[Long](1, 0, 0, 2, 1, 0, 1)).forall(x => x._1 === x._2))
    assert(summarizer1.countInvalid === 0)
    assert(summarizer1.numClasses === 7)//分类数

    val summarizer2 = (new MultiClassSummarizer)
      .add(1.0).add(5.0).add(3.0).add(0.0).add(4.0).add(1.0)
    assert(summarizer2.histogram.zip(Array[Long](1, 2, 0, 1, 1, 1)).forall(x => x._1 === x._2))
    assert(summarizer2.countInvalid === 0)
    //numClasses 分类数
    assert(summarizer2.numClasses === 6)

    val summarizer3 = (new MultiClassSummarizer)
      .add(0.0).add(1.3).add(5.2).add(2.5).add(2.0).add(4.0).add(4.0).add(4.0).add(1.0)
    assert(summarizer3.histogram.zip(Array[Long](1, 1, 1, 0, 3)).forall(x => x._1 === x._2))
    //numClasses 分类数
    assert(summarizer3.countInvalid === 3)
    assert(summarizer3.numClasses === 5)

    val summarizer4 = (new MultiClassSummarizer)
      .add(3.1).add(4.3).add(2.0).add(1.0).add(3.0)
    //直方图
    assert(summarizer4.histogram.zip(Array[Long](0, 1, 1, 1)).forall(x => x._1 === x._2))
    assert(summarizer4.countInvalid === 2)
    //numClasses 分类数
    assert(summarizer4.numClasses === 4)

    // small map merges large one
    //小Map合并大Map
    val summarizerA = summarizer1.merge(summarizer2)
    assert(summarizerA.hashCode() === summarizer2.hashCode())
    //直方图
    assert(summarizerA.histogram.zip(Array[Long](2, 2, 0, 3, 2, 1, 1)).forall(x => x._1 === x._2))
    //numClasses 分类数
    assert(summarizerA.countInvalid === 0)
    assert(summarizerA.numClasses === 7)

    // large map merges small one
    //大map合并小map
    val summarizerB = summarizer3.merge(summarizer4)
    assert(summarizerB.hashCode() === summarizer3.hashCode())
    assert(summarizerB.histogram.zip(Array[Long](1, 2, 2, 1, 3)).forall(x => x._1 === x._2))
    assert(summarizerB.countInvalid === 5)
    assert(summarizerB.numClasses === 5)
  }
  //不规则截取的二元逻辑回归
  test("binary logistic regression with intercept without regularization") {
  //是否训练拦截对象,训练模型前是否需要对训练特征进行标准化处理
    val trainer1 = (new LogisticRegression).setFitIntercept(true).setStandardization(true)
    val trainer2 = (new LogisticRegression).setFitIntercept(true).setStandardization(false)
     //fit()方法将DataFrame转化为一个Transformer的算法
    val model1 = trainer1.fit(binaryDataset)
    val model2 = trainer2.fit(binaryDataset)

    /*
       Using the following R code to load the data and train the model using glmnet package.

       library("glmnet")
       data <- read.csv("path", header=FALSE)
       label = factor(data$V1)
       features = as.matrix(data.frame(data$V2, data$V3, data$V4, data$V5))
       family 模型中使用的误差分布类型
       weights = coef(glmnet(features,label, family="binomial", alpha = 0, lambda = 0))
       weights

       5 x 1 sparse Matrix of class "dgCMatrix"
                           s0
       (Intercept)  2.8366423
       data.V2     -0.5895848
       data.V3      0.8931147
       data.V4     -0.3925051
       data.V5     -0.7996864
     */
    val interceptR = 2.8366423
    val weightsR = Vectors.dense(-0.5895848, 0.8931147, -0.3925051, -0.7996864)

    assert(model1.intercept ~== interceptR relTol 1E-3)
    assert(model1.weights ~= weightsR relTol 1E-3)

    // Without regularization, with or without standardization will converge to the same solution.
    assert(model2.intercept ~== interceptR relTol 1E-3)
    assert(model2.weights ~= weightsR relTol 1E-3)
  }
  //不规则正则化的二元逻辑回归
  test("binary logistic regression without intercept without regularization") {
  //是否训练拦截对象,训练模型前是否需要对训练特征进行标准化处理
    val trainer1 = (new LogisticRegression).setFitIntercept(false).setStandardization(true)
    val trainer2 = (new LogisticRegression).setFitIntercept(false).setStandardization(false)

    val model1 = trainer1.fit(binaryDataset)
    val model2 = trainer2.fit(binaryDataset)

    /*
       Using the following R code to load the data and train the model using glmnet package.

       library("glmnet")
       data <- read.csv("path", header=FALSE)
       label = factor(data$V1)
       features = as.matrix(data.frame(data$V2, data$V3, data$V4, data$V5))
       weights =
	  //family 模型中使用的误差分布类型
           coef(glmnet(features,label, family="binomial", alpha = 0, lambda = 0, intercept=FALSE))
       weights

       5 x 1 sparse Matrix of class "dgCMatrix"
                           s0
       (Intercept)   .
       data.V2     -0.3534996
       data.V3      1.2964482
       data.V4     -0.3571741
       data.V5     -0.7407946
     */
    val interceptR = 0.0
    val weightsR = Vectors.dense(-0.3534996, 1.2964482, -0.3571741, -0.7407946)

    assert(model1.intercept ~== interceptR relTol 1E-3)
    assert(model1.weights ~= weightsR relTol 1E-2)

    // Without regularization, with or without standardization should converge to the same solution.
    //没有正规化,有或无标准化应收敛到同一溶液
    assert(model2.intercept ~== interceptR relTol 1E-3)
    assert(model2.weights ~= weightsR relTol 1E-2)
  }
  //使用L1正则化拦截的Logistic回归分析
  test("binary logistic regression with intercept with L1 regularization") {
    val trainer1 = (new LogisticRegression).setFitIntercept(true)
      //ElasticNetParam=0.0为L2正则化 1.0为L1正则化,
      .setElasticNetParam(1.0).setRegParam(0.12).setStandardization(true)
    val trainer2 = (new LogisticRegression).setFitIntercept(true)
    //ElasticNetParam=0.0为L2正则化 1.0为L1正则化,训练模型前是否需要对训练特征进行标准化处理
      .setElasticNetParam(1.0).setRegParam(0.12).setStandardization(false)
	//fit()方法将DataFrame转化为一个Transformer的算法
    val model1 = trainer1.fit(binaryDataset)
    val model2 = trainer2.fit(binaryDataset)

    /*
       Using the following R code to load the data and train the model using glmnet package.
			 使用下面的代码来加载数据和使用glmnet包训练模型
       library("glmnet")
       data <- read.csv("path", header=FALSE)
       label = factor(data$V1)
       features = as.matrix(data.frame(data$V2, data$V3, data$V4, data$V5))
       weights = coef(glmnet(features,label, family="binomial", alpha = 1, lambda = 0.12))
       weights

       5 x 1 sparse Matrix of class "dgCMatrix"
                            s0
       (Intercept) -0.05627428
       data.V2       .
       data.V3       .
       data.V4     -0.04325749
       data.V5     -0.02481551
     */
    val interceptR1 = -0.05627428
    val weightsR1 = Vectors.dense(0.0, 0.0, -0.04325749, -0.02481551)

    assert(model1.intercept ~== interceptR1 relTol 1E-2)
    assert(model1.weights ~= weightsR1 absTol 2E-2)

    /*
       Using the following R code to load the data and train the model using glmnet package.

       library("glmnet")
       data <- read.csv("path", header=FALSE)
       label = factor(data$V1)
       features = as.matrix(data.frame(data$V2, data$V3, data$V4, data$V5))
       weights = coef(glmnet(features,label, family="binomial", alpha = 1, lambda = 0.12,
           standardize=FALSE))
       weights

       5 x 1 sparse Matrix of class "dgCMatrix"
                           s0
       (Intercept)  0.3722152
       data.V2       .
       data.V3       .
       data.V4     -0.1665453
       data.V5       .
     */
    val interceptR2 = 0.3722152
    val weightsR2 = Vectors.dense(0.0, 0.0, -0.1665453, 0.0)

    assert(model2.intercept ~== interceptR2 relTol 1E-2)
    assert(model2.weights ~= weightsR2 absTol 1E-3)
  }
  //Logistic回归分析没有L1正则化拦截
  test("binary logistic regression without intercept with L1 regularization") {
  //是否训练拦截对象
    val trainer1 = (new LogisticRegression).setFitIntercept(false)
    //ElasticNetParam=0.0为L2正则化 1.0为L1正则化,训练模型前是否需要对训练特征进行标准化处理
      .setElasticNetParam(1.0).setRegParam(0.12).setStandardization(true)
    val trainer2 = (new LogisticRegression).setFitIntercept(false)
      .setElasticNetParam(1.0).setRegParam(0.12).setStandardization(false)
     //fit()方法将DataFrame转化为一个Transformer的算法
    val model1 = trainer1.fit(binaryDataset)
    val model2 = trainer2.fit(binaryDataset)

    /*
       Using the following R code to load the data and train the model using glmnet package.

       library("glmnet")
       data <- read.csv("path", header=FALSE)
       label = factor(data$V1)
       features = as.matrix(data.frame(data$V2, data$V3, data$V4, data$V5))
       weights = coef(glmnet(features,label, family="binomial", alpha = 1, lambda = 0.12,
           intercept=FALSE))
       weights

       5 x 1 sparse Matrix of class "dgCMatrix"
                            s0
       (Intercept)   .
       data.V2       .
       data.V3       .
       data.V4     -0.05189203
       data.V5     -0.03891782
     */
    val interceptR1 = 0.0
    val weightsR1 = Vectors.dense(0.0, 0.0, -0.05189203, -0.03891782)

    assert(model1.intercept ~== interceptR1 relTol 1E-3)
    assert(model1.weights ~= weightsR1 absTol 1E-3)

    /*
       Using the following R code to load the data and train the model using glmnet package.

       library("glmnet")
       data <- read.csv("path", header=FALSE)
       label = factor(data$V1)
       features = as.matrix(data.frame(data$V2, data$V3, data$V4, data$V5))
       weights = coef(glmnet(features,label, family="binomial", alpha = 1, lambda = 0.12,
           intercept=FALSE, standardize=FALSE))
       weights

       5 x 1 sparse Matrix of class "dgCMatrix"
                            s0
       (Intercept)   .
       data.V2       .
       data.V3       .
       data.V4     -0.08420782
       data.V5       .
     */
    val interceptR2 = 0.0
    val weightsR2 = Vectors.dense(0.0, 0.0, -0.08420782, 0.0)

    assert(model2.intercept ~== interceptR2 absTol 1E-3)
    assert(model2.weights ~= weightsR2 absTol 1E-3)
  }
  //具有L2正则化的截距的二元Logistic回归
  test("binary logistic regression with intercept with L2 regularization") {
    val trainer1 = (new LogisticRegression).setFitIntercept(true)
       //ElasticNetParam=0.0为L2正则化 1.0为L1正则化,RegParam 正则化参数>=0
      .setElasticNetParam(0.0).setRegParam(1.37).setStandardization(true)
    val trainer2 = (new LogisticRegression).setFitIntercept(true)
    //ElasticNetParam=0.0为L2正则化 1.0为L1正则化,RegParam 正则化参数>=0
      .setElasticNetParam(0.0).setRegParam(1.37).setStandardization(false)
    //fit()方法将DataFrame转化为一个Transformer的算法
    val model1 = trainer1.fit(binaryDataset)
    val model2 = trainer2.fit(binaryDataset)

    /*
       Using the following R code to load the data and train the model using glmnet package.

       library("glmnet")
       data <- read.csv("path", header=FALSE)
       label = factor(data$V1)
       features = as.matrix(data.frame(data$V2, data$V3, data$V4, data$V5))
       weights = coef(glmnet(features,label, family="binomial", alpha = 0, lambda = 1.37))
       weights

       5 x 1 sparse Matrix of class "dgCMatrix"
                            s0
       (Intercept)  0.15021751
       data.V2     -0.07251837
       data.V3      0.10724191
       data.V4     -0.04865309
       data.V5     -0.10062872
     */
    val interceptR1 = 0.15021751
    val weightsR1 = Vectors.dense(-0.07251837, 0.10724191, -0.04865309, -0.10062872)

    assert(model1.intercept ~== interceptR1 relTol 1E-3)
    assert(model1.weights ~= weightsR1 relTol 1E-3)

    /*
       Using the following R code to load the data and train the model using glmnet package.

       library("glmnet")
       data <- read.csv("path", header=FALSE)
       label = factor(data$V1)
       features = as.matrix(data.frame(data$V2, data$V3, data$V4, data$V5))
       weights = coef(glmnet(features,label, family="binomial", alpha = 0, lambda = 1.37,
           standardize=FALSE))
       weights

       5 x 1 sparse Matrix of class "dgCMatrix"
                            s0
       (Intercept)  0.48657516
       data.V2     -0.05155371
       data.V3      0.02301057
       data.V4     -0.11482896
       data.V5     -0.06266838
     */
    val interceptR2 = 0.48657516
    val weightsR2 = Vectors.dense(-0.05155371, 0.02301057, -0.11482896, -0.06266838)

    assert(model2.intercept ~== interceptR2 relTol 1E-3)
    assert(model2.weights ~= weightsR2 relTol 1E-3)
  }
  //没有拦截的二元Logistic回归与L2正则化
  test("binary logistic regression without intercept with L2 regularization") {
    val trainer1 = (new LogisticRegression).setFitIntercept(false)
    //ElasticNetParam=0.0为L2正则化 1.0为L1正则化
      .setElasticNetParam(0.0).setRegParam(1.37).setStandardization(true)
    val trainer2 = (new LogisticRegression).setFitIntercept(false)
    //ElasticNetParam=0.0为L2正则化 1.0为L1正则化,RegParam 正则化参数>=0
    //训练模型前是否需要对训练特征进行标准化处理
      .setElasticNetParam(0.0).setRegParam(1.37).setStandardization(false)
     //fit()方法将DataFrame转化为一个Transformer的算法
    val model1 = trainer1.fit(binaryDataset)
    val model2 = trainer2.fit(binaryDataset)

    /*
       Using the following R code to load the data and train the model using glmnet package.

       library("glmnet")
       data <- read.csv("path", header=FALSE)
       label = factor(data$V1)
       features = as.matrix(data.frame(data$V2, data$V3, data$V4, data$V5))
       weights = coef(glmnet(features,label, family="binomial", alpha = 0, lambda = 1.37,
           intercept=FALSE))
       weights

       5 x 1 sparse Matrix of class "dgCMatrix"
                            s0
       (Intercept)   .
       data.V2     -0.06099165
       data.V3      0.12857058
       data.V4     -0.04708770
       data.V5     -0.09799775
     */
    val interceptR1 = 0.0
    val weightsR1 = Vectors.dense(-0.06099165, 0.12857058, -0.04708770, -0.09799775)

    assert(model1.intercept ~== interceptR1 absTol 1E-3)
    assert(model1.weights ~= weightsR1 relTol 1E-2)

    /*
       Using the following R code to load the data and train the model using glmnet package.

       library("glmnet")
       data <- read.csv("path", header=FALSE)
       label = factor(data$V1)
       features = as.matrix(data.frame(data$V2, data$V3, data$V4, data$V5))
       weights = coef(glmnet(features,label, family="binomial", alpha = 0, lambda = 1.37,
           intercept=FALSE, standardize=FALSE))
       weights

       5 x 1 sparse Matrix of class "dgCMatrix"
                             s0
       (Intercept)   .
       data.V2     -0.005679651
       data.V3      0.048967094
       data.V4     -0.093714016
       data.V5     -0.053314311
     */
    val interceptR2 = 0.0
    val weightsR2 = Vectors.dense(-0.005679651, 0.048967094, -0.093714016, -0.053314311)

    assert(model2.intercept ~== interceptR2 absTol 1E-3)
    assert(model2.weights ~= weightsR2 relTol 1E-2)
  }
  //ElasticNet正则化与拦截的Logistic回归分析
  test("binary logistic regression with intercept with ElasticNet regularization") {
    val trainer1 = (new LogisticRegression).setFitIntercept(true)
    //ElasticNetParam=0.0为L2正则化 1.0为L1正则化
      .setElasticNetParam(0.38).setRegParam(0.21).setStandardization(true)
    val trainer2 = (new LogisticRegression).setFitIntercept(true)
    //ElasticNetParam=0.0为L2正则化 1.0为L1正则化,RegParam 正则化参数>=0
      .setElasticNetParam(0.38).setRegParam(0.21).setStandardization(false)
    //fit()方法将DataFrame转化为一个Transformer的算法
    val model1 = trainer1.fit(binaryDataset)
    val model2 = trainer2.fit(binaryDataset)

    /*
       Using the following R code to load the data and train the model using glmnet package.

       library("glmnet")
       data <- read.csv("path", header=FALSE)
       label = factor(data$V1)
       features = as.matrix(data.frame(data$V2, data$V3, data$V4, data$V5))
       weights = coef(glmnet(features,label, family="binomial", alpha = 0.38, lambda = 0.21))
       weights

       5 x 1 sparse Matrix of class "dgCMatrix"
                            s0
       (Intercept)  0.57734851
       data.V2     -0.05310287
       data.V3       .
       data.V4     -0.08849250
       data.V5     -0.15458796
     */
    val interceptR1 = 0.57734851
    val weightsR1 = Vectors.dense(-0.05310287, 0.0, -0.08849250, -0.15458796)

    assert(model1.intercept ~== interceptR1 relTol 6E-3)
    assert(model1.weights ~== weightsR1 absTol 5E-3)

    /*
       Using the following R code to load the data and train the model using glmnet package.

       library("glmnet")
       data <- read.csv("path", header=FALSE)
       label = factor(data$V1)
       features = as.matrix(data.frame(data$V2, data$V3, data$V4, data$V5))
       weights = coef(glmnet(features,label, family="binomial", alpha = 0.38, lambda = 0.21,
           standardize=FALSE))
       weights

       5 x 1 sparse Matrix of class "dgCMatrix"
                            s0
       (Intercept)  0.51555993
       data.V2       .
       data.V3       .
       data.V4     -0.18807395
       data.V5     -0.05350074
     */
    val interceptR2 = 0.51555993
    val weightsR2 = Vectors.dense(0.0, 0.0, -0.18807395, -0.05350074)

    assert(model2.intercept ~== interceptR2 relTol 6E-3)
    assert(model2.weights ~= weightsR2 absTol 1E-3)
  }
  //Logistic回归分析没有ElasticNet正则化与拦截
  test("binary logistic regression without intercept with ElasticNet regularization") {
    val trainer1 = (new LogisticRegression).setFitIntercept(false)
    //ElasticNetParam=0.0为L2正则化 1.0为L1正则化,训练模型前是否需要对训练特征进行标准化处理
      .setElasticNetParam(0.38).setRegParam(0.21).setStandardization(true)
    val trainer2 = (new LogisticRegression).setFitIntercept(false)
    //ElasticNetParam=0.0为L2正则化 1.0为L1正则化,RegParam 正则化参数>=0
      .setElasticNetParam(0.38).setRegParam(0.21).setStandardization(false)
    //fit()方法将DataFrame转化为一个Transformer的算法
    val model1 = trainer1.fit(binaryDataset)
    val model2 = trainer2.fit(binaryDataset)

    /*
       Using the following R code to load the data and train the model using glmnet package.

       library("glmnet")
       data <- read.csv("path", header=FALSE)
       label = factor(data$V1)
       features = as.matrix(data.frame(data$V2, data$V3, data$V4, data$V5))
       weights = coef(glmnet(features,label, family="binomial", alpha = 0.38, lambda = 0.21,
           intercept=FALSE))
       weights

       5 x 1 sparse Matrix of class "dgCMatrix"
                            s0
       (Intercept)   .
       data.V2     -0.001005743
       data.V3      0.072577857
       data.V4     -0.081203769
       data.V5     -0.142534158
     */
    val interceptR1 = 0.0
    val weightsR1 = Vectors.dense(-0.001005743, 0.072577857, -0.081203769, -0.142534158)

    assert(model1.intercept ~== interceptR1 relTol 1E-3)
    assert(model1.weights ~= weightsR1 absTol 1E-2)

    /*
       Using the following R code to load the data and train the model using glmnet package.

       library("glmnet")
       data <- read.csv("path", header=FALSE)
       label = factor(data$V1)
       features = as.matrix(data.frame(data$V2, data$V3, data$V4, data$V5))
       family 模型中使用的误差分布类型
       weights = coef(glmnet(features,label, family="binomial", alpha = 0.38, lambda = 0.21,
           intercept=FALSE, standardize=FALSE))
       weights

       5 x 1 sparse Matrix of class "dgCMatrix"
                            s0
       (Intercept)   .
       data.V2       .
       data.V3      0.03345223
       data.V4     -0.11304532
       data.V5       .
     */
    val interceptR2 = 0.0
    val weightsR2 = Vectors.dense(0.0, 0.03345223, -0.11304532, 0.0)

    assert(model2.intercept ~== interceptR2 absTol 1E-3)
    assert(model2.weights ~= weightsR2 absTol 1E-3)
  }
  //二分类Logistic回归与强大的L1正则化拦截
  test("binary logistic regression with intercept with strong L1 regularization") {
    val trainer1 = (new LogisticRegression).setFitIntercept(true)
    //ElasticNetParam=0.0为L2正则化 1.0为L1正则化
      .setElasticNetParam(1.0).setRegParam(6.0).setStandardization(true)
    val trainer2 = (new LogisticRegression).setFitIntercept(true)
    //ElasticNetParam=0.0为L2正则化 1.0为L1正则化,RegParam 正则化参数>=0
      .setElasticNetParam(1.0).setRegParam(6.0).setStandardization(false)
   //fit()方法将DataFrame转化为一个Transformer的算法
    val model1 = trainer1.fit(binaryDataset)
    val model2 = trainer2.fit(binaryDataset)

    val histogram = binaryDataset.map { case Row(label: Double, features: Vector) => label }
      .treeAggregate(new MultiClassSummarizer)(
        seqOp = (c, v) => (c, v) match {
          case (classSummarizer: MultiClassSummarizer, label: Double) => classSummarizer.add(label)
        },
        combOp = (c1, c2) => (c1, c2) match {
          case (classSummarizer1: MultiClassSummarizer, classSummarizer2: MultiClassSummarizer) =>
            classSummarizer1.merge(classSummarizer2)
        }).histogram

    /*
       For binary logistic regression with strong L1 regularization, all the weights will be zeros.
       As a result,
       {{{
       P(0) = 1 / (1 + \exp(b)), and
       P(1) = \exp(b) / (1 + \exp(b))
       }}}, hence
       {{{
       b = \log{P(1) / P(0)} = \log{count_1 / count_0}
       }}}
     */
    val interceptTheory = math.log(histogram(1).toDouble / histogram(0).toDouble)
    val weightsTheory = Vectors.dense(0.0, 0.0, 0.0, 0.0)

    assert(model1.intercept ~== interceptTheory relTol 1E-5)
    assert(model1.weights ~= weightsTheory absTol 1E-6)

    assert(model2.intercept ~== interceptTheory relTol 1E-5)
    assert(model2.weights ~= weightsTheory absTol 1E-6)

    /*
       Using the following R code to load the data and train the model using glmnet package.

       library("glmnet")
       data <- read.csv("path", header=FALSE)
       label = factor(data$V1)
       features = as.matrix(data.frame(data$V2, data$V3, data$V4, data$V5))
       family 模型中使用的误差分布类型
       weights = coef(glmnet(features,label, family="binomial", alpha = 1.0, lambda = 6.0))
       weights

       5 x 1 sparse Matrix of class "dgCMatrix"
                            s0
       (Intercept) -0.2480643
       data.V2      0.0000000
       data.V3       .
       data.V4       .
       data.V5       .
     */
    val interceptR = -0.248065
    val weightsR = Vectors.dense(0.0, 0.0, 0.0, 0.0)

    assert(model1.intercept ~== interceptR relTol 1E-5)
    assert(model1.weights ~== weightsR absTol 1E-6)
  }

  test("evaluate on test set") {//测试集的评估
    // Evaluate on test set should be same as that of the transformed training data.
    //测试集的评估应与转化的训练数据的评估相同
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(1.0)//正则化参数>=0
      //在二进制分类中设置阈值,范围为[0,1],如果类标签1的估计概率>Threshold,则预测1,否则0
      .setThreshold(0.6)
      //fit()方法将DataFrame转化为一个Transformer的算法
    val model = lr.fit(dataset)
    val summary = model.summary.asInstanceOf[BinaryLogisticRegressionSummary]

    val sameSummary = model.evaluate(dataset).asInstanceOf[BinaryLogisticRegressionSummary]
    //AUC下的面积表示平均准确率,平均准确率等于训练样本中被正确分类的数目除以样本总数
    assert(summary.areaUnderROC === sameSummary.areaUnderROC)
    assert(summary.roc.collect() === sameSummary.roc.collect())
    assert(summary.pr.collect === sameSummary.pr.collect())
    assert(
      summary.fMeasureByThreshold.collect() === sameSummary.fMeasureByThreshold.collect())
    assert(summary.recallByThreshold.collect() === sameSummary.recallByThreshold.collect())
    assert(
      summary.precisionByThreshold.collect() === sameSummary.precisionByThreshold.collect())
  }

  test("statistics on training data") {//训练数据统计
    // Test that loss is monotonically decreasing.
    //测试，损失是单调递减
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(1.0)//正则化参数>=0
      //在二进制分类中设置阈值,范围为[0，1],如果类标签1的估计概率>Threshold,则预测1,否则0
      .setThreshold(0.6)
      //fit()方法将DataFrame转化为一个Transformer的算法
    val model = lr.fit(dataset)
    assert(
      model.summary
        .objectiveHistory
        .sliding(2)
        .forall(x => x(0) >= x(1)))

  }
}
