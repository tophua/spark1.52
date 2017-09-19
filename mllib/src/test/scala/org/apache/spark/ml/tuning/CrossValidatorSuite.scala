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

package org.apache.spark.ml.tuning

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.util.MLTestingUtils
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, Evaluator, RegressionEvaluator}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.HasInputCol
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.classification.LogisticRegressionSuite.generateLogisticInput
import org.apache.spark.mllib.util.{LinearDataGenerator, MLlibTestSparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.types.StructType
/**
 * 交叉验证测试套件
 * 数据量小的时候可以用CrossValidator进行交叉验证,数据量大的时候可以直接用trainValidationSplit
 */
class CrossValidatorSuite extends SparkFunSuite with MLlibTestSparkContext {

  @transient var dataset: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val sqlContext = new SQLContext(sc)
    val input=generateLogisticInput(1.0, 1.0, 100, 42)
    println("input:"+input)
    /**
     * input:Vector((1.0,[1.1419053154730547]), (0.0,[0.9194079489827879]), (0.0,[-0.9498666368908959]), 
     * (1.0,[-1.1069902863993377]), (0.0,[0.2809776380727795]), (1.0,[0.6846227956326554]), 
     * (1.0,[-0.8172214073987268]), (0.0,[-1.3966434026780434]), (1.0,[-0.19094451307087512]),
     * (1.0,[1.4862133923906502]), (1.0,[0.8023071496873626]), (0.0,[-0.12151292466549345]), 
     * (1.0,[0.2574914085090889]), (0.0,[-0.3199143760045327]), (0.0,[-1.7684998592513064]))
     */
    dataset = sqlContext.createDataFrame(
      sc.parallelize(generateLogisticInput(1.0, 1.0, 100, 42), 2))
  }

  test("cross validation with logistic regression") {//逻辑回归的交叉验证
    //交叉验证
    val lr = new LogisticRegression//逻辑回归
    //ParamGridBuilder构建待选参数(如:logistic regression的regParam)
    val lrParamMaps = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.001, 1000.0))
      .addGrid(lr.maxIter, Array(0, 10))
      .build()
    val eval = new BinaryClassificationEvaluator //二分类评估
    val cv = new CrossValidator()
      .setEstimator(lr)//被评估模型
      .setEstimatorParamMaps(lrParamMaps)//评估参数
      .setEvaluator(eval)//评估模型
      .setNumFolds(3)//交叉验证的折叠数参数,默认3,必须大于2
      //fit()方法将DataFrame转化为一个Transformer的算法
    val cvModel = cv.fit(dataset)

    // copied model must have the same paren.
    //复制模型必须具有相同的实质
     MLTestingUtils.checkCopy(cvModel)
    //最好的模型参数
    val parent = cvModel.bestModel.parent.asInstanceOf[LogisticRegression]
    assert(parent.getRegParam === 0.001)//正则化参数
    assert(parent.getMaxIter === 10)//最大迭代次数
    //avg Metrics平均指标
    for(x<-cvModel.avgMetrics){
      /**
        avgMetrics:0.5
        avgMetrics:0.5
        avgMetrics:0.8332341269841269
        avgMetrics:0.8332341269841269*/
      println("avgMetrics:"+x)
    }
    assert(cvModel.avgMetrics.length === lrParamMaps.length)
  }

  test("cross validation with linear regression") {//线性回归的交叉验证
    val dataset = sqlContext.createDataFrame(
      sc.parallelize(LinearDataGenerator.generateLinearInput(
        6.3, Array(4.7, 7.2), Array(0.9, -1.3), Array(0.7, 1.2), 100, 42, 0.1), 2))
    //线性回归
    val trainer = new LinearRegression
    //ParamGridBuilder构建待选参数(如:logistic regression的regParam)
    val lrParamMaps = new ParamGridBuilder() //模型参数
      .addGrid(trainer.regParam, Array(1000.0, 0.001))
      .addGrid(trainer.maxIter, Array(0, 10))
      .build()
     //回归评估
    val eval = new RegressionEvaluator()
    val cv = new CrossValidator()
      .setEstimator(trainer)//算法模型
      .setEstimatorParamMaps(lrParamMaps)//设置被评估模型的参数
      .setEvaluator(eval)//评估
      .setNumFolds(3)
      //fit()方法将DataFrame转化为一个Transformer的算法
    val cvModel = cv.fit(dataset)
    //获得线性回归最佳参数
    val parent = cvModel.bestModel.parent.asInstanceOf[LinearRegression]
    assert(parent.getRegParam === 0.001)
    assert(parent.getMaxIter === 10)
    //使用使用准确率
    assert(cvModel.avgMetrics.length === lrParamMaps.length)
    
    eval.setMetricName("r2")//定义使用r2方式评估
    //fit()方法将DataFrame转化为一个Transformer的算法
    val cvModel2 = cv.fit(dataset)
     //获得线性回归最佳参数
    val parent2 = cvModel2.bestModel.parent.asInstanceOf[LinearRegression]
    assert(parent2.getRegParam === 0.001)//正则化参数>=0
    assert(parent2.getMaxIter === 10)
    assert(cvModel2.avgMetrics.length === lrParamMaps.length)
  }
  //验证参数检查评估parammaps
  test("validateParams should check estimatorParamMaps") {
    import CrossValidatorSuite._

    val est = new MyEstimator("est")
    val eval = new MyEvaluator
    //ParamGridBuilder构建待选参数(如:logistic regression的regParam)
    val paramMaps = new ParamGridBuilder()
      .addGrid(est.inputCol, Array("input1", "input2"))
      .build()

    val cv = new CrossValidator()
      .setEstimator(est)
      .setEstimatorParamMaps(paramMaps)
      .setEvaluator(eval)
    //这应该通过
    cv.validateParams() // This should pass.

    val invalidParamMaps = paramMaps :+ ParamMap(est.inputCol -> "")
    cv.setEstimatorParamMaps(invalidParamMaps)
    intercept[IllegalArgumentException] {
      cv.validateParams()
    }
  }
}
/**
 * 交叉验证套件
 */
object CrossValidatorSuite {

  abstract class MyModel extends Model[MyModel]

  class MyEstimator(override val uid: String) extends Estimator[MyModel] with HasInputCol {

    override def validateParams(): Unit = require($(inputCol).nonEmpty)
	//fit()方法将DataFrame转化为一个Transformer的算法
    override def fit(dataset: DataFrame): MyModel = {
      throw new UnsupportedOperationException
    }

    override def transformSchema(schema: StructType): StructType = {
      throw new UnsupportedOperationException
    }

    override def copy(extra: ParamMap): MyEstimator = defaultCopy(extra)
  }

  class MyEvaluator extends Evaluator {

    override def evaluate(dataset: DataFrame): Double = {
      throw new UnsupportedOperationException
    }

    override def isLargerBetter: Boolean = true

    override val uid: String = "eval"

    override def copy(extra: ParamMap): MyEvaluator = defaultCopy(extra)
  }
}
