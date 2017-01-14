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
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, Evaluator, RegressionEvaluator}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.HasInputCol
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.classification.LogisticRegressionSuite.generateLogisticInput
import org.apache.spark.mllib.util.{LinearDataGenerator, MLlibTestSparkContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
/**
 * 训练分割验证套件
 * 数据量小的时候可以用CrossValidator进行交叉验证,数据量大的时候可以直接用trainValidationSplit
 */
class TrainValidationSplitSuite extends SparkFunSuite with MLlibTestSparkContext {
  test("train validation with logistic regression") {//训练验证逻辑回归
    val dataset = sqlContext.createDataFrame(
      sc.parallelize(generateLogisticInput(1.0, 1.0, 100, 42), 2))

    val lr = new LogisticRegression
    //ParamGridBuilder构建待选参数(如:logistic regression的regParam)
    val lrParamMaps = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.001, 1000.0))
      .addGrid(lr.maxIter, Array(0, 10))
      .build()
    val eval = new BinaryClassificationEvaluator
    val cv = new TrainValidationSplit()//训练分隔验证
      .setEstimator(lr)
      .setEstimatorParamMaps(lrParamMaps)
      .setEvaluator(eval)
      .setTrainRatio(0.5)
      //fit()方法将DataFrame转化为一个Transformer的算法
    val cvModel = cv.fit(dataset)
    //获得逻辑回归最佳参数
    val parent = cvModel.bestModel.parent.asInstanceOf[LogisticRegression]
    assert(cv.getTrainRatio === 0.5)//训练比率
    assert(parent.getRegParam === 0.001)//正则化参数
    assert(parent.getMaxIter === 10)//迭代次数
    assert(cvModel.validationMetrics.length === lrParamMaps.length)
  }

  test("train validation with linear regression") {//训练验证线性回归
    val dataset = sqlContext.createDataFrame(
        sc.parallelize(LinearDataGenerator.generateLinearInput(
            6.3, Array(4.7, 7.2), Array(0.9, -1.3), Array(0.7, 1.2), 100, 42, 0.1), 2))

    val trainer = new LinearRegression
    //ParamGridBuilder构建待选参数(如:logistic regression的regParam)
    val lrParamMaps = new ParamGridBuilder()
      .addGrid(trainer.regParam, Array(1000.0, 0.001))
      .addGrid(trainer.maxIter, Array(0, 10))
      .build()
    val eval = new RegressionEvaluator()
    val cv = new TrainValidationSplit()
      .setEstimator(trainer)
      .setEstimatorParamMaps(lrParamMaps)
      .setEvaluator(eval)
      .setTrainRatio(0.5)
      //fit()方法将DataFrame转化为一个Transformer的算法
    val cvModel = cv.fit(dataset)
    //获得线性回归最佳参数
    val parent = cvModel.bestModel.parent.asInstanceOf[LinearRegression]
    assert(parent.getRegParam === 0.001)//正则化参数
    assert(parent.getMaxIter === 10)//迭代次数
    assert(cvModel.validationMetrics.length === lrParamMaps.length)
     //定义使用r2方式评估
      eval.setMetricName("r2")
      //fit()方法将DataFrame转化为一个Transformer的算法
    val cvModel2 = cv.fit(dataset)
   //获得线性回归最佳参数
    val parent2 = cvModel2.bestModel.parent.asInstanceOf[LinearRegression]
    assert(parent2.getRegParam === 0.001)//正则化参数
    assert(parent2.getMaxIter === 10)//迭代次数
    assert(cvModel2.validationMetrics.length === lrParamMaps.length)
  }

  test("validateParams should check estimatorParamMaps") {//验证参数应检查估计参数Map
    import TrainValidationSplitSuite._

    val est = new MyEstimator("est")
    val eval = new MyEvaluator
    //ParamGridBuilder构建待选参数(如:logistic regression的regParam)
    val paramMaps = new ParamGridBuilder()
      .addGrid(est.inputCol, Array("input1", "input2"))
      .build()

    val cv = new TrainValidationSplit()
      .setEstimator(est)
      .setEstimatorParamMaps(paramMaps)
      .setEvaluator(eval)
      .setTrainRatio(0.5)
    cv.validateParams() // This should pass.

    val invalidParamMaps = paramMaps :+ ParamMap(est.inputCol -> "")
    cv.setEstimatorParamMaps(invalidParamMaps)
    intercept[IllegalArgumentException] {
      cv.validateParams()
    }
  }
}
/**
 * 训练分割验证套件
 */
object TrainValidationSplitSuite {

  abstract class MyModel extends Model[MyModel]

  class MyEstimator(override val uid: String) extends Estimator[MyModel] with HasInputCol {

    override def validateParams(): Unit = require($(inputCol).nonEmpty)

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
