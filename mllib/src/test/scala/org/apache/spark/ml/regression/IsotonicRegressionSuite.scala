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

package org.apache.spark.ml.regression

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.MLTestingUtils
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{DataFrame, Row}
/**
 * 保序回归
 */
class IsotonicRegressionSuite extends SparkFunSuite with MLlibTestSparkContext {
  private def generateIsotonicInput(labels: Seq[Double]): DataFrame = {
    sqlContext.createDataFrame(
      labels.zipWithIndex.map { case (label, i) => (label, i.toDouble, 1.0) }
    ).toDF("label", "features", "weight")
  }

  private def generatePredictionInput(features: Seq[Double]): DataFrame = {
    sqlContext.createDataFrame(features.map(Tuple1.apply))
      .toDF("features")
  }

  test("isotonic regression predictions") {//保序回归预测
    val dataset = generateIsotonicInput(Seq(1, 2, 3, 1, 6, 17, 16, 17, 18))
    val ir = new IsotonicRegression().setIsotonic(true)

    val model = ir.fit(dataset)

    val predictions = model
      .transform(dataset)
      .select("prediction").map { case Row(pred) =>
        pred
      }.collect()

    assert(predictions === Array(1, 2, 2, 2, 6, 16.5, 16.5, 17, 18))

    assert(model.boundaries === Vectors.dense(0, 1, 3, 4, 5, 6, 7, 8))
    //预测（Prediction）
    assert(model.predictions === Vectors.dense(1, 2, 2, 6, 16.5, 16.5, 17.0, 18.0))
    assert(model.getIsotonic)
  }
   //预测（Prediction）
  test("antitonic regression predictions") {//逆序回归预测
    val dataset = generateIsotonicInput(Seq(7, 5, 3, 5, 1))
    val ir = new IsotonicRegression().setIsotonic(false)

    val model = ir.fit(dataset)
    val features = generatePredictionInput(Seq(-2.0, -1.0, 0.5, 0.75, 1.0, 2.0, 9.0))

    val predictions = model
      .transform(features)
      .select("prediction").map {
        case Row(pred) => pred
      }.collect()

    assert(predictions === Array(7, 7, 6, 5.5, 5, 4, 1))
  }

  test("params validation") {//参数验证
    val dataset = generateIsotonicInput(Seq(1, 2, 3))
    val ir = new IsotonicRegression
    ParamsSuite.checkParams(ir)
    val model = ir.fit(dataset)
    ParamsSuite.checkParams(model)
  }

  test("default params") {//默认参数
    val dataset = generateIsotonicInput(Seq(1, 2, 3))
    val ir = new IsotonicRegression()
    assert(ir.getLabelCol === "label")
    assert(ir.getFeaturesCol === "features")
    assert(ir.getPredictionCol === "prediction")
    assert(!ir.isDefined(ir.weightCol))
    assert(ir.getIsotonic)
    assert(ir.getFeatureIndex === 0)

    val model = ir.fit(dataset)

    // copied model must have the same parent.
    //复制的模型必须有相同的父
    MLTestingUtils.checkCopy(model)

    model.transform(dataset)
      .select("label", "features", "prediction", "weight")
      .collect()

    assert(model.getLabelCol === "label")
    assert(model.getFeaturesCol === "features")
    assert(model.getPredictionCol === "prediction")
    assert(!model.isDefined(model.weightCol))
    assert(model.getIsotonic)
    assert(model.getFeatureIndex === 0)
    assert(model.hasParent)
  }

  test("set parameters") {//设置参数
    val isotonicRegression = new IsotonicRegression()
      .setIsotonic(false)
      .setWeightCol("w")
      .setFeaturesCol("f")
      .setLabelCol("l")
      .setPredictionCol("p")

    assert(!isotonicRegression.getIsotonic)
    assert(isotonicRegression.getWeightCol === "w")
    assert(isotonicRegression.getFeaturesCol === "f")
    assert(isotonicRegression.getLabelCol === "l")
    assert(isotonicRegression.getPredictionCol === "p")
  }

  test("missing column") {//缺少列
    val dataset = generateIsotonicInput(Seq(1, 2, 3))

    intercept[IllegalArgumentException] {
      new IsotonicRegression().setWeightCol("w").fit(dataset)
    }

    intercept[IllegalArgumentException] {
      new IsotonicRegression().setFeaturesCol("f").fit(dataset)
    }

    intercept[IllegalArgumentException] {
      new IsotonicRegression().setLabelCol("l").fit(dataset)
    }

    intercept[IllegalArgumentException] {
      new IsotonicRegression().fit(dataset).setFeaturesCol("f").transform(dataset)
    }
  }

  test("vector features column with feature index") {//具有特征索引的向量特征列
    val dataset = sqlContext.createDataFrame(Seq(
      (4.0, Vectors.dense(0.0, 1.0)),
      (3.0, Vectors.dense(0.0, 2.0)),
      (5.0, Vectors.sparse(2, Array(1), Array(3.0))))
    ).toDF("label", "features")

    val ir = new IsotonicRegression()
      .setFeatureIndex(1)

    val model = ir.fit(dataset)

    val features = generatePredictionInput(Seq(2.0, 3.0, 4.0, 5.0))

    val predictions = model
      .transform(features)
      .select("prediction").map {
      case Row(pred) => pred
    }.collect()

    assert(predictions === Array(3.5, 5.0, 5.0, 5.0))
  }
}
