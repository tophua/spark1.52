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

import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.param.{Param, ParamMap, ParamValidators}
import org.apache.spark.ml.param.shared.{HasLabelCol, HasPredictionCol}
import org.apache.spark.ml.util.{Identifiable, SchemaUtils}
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.DoubleType

/**
 * :: Experimental ::
 * Evaluator for regression, which expects two input columns: prediction and label.
 */
@Experimental
final class RegressionEvaluator(override val uid: String)
  extends Evaluator with HasPredictionCol with HasLabelCol {

  def this() = this(Identifiable.randomUID("regEval"))

  /**
   * param for metric name in evaluation (supports `"rmse"` (default), `"mse"`, `"r2"`, and `"mae"`)
   * MAE平均绝对误差是所有单个观测值与算术平均值的偏差的绝对值的平均
   * R2平方系统也称判定系数,用来评估模型拟合数据的好坏
   * 默认rmse均方根误差说明样本的离散程度
   * mse均方误差
   * 评价指标名称参数支持(supports `"rmse"`均方根误差  (default), `"mse"均方误差`, `"r2"`, and `"mae"`)
   * 
   * Because we will maximize evaluation value (ref: `CrossValidator`),
   * 因为我们将最大化评价价值(ref: `CrossValidator`)
   * when we evaluate a metric that is needed to minimize (e.g., `"rmse"`, `"mse"`, `"mae"`),
   * 当我们评估一个需要最小化的度量
   * we take and output the negative of this metric.
   * @group param
   */
  val metricName: Param[String] = {
    val allowedParams = ParamValidators.inArray(Array("mse", "rmse", "r2", "mae"))
    new Param(this, "metricName", "metric name in evaluation (mse|rmse|r2|mae)", allowedParams)
  }

  /** @group getParam */
  def getMetricName: String = $(metricName)

  /** @group setParam */
  def setMetricName(value: String): this.type = set(metricName, value)

  /** @group setParam */
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  def setLabelCol(value: String): this.type = set(labelCol, value)
  //默认均方根误差
  setDefault(metricName -> "rmse")

  override def evaluate(dataset: DataFrame): Double = {
    val schema = dataset.schema
    SchemaUtils.checkColumnType(schema, $(predictionCol), DoubleType)
    SchemaUtils.checkColumnType(schema, $(labelCol), DoubleType)

    val predictionAndLabels = dataset.select($(predictionCol), $(labelCol))
      .map { case Row(prediction: Double, label: Double) =>
        (prediction, label)
      }     
    val metrics = new RegressionMetrics(predictionAndLabels)
    val metric = $(metricName) match {
      //均方根误差
      case "rmse" => metrics.rootMeanSquaredError
      //均方差
      case "mse" => metrics.meanSquaredError
      case "r2" => metrics.r2
      //平均绝对误差
      case "mae" => metrics.meanAbsoluteError
    }
    metric
  }

  override def isLargerBetter: Boolean = $(metricName) match {
    case "rmse" => false//均方根误差
    case "mse" => false//均方差
    case "r2" => true//平方系统
    case "mae" => false//平均绝对误差
  }

  override def copy(extra: ParamMap): RegressionEvaluator = defaultCopy(extra)
}
