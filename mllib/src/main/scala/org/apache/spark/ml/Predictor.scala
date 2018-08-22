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

package org.apache.spark.ml

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.mllib.linalg.{Vector, VectorUDT}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, DoubleType, StructType}
import org.apache.spark.sql.{DataFrame, Row}

/**
 * (private[ml])  Trait for parameters for prediction (regression and classification).
  * 私有[ml]预测参数的特征（回归和分类）
 */
private[ml] trait PredictorParams extends Params
  with HasLabelCol with HasFeaturesCol with HasPredictionCol {

  /**
   * Validates and transforms the input schema with the provided param map.
    * 使用提供的参数映射验证和转换输入模式
   * @param schema input schema
   * @param fitting whether this is in fitting
   * @param featuresDataType  SQL DataType for FeaturesType.
   *                          E.g., [[org.apache.spark.mllib.linalg.VectorUDT]] for vector features.
   * @return output schema
   */
  protected def validateAndTransformSchema(
      schema: StructType,
      fitting: Boolean,
      featuresDataType: DataType): StructType = {
    // TODO: Support casting Array[Double] and Array[Float] to Vector when FeaturesType = Vector
    SchemaUtils.checkColumnType(schema, $(featuresCol), featuresDataType)
    if (fitting) {
      // TODO: Allow other numeric types
      SchemaUtils.checkColumnType(schema, $(labelCol), DoubleType)
    }
    SchemaUtils.appendColumn(schema, $(predictionCol), DoubleType)
  }
}

/**
 * :: DeveloperApi ::
 * Abstraction for prediction problems (regression and classification).
  * 预测问题的抽象（回归和分类）
 *
 * @tparam FeaturesType  Type of features.
 *                       E.g., [[org.apache.spark.mllib.linalg.VectorUDT]] for vector features.
 * @tparam Learner  Specialization of this class.  If you subclass this type, use this type
 *                  parameter to specify the concrete type.
 * @tparam M  Specialization of [[PredictionModel]].  If you subclass this type, use this type
 *            parameter to specify the concrete type for the corresponding model.
 */
@DeveloperApi
abstract class Predictor[
    FeaturesType,
    Learner <: Predictor[FeaturesType, Learner, M],
    M <: PredictionModel[FeaturesType, M]]
  extends Estimator[M] with PredictorParams {

  /** @group setParam */
  def setLabelCol(value: String): Learner = set(labelCol, value).asInstanceOf[Learner]

  /** @group setParam */
  def setFeaturesCol(value: String): Learner = set(featuresCol, value).asInstanceOf[Learner]

  /** @group setParam */
  def setPredictionCol(value: String): Learner = set(predictionCol, value).asInstanceOf[Learner]

  override def fit(dataset: DataFrame): M = {
    // This handles a few items such as schema validation.
    // Developers only need to implement train().
    transformSchema(dataset.schema, logging = true)
    copyValues(train(dataset).setParent(this))
  }

  override def copy(extra: ParamMap): Learner

  /**
   * Train a model using the given dataset and parameters.
    * 使用给定的数据集和参数训练模型
   * Developers can implement this instead of [[fit()]] to avoid dealing with schema validation
   * and copying parameters into the model.
    * 开发人员可以实现此而不是[[fit()]]来避免处理模式验证并将参数复制到模型中
   *
   * @param dataset  Training dataset
   * @return  Fitted model
   */
  protected def train(dataset: DataFrame): M

  /**
   * Returns the SQL DataType corresponding to the FeaturesType type parameter.
   * 返回与Feature Type类型参数对应的SQL数据类型
   * This is used by [[validateAndTransformSchema()]].
   * This workaround is needed since SQL has different APIs for Scala and Java.
    * 由于SQL具有针对Scala和Java的不同API,因此需要此解决方法
   *
   * The default value is VectorUDT, but it may be overridden if FeaturesType is not Vector.
    * 默认值为VectorUDT,但如果FeaturesType不是Vector,则可以覆盖它
   */
  private[ml] def featuresDataType: DataType = new VectorUDT

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema, fitting = true, featuresDataType)
  }

  /**
   * Extract [[labelCol]] and [[featuresCol]] from the given dataset,
   * and put it in an RDD with strong types.
    * 从给定数据集中提取[[labelCol]]和[[featuresCol]],并将其放在具有强类型的RDD中
   */
  protected def extractLabeledPoints(dataset: DataFrame): RDD[LabeledPoint] = {
    dataset.select($(labelCol), $(featuresCol))
      .map { case Row(label: Double, features: Vector) => LabeledPoint(label, features) }
  }
}

/**
 * :: DeveloperApi ::
 * Abstraction for a model for prediction tasks (regression and classification).
 * 用于预测任务的模型的抽象(回归和分类)
 * @tparam FeaturesType  Type of features.
 *                       E.g., [[org.apache.spark.mllib.linalg.VectorUDT]] for vector features.
 * @tparam M  Specialization of [[PredictionModel]].  If you subclass this type, use this type
 *            parameter to specify the concrete type for the corresponding model.
 */
@DeveloperApi
abstract class PredictionModel[FeaturesType, M <: PredictionModel[FeaturesType, M]]
  extends Model[M] with PredictorParams {

  /** @group setParam */
  def setFeaturesCol(value: String): M = set(featuresCol, value).asInstanceOf[M]

  /** @group setParam */
  def setPredictionCol(value: String): M = set(predictionCol, value).asInstanceOf[M]

  /**
   * Returns the SQL DataType corresponding to the FeaturesType type parameter.
   * 返回与Feature Type类型参数对应的SQL数据类型
   * This is used by [[validateAndTransformSchema()]].
   * This workaround is needed since SQL has different APIs for Scala and Java.
   * 由于SQL具有针对Scala和Java的不同API，因此需要此解决方法
   * The default value is VectorUDT, but it may be overridden if FeaturesType is not Vector.
   */
  protected def featuresDataType: DataType = new VectorUDT

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema, fitting = false, featuresDataType)
  }

  /**
   * Transforms dataset by reading from [[featuresCol]], calling [[predict()]], and storing
   * the predictions as a new column [[predictionCol]].
   * 通过读取[[featuresCol]],调用[[predict()]]并将预测存储为新列[[predictionCol]]来转换数据集
   * @param dataset input dataset
   * @return transformed dataset with [[predictionCol]] of type [[Double]]
   */
  override def transform(dataset: DataFrame): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    if ($(predictionCol).nonEmpty) {
      transformImpl(dataset)
    } else {
      this.logWarning(s"$uid: Predictor.transform() was called as NOOP" +
        " since no output columns were set.")
      dataset
    }
  }

  protected def transformImpl(dataset: DataFrame): DataFrame = {
    val predictUDF = udf { (features: Any) =>
      predict(features.asInstanceOf[FeaturesType])
    }
    dataset.withColumn($(predictionCol), predictUDF(col($(featuresCol))))
  }

  /**
   * Predict label for the given features.
   * This internal method is used to implement [[transform()]] and output [[predictionCol]].
   */
  protected def predict(features: FeaturesType): Double
}
