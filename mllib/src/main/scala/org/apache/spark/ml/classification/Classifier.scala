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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.{PredictionModel, PredictorParams, Predictor}
import org.apache.spark.ml.param.shared.HasRawPredictionCol
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.mllib.linalg.{Vector, VectorUDT}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, StructType}


/**
 * (private[spark]) Params for classification.
  * 私有[Spark]参数分类
 */
private[spark] trait ClassifierParams
  extends PredictorParams with HasRawPredictionCol {

  override protected def validateAndTransformSchema(
      schema: StructType,
      fitting: Boolean,
      featuresDataType: DataType): StructType = {
    val parentSchema = super.validateAndTransformSchema(schema, fitting, featuresDataType)
    SchemaUtils.appendColumn(parentSchema, $(rawPredictionCol), new VectorUDT)
  }
}

/**
 * :: DeveloperApi ::
 *
 * Single-label binary or multiclass classification.
  * 单标签二进制或多类分类
 * Classes are indexed {0, 1, ..., numClasses - 1}.
 *
 * @tparam FeaturesType  Type of input features.  E.g., [[Vector]]
 * @tparam E  Concrete Estimator type
 * @tparam M  Concrete Model type
 */
@DeveloperApi
abstract class Classifier[
    FeaturesType,
    E <: Classifier[FeaturesType, E, M],
    M <: ClassificationModel[FeaturesType, M]]
  extends Predictor[FeaturesType, E, M] with ClassifierParams {

  /** @group setParam */
  def setRawPredictionCol(value: String): E = set(rawPredictionCol, value).asInstanceOf[E]

  // TODO: defaultEvaluator (follow-up PR)
}

/**
 * :: DeveloperApi ::
 *
 * Model produced by a [[Classifier]].
  * 由[[Classifier]]生成的模型
 * Classes are indexed {0, 1, ..., numClasses - 1}.
 *
 * @tparam FeaturesType  Type of input features.  E.g., [[Vector]]
 * @tparam M  Concrete Model type
 */
@DeveloperApi
abstract class ClassificationModel[FeaturesType, M <: ClassificationModel[FeaturesType, M]]
  extends PredictionModel[FeaturesType, M] with ClassifierParams {

  /** 
  * @group setParam 
  * 原始的算法预测结果的存储列的名称, 默认是”rawPrediction”
  */
  def setRawPredictionCol(value: String): M = set(rawPredictionCol, value).asInstanceOf[M]

  /** Number of classes (values which the label can take). */
  def numClasses: Int

  /**
   * Transforms dataset by reading from [[featuresCol]], and appending new columns as specified by
   * parameters:
   *  - predicted labels as [[predictionCol]] of type [[Double]]
   *  - raw predictions (confidences) as [[rawPredictionCol]] of type [[Vector]].
   *  probabilityCol 类别预测结果的条件概率值存储列的名称, 默认值是”probability”
   *  rawPredictionCol 原始的算法预测结果的存储列的名称, 默认是”rawPrediction”
   * @param dataset input dataset
   * @return transformed dataset
   */
  override def transform(dataset: DataFrame): DataFrame = {
    transformSchema(dataset.schema, logging = true)

    // Output selected columns only.仅输出所选列
    // This is a bit complicated since it tries to avoid repeated computation.
    //这有点复杂,因为它试图避免重复计算
    var outputData = dataset
    var numColsOutput = 0
    if (getRawPredictionCol != "") {
      val predictRawUDF = udf { (features: Any) =>
        predictRaw(features.asInstanceOf[FeaturesType])
      }
      outputData = outputData.withColumn(getRawPredictionCol, predictRawUDF(col(getFeaturesCol)))
      numColsOutput += 1
    }
    if (getPredictionCol != "") {
      val predUDF = if (getRawPredictionCol != "") {
        udf(raw2prediction _).apply(col(getRawPredictionCol))
      } else {
        val predictUDF = udf { (features: Any) =>
          predict(features.asInstanceOf[FeaturesType])
        }
        predictUDF(col(getFeaturesCol))
      }
      outputData = outputData.withColumn(getPredictionCol, predUDF)
      numColsOutput += 1
    }

    if (numColsOutput == 0) {
      logWarning(s"$uid: ClassificationModel.transform() was called as NOOP" +
        " since no output columns were set.")
    }
    outputData
  }

  /**
   * Predict label for the given features.
   * This internal method is used to implement [[transform()]] and output [[predictionCol]].
    * 此内部方法用于实现[[transform()]]和输出[[predictionCol]]
   *
   * This default implementation for classification predicts the index of the maximum value
   * from [[predictRaw()]].
   */
  override protected def predict(features: FeaturesType): Double = {
    raw2prediction(predictRaw(features))
  }

  /**
   * Raw prediction for each possible label.
    * 每个可能标签的原始预测
   * The meaning of a "raw" prediction may vary between algorithms, but it intuitively gives
   * a measure of confidence in each possible label (where larger = more confident).
    * “原始”预测的含义可能因算法而异,但它直观地给出了每个可能标签的置信度(更大=更自信)
   * This internal method is used to implement [[transform()]] and output [[rawPredictionCol]].
   *
   * @return  vector where element i is the raw prediction for label i.
   *          This raw prediction may be any real number, where a larger value indicates greater
   *          confidence for that label.
   */
  protected def predictRaw(features: FeaturesType): Vector

  /**
   * Given a vector of raw predictions, select the predicted label.
    * 给定原始预测的矢量,选择预测标签
   * This may be overridden to support thresholds which favor particular labels.
    * 这可能会被覆盖以支持有利于特定标签的阈值
   * @return  predicted label
   */
  protected def raw2prediction(rawPrediction: Vector): Double = rawPrediction.argmax
}
