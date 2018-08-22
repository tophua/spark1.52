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

package org.apache.spark.ml.feature

import org.apache.spark.annotation.Experimental
import org.apache.spark.ml._
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.linalg.{Vector, VectorUDT}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Params for [[StandardScaler]] and [[StandardScalerModel]].
 * StandardScaler 零均值规范化
 * 将某个特征向量(由所有样本某一个特征组成的向量)进行标准化,使数据均值为0,方差为1
 */
private[feature] trait StandardScalerParams extends Params with HasInputCol with HasOutputCol {

  /**
   * Centers the data with mean before scaling.
    * 在缩放之前使用均值将数据居中
   * It will build a dense output, so this does not work on sparse input
   * and will raise an exception.
    * 它将构建一个密集的输出,因此这不适用于稀疏输入并将引发异常
   * Default: false
   * @group param
   */
  val withMean: BooleanParam = new BooleanParam(this, "withMean", "Center data with mean")

  /**
   * Scales the data to unit standard deviation.
    * 将数据缩放到单位标准偏差
   * Default: true
   * @group param
   */
  val withStd: BooleanParam = new BooleanParam(this, "withStd", "Scale to unit standard deviation")
}

/**
 * :: Experimental ::
 * Standardizes features by removing the mean and scaling to unit variance using column summary
 * statistics on the samples in the training set.
  * 使用训练集中样本的列摘要统计信息,通过删除均值和缩放到单位方差来标准化功能
 */
@Experimental
class StandardScaler(override val uid: String) extends Estimator[StandardScalerModel]
  with StandardScalerParams {

  def this() = this(Identifiable.randomUID("stdScal"))

  setDefault(withMean -> false, withStd -> true)

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  def setWithMean(value: Boolean): this.type = set(withMean, value)

  /** @group setParam */
  def setWithStd(value: Boolean): this.type = set(withStd, value)

  override def fit(dataset: DataFrame): StandardScalerModel = {
    transformSchema(dataset.schema, logging = true)
    val input = dataset.select($(inputCol)).map { case Row(v: Vector) => v }
    val scaler = new feature.StandardScaler(withMean = $(withMean), withStd = $(withStd))
    val scalerModel = scaler.fit(input)
    copyValues(new StandardScalerModel(uid, scalerModel).setParent(this))
  }

  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    require(inputType.isInstanceOf[VectorUDT],
      s"Input column ${$(inputCol)} must be a vector column")
    require(!schema.fieldNames.contains($(outputCol)),
      s"Output column ${$(outputCol)} already exists.")
    val outputFields = schema.fields :+ StructField($(outputCol), new VectorUDT, false)
    StructType(outputFields)
  }

  override def copy(extra: ParamMap): StandardScaler = defaultCopy(extra)
}

/**
 * :: Experimental ::
 * Model fitted by [[StandardScaler]].
 */
@Experimental
class StandardScalerModel private[ml] (
    override val uid: String,
    scaler: feature.StandardScalerModel)
  extends Model[StandardScalerModel] with StandardScalerParams {

  /** Standard deviation of the StandardScalerModel
    * StandardScalerModel的标准偏差*/
  val std: Vector = scaler.std

  /** Mean of the StandardScalerModel */
  val mean: Vector = scaler.mean

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: DataFrame): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    val scale = udf { scaler.transform _ }
    dataset.withColumn($(outputCol), scale(col($(inputCol))))
  }

  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    require(inputType.isInstanceOf[VectorUDT],
      s"Input column ${$(inputCol)} must be a vector column")
    require(!schema.fieldNames.contains($(outputCol)),
      s"Output column ${$(outputCol)} already exists.")
    val outputFields = schema.fields :+ StructField($(outputCol), new VectorUDT, false)
    StructType(outputFields)
  }

  override def copy(extra: ParamMap): StandardScalerModel = {
    val copied = new StandardScalerModel(uid, scaler)
    copyValues(copied, extra).setParent(parent)
  }
}
