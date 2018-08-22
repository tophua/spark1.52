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

import scala.annotation.varargs

import org.apache.spark.Logging
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * :: DeveloperApi ::
 * Abstract class for transformers that transform one dataset into another.
  * 变换器的抽象类,用于将一个数据集转换为另一个数据集
 */
@DeveloperApi
abstract class Transformer extends PipelineStage {

  /**
   * Transforms the dataset with optional parameters
    * 使用可选参数转换数据集
   * @param dataset input dataset
   * @param firstParamPair the first param pair, overwrite embedded params
   * @param otherParamPairs other param pairs, overwrite embedded params
   * @return transformed dataset
   */
  @varargs
  def transform(
      dataset: DataFrame,
      firstParamPair: ParamPair[_],
      otherParamPairs: ParamPair[_]*): DataFrame = {
    val map = new ParamMap()
      .put(firstParamPair)
      .put(otherParamPairs: _*)
    transform(dataset, map)
  }

  /**
   * Transforms the dataset with provided parameter map as additional parameters.
    * 使用提供的参数映射将数据集转换为附加参数
   * @param dataset input dataset
   * @param paramMap additional parameters, overwrite embedded params
   * @return transformed dataset
   */
  def transform(dataset: DataFrame, paramMap: ParamMap): DataFrame = {
    this.copy(paramMap).transform(dataset)
  }

  /**
   * Transforms the input dataset.
    * 转换输入数据集
   */
  def transform(dataset: DataFrame): DataFrame

  override def copy(extra: ParamMap): Transformer
}

/**
 * :: DeveloperApi ::
 * Abstract class for transformers that take one input column, apply transformation, and output the
 * result as a new column.
  * 变换器的抽象类,它接受一个输入列,应用转换,并将结果输出为新列
 */
@DeveloperApi
abstract class UnaryTransformer[IN, OUT, T <: UnaryTransformer[IN, OUT, T]]
  extends Transformer with HasInputCol with HasOutputCol with Logging {

  /** @group setParam */
  def setInputCol(value: String): T = set(inputCol, value).asInstanceOf[T]

  /** @group setParam */
  def setOutputCol(value: String): T = set(outputCol, value).asInstanceOf[T]

  /**
   * Creates the transform function using the given param map. The input param map already takes
   * account of the embedded param map. So the param values should be determined solely by the input
   * param map.
    * 使用给定的param映射创建转换函数,输入参数映射已经考虑了嵌入的参数映射, 因此,参数值应仅由输入参数图确定
   */
  protected def createTransformFunc: IN => OUT

  /**
   * Returns the data type of the output column.
    * 返回输出列的数据类型
   */
  protected def outputDataType: DataType

  /**
   * Validates the input type. Throw an exception if it is invalid.
    * 验证输入类型,如果无效则抛出异常
   */
  protected def validateInputType(inputType: DataType): Unit = {}

  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    validateInputType(inputType)
    if (schema.fieldNames.contains($(outputCol))) {
      throw new IllegalArgumentException(s"Output column ${$(outputCol)} already exists.")
    }
    val outputFields = schema.fields :+
      StructField($(outputCol), outputDataType, nullable = false)
    StructType(outputFields)
  }

  override def transform(dataset: DataFrame): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    dataset.withColumn($(outputCol),
      callUDF(this.createTransformFunc, outputDataType, dataset($(inputCol))))
  }

  override def copy(extra: ParamMap): T = defaultCopy(extra)
}
