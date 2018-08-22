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

import java.{util => ju}

import org.apache.spark.SparkException
import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.Model
import org.apache.spark.ml.attribute.NominalAttribute
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util.{Identifiable, SchemaUtils}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

/**
 * :: Experimental ::
 * `Bucketizer` maps a column of continuous features to a column of feature buckets.
  * `Bucketizer`将一列连续特征映射到一列特征桶
 */
@Experimental
final class Bucketizer(override val uid: String)
  extends Model[Bucketizer] with HasInputCol with HasOutputCol {

  def this() = this(Identifiable.randomUID("bucketizer"))

  /**
   * Parameter for mapping continuous features into buckets. With n+1 splits, there are n buckets.
    * 用于将连续要素映射到存储桶的参数,对于n + 1个分裂,有n个桶
    *
   * A bucket defined by splits x,y holds values in the range [x,y) except the last bucket, which
   * also includes y. Splits should be strictly increasing.
    * 由拆分x，y定义的存储区包含除最后一个存储区之外的[x，y]范围内的值,
    * 由拆分x，y定义的存储区保存除最后一个存储区之外的[x，y]范围内的值，
   * Values at -inf, inf must be explicitly provided to cover all Double values;
   * otherwise, values outside the splits specified will be treated as errors.
    * 必须显式提供-inf，inf处的值以涵盖所有Double值;否则，指定的拆分之外的值将被视为错误。
   * @group param
   */
  val splits: DoubleArrayParam = new DoubleArrayParam(this, "splits",
    "Split points for mapping continuous features into buckets. With n+1 splits, there are n " +
      "buckets. A bucket defined by splits x,y holds values in the range [x,y) except the last " +
      "bucket, which also includes y. The splits should be strictly increasing. " +
      "Values at -inf, inf must be explicitly provided to cover all Double values; " +
      "otherwise, values outside the splits specified will be treated as errors.",
    Bucketizer.checkSplits)

  /** @group getParam */
  def getSplits: Array[Double] = $(splits)

  /** @group setParam */
  def setSplits(value: Array[Double]): this.type = set(splits, value)

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: DataFrame): DataFrame = {
    transformSchema(dataset.schema)
    val bucketizer = udf { feature: Double =>
      Bucketizer.binarySearchForBuckets($(splits), feature)
    }
    val newCol = bucketizer(dataset($(inputCol)))
    val newField = prepOutputField(dataset.schema)
    dataset.withColumn($(outputCol), newCol.as($(outputCol), newField.metadata))
  }

  private def prepOutputField(schema: StructType): StructField = {
    val buckets = $(splits).sliding(2).map(bucket => bucket.mkString(", ")).toArray
    val attr = new NominalAttribute(name = Some($(outputCol)), isOrdinal = Some(true),
      values = Some(buckets))
    attr.toStructField()
  }

  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(inputCol), DoubleType)
    SchemaUtils.appendColumn(schema, prepOutputField(schema))
  }

  override def copy(extra: ParamMap): Bucketizer = {
    defaultCopy[Bucketizer](extra).setParent(parent)
  }
}

private[feature] object Bucketizer {
  /** We require splits to be of length >= 3 and to be in strictly increasing order.
    * 我们要求拆分长度> = 3并且严格按顺序递增
    * */
  def checkSplits(splits: Array[Double]): Boolean = {
    if (splits.length < 3) {
      false
    } else {
      var i = 0
      val n = splits.length - 1
      while (i < n) {
        if (splits(i) >= splits(i + 1)) return false
        i += 1
      }
      true
    }
  }

  /**
   * Binary searching in several buckets to place each data point.
    * 在几个桶中进行二进制搜索以放置每个数据点
   * @throws SparkException if a feature is < splits.head or > splits.last
   */
  def binarySearchForBuckets(splits: Array[Double], feature: Double): Double = {
    if (feature == splits.last) {
      splits.length - 2
    } else {
      val idx = ju.Arrays.binarySearch(splits, feature)
      if (idx >= 0) {
        idx
      } else {
        val insertPos = -idx - 1
        if (insertPos == 0 || insertPos == splits.length) {
          throw new SparkException(s"Feature value $feature out of Bucketizer bounds" +
            s" [${splits.head}, ${splits.last}].  Check your features, or loosen " +
            s"the lower/upper bound constraints.")
        } else {
          insertPos - 1
        }
      }
    }
  }
}
