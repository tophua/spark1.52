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

package org.apache.spark.ml.attribute

import scala.annotation.varargs

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.types.{DoubleType, NumericType, Metadata, MetadataBuilder, StructField}

/**
 * :: DeveloperApi ::
 * Abstract class for ML attributes.
  * ML属性的抽象类
 */
@DeveloperApi
sealed abstract class Attribute extends Serializable {

  name.foreach { n =>
    require(n.nonEmpty, "Cannot have an empty string for name.")
  }
  index.foreach { i =>
    require(i >= 0, s"Index cannot be negative but got $i")
  }

  /** Attribute type. 属性类型*/
  def attrType: AttributeType

  /** Name of the attribute. None if it is not set.
    * 属性的名称,没有,如果没有设置*/
  def name: Option[String]

  /** Copy with a new name. 使用新名称复制*/
  def withName(name: String): Attribute

  /** Copy without the name. 没有名字的复制*/
  def withoutName: Attribute

  /** Index of the attribute. None if it is not set. 属性索引,没有,如果没有设置。*/
  def index: Option[Int]

  /** Copy with a new index. 使用新索引复制*/
  def withIndex(index: Int): Attribute

  /** Copy without the index. 没有索引的复制*/
  def withoutIndex: Attribute

  /**
   * Tests whether this attribute is numeric, true for [[NumericAttribute]] and [[BinaryAttribute]].
    * 测试此属性是否为数字,[[NumericAttribute]]和[[BinaryAttribute]]为true
   */
  def isNumeric: Boolean

  /**
   * Tests whether this attribute is nominal, true for [[NominalAttribute]] and [[BinaryAttribute]].
    * 测试此属性是否为名义,[[NominalAttribute]]和[[BinaryAttribute]]为true
   */
  def isNominal: Boolean

  /**
   * Converts this attribute to [[Metadata]].
   * @param withType whether to include the type info
   */
  private[attribute] def toMetadataImpl(withType: Boolean): Metadata

  /**
   * Converts this attribute to [[Metadata]]. For numeric attributes, the type info is excluded to
   * save space, because numeric type is the default attribute type. For nominal and binary
   * attributes, the type info is included.
    * 将此属性转换为[[Metadata]],对于数字属性,排除类型信息以节省空间,
    * 因为数字类型是默认属性类型,对于名义和二进制属性,包括类型信息
   */
  private[attribute] def toMetadataImpl(): Metadata = {
    if (attrType == AttributeType.Numeric) {
      toMetadataImpl(withType = false)
    } else {
      toMetadataImpl(withType = true)
    }
  }

  /** Converts to ML metadata with some existing metadata.
    * 使用一些现有元数据转换为ML元数据*/
  def toMetadata(existingMetadata: Metadata): Metadata = {
    new MetadataBuilder()
      .withMetadata(existingMetadata)
      .putMetadata(AttributeKeys.ML_ATTR, toMetadataImpl())
      .build()
  }

  /** Converts to ML metadata 转换为ML元数据*/
  def toMetadata(): Metadata = toMetadata(Metadata.empty)

  /**
   * Converts to a [[StructField]] with some existing metadata.
    * 使用一些现有元数据转换为[[StructField]]
   * @param existingMetadata existing metadata to carry over
   */
  def toStructField(existingMetadata: Metadata): StructField = {
    val newMetadata = new MetadataBuilder()
      .withMetadata(existingMetadata)
      .putMetadata(AttributeKeys.ML_ATTR, withoutName.withoutIndex.toMetadataImpl())
      .build()
    StructField(name.get, DoubleType, nullable = false, newMetadata)
  }

  /** Converts to a [[StructField]].
    * 转换为[[StructField]]*/
  def toStructField(): StructField = toStructField(Metadata.empty)

  override def toString: String = toMetadataImpl(withType = true).toString
}

/** Trait for ML attribute factories.
  * ML属性工厂的特征*/
private[attribute] trait AttributeFactory {

  /**
   * Creates an [[Attribute]] from a [[Metadata]] instance.
    * 从[[Metadata]]实例创建[[Attribute]]
   */
  private[attribute] def fromMetadata(metadata: Metadata): Attribute

  /**
   * Creates an [[Attribute]] from a [[StructField]] instance.
    * 从[[StructField]]实例创建[[Attribute]]
   */
  def fromStructField(field: StructField): Attribute = {
    require(field.dataType.isInstanceOf[NumericType])
    val metadata = field.metadata
    val mlAttr = AttributeKeys.ML_ATTR
    if (metadata.contains(mlAttr)) {
      fromMetadata(metadata.getMetadata(mlAttr)).withName(field.name)
    } else {
      UnresolvedAttribute
    }
  }
}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
object Attribute extends AttributeFactory {

  private[attribute] override def fromMetadata(metadata: Metadata): Attribute = {
    import org.apache.spark.ml.attribute.AttributeKeys._
    val attrType = if (metadata.contains(TYPE)) {
      metadata.getString(TYPE)
    } else {
      AttributeType.Numeric.name
    }
    getFactory(attrType).fromMetadata(metadata)
  }

  /** Gets the attribute factory given the attribute type name.
    * 获取给定属性类型名称的属性工厂 */
  private def getFactory(attrType: String): AttributeFactory = {
    if (attrType == AttributeType.Numeric.name) {
      NumericAttribute
    } else if (attrType == AttributeType.Nominal.name) {
      NominalAttribute
    } else if (attrType == AttributeType.Binary.name) {
      BinaryAttribute
    } else {
      throw new IllegalArgumentException(s"Cannot recognize type $attrType.")
    }
  }
}


/**
 * :: DeveloperApi ::
 * A numeric attribute with optional summary statistics.
  * 带有可选摘要统计信息的数字属性
 * @param name optional name
 * @param index optional index
 * @param min optional min value
 * @param max optional max value
 * @param std optional standard deviation
 * @param sparsity optional sparsity (ratio of zeros)
 */
@DeveloperApi
class NumericAttribute private[ml] (
    override val name: Option[String] = None,
    override val index: Option[Int] = None,
    val min: Option[Double] = None,
    val max: Option[Double] = None,
    val std: Option[Double] = None,
    val sparsity: Option[Double] = None) extends Attribute {

  std.foreach { s =>
    require(s >= 0.0, s"Standard deviation cannot be negative but got $s.")
  }
  sparsity.foreach { s =>
    require(s >= 0.0 && s <= 1.0, s"Sparsity must be in [0, 1] but got $s.")
  }

  override def attrType: AttributeType = AttributeType.Numeric

  override def withName(name: String): NumericAttribute = copy(name = Some(name))
  override def withoutName: NumericAttribute = copy(name = None)

  override def withIndex(index: Int): NumericAttribute = copy(index = Some(index))
  override def withoutIndex: NumericAttribute = copy(index = None)

  /** Copy with a new min value. 使用新的最小值复制*/
  def withMin(min: Double): NumericAttribute = copy(min = Some(min))

  /** Copy without the min value. 复制没有最小值*/
  def withoutMin: NumericAttribute = copy(min = None)


  /** Copy with a new max value. 使用新的最大值复制*/
  def withMax(max: Double): NumericAttribute = copy(max = Some(max))

  /** Copy without the max value. 复制没有最大值*/
  def withoutMax: NumericAttribute = copy(max = None)

  /** Copy with a new standard deviation. 使用新的标准偏差复制*/
  def withStd(std: Double): NumericAttribute = copy(std = Some(std))

  /** Copy without the standard deviation. 没有标准偏差的复制*/
  def withoutStd: NumericAttribute = copy(std = None)

  /** Copy with a new sparsity. 复制一个新的稀疏性*/
  def withSparsity(sparsity: Double): NumericAttribute = copy(sparsity = Some(sparsity))

  /** Copy without the sparsity. 复制没有稀疏性*/
  def withoutSparsity: NumericAttribute = copy(sparsity = None)

  /** Copy without summary statistics. 复制没有摘要统计*/
  def withoutSummary: NumericAttribute = copy(min = None, max = None, std = None, sparsity = None)

  override def isNumeric: Boolean = true

  override def isNominal: Boolean = false

  /** Convert this attribute to metadata.将此属性转换为元数据 */
  override private[attribute] def toMetadataImpl(withType: Boolean): Metadata = {
    import org.apache.spark.ml.attribute.AttributeKeys._
    val bldr = new MetadataBuilder()
    if (withType) bldr.putString(TYPE, attrType.name)
    name.foreach(bldr.putString(NAME, _))
    index.foreach(bldr.putLong(INDEX, _))
    min.foreach(bldr.putDouble(MIN, _))
    max.foreach(bldr.putDouble(MAX, _))
    std.foreach(bldr.putDouble(STD, _))
    sparsity.foreach(bldr.putDouble(SPARSITY, _))
    bldr.build()
  }

  /** Creates a copy of this attribute with optional changes. 使用可选更改创建此属性的副本*/
  private def copy(
      name: Option[String] = name,
      index: Option[Int] = index,
      min: Option[Double] = min,
      max: Option[Double] = max,
      std: Option[Double] = std,
      sparsity: Option[Double] = sparsity): NumericAttribute = {
    new NumericAttribute(name, index, min, max, std, sparsity)
  }

  override def equals(other: Any): Boolean = {
    other match {
      case o: NumericAttribute =>
        (name == o.name) &&
          (index == o.index) &&
          (min == o.min) &&
          (max == o.max) &&
          (std == o.std) &&
          (sparsity == o.sparsity)
      case _ =>
        false
    }
  }

  override def hashCode: Int = {
    var sum = 17
    sum = 37 * sum + name.hashCode
    sum = 37 * sum + index.hashCode
    sum = 37 * sum + min.hashCode
    sum = 37 * sum + max.hashCode
    sum = 37 * sum + std.hashCode
    sum = 37 * sum + sparsity.hashCode
    sum
  }
}

/**
 * :: DeveloperApi ::
 * Factory methods for numeric attributes.
  * 数字属性的工厂方法
 */
@DeveloperApi
object NumericAttribute extends AttributeFactory {

  /** The default numeric attribute.
    * 默认的数字属性*/
  val defaultAttr: NumericAttribute = new NumericAttribute

  private[attribute] override def fromMetadata(metadata: Metadata): NumericAttribute = {
    import org.apache.spark.ml.attribute.AttributeKeys._
    val name = if (metadata.contains(NAME)) Some(metadata.getString(NAME)) else None
    val index = if (metadata.contains(INDEX)) Some(metadata.getLong(INDEX).toInt) else None
    val min = if (metadata.contains(MIN)) Some(metadata.getDouble(MIN)) else None
    val max = if (metadata.contains(MAX)) Some(metadata.getDouble(MAX)) else None
    val std = if (metadata.contains(STD)) Some(metadata.getDouble(STD)) else None
    val sparsity = if (metadata.contains(SPARSITY)) Some(metadata.getDouble(SPARSITY)) else None
    new NumericAttribute(name, index, min, max, std, sparsity)
  }
}

/**
 * :: DeveloperApi ::
 * A nominal attribute.名义属性
 * @param name optional name
 * @param index optional index
 * @param isOrdinal whether this attribute is ordinal (optional)
 * @param numValues optional number of values. At most one of `numValues` and `values` can be
 *                  defined.
 * @param values optional values. At most one of `numValues` and `values` can be defined.
 */
@DeveloperApi
class NominalAttribute private[ml] (
    override val name: Option[String] = None,
    override val index: Option[Int] = None,
    val isOrdinal: Option[Boolean] = None,
    val numValues: Option[Int] = None,
    val values: Option[Array[String]] = None) extends Attribute {

  numValues.foreach { n =>
    require(n >= 0, s"numValues cannot be negative but got $n.")
  }
  require(!(numValues.isDefined && values.isDefined),
    "Cannot have both numValues and values defined.")

  override def attrType: AttributeType = AttributeType.Nominal

  override def isNumeric: Boolean = false

  override def isNominal: Boolean = true

  private lazy val valueToIndex: Map[String, Int] = {
    values.map(_.zipWithIndex.toMap).getOrElse(Map.empty)
  }

  /** Index of a specific value. 特定值的索引*/
  def indexOf(value: String): Int = {
    valueToIndex(value)
  }

  /** Tests whether this attribute contains a specific value. 测试此属性是否包含特定值*/
  def hasValue(value: String): Boolean = valueToIndex.contains(value)

  /** Gets a value given its index. 获取给定索引的值*/
  def getValue(index: Int): String = values.get(index)

  override def withName(name: String): NominalAttribute = copy(name = Some(name))
  override def withoutName: NominalAttribute = copy(name = None)

  override def withIndex(index: Int): NominalAttribute = copy(index = Some(index))
  override def withoutIndex: NominalAttribute = copy(index = None)

  /** Copy with new values and empty `numValues`. 使用新值复制并清空`numValues`*/
  def withValues(values: Array[String]): NominalAttribute = {
    copy(numValues = None, values = Some(values))
  }

  /** Copy with new values and empty `numValues`. 使用新值复制并清空`numValues`*/
  @varargs
  def withValues(first: String, others: String*): NominalAttribute = {
    copy(numValues = None, values = Some((first +: others).toArray))
  }

  /** Copy without the values. 复制没有值*/
  def withoutValues: NominalAttribute = {
    copy(values = None)
  }

  /** Copy with a new `numValues` and empty `values`. 使用新的`numValues`复制并清空`values`*/
  def withNumValues(numValues: Int): NominalAttribute = {
    copy(numValues = Some(numValues), values = None)
  }

  /** Copy without the `numValues`. 复制没有`numValues`*/
  def withoutNumValues: NominalAttribute = copy(numValues = None)

  /**
   * Get the number of values, either from `numValues` or from `values`.
   * Return None if unknown.
    * 从`numValues`或`values`获取值的数量,如果不知道则返回None
   */
  def getNumValues: Option[Int] = {
    if (numValues.nonEmpty) {
      numValues
    } else if (values.nonEmpty) {
      Some(values.get.length)
    } else {
      None
    }
  }

  /** 
   *  Creates a copy of this attribute with optional changes.
   *  创建可更改属性的副本. 
   *  */
  private def copy(
      name: Option[String] = name,
      index: Option[Int] = index,
      isOrdinal: Option[Boolean] = isOrdinal,
      numValues: Option[Int] = numValues,
      values: Option[Array[String]] = values): NominalAttribute = {
    new NominalAttribute(name, index, isOrdinal, numValues, values)
  }

  override private[attribute] def toMetadataImpl(withType: Boolean): Metadata = {
    import org.apache.spark.ml.attribute.AttributeKeys._
    val bldr = new MetadataBuilder()
    if (withType) bldr.putString(TYPE, attrType.name)
    name.foreach(bldr.putString(NAME, _))
    index.foreach(bldr.putLong(INDEX, _))
    isOrdinal.foreach(bldr.putBoolean(ORDINAL, _))
    numValues.foreach(bldr.putLong(NUM_VALUES, _))
    values.foreach(v => bldr.putStringArray(VALUES, v))
    bldr.build()
  }

  override def equals(other: Any): Boolean = {
    other match {
      case o: NominalAttribute =>
        (name == o.name) &&
          (index == o.index) &&
          (isOrdinal == o.isOrdinal) &&
          (numValues == o.numValues) &&
          (values.map(_.toSeq) == o.values.map(_.toSeq))
      case _ =>
        false
    }
  }

  override def hashCode: Int = {
    var sum = 17
    sum = 37 * sum + name.hashCode
    sum = 37 * sum + index.hashCode
    sum = 37 * sum + isOrdinal.hashCode
    sum = 37 * sum + numValues.hashCode
    sum = 37 * sum + values.map(_.toSeq).hashCode
    sum
  }
}

/**
 * :: DeveloperApi ::
 * Factory methods for nominal attributes.
  * 标称属性的工厂方法
 */
@DeveloperApi
object NominalAttribute extends AttributeFactory {

  /** 
   *  The default nominal attribute.
   *  默认列名的属性 
   *  */
  final val defaultAttr: NominalAttribute = new NominalAttribute

  private[attribute] override def fromMetadata(metadata: Metadata): NominalAttribute = {
    import org.apache.spark.ml.attribute.AttributeKeys._
    val name = if (metadata.contains(NAME)) Some(metadata.getString(NAME)) else None
    val index = if (metadata.contains(INDEX)) Some(metadata.getLong(INDEX).toInt) else None
    val isOrdinal = if (metadata.contains(ORDINAL)) Some(metadata.getBoolean(ORDINAL)) else None
    val numValues =
      if (metadata.contains(NUM_VALUES)) Some(metadata.getLong(NUM_VALUES).toInt) else None
    val values =
      if (metadata.contains(VALUES)) Some(metadata.getStringArray(VALUES)) else None
    new NominalAttribute(name, index, isOrdinal, numValues, values)
  }
}

/**
 * :: DeveloperApi ::
 * A binary attribute.
  * 二进制属性
 * @param name optional name
 * @param index optional index
 * @param values optionla values. If set, its size must be 2.
 */
@DeveloperApi
class BinaryAttribute private[ml] (
    override val name: Option[String] = None,
    override val index: Option[Int] = None,
    val values: Option[Array[String]] = None)
  extends Attribute {

  values.foreach { v =>
    require(v.length == 2, s"Number of values must be 2 for a binary attribute but got ${v.toSeq}.")
  }

  override def attrType: AttributeType = AttributeType.Binary

  override def isNumeric: Boolean = true

  override def isNominal: Boolean = true

  override def withName(name: String): BinaryAttribute = copy(name = Some(name))
  override def withoutName: BinaryAttribute = copy(name = None)

  override def withIndex(index: Int): BinaryAttribute = copy(index = Some(index))
  override def withoutIndex: BinaryAttribute = copy(index = None)

  /**
   * Copy with new values.使用新值复制
   * @param negative name for negative
   * @param positive name for positive
   */
  def withValues(negative: String, positive: String): BinaryAttribute =
    copy(values = Some(Array(negative, positive)))

  /** Copy without the values. 复制没有值*/
  def withoutValues: BinaryAttribute = copy(values = None)

  /** Creates a copy of this attribute with optional changes.
    * 使用可选更改创建此属性的副本*/
  private def copy(
      name: Option[String] = name,
      index: Option[Int] = index,
      values: Option[Array[String]] = values): BinaryAttribute = {
    new BinaryAttribute(name, index, values)
  }

  override private[attribute] def toMetadataImpl(withType: Boolean): Metadata = {
    import org.apache.spark.ml.attribute.AttributeKeys._
    val bldr = new MetadataBuilder
    if (withType) bldr.putString(TYPE, attrType.name)
    name.foreach(bldr.putString(NAME, _))
    index.foreach(bldr.putLong(INDEX, _))
    values.foreach(v => bldr.putStringArray(VALUES, v))
    bldr.build()
  }

  override def equals(other: Any): Boolean = {
    other match {
      case o: BinaryAttribute =>
        (name == o.name) &&
          (index == o.index) &&
          (values.map(_.toSeq) == o.values.map(_.toSeq))
      case _ =>
        false
    }
  }

  override def hashCode: Int = {
    var sum = 17
    sum = 37 * sum + name.hashCode
    sum = 37 * sum + index.hashCode
    sum = 37 * sum + values.map(_.toSeq).hashCode
    sum
  }
}

/**
 * :: DeveloperApi ::
 * Factory methods for binary attributes.
  * 二进制属性的工厂方法
 */
@DeveloperApi
object BinaryAttribute extends AttributeFactory {

  /** The default binary attribute.
    * 默认的二进制属性*/
  final val defaultAttr: BinaryAttribute = new BinaryAttribute

  private[attribute] override def fromMetadata(metadata: Metadata): BinaryAttribute = {
    import org.apache.spark.ml.attribute.AttributeKeys._
    val name = if (metadata.contains(NAME)) Some(metadata.getString(NAME)) else None
    val index = if (metadata.contains(INDEX)) Some(metadata.getLong(INDEX).toInt) else None
    val values =
      if (metadata.contains(VALUES)) Some(metadata.getStringArray(VALUES)) else None
    new BinaryAttribute(name, index, values)
  }
}

/**
 * :: DeveloperApi ::
 * An unresolved attribute.
 */
@DeveloperApi
object UnresolvedAttribute extends Attribute {

  override def attrType: AttributeType = AttributeType.Unresolved

  override def withIndex(index: Int): Attribute = this

  override def isNumeric: Boolean = false

  override def withoutIndex: Attribute = this

  override def isNominal: Boolean = false

  override def name: Option[String] = None

  override private[attribute] def toMetadataImpl(withType: Boolean): Metadata = {
    Metadata.empty
  }

  override def withoutName: Attribute = this

  override def index: Option[Int] = None

  override def withName(name: String): Attribute = this

}
