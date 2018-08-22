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

package org.apache.spark.sql.types

import scala.collection.mutable.ArrayBuffer
import scala.math.max

import org.json4s.JsonDSL._

import org.apache.spark.SparkException
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, InterpretedOrdering}


/**
 * :: DeveloperApi ::
 * A [[StructType]] object can be constructed by
  * 可以通过构造[[StructType]]对象
 * {{{
 * StructType(fields: Seq[StructField])
 * }}}
 * For a [[StructType]] object, one or multiple [[StructField]]s can be extracted by names.
  * 对于[[StructType]]对象，可以通过名称提取一个或多个[[StructField]]
 * If multiple [[StructField]]s are extracted, a [[StructType]] object will be returned.
  *如果提取了多个[[StructField]]，将返回[[StructType]]对象。
 * If a provided name does not have a matching field, it will be ignored. For the case
 * of extracting a single StructField, a `null` will be returned.
  * 如果提供的名称没有匹配的字段,则将忽略该字段,对于提取单个StructField的情况,将返回“null”
 * Example:
 * {{{
 * import org.apache.spark.sql._
 *
 * val struct =
 *   StructType(
 *     StructField("a", IntegerType, true) ::
 *     StructField("b", LongType, false) ::
 *     StructField("c", BooleanType, false) :: Nil)
 *
 * // Extract a single StructField.
 * val singleField = struct("b")
 * // singleField: StructField = StructField(b,LongType,false)
 *
 * // This struct does not have a field called "d". null will be returned.
 * val nonExisting = struct("d")
 * // nonExisting: StructField = null
 *
 * // Extract multiple StructFields. Field names are provided in a set.
 * // A StructType object will be returned.
 * val twoFields = struct(Set("b", "c"))
 * // twoFields: StructType =
 * //   StructType(List(StructField(b,LongType,false), StructField(c,BooleanType,false)))
 *
 * // Any names without matching fields will be ignored.
 * // For the case shown below, "d" will be ignored and
 * // it is treated as struct(Set("b", "c")).
 * val ignoreNonExisting = struct(Set("b", "c", "d"))
 * // ignoreNonExisting: StructType =
 * //   StructType(List(StructField(b,LongType,false), StructField(c,BooleanType,false)))
 * }}}
 *
 * A [[org.apache.spark.sql.Row]] object is used as a value of the StructType.
 * Example:
 * {{{
 * import org.apache.spark.sql._
 *
 * val innerStruct =
 *   StructType(
 *     StructField("f1", IntegerType, true) ::
 *     StructField("f2", LongType, false) ::
 *     StructField("f3", BooleanType, false) :: Nil)
 *
 * val struct = StructType(
 *   StructField("a", innerStruct, true) :: Nil)
 *
 * // Create a Row with the schema defined by struct
 * val row = Row(Row(1, 2, true))
 * // row: Row = [[1,2,true]]
 * }}}
 */
@DeveloperApi
/**
 * StructType代表一张表
 */
case class StructType(fields: Array[StructField]) extends DataType with Seq[StructField] {

  /** No-arg constructor for kryo. */
  def this() = this(Array.empty[StructField])

  /** Returns all field names in an array.
    * 返回数组中的所有字段名称*/
  def fieldNames: Array[String] = fields.map(_.name)

  private lazy val fieldNamesSet: Set[String] = fieldNames.toSet
  private lazy val nameToField: Map[String, StructField] = fields.map(f => f.name -> f).toMap
  private lazy val nameToIndex: Map[String, Int] = fieldNames.zipWithIndex.toMap

  /**
   * Creates a new [[StructType]] by adding a new field.
    * 通过添加新字段创建新的[[StructType]]
   * {{{
   * val struct = (new StructType)
   *   .add(StructField("a", IntegerType, true))
   *   .add(StructField("b", LongType, false))
   *   .add(StructField("c", StringType, true))
   *}}}
   */
  def add(field: StructField): StructType = {
    StructType(fields :+ field)
  }

  /**
   * Creates a new [[StructType]] by adding a new nullable field with no metadata.
    * 通过添加没有元数据的新可空字段来创建新的[[StructType]]
   *
   * val struct = (new StructType)
   *   .add("a", IntegerType)
   *   .add("b", LongType)
   *   .add("c", StringType)
   */
  def add(name: String, dataType: DataType): StructType = {
    StructType(fields :+ new StructField(name, dataType, nullable = true, Metadata.empty))
  }

  /**
   * Creates a new [[StructType]] by adding a new field with no metadata.
   * 通过添加没有元数据的新字段来创建新的[[StructType]]
   * val struct = (new StructType)
   *   .add("a", IntegerType, true)
   *   .add("b", LongType, false)
   *   .add("c", StringType, true)
   */
  def add(name: String, dataType: DataType, nullable: Boolean): StructType = {
    StructType(fields :+ new StructField(name, dataType, nullable, Metadata.empty))
  }

  /**
   * Creates a new [[StructType]] by adding a new field and specifying metadata.
    * 通过添加新字段并指定元数据来创建新的[[StructType]]
   * {{{
   * val struct = (new StructType)
   *   .add("a", IntegerType, true, Metadata.empty)
   *   .add("b", LongType, false, Metadata.empty)
   *   .add("c", StringType, true, Metadata.empty)
   * }}}
   */
  def add(
      name: String,
      dataType: DataType,
      nullable: Boolean,
      metadata: Metadata): StructType = {
    StructType(fields :+ new StructField(name, dataType, nullable, metadata))
  }

  /**
   * Creates a new [[StructType]] by adding a new nullable field with no metadata where the
   * dataType is specified as a String.
   * 通过添加一个没有元数据的新的可空字段来创建新的[[StructType]],其中dataType被指定为String
   * {{{
   * val struct = (new StructType)
   *   .add("a", "int")
   *   .add("b", "long")
   *   .add("c", "string")
   * }}}
   */
  def add(name: String, dataType: String): StructType = {
    add(name, DataTypeParser.parse(dataType), nullable = true, Metadata.empty)
  }

  /**
   * Creates a new [[StructType]] by adding a new field with no metadata where the
   * dataType is specified as a String.
   * 通过添加没有元数据的新字段来创建新的[[StructType]],其中dataType被指定为String。
   * {{{
   * val struct = (new StructType)
   *   .add("a", "int", true)
   *   .add("b", "long", false)
   *   .add("c", "string", true)
   * }}}
   */
  def add(name: String, dataType: String, nullable: Boolean): StructType = {
    add(name, DataTypeParser.parse(dataType), nullable, Metadata.empty)
  }

  /**
   * Creates a new [[StructType]] by adding a new field and specifying metadata where the
   * dataType is specified as a String.
    * 通过添加新字段并指定将dataType指定为String的元数据来创建新的[[StructType]]
   * {{{
   * val struct = (new StructType)
   *   .add("a", "int", true, Metadata.empty)
   *   .add("b", "long", false, Metadata.empty)
   *   .add("c", "string", true, Metadata.empty)
   * }}}
   */
  def add(
      name: String,
      dataType: String,
      nullable: Boolean,
      metadata: Metadata): StructType = {
    add(name, DataTypeParser.parse(dataType), nullable, metadata)
  }

  /**
   * Extracts a [[StructField]] of the given name. If the [[StructType]] object does not
   * have a name matching the given name, `null` will be returned.
    * 提取给定名称的[[StructField]],如果[[StructType]]对象没有与给定名称匹配的名称,则返回“null”
   */
  def apply(name: String): StructField = {
    nameToField.getOrElse(name,
      throw new IllegalArgumentException(s"""Field "$name" does not exist."""))
  }

  /**
   * Returns a [[StructType]] containing [[StructField]]s of the given names, preserving the
   * original order of fields. Those names which do not have matching fields will be ignored.
    * 返回包含给定名称的[[StructField]]的[[StructType]],保留字段的原始顺序,那些没有匹配字段的名称将被忽略。
   */
  def apply(names: Set[String]): StructType = {
    val nonExistFields = names -- fieldNamesSet
    if (nonExistFields.nonEmpty) {
      throw new IllegalArgumentException(
        s"Field ${nonExistFields.mkString(",")} does not exist.")
    }
    // Preserve the original order of fields.
    //保留字段的原始顺序
    StructType(fields.filter(f => names.contains(f.name)))
  }

  /**
   * Returns index of a given field
    * 返回给定字段的索引
   */
  def fieldIndex(name: String): Int = {
    nameToIndex.getOrElse(name,
      throw new IllegalArgumentException(s"""Field "$name" does not exist."""))
  }

  private[sql] def getFieldIndex(name: String): Option[Int] = {
    nameToIndex.get(name)
  }

  protected[sql] def toAttributes: Seq[AttributeReference] =
    map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())

  def treeString: String = {
    val builder = new StringBuilder
    builder.append("root\n")
    val prefix = " |"
    fields.foreach(field => field.buildFormattedString(prefix, builder))

    builder.toString()
  }

  // scalastyle:off println
  def printTreeString(): Unit = println(treeString)
  // scalastyle:on println

  private[sql] def buildFormattedString(prefix: String, builder: StringBuilder): Unit = {
    fields.foreach(field => field.buildFormattedString(prefix, builder))
  }

  override private[sql] def jsonValue =
    ("type" -> typeName) ~
      ("fields" -> map(_.jsonValue))

  override def apply(fieldIndex: Int): StructField = fields(fieldIndex)

  override def length: Int = fields.length

  override def iterator: Iterator[StructField] = fields.iterator

  /**
   * The default size of a value of the StructType is the total default sizes of all field types.
    * StructType值的默认大小是所有字段类型的总默认大小
   */
  override def defaultSize: Int = fields.map(_.dataType.defaultSize).sum

  override def simpleString: String = {
    val fieldTypes = fields.map(field => s"${field.name}:${field.dataType.simpleString}")
    s"struct<${fieldTypes.mkString(",")}>"
  }

  /**
   * Merges with another schema (`StructType`).  For a struct field A from `this` and a struct field
   * B from `that`,
    * 与另一个模式（`StructType`）合并,对于来自`this`的结构域A和来自`that`的结构域B
   *
   * 1. If A and B have the same name and data type, they are merged to a field C with the same name
   *    and data type.  C is nullable if and only if either A or B is nullable.
    *    如果A和B具有相同的名称和数据类型,则它们将合并到具有相同名称和数据类型的字段C,当且仅当A或B可为空时,C才可为空
   * 2. If A doesn't exist in `that`, it's included in the result schema.如果`that`中不存在A，它将包含在结果模式中。
   * 3. If B doesn't exist in `this`, it's also included in the result schema.如果`this`中不存在B，它也包含在结果模式中。
   * 4. Otherwise, `this` and `that` are considered as conflicting schemas and an exception would be
   *    thrown.
   */
  private[sql] def merge(that: StructType): StructType =
    StructType.merge(this, that).asInstanceOf[StructType]

  override private[spark] def asNullable: StructType = {
    val newFields = fields.map {
      case StructField(name, dataType, nullable, metadata) =>
        StructField(name, dataType.asNullable, nullable = true, metadata)
    }

    StructType(newFields)
  }

  override private[spark] def existsRecursively(f: (DataType) => Boolean): Boolean = {
    f(this) || fields.exists(field => field.dataType.existsRecursively(f))
  }

  @transient
  private[sql] lazy val interpretedOrdering =
    InterpretedOrdering.forSchema(this.fields.map(_.dataType))
}

object StructType extends AbstractDataType {

  override private[sql] def defaultConcreteType: DataType = new StructType

  override private[sql] def acceptsType(other: DataType): Boolean = {
    other.isInstanceOf[StructType]
  }

  override private[sql] def simpleString: String = "struct"

  private[sql] def fromString(raw: String): StructType = DataType.fromString(raw) match {
    case t: StructType => t
    case _ => throw new RuntimeException(s"Failed parsing StructType: $raw")
  }

  def apply(fields: Seq[StructField]): StructType = StructType(fields.toArray)

  def apply(fields: java.util.List[StructField]): StructType = {
    StructType(fields.toArray.asInstanceOf[Array[StructField]])
  }

  protected[sql] def fromAttributes(attributes: Seq[Attribute]): StructType =
    StructType(attributes.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))

  private[sql] def merge(left: DataType, right: DataType): DataType =
    (left, right) match {
      case (ArrayType(leftElementType, leftContainsNull),
      ArrayType(rightElementType, rightContainsNull)) =>
        ArrayType(
          merge(leftElementType, rightElementType),
          leftContainsNull || rightContainsNull)

      case (MapType(leftKeyType, leftValueType, leftContainsNull),
      MapType(rightKeyType, rightValueType, rightContainsNull)) =>
        MapType(
          merge(leftKeyType, rightKeyType),
          merge(leftValueType, rightValueType),
          leftContainsNull || rightContainsNull)

      case (StructType(leftFields), StructType(rightFields)) =>
        val newFields = ArrayBuffer.empty[StructField]

        val rightMapped = fieldsMap(rightFields)
        leftFields.foreach {
          case leftField @ StructField(leftName, leftType, leftNullable, _) =>
            rightMapped.get(leftName)
              .map { case rightField @ StructField(_, rightType, rightNullable, _) =>
              leftField.copy(
                dataType = merge(leftType, rightType),
                nullable = leftNullable || rightNullable)
            }
              .orElse(Some(leftField))
              .foreach(newFields += _)
        }

        val leftMapped = fieldsMap(leftFields)
        rightFields
          .filterNot(f => leftMapped.get(f.name).nonEmpty)
          .foreach(newFields += _)

        StructType(newFields)

      case (DecimalType.Fixed(leftPrecision, leftScale),
        DecimalType.Fixed(rightPrecision, rightScale)) =>
        if ((leftPrecision == rightPrecision) && (leftScale == rightScale)) {
          DecimalType(leftPrecision, leftScale)
        } else if ((leftPrecision != rightPrecision) && (leftScale != rightScale)) {
          throw new SparkException("Failed to merge Decimal Tpes with incompatible " +
            s"precision $leftPrecision and $rightPrecision & scale $leftScale and $rightScale")
        } else if (leftPrecision != rightPrecision) {
          throw new SparkException("Failed to merge Decimal Tpes with incompatible " +
            s"precision $leftPrecision and $rightPrecision")
        } else {
          throw new SparkException("Failed to merge Decimal Tpes with incompatible " +
            s"scala $leftScale and $rightScale")
        }

      case (leftUdt: UserDefinedType[_], rightUdt: UserDefinedType[_])
        if leftUdt.userClass == rightUdt.userClass => leftUdt

      case (leftType, rightType) if leftType == rightType =>
        leftType

      case _ =>
        throw new SparkException(s"Failed to merge incompatible data types $left and $right")
    }

  private[sql] def fieldsMap(fields: Array[StructField]): Map[String, StructField] = {
    import scala.collection.breakOut
    fields.map(s => (s.name, s))(breakOut)
  }
}
