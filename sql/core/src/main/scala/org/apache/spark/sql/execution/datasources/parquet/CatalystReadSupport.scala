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

package org.apache.spark.sql.execution.datasources.parquet

import java.util.{Map => JMap}

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.ReadSupport.ReadContext
import org.apache.parquet.hadoop.api.{InitContext, ReadSupport}
import org.apache.parquet.io.api.RecordMaterializer
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema._

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

private[parquet] class CatalystReadSupport extends ReadSupport[InternalRow] with Logging {
  // Called after `init()` when initializing Parquet record reader.
  override def prepareForRead(
      conf: Configuration,
      keyValueMetaData: JMap[String, String],
      fileSchema: MessageType,
      readContext: ReadContext): RecordMaterializer[InternalRow] = {
    log.debug(s"Preparing for read Parquet file with message type: $fileSchema")

    val toCatalyst = new CatalystSchemaConverter(conf)
    val parquetRequestedSchema = readContext.getRequestedSchema

    val catalystRequestedSchema =
      Option(readContext.getReadSupportMetadata).map(_.asScala).flatMap { metadata =>
        metadata
          // First tries to read requested schema, which may result from projections
          //首先尝试读取请求的架构,这可能是由投影产生的
          .get(CatalystReadSupport.SPARK_ROW_REQUESTED_SCHEMA)
          // If not available, tries to read Catalyst schema from file metadata.  It's only
          // available if the target file is written by Spark SQL.
          //如果不可用,请尝试从文件元数据中读取Catalyst架构,仅当目标文件由Spark SQL编写时才可用。
          .orElse(metadata.get(CatalystReadSupport.SPARK_METADATA_KEY))
      }.map(StructType.fromString).getOrElse {
        logInfo("Catalyst schema not available, falling back to Parquet schema")
        toCatalyst.convert(parquetRequestedSchema)
      }

    logInfo {
      s"""Going to read the following fields from the Parquet file:
         |
         |Parquet form:
         |$parquetRequestedSchema
         |Catalyst form:
         |$catalystRequestedSchema
       """.stripMargin
    }

    new CatalystRecordMaterializer(parquetRequestedSchema, catalystRequestedSchema)
  }

  // Called before `prepareForRead()` when initializing Parquet record reader.
  //在初始化Parquet记录阅读器时调用`prepareForRead（）`之前调用。
  override def init(context: InitContext): ReadContext = {
    val conf = context.getConfiguration

    // If the target file was written by Spark SQL, we should be able to find a serialized Catalyst
    // schema of this file from its metadata.
    //如果目标文件是由Spark SQL编写的,我们应该能够从其元数据中找到该文件的序列化Catalyst模式
    val maybeRowSchema = Option(conf.get(RowWriteSupport.SPARK_ROW_SCHEMA))

    // Optional schema of requested columns, in the form of a string serialized from a Catalyst
    // `StructType` containing all requested columns.
    //请求列的可选模式,采用从包含所有请求列的Catalyst`StructType`序列化的字符串形式
    val maybeRequestedSchema = Option(conf.get(CatalystReadSupport.SPARK_ROW_REQUESTED_SCHEMA))

    val parquetRequestedSchema =
      maybeRequestedSchema.fold(context.getFileSchema) { schemaString =>
        val catalystRequestedSchema = StructType.fromString(schemaString)
        CatalystReadSupport.clipParquetSchema(context.getFileSchema, catalystRequestedSchema)
      }

    val metadata =
      Map.empty[String, String] ++
        maybeRequestedSchema.map(CatalystReadSupport.SPARK_ROW_REQUESTED_SCHEMA -> _) ++
        maybeRowSchema.map(RowWriteSupport.SPARK_ROW_SCHEMA -> _)

    new ReadContext(parquetRequestedSchema, metadata.asJava)
  }
}

private[parquet] object CatalystReadSupport {
  val SPARK_ROW_REQUESTED_SCHEMA = "org.apache.spark.sql.parquet.row.requested_schema"

  val SPARK_METADATA_KEY = "org.apache.spark.sql.parquet.row.metadata"

  /**
   * Tailors `parquetSchema` according to `catalystSchema` by removing column paths don't exist
   * in `catalystSchema`, and adding those only exist in `catalystSchema`.
    * `catalystSchema`中的裁剪`parquetSchema`通过删除列路径在`catalystSchema`中不存在,
    * 并且添加它们仅存在于`catalystSchema`中。
   */
  def clipParquetSchema(parquetSchema: MessageType, catalystSchema: StructType): MessageType = {
    val clippedParquetFields = clipParquetGroupFields(parquetSchema.asGroupType(), catalystSchema)
    Types.buildMessage().addFields(clippedParquetFields: _*).named("root")
  }

  private def clipParquetType(parquetType: Type, catalystType: DataType): Type = {
    catalystType match {
      case t: ArrayType if !isPrimitiveCatalystType(t.elementType) =>
        // Only clips array types with nested type as element type.
        //仅剪辑具有嵌套类型作为元素类型的数组类型
        clipParquetListType(parquetType.asGroupType(), t.elementType)

      case t: MapType
        if !isPrimitiveCatalystType(t.keyType) ||
           !isPrimitiveCatalystType(t.valueType) =>
        // Only clips map types with nested key type or value type
        //仅剪辑具有嵌套键类型或值类型的Map类型
        clipParquetMapType(parquetType.asGroupType(), t.keyType, t.valueType)

      case t: StructType =>
        clipParquetGroup(parquetType.asGroupType(), t)

      case _ =>
        // UDTs and primitive types are not clipped.  For UDTs, a clipped version might not be able
        // to be mapped to desired user-space types.  So UDTs shouldn't participate schema merging.
        //UDT和原始类型不会被剪裁,对于UDT,剪切版本可能无法映射到所需的用户空间类型,因此UDT不应该参与模式合并。
        parquetType
    }
  }

  /**
   * Whether a Catalyst [[DataType]] is primitive.  Primitive [[DataType]] is not equivalent to
   * [[AtomicType]].  For example, [[CalendarIntervalType]] is primitive, but it's not an
   * [[AtomicType]].
    * Catalyst [Data Type]是否是原始的。 原始[数据类型不等同于[AtomicType]
   */
  private def isPrimitiveCatalystType(dataType: DataType): Boolean = {
    dataType match {
      case _: ArrayType | _: MapType | _: StructType => false
      case _ => true
    }
  }

  /**
   * Clips a Parquet [[GroupType]] which corresponds to a Catalyst [[ArrayType]].  The element type
   * of the [[ArrayType]] should also be a nested type, namely an [[ArrayType]], a [[MapType]], or a
   * [[StructType]].
   */
  private def clipParquetListType(parquetList: GroupType, elementType: DataType): Type = {
    // Precondition of this method, should only be called for lists with nested element types.
    //只应针对具有嵌套元素类型的列表调用此方法的前提条件
    assert(!isPrimitiveCatalystType(elementType))

    // Unannotated repeated group should be interpreted as required list of required element, so
    // list element type is just the group itself.  Clip it.
    //未注释的重复组应该被解释为所需元素的必需列表,因此列表元素类型只是组本身,剪辑它
    if (parquetList.getOriginalType == null && parquetList.isRepetition(Repetition.REPEATED)) {
      clipParquetType(parquetList, elementType)
    } else {
      assert(
        parquetList.getOriginalType == OriginalType.LIST,
        "Invalid Parquet schema. " +
          "Original type of annotated Parquet lists must be LIST: " +
          parquetList.toString)

      assert(
        parquetList.getFieldCount == 1 && parquetList.getType(0).isRepetition(Repetition.REPEATED),
        "Invalid Parquet schema. " +
          "LIST-annotated group should only have exactly one repeated field: " +
          parquetList)

      // Precondition of this method, should only be called for lists with nested element types.
      //只应针对具有嵌套元素类型的列表调用此方法的前提条件
      assert(!parquetList.getType(0).isPrimitive)

      val repeatedGroup = parquetList.getType(0).asGroupType()

      // If the repeated field is a group with multiple fields, or the repeated field is a group
      // with one field and is named either "array" or uses the LIST-annotated group's name with
      // "_tuple" appended then the repeated type is the element type and elements are required.
      // Build a new LIST-annotated group with clipped `repeatedGroup` as element type and the
      // only field.
      if (
        repeatedGroup.getFieldCount > 1 ||
        repeatedGroup.getName == "array" ||
        repeatedGroup.getName == parquetList.getName + "_tuple"
      ) {
        Types
          .buildGroup(parquetList.getRepetition)
          .as(OriginalType.LIST)
          .addField(clipParquetType(repeatedGroup, elementType))
          .named(parquetList.getName)
      } else {
        // Otherwise, the repeated field's type is the element type with the repeated field's
        // repetition.
        //否则,重复字段的类型是具有重复字段重复的元素类型
        Types
          .buildGroup(parquetList.getRepetition)
          .as(OriginalType.LIST)
          .addField(
            Types
              .repeatedGroup()
              .addField(clipParquetType(repeatedGroup.getType(0), elementType))
              .named(repeatedGroup.getName))
          .named(parquetList.getName)
      }
    }
  }

  /**
   * Clips a Parquet [[GroupType]] which corresponds to a Catalyst [[MapType]].  Either key type or
   * value type of the [[MapType]] must be a nested type, namely an [[ArrayType]], a [[MapType]], or
   * a [[StructType]].
    * 剪辑Parquet [[GroupType]],它对应于Catalyst [[MapType]],[[MapType]]的键类型或值类型必须是嵌套类型,
    * 即[[ArrayType]]，[[MapType]]或[[StructType]]。
   */
  private def clipParquetMapType(
      parquetMap: GroupType, keyType: DataType, valueType: DataType): GroupType = {
    // Precondition of this method, only handles maps with nested key types or value types.
    //此方法的前提条件仅处理具有嵌套键类型或值类型的映射
    assert(!isPrimitiveCatalystType(keyType) || !isPrimitiveCatalystType(valueType))

    val repeatedGroup = parquetMap.getType(0).asGroupType()
    val parquetKeyType = repeatedGroup.getType(0)
    val parquetValueType = repeatedGroup.getType(1)

    val clippedRepeatedGroup =
      Types
        .repeatedGroup()
        .as(repeatedGroup.getOriginalType)
        .addField(clipParquetType(parquetKeyType, keyType))
        .addField(clipParquetType(parquetValueType, valueType))
        .named(repeatedGroup.getName)

    Types
      .buildGroup(parquetMap.getRepetition)
      .as(parquetMap.getOriginalType)
      .addField(clippedRepeatedGroup)
      .named(parquetMap.getName)
  }

  /**
   * Clips a Parquet [[GroupType]] which corresponds to a Catalyst [[StructType]].
   * 剪辑Parquet [[GroupType]],它对应于Catalyst [[StructType]]
   * @return A clipped [[GroupType]], which has at least one field.
   * @note Parquet doesn't allow creating empty [[GroupType]] instances except for empty
   *       [[MessageType]].  Because it's legal to construct an empty requested schema for column
   *       pruning.
   */
  private def clipParquetGroup(parquetRecord: GroupType, structType: StructType): GroupType = {
    val clippedParquetFields = clipParquetGroupFields(parquetRecord, structType)
    Types
      .buildGroup(parquetRecord.getRepetition)
      .as(parquetRecord.getOriginalType)
      .addFields(clippedParquetFields: _*)
      .named(parquetRecord.getName)
  }

  /**
   * Clips a Parquet [[GroupType]] which corresponds to a Catalyst [[StructType]].
    * 剪辑Parquet [[GroupType]],它对应于Catalyst [[StructType]]
   *
   * @return A list of clipped [[GroupType]] fields, which can be empty.
   */
  private def clipParquetGroupFields(
      parquetRecord: GroupType, structType: StructType): Seq[Type] = {
    val parquetFieldMap = parquetRecord.getFields.asScala.map(f => f.getName -> f).toMap
    val toParquet = new CatalystSchemaConverter(followParquetFormatSpec = true)
    structType.map { f =>
      parquetFieldMap
        .get(f.name)
        .map(clipParquetType(_, f.dataType))
        .getOrElse(toParquet.convertField(f))
    }
  }
}
