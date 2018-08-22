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

package org.apache.spark.sql.execution.datasources.json

import com.fasterxml.jackson.core._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.HiveTypeCoercion
import org.apache.spark.sql.execution.datasources.json.JacksonUtils.nextUntil
import org.apache.spark.sql.types._

private[sql] object InferSchema {
  /**
   * Infer the type of a collection of json records in three stages:
    * 推断JSON记录集合的类型分为三个阶段：
   *   1. Infer the type of each record 推断每条记录的类型
   *   2. Merge types by choosing the lowest type necessary to cover equal keys
    *   通过选择覆盖相等键所需的最低类型来合并类型
   *   3. Replace any remaining null fields with string, the top type
    *   用字符串(顶部类型)替换任何剩余的空字段
   */
  def apply(
      json: RDD[String],
      samplingRatio: Double = 1.0,
      columnNameOfCorruptRecords: String): StructType = {
    require(samplingRatio > 0, s"samplingRatio ($samplingRatio) should be greater than 0")
    val schemaData = if (samplingRatio > 0.99) {
      json
    } else {
      json.sample(withReplacement = false, samplingRatio, 1)
    }

    // perform schema inference on each row and merge afterwards
    //在每一行上执行模式推断并在之后合并
    val rootType = schemaData.mapPartitions { iter =>
      val factory = new JsonFactory()
      iter.map { row =>
        try {
          val parser = factory.createParser(row)
          parser.nextToken()
          inferField(parser)
        } catch {
          case _: JsonParseException =>
            StructType(Seq(StructField(columnNameOfCorruptRecords, StringType)))
        }
      }
    }.treeAggregate[DataType](StructType(Seq()))(compatibleRootType, compatibleRootType)

    canonicalizeType(rootType) match {
      case Some(st: StructType) => st
      case _ =>
        // canonicalizeType erases all empty structs, including the only one we want to keep
        //canonicalizeType删除所有空结构,包括我们想要保留的唯一结构
        StructType(Seq())
    }
  }

  /**
   * Infer the type of a json document from the parser's token stream
    * 从解析器的令牌流中推断出json文档的类型
   */
  private def inferField(parser: JsonParser): DataType = {
    import com.fasterxml.jackson.core.JsonToken._
    parser.getCurrentToken match {
      case null | VALUE_NULL => NullType

      case FIELD_NAME =>
        parser.nextToken()
        inferField(parser)

      case VALUE_STRING if parser.getTextLength < 1 =>
        // Zero length strings and nulls have special handling to deal
        // with JSON generators that do not distinguish between the two.
        //零长度字符串和空值具有处理JSON生成器的特殊处理,这些生成器不区分两者
        // To accurately infer types for empty strings that are really
        // meant to represent nulls we assume that the two are isomorphic
        // but will defer treating null fields as strings until all the
        // record fields' types have been combined.
        NullType

      case VALUE_STRING => StringType
      case START_OBJECT =>
        val builder = Seq.newBuilder[StructField]
        while (nextUntil(parser, END_OBJECT)) {
          builder += StructField(parser.getCurrentName, inferField(parser), nullable = true)
        }

        StructType(builder.result().sortBy(_.name))

      case START_ARRAY =>
        // If this JSON array is empty, we use NullType as a placeholder.
        //如果此JSON数组为空,我们使用NullType作为占位符。
        // If this array is not empty in other JSON objects, we can resolve
        // the type as we pass through all JSON objects.
        //如果此数组在其他JSON对象中不为空,我们可以在通过所有JSON对象时解析该类型
        var elementType: DataType = NullType
        while (nextUntil(parser, END_ARRAY)) {
          elementType = compatibleType(elementType, inferField(parser))
        }

        ArrayType(elementType)

      case VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT =>
        import JsonParser.NumberType._
        parser.getNumberType match {
          // For Integer values, use LongType by default.
            //对于Integer值,默认情况下使用LongType
          case INT | LONG => LongType
          // Since we do not have a data type backed by BigInteger,
          // when we see a Java BigInteger, we use DecimalType.
            //由于我们没有BigInteger支持的数据类型,
          //因此当我们看到Java BigInteger时,我们使用DecimalType。
          case BIG_INTEGER | BIG_DECIMAL =>
            val v = parser.getDecimalValue
            DecimalType(v.precision(), v.scale())
          case FLOAT | DOUBLE =>
            // TODO(davies): Should we use decimal if possible?
            DoubleType
        }

      case VALUE_TRUE | VALUE_FALSE => BooleanType
    }
  }

  /**
   * Convert NullType to StringType and remove StructTypes with no fields
    * 将NullType转换为StringType并删除没有字段的StructType
   */
  private def canonicalizeType: DataType => Option[DataType] = {
    case at @ ArrayType(elementType, _) =>
      for {
        canonicalType <- canonicalizeType(elementType)
      } yield {
        at.copy(canonicalType)
      }

    case StructType(fields) =>
      val canonicalFields = for {
        field <- fields
        if field.name.nonEmpty
        canonicalType <- canonicalizeType(field.dataType)
      } yield {
        field.copy(dataType = canonicalType)
      }

      if (canonicalFields.nonEmpty) {
        Some(StructType(canonicalFields))
      } else {
        // per SPARK-8093: empty structs should be deleted
        None
      }

    case NullType => Some(StringType)
    case other => Some(other)
  }

  /**
   * Remove top-level ArrayType wrappers and merge the remaining schemas
    * 删除顶级ArrayType包装器并合并剩余的模式
   */
  private def compatibleRootType: (DataType, DataType) => DataType = {
    case (ArrayType(ty1, _), ty2) => compatibleRootType(ty1, ty2)
    case (ty1, ArrayType(ty2, _)) => compatibleRootType(ty1, ty2)
    case (ty1, ty2) => compatibleType(ty1, ty2)
  }

  /**
   * Returns the most general data type for two given data types.
   * 返回两个给定的数据类型最常用的数据类型
   */
  private[json] def compatibleType(t1: DataType, t2: DataType): DataType = {
    HiveTypeCoercion.findTightestCommonTypeOfTwo(t1, t2).getOrElse {
      // t1 or t2 is a StructType, ArrayType, or an unexpected type.
      //t1或t2是StructType,ArrayType或意常类型。
      (t1, t2) match {
        // Double support larger range than fixed decimal, DecimalType.Maximum should be enough
        // in most case, also have better precision.
          //Double支持比固定小数范围更大的范围,DecimalType.Maximum在大多数情况下应该足够,也有更好的精度
        case (DoubleType, t: DecimalType) =>
          DoubleType
        case (t: DecimalType, DoubleType) =>
          DoubleType
        case (t1: DecimalType, t2: DecimalType) =>
          val scale = math.max(t1.scale, t2.scale)
          val range = math.max(t1.precision - t1.scale, t2.precision - t2.scale)
          if (range + scale > 38) {
            // DecimalType can't support precision > 38
            DoubleType
          } else {
            DecimalType(range + scale, scale)
          }

        case (StructType(fields1), StructType(fields2)) =>
          val newFields = (fields1 ++ fields2).groupBy(field => field.name).map {
            case (name, fieldTypes) =>
              val dataType = fieldTypes.view.map(_.dataType).reduce(compatibleType)
              StructField(name, dataType, nullable = true)
          }
          StructType(newFields.toSeq.sortBy(_.name))

        case (ArrayType(elementType1, containsNull1), ArrayType(elementType2, containsNull2)) =>
          ArrayType(compatibleType(elementType1, elementType2), containsNull1 || containsNull2)

        // strings and every string is a Json object.
          //字符串和每个字符串都是Json对象
        case (_, _) => StringType
      }
    }
  }
}
