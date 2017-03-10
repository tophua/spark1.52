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

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

import org.apache.parquet.schema.MessageTypeParser

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._

abstract class ParquetSchemaTest extends ParquetTest with SharedSQLContext {

  /**
   * Checks whether the reflected Parquet message type for product type `T` conforms `messageType`.
   * 检查产品类型`T'的反射Parquet消息类型是否符合`messageType`
   */
  protected def testSchemaInference[T <: Product: ClassTag: TypeTag](
      testName: String,
      messageType: String,
      binaryAsString: Boolean = true,
      int96AsTimestamp: Boolean = true,
      followParquetFormatSpec: Boolean = false,
      isThriftDerived: Boolean = false): Unit = {
    testSchema(
      testName,
      StructType.fromAttributes(ScalaReflection.attributesFor[T]),
      messageType,
      binaryAsString,
      int96AsTimestamp,
      followParquetFormatSpec,
      isThriftDerived)
  }
  //测试Parquet优化器
  protected def testParquetToCatalyst(
      testName: String,
      sqlSchema: StructType,
      parquetSchema: String,
      binaryAsString: Boolean = true,
      int96AsTimestamp: Boolean = true,
      followParquetFormatSpec: Boolean = false,
      isThriftDerived: Boolean = false): Unit = {
    val converter = new CatalystSchemaConverter(
      assumeBinaryIsString = binaryAsString,
      assumeInt96IsTimestamp = int96AsTimestamp,
      followParquetFormatSpec = followParquetFormatSpec)

    test(s"sql <= parquet: $testName") {
      val actual = converter.convert(MessageTypeParser.parseMessageType(parquetSchema))
      val expected = sqlSchema
      assert(
        actual === expected,
        s"""Schema mismatch.
           |Expected schema: ${expected.json}
           |Actual schema:   ${actual.json}
         """.stripMargin)
    }
  }
  //测试催化剂到Parquet
  protected def testCatalystToParquet(
      testName: String,
      sqlSchema: StructType,
      parquetSchema: String,
      binaryAsString: Boolean = true,
      int96AsTimestamp: Boolean = true,
      followParquetFormatSpec: Boolean = false,
      isThriftDerived: Boolean = false): Unit = {
    val converter = new CatalystSchemaConverter(
      assumeBinaryIsString = binaryAsString,
      assumeInt96IsTimestamp = int96AsTimestamp,
      followParquetFormatSpec = followParquetFormatSpec)

    test(s"sql => parquet: $testName") {
      val actual = converter.convert(sqlSchema)
      val expected = MessageTypeParser.parseMessageType(parquetSchema)
      actual.checkContains(expected)
      expected.checkContains(actual)
    }
  }
  //测试模式
  protected def testSchema(
      testName: String,
      sqlSchema: StructType,
      parquetSchema: String,
      binaryAsString: Boolean = true,
      int96AsTimestamp: Boolean = true,
      followParquetFormatSpec: Boolean = false,
      isThriftDerived: Boolean = false): Unit = {

    testCatalystToParquet(//测试Catalyst到Parquet
      testName,
      sqlSchema,
      parquetSchema,
      binaryAsString,
      int96AsTimestamp,
      followParquetFormatSpec,
      isThriftDerived)

    testParquetToCatalyst(//测试Parquet到Catalyst
      testName,
      sqlSchema,
      parquetSchema,
      binaryAsString,
      int96AsTimestamp,
      followParquetFormatSpec,
      isThriftDerived)
  }
}
  //Parquet模式接口测试
class ParquetSchemaInferenceSuite extends ParquetSchemaTest {
  testSchemaInference[(Boolean, Int, Long, Float, Double, Array[Byte])](
    "basic types",
    """
      |message root {
      |  required boolean _1;
      |  required int32   _2;
      |  required int64   _3;
      |  required float   _4;
      |  required double  _5;
      |  optional binary  _6;
      |}
    """.stripMargin,
    binaryAsString = false)

  testSchemaInference[(Byte, Short, Int, Long, java.sql.Date)](
    "logical integral types",
    """
      |message root {
      |  required int32 _1 (INT_8);
      |  required int32 _2 (INT_16);
      |  required int32 _3 (INT_32);
      |  required int64 _4 (INT_64);
      |  optional int32 _5 (DATE);
      |}
    """.stripMargin)

  testSchemaInference[Tuple1[String]](
    "string",
    """
      |message root {
      |  optional binary _1 (UTF8);
      |}
    """.stripMargin,
    binaryAsString = true)

  testSchemaInference[Tuple1[String]](
    //二进制枚举作为字符串
    "binary enum as string",
    """
      |message root {
      |  optional binary _1 (ENUM);
      |}
    """.stripMargin)

  testSchemaInference[Tuple1[Seq[Int]]](
    //非空数组 - 非标准
    "non-nullable array - non-standard",
    """
      |message root {
      |  optional group _1 (LIST) {
      |    repeated int32 array;
      |  }
      |}
    """.stripMargin)

  testSchemaInference[Tuple1[Seq[Int]]](
    //非可空数组 - 标准
    "non-nullable array - standard",
    """
      |message root {
      |  optional group _1 (LIST) {
      |    repeated group list {
      |      required int32 element;
      |    }
      |  }
      |}
    """.stripMargin,
    followParquetFormatSpec = true)

  testSchemaInference[Tuple1[Seq[Integer]]](
    //可空数组 - 非标准
    "nullable array - non-standard",
    """
      |message root {
      |  optional group _1 (LIST) {
      |    repeated group bag {
      |      optional int32 array;
      |    }
      |  }
      |}
    """.stripMargin)

  testSchemaInference[Tuple1[Seq[Integer]]](
    //可空数组 - 标准
    "nullable array - standard",
    """
      |message root {
      |  optional group _1 (LIST) {
      |    repeated group list {
      |      optional int32 element;
      |    }
      |  }
      |}
    """.stripMargin,
    followParquetFormatSpec = true)

  testSchemaInference[Tuple1[Map[Int, String]]](
     //map-标准
    "map - standard",
    """
      |message root {
      |  optional group _1 (MAP) {
      |    repeated group key_value {
      |      required int32 key;
      |      optional binary value (UTF8);
      |    }
      |  }
      |}
    """.stripMargin,
    followParquetFormatSpec = true)

  testSchemaInference[Tuple1[Map[Int, String]]](
    //map - 非标准
    "map - non-standard",
    """
      |message root {
      |  optional group _1 (MAP) {
      |    repeated group map (MAP_KEY_VALUE) {
      |      required int32 key;
      |      optional binary value (UTF8);
      |    }
      |  }
      |}
    """.stripMargin)

  testSchemaInference[Tuple1[Pair[Int, String]]](
    "struct",
    """
      |message root {
      |  optional group _1 {
      |    required int32 _1;
      |    optional binary _2 (UTF8);
      |  }
      |}
    """.stripMargin,
    followParquetFormatSpec = true)

  testSchemaInference[Tuple1[Map[Int, (String, Seq[(Int, Double)])]]](
     //深层嵌套类型 - 非标准
    "deeply nested type - non-standard",
    """
      |message root {
      |  optional group _1 (MAP_KEY_VALUE) {
      |    repeated group map {
      |      required int32 key;
      |      optional group value {
      |        optional binary _1 (UTF8);
      |        optional group _2 (LIST) {
      |          repeated group bag {
      |            optional group array {
      |              required int32 _1;
      |              required double _2;
      |            }
      |          }
      |        }
      |      }
      |    }
      |  }
      |}
    """.stripMargin)

  testSchemaInference[Tuple1[Map[Int, (String, Seq[(Int, Double)])]]](
    //深层嵌套类型 - 标准
    "deeply nested type - standard",
    """
      |message root {
      |  optional group _1 (MAP) {
      |    repeated group key_value {
      |      required int32 key;
      |      optional group value {
      |        optional binary _1 (UTF8);
      |        optional group _2 (LIST) {
      |          repeated group list {
      |            optional group element {
      |              required int32 _1;
      |              required double _2;
      |            }
      |          }
      |        }
      |      }
      |    }
      |  }
      |}
    """.stripMargin,
    followParquetFormatSpec = true)

  testSchemaInference[(Option[Int], Map[Int, Option[Double]])](
    //可选类型
    "optional types",
    """
      |message root {
      |  optional int32 _1;
      |  optional group _2 (MAP) {
      |    repeated group key_value {
      |      required int32 key;
      |      optional double value;
      |    }
      |  }
      |}
    """.stripMargin,
    followParquetFormatSpec = true)

  // Parquet files generated by parquet-thrift are already handled by the schema converter, but
  //由parquet-thrift生成的Parquet文件已经由模式转换器处理
  // let's leave this test here until both read path and write path are all updated.
  //但是让我们离开这个测试,直到读取路径和写入路径都被更新
    //节俭产生地板模式
  ignore("thrift generated parquet schema") {
    // Test for SPARK-4520 -- ensure that thrift generated parquet schema is generated
    // as expected from attributes
    //确保thrift生成的parquet模式根据预期从属性生成
    testSchemaInference[(
      Array[Byte], Array[Byte], Array[Byte], Seq[Int], Map[Array[Byte], Seq[Int]])](
      //thrift生成的parquet模式
      "thrift generated parquet schema",
      """
        |message root {
        |  optional binary _1 (UTF8);
        |  optional binary _2 (UTF8);
        |  optional binary _3 (UTF8);
        |  optional group _4 (LIST) {
        |    repeated int32 _4_tuple;
        |  }
        |  optional group _5 (MAP) {
        |    repeated group map (MAP_KEY_VALUE) {
        |      required binary key (UTF8);
        |      optional group value (LIST) {
        |        repeated int32 value_tuple;
        |      }
        |    }
        |  }
        |}
      """.stripMargin,
      isThriftDerived = true)
  }
}

class ParquetSchemaSuite extends ParquetSchemaTest {
  test("DataType string parser compatibility") {//DataType字符串解析器兼容性
    // This is the generated string from previous versions of the Spark SQL, using the following:
    //这是从以前版本的Spark SQL生成的字符串,使用以下:
    // val schema = StructType(List(
    //  StructField("c1", IntegerType, false),
    //  StructField("c2", BinaryType, true)))
    val caseClassString =
      "StructType(List(StructField(c1,IntegerType,false), StructField(c2,BinaryType,true)))"

    // scalastyle:off
    val jsonString = """{"type":"struct","fields":[{"name":"c1","type":"integer","nullable":false,"metadata":{}},{"name":"c2","type":"binary","nullable":true,"metadata":{}}]}"""
    // scalastyle:on

    val fromCaseClassString = ParquetTypesConverter.convertFromString(caseClassString)
    val fromJson = ParquetTypesConverter.convertFromString(jsonString)

    (fromCaseClassString, fromJson).zipped.foreach { (a, b) =>
      assert(a.name == b.name)
      assert(a.dataType === b.dataType)
      assert(a.nullable === b.nullable)
    }
  }
  //合并元数据模式
  test("merge with metastore schema") {
    // Field type conflict resolution
    //字段类型冲突解决
    assertResult(
      StructType(Seq(
        StructField("lowerCase", StringType),
        StructField("UPPERCase", DoubleType, nullable = false)))) {

      ParquetRelation.mergeMetastoreParquetSchema(
        StructType(Seq(
          StructField("lowercase", StringType),
          StructField("uppercase", DoubleType, nullable = false))),

        StructType(Seq(
          StructField("lowerCase", BinaryType),
          StructField("UPPERCase", IntegerType, nullable = true))))
    }

    // MetaStore schema is subset of parquet schema
    //MetaStore模式是parquet模式的子集
    assertResult(
      StructType(Seq(
        StructField("UPPERCase", DoubleType, nullable = false)))) {

      ParquetRelation.mergeMetastoreParquetSchema(
        StructType(Seq(
          StructField("uppercase", DoubleType, nullable = false))),

        StructType(Seq(
          StructField("lowerCase", BinaryType),
          StructField("UPPERCase", IntegerType, nullable = true))))
    }

    // Metastore schema contains additional non-nullable fields.
    //Metastore模式包含其他不可为空的字段
    assert(intercept[Throwable] {
      ParquetRelation.mergeMetastoreParquetSchema(
        StructType(Seq(
          StructField("uppercase", DoubleType, nullable = false),
          StructField("lowerCase", BinaryType, nullable = false))),

        StructType(Seq(
          StructField("UPPERCase", IntegerType, nullable = true))))
    }.getMessage.contains("detected conflicting schemas"))

    // Conflicting non-nullable field names
    // 冲突不可为空的字段名称
    intercept[Throwable] {
      ParquetRelation.mergeMetastoreParquetSchema(
        StructType(Seq(StructField("lower", StringType, nullable = false))),
        StructType(Seq(StructField("lowerCase", BinaryType))))
    }
  }
  //从Metastore模式合并缺少可空字段
  test("merge missing nullable fields from Metastore schema") {
    // Standard case: Metastore schema contains additional nullable fields not present
    // in the Parquet file schema.
    //标准案例：Metastore模式包含Parquet文件模式中不存在的其他可空字段
    assertResult(
      StructType(Seq(
        StructField("firstField", StringType, nullable = true),
        StructField("secondField", StringType, nullable = true),
        StructField("thirdfield", StringType, nullable = true)))) {
      ParquetRelation.mergeMetastoreParquetSchema(
        StructType(Seq(
          StructField("firstfield", StringType, nullable = true),
          StructField("secondfield", StringType, nullable = true),
          StructField("thirdfield", StringType, nullable = true))),
        StructType(Seq(
          StructField("firstField", StringType, nullable = true),
          StructField("secondField", StringType, nullable = true))))
    }

    // Merge should fail if the Metastore contains any additional fields that are not
    // nullable.
    //如果Metastore包含任何不可为空的附加字段,则合并应失败
    assert(intercept[Throwable] {
      ParquetRelation.mergeMetastoreParquetSchema(
        StructType(Seq(
          StructField("firstfield", StringType, nullable = true),
          StructField("secondfield", StringType, nullable = true),
          StructField("thirdfield", StringType, nullable = false))),
        StructType(Seq(
          StructField("firstField", StringType, nullable = true),
          StructField("secondField", StringType, nullable = true))))
    }.getMessage.contains("detected conflicting schemas"))
  }

  // =======================================================
  // Tests for converting Parquet LIST to Catalyst ArrayType
  // 将Parquet LIST转换为Catalyst ArrayType的测试
  // =======================================================

  testParquetToCatalyst(
    //向后兼容性:具有可空元素类型的LIST - 1 - 标准
    "Backwards-compatibility: LIST with nullable element type - 1 - standard",
    StructType(Seq(
      StructField(
        "f1",
        ArrayType(IntegerType, containsNull = true),
        nullable = true))),
    """message root {
      |  optional group f1 (LIST) {
      |    repeated group list {
      |      optional int32 element;
      |    }
      |  }
      |}
    """.stripMargin)

  testParquetToCatalyst(
    //向后兼容性：具有可空元素类型的LIST - 2
    "Backwards-compatibility: LIST with nullable element type - 2",
    StructType(Seq(
      StructField(
        "f1",
        ArrayType(IntegerType, containsNull = true),
        nullable = true))),
    """message root {
      |  optional group f1 (LIST) {
      |    repeated group element {
      |      optional int32 num;
      |    }
      |  }
      |}
    """.stripMargin)

  testParquetToCatalyst(
    //向后兼容性：具有不可为空的元素类型的LIST - 1 - 标准
    "Backwards-compatibility: LIST with non-nullable element type - 1 - standard",
    StructType(Seq(
      StructField("f1", ArrayType(IntegerType, containsNull = false), nullable = true))),
    """message root {
      |  optional group f1 (LIST) {
      |    repeated group list {
      |      required int32 element;
      |    }
      |  }
      |}
    """.stripMargin)

  testParquetToCatalyst(
    //向后兼容性：具有不可为空的元素类型的LIST - 2
    "Backwards-compatibility: LIST with non-nullable element type - 2",
    StructType(Seq(
      StructField("f1", ArrayType(IntegerType, containsNull = false), nullable = true))),
    """message root {
      |  optional group f1 (LIST) {
      |    repeated group element {
      |      required int32 num;
      |    }
      |  }
      |}
    """.stripMargin)

  testParquetToCatalyst(
    //向后兼容性：具有不可为空的元素类型的LIST - 3
    "Backwards-compatibility: LIST with non-nullable element type - 3",
    StructType(Seq(
      StructField("f1", ArrayType(IntegerType, containsNull = false), nullable = true))),
    """message root {
      |  optional group f1 (LIST) {
      |    repeated int32 element;
      |  }
      |}
    """.stripMargin)

  testParquetToCatalyst(
    //向后兼容性：具有不可为空的元素类型的LIST - 3
    "Backwards-compatibility: LIST with non-nullable element type - 4",
    StructType(Seq(
      StructField(
        "f1",
        ArrayType(
          StructType(Seq(
            StructField("str", StringType, nullable = false),
            StructField("num", IntegerType, nullable = false))),
          containsNull = false),
        nullable = true))),
    """message root {
      |  optional group f1 (LIST) {
      |    repeated group element {
      |      required binary str (UTF8);
      |      required int32 num;
      |    }
      |  }
      |}
    """.stripMargin)

  testParquetToCatalyst(
    //向后兼容性：具有非可空元素类型的LIST - 5 - parquet-avro 风格
    "Backwards-compatibility: LIST with non-nullable element type - 5 - parquet-avro style",
    StructType(Seq(
      StructField(
        "f1",
        ArrayType(
          StructType(Seq(
            StructField("str", StringType, nullable = false))),
          containsNull = false),
        nullable = true))),
    """message root {
      |  optional group f1 (LIST) {
      |    repeated group array {
      |      required binary str (UTF8);
      |    }
      |  }
      |}
    """.stripMargin)

  testParquetToCatalyst( //向后兼容性：具有非可空元素类型的LIST - 5 - parquet-thrift 风格
    "Backwards-compatibility: LIST with non-nullable element type - 6 - parquet-thrift style",
    StructType(Seq(
      StructField(
        "f1",
        ArrayType(
          StructType(Seq(
            StructField("str", StringType, nullable = false))),
          containsNull = false),
        nullable = true))),
    """message root {
      |  optional group f1 (LIST) {
      |    repeated group f1_tuple {
      |      required binary str (UTF8);
      |    }
      |  }
      |}
    """.stripMargin)

  testParquetToCatalyst(
    "Backwards-compatibility: LIST with non-nullable element type 7 - " +
      "parquet-protobuf primitive lists",
    new StructType()
      .add("f1", ArrayType(IntegerType, containsNull = false), nullable = false),
    """message root {
      |  repeated int32 f1;
      |}
    """.stripMargin)

  testParquetToCatalyst(
    "Backwards-compatibility: LIST with non-nullable element type 8 - " +
      "parquet-protobuf non-primitive lists",
    {
      val elementType =
        new StructType()
          .add("c1", StringType, nullable = true)
          .add("c2", IntegerType, nullable = false)

      new StructType()
        .add("f1", ArrayType(elementType, containsNull = false), nullable = false)
    },
    """message root {
      |  repeated group f1 {
      |    optional binary c1 (UTF8);
      |    required int32 c2;
      |  }
      |}
    """.stripMargin)

  // =======================================================
  // Tests for converting Catalyst ArrayType to Parquet LIST
  // 将Catalyst ArrayType转换为Parquet LIST的测试
  // =======================================================

  testCatalystToParquet(//向后兼容性：具有可空元素类型的LIST - 1 - 标准
    "Backwards-compatibility: LIST with nullable element type - 1 - standard",
    StructType(Seq(
      StructField(
        "f1",
        ArrayType(IntegerType, containsNull = true),
        nullable = true))),
    """message root {
      |  optional group f1 (LIST) {
      |    repeated group list {
      |      optional int32 element;
      |    }
      |  }
      |}
    """.stripMargin,
    followParquetFormatSpec = true)

  testCatalystToParquet(//向后兼容性：LIST与可空元素类型 - 2 - 1.4.x之前
    "Backwards-compatibility: LIST with nullable element type - 2 - prior to 1.4.x",
    StructType(Seq(
      StructField(
        "f1",
        ArrayType(IntegerType, containsNull = true),
        nullable = true))),
    """message root {
      |  optional group f1 (LIST) {
      |    repeated group bag {
      |      optional int32 array;
      |    }
      |  }
      |}
    """.stripMargin)

  testCatalystToParquet(//向后兼容性：具有不可为空的元素类型的LIST - 1 - 标准
    "Backwards-compatibility: LIST with non-nullable element type - 1 - standard",
    StructType(Seq(
      StructField(
        "f1",
        ArrayType(IntegerType, containsNull = false),
        nullable = true))),
    """message root {
      |  optional group f1 (LIST) {
      |    repeated group list {
      |      required int32 element;
      |    }
      |  }
      |}
    """.stripMargin,
    followParquetFormatSpec = true)

  testCatalystToParquet(//向后兼容性：具有不可为空的元素类型的LIST - 2 - 在1.4.x之前
    "Backwards-compatibility: LIST with non-nullable element type - 2 - prior to 1.4.x",
    StructType(Seq(
      StructField(
        "f1",
        ArrayType(IntegerType, containsNull = false),
        nullable = true))),
    """message root {
      |  optional group f1 (LIST) {
      |    repeated int32 array;
      |  }
      |}
    """.stripMargin)

  // ====================================================
  // Tests for converting Parquet Map to Catalyst MapType
  // 将Parquet Map转换为Catalyst MapType的测试
  // ====================================================

  testParquetToCatalyst(
    //具有非可空值类型的MAP - 1 - 标准
    "Backwards-compatibility: MAP with non-nullable value type - 1 - standard",
    StructType(Seq(
      StructField(
        "f1",
        MapType(IntegerType, StringType, valueContainsNull = false),
        nullable = true))),
    """message root {
      |  optional group f1 (MAP) {
      |    repeated group key_value {
      |      required int32 key;
      |      required binary value (UTF8);
      |    }
      |  }
      |}
    """.stripMargin)

  testParquetToCatalyst(//向后兼容性：具有非可为空值类型的MAP - 2
    "Backwards-compatibility: MAP with non-nullable value type - 2",
    StructType(Seq(
      StructField(
        "f1",
        MapType(IntegerType, StringType, valueContainsNull = false),
        nullable = true))),
    """message root {
      |  optional group f1 (MAP_KEY_VALUE) {
      |    repeated group map {
      |      required int32 num;
      |      required binary str (UTF8);
      |    }
      |  }
      |}
    """.stripMargin)

  testParquetToCatalyst(
    //向后兼容性：具有非可空值的类型的MAP - 3 - 在1.4.x之前
    "Backwards-compatibility: MAP with non-nullable value type - 3 - prior to 1.4.x",
    StructType(Seq(
      StructField(
        "f1",
        MapType(IntegerType, StringType, valueContainsNull = false),
        nullable = true))),
    """message root {
      |  optional group f1 (MAP) {
      |    repeated group map (MAP_KEY_VALUE) {
      |      required int32 key;
      |      required binary value (UTF8);
      |    }
      |  }
      |}
    """.stripMargin)

  testParquetToCatalyst(
    //向后兼容性：MAP具有可空值的类型 - 1 - 标准
    "Backwards-compatibility: MAP with nullable value type - 1 - standard",
    StructType(Seq(
      StructField(
        "f1",
        MapType(IntegerType, StringType, valueContainsNull = true),
        nullable = true))),
    """message root {
      |  optional group f1 (MAP) {
      |    repeated group key_value {
      |      required int32 key;
      |      optional binary value (UTF8);
      |    }
      |  }
      |}
    """.stripMargin)

  testParquetToCatalyst(//向后兼容性：MAP具有可空值的类型 - 2
    "Backwards-compatibility: MAP with nullable value type - 2",
    StructType(Seq(
      StructField(
        "f1",
        MapType(IntegerType, StringType, valueContainsNull = true),
        nullable = true))),
    """message root {
      |  optional group f1 (MAP_KEY_VALUE) {
      |    repeated group map {
      |      required int32 num;
      |      optional binary str (UTF8);
      |    }
      |  }
      |}
    """.stripMargin)

  testParquetToCatalyst(//向后兼容性：MAP具有可空值类型 - 3 - parquet-avro风格
    "Backwards-compatibility: MAP with nullable value type - 3 - parquet-avro style",
    StructType(Seq(
      StructField(
        "f1",
        MapType(IntegerType, StringType, valueContainsNull = true),
        nullable = true))),
    """message root {
      |  optional group f1 (MAP) {
      |    repeated group map (MAP_KEY_VALUE) {
      |      required int32 key;
      |      optional binary value (UTF8);
      |    }
      |  }
      |}
    """.stripMargin)

  // ====================================================
  // Tests for converting Catalyst MapType to Parquet Map
  // 将Catalyst MapType转换为Parquet Map的测试
  // ====================================================

  testCatalystToParquet(
    //向后兼容性：具有非可空值值类型的MAP - 1 - 标准
    "Backwards-compatibility: MAP with non-nullable value type - 1 - standard",
    StructType(Seq(
      StructField(
        "f1",
        MapType(IntegerType, StringType, valueContainsNull = false),
        nullable = true))),
    """message root {
      |  optional group f1 (MAP) {
      |    repeated group key_value {
      |      required int32 key;
      |      required binary value (UTF8);
      |    }
      |  }
      |}
    """.stripMargin,
    followParquetFormatSpec = true)

  testCatalystToParquet(
    //向后兼容性:具有不可为空值的类型的MAP-2-在1.4.x之前
    "Backwards-compatibility: MAP with non-nullable value type - 2 - prior to 1.4.x",
    StructType(Seq(
      StructField(
        "f1",
        MapType(IntegerType, StringType, valueContainsNull = false),
        nullable = true))),
    """message root {
      |  optional group f1 (MAP) {
      |    repeated group map (MAP_KEY_VALUE) {
      |      required int32 key;
      |      required binary value (UTF8);
      |    }
      |  }
      |}
    """.stripMargin)

  testCatalystToParquet(
    "Backwards-compatibility: MAP with nullable value type - 1 - standard",
    StructType(Seq(
      StructField(
        "f1",
        MapType(IntegerType, StringType, valueContainsNull = true),
        nullable = true))),
    """message root {
      |  optional group f1 (MAP) {
      |    repeated group key_value {
      |      required int32 key;
      |      optional binary value (UTF8);
      |    }
      |  }
      |}
    """.stripMargin,
    followParquetFormatSpec = true)

  testCatalystToParquet(
    "Backwards-compatibility: MAP with nullable value type - 3 - prior to 1.4.x",
    StructType(Seq(
      StructField(
        "f1",
        MapType(IntegerType, StringType, valueContainsNull = true),
        nullable = true))),
    """message root {
      |  optional group f1 (MAP) {
      |    repeated group map (MAP_KEY_VALUE) {
      |      required int32 key;
      |      optional binary value (UTF8);
      |    }
      |  }
      |}
    """.stripMargin)

  // =================================
  // Tests for conversion for decimals
  // 小数转换测试
  // =================================

  testSchema(
    "DECIMAL(1, 0) - standard",
    StructType(Seq(StructField("f1", DecimalType(1, 0)))),
    """message root {
      |  optional int32 f1 (DECIMAL(1, 0));
      |}
    """.stripMargin,
    followParquetFormatSpec = true)

  testSchema(
    "DECIMAL(8, 3) - standard",
    StructType(Seq(StructField("f1", DecimalType(8, 3)))),
    """message root {
      |  optional int32 f1 (DECIMAL(8, 3));
      |}
    """.stripMargin,
    followParquetFormatSpec = true)

  testSchema(
    "DECIMAL(9, 3) - standard",
    StructType(Seq(StructField("f1", DecimalType(9, 3)))),
    """message root {
      |  optional int32 f1 (DECIMAL(9, 3));
      |}
    """.stripMargin,
    followParquetFormatSpec = true)

  testSchema(
    "DECIMAL(18, 3) - standard",
    StructType(Seq(StructField("f1", DecimalType(18, 3)))),
    """message root {
      |  optional int64 f1 (DECIMAL(18, 3));
      |}
    """.stripMargin,
    followParquetFormatSpec = true)

  testSchema(
    "DECIMAL(19, 3) - standard",
    StructType(Seq(StructField("f1", DecimalType(19, 3)))),
    """message root {
      |  optional fixed_len_byte_array(9) f1 (DECIMAL(19, 3));
      |}
    """.stripMargin,
    followParquetFormatSpec = true)

  testSchema(
    "DECIMAL(1, 0) - prior to 1.4.x",
    StructType(Seq(StructField("f1", DecimalType(1, 0)))),
    """message root {
      |  optional fixed_len_byte_array(1) f1 (DECIMAL(1, 0));
      |}
    """.stripMargin)

  testSchema(
    "DECIMAL(8, 3) - prior to 1.4.x",
    StructType(Seq(StructField("f1", DecimalType(8, 3)))),
    """message root {
      |  optional fixed_len_byte_array(4) f1 (DECIMAL(8, 3));
      |}
    """.stripMargin)

  testSchema(
    "DECIMAL(9, 3) - prior to 1.4.x",//1.4.x之前
    StructType(Seq(StructField("f1", DecimalType(9, 3)))),
    """message root {
      |  optional fixed_len_byte_array(5) f1 (DECIMAL(9, 3));
      |}
    """.stripMargin)

  testSchema(
    "DECIMAL(18, 3) - prior to 1.4.x",
    StructType(Seq(StructField("f1", DecimalType(18, 3)))),
    """message root {
      |  optional fixed_len_byte_array(8) f1 (DECIMAL(18, 3));
      |}
    """.stripMargin)

  private def testSchemaClipping(//测试模式剪切
      testName: String,
      parquetSchema: String,
      catalystSchema: StructType,
      expectedSchema: String): Unit = {
    test(s"Clipping - $testName") {
      val expected = MessageTypeParser.parseMessageType(expectedSchema)
      val actual = CatalystReadSupport.clipParquetSchema(
        MessageTypeParser.parseMessageType(parquetSchema), catalystSchema)

      try {
        expected.checkContains(actual)
        actual.checkContains(expected)
      } catch { case cause: Throwable =>
        fail(
            //预期剪辑模式
          s"""Expected clipped schema:
             |$expected
             |Actual clipped schema:
             |$actual
          """.stripMargin,
          cause)
      }
    }
  }

  testSchemaClipping(
    "simple nested struct",

    parquetSchema =
      """message root {
        |  required group f0 {
        |    optional int32 f00;
        |    optional int32 f01;
        |  }
        |}
      """.stripMargin,

    catalystSchema = {
      val f0Type = new StructType().add("f00", IntegerType, nullable = true)
      new StructType()
        .add("f0", f0Type, nullable = false)
        .add("f1", IntegerType, nullable = true)
    },

    expectedSchema =
      """message root {
        |  required group f0 {
        |    optional int32 f00;
        |  }
        |  optional int32 f1;
        |}
      """.stripMargin)

  testSchemaClipping(
    "parquet-protobuf style array",

    parquetSchema =
      """message root {
        |  required group f0 {
        |    repeated binary f00 (UTF8);
        |    repeated group f01 {
        |      optional int32 f010;
        |      optional double f011;
        |    }
        |  }
        |}
      """.stripMargin,

    catalystSchema = {
      val f00Type = ArrayType(StringType, containsNull = false)
      val f01Type = ArrayType(
        new StructType()
          .add("f011", DoubleType, nullable = true),
        containsNull = false)

      val f0Type = new StructType()
        .add("f00", f00Type, nullable = false)
        .add("f01", f01Type, nullable = false)
      val f1Type = ArrayType(IntegerType, containsNull = true)

      new StructType()
        .add("f0", f0Type, nullable = false)
        .add("f1", f1Type, nullable = true)
    },

    expectedSchema =
      """message root {
        |  required group f0 {
        |    repeated binary f00 (UTF8);
        |    repeated group f01 {
        |      optional double f011;
        |    }
        |  }
        |
        |  optional group f1 (LIST) {
        |    repeated group list {
        |      optional int32 element;
        |    }
        |  }
        |}
      """.stripMargin)

  testSchemaClipping(
    "parquet-thrift style array",

    parquetSchema =
      """message root {
        |  required group f0 {
        |    optional group f00 (LIST) {
        |      repeated binary f00_tuple (UTF8);
        |    }
        |
        |    optional group f01 (LIST) {
        |      repeated group f01_tuple {
        |        optional int32 f010;
        |        optional double f011;
        |      }
        |    }
        |  }
        |}
      """.stripMargin,

    catalystSchema = {
      val f01ElementType = new StructType()
        .add("f011", DoubleType, nullable = true)
        .add("f012", LongType, nullable = true)

      val f0Type = new StructType()
        .add("f00", ArrayType(StringType, containsNull = false), nullable = true)
        .add("f01", ArrayType(f01ElementType, containsNull = false), nullable = true)

      new StructType().add("f0", f0Type, nullable = false)
    },

    expectedSchema =
      """message root {
        |  required group f0 {
        |    optional group f00 (LIST) {
        |      repeated binary f00_tuple (UTF8);
        |    }
        |
        |    optional group f01 (LIST) {
        |      repeated group f01_tuple {
        |        optional double f011;
        |        optional int64 f012;
        |      }
        |    }
        |  }
        |}
      """.stripMargin)

  testSchemaClipping(//parquet avro样式列阵
    "parquet-avro style array",

    parquetSchema =
      """message root {
        |  required group f0 {
        |    optional group f00 (LIST) {
        |      repeated binary array (UTF8);
        |    }
        |
        |    optional group f01 (LIST) {
        |      repeated group array {
        |        optional int32 f010;
        |        optional double f011;
        |      }
        |    }
        |  }
        |}
      """.stripMargin,

    catalystSchema = {
      val f01ElementType = new StructType()
        .add("f011", DoubleType, nullable = true)
        .add("f012", LongType, nullable = true)

      val f0Type = new StructType()
        .add("f00", ArrayType(StringType, containsNull = false), nullable = true)
        .add("f01", ArrayType(f01ElementType, containsNull = false), nullable = true)

      new StructType().add("f0", f0Type, nullable = false)
    },

    expectedSchema =
      """message root {
        |  required group f0 {
        |    optional group f00 (LIST) {
        |      repeated binary array (UTF8);
        |    }
        |
        |    optional group f01 (LIST) {
        |      repeated group array {
        |        optional double f011;
        |        optional int64 f012;
        |      }
        |    }
        |  }
        |}
      """.stripMargin)

  testSchemaClipping(
    "parquet-hive style array",//parquet hive样式数组

    parquetSchema =
      """message root {
        |  optional group f0 {
        |    optional group f00 (LIST) {
        |      repeated group bag {
        |        optional binary array_element;
        |      }
        |    }
        |
        |    optional group f01 (LIST) {
        |      repeated group bag {
        |        optional group array_element {
        |          optional int32 f010;
        |          optional double f011;
        |        }
        |      }
        |    }
        |  }
        |}
      """.stripMargin,

    catalystSchema = {
      val f01ElementType = new StructType()
        .add("f011", DoubleType, nullable = true)
        .add("f012", LongType, nullable = true)

      val f0Type = new StructType()
        .add("f00", ArrayType(StringType, containsNull = true), nullable = true)
        .add("f01", ArrayType(f01ElementType, containsNull = true), nullable = true)

      new StructType().add("f0", f0Type, nullable = true)
    },

    expectedSchema =
      """message root {
        |  optional group f0 {
        |    optional group f00 (LIST) {
        |      repeated group bag {
        |        optional binary array_element;
        |      }
        |    }
        |
        |    optional group f01 (LIST) {
        |      repeated group bag {
        |        optional group array_element {
        |          optional double f011;
        |          optional int64 f012;
        |        }
        |      }
        |    }
        |  }
        |}
      """.stripMargin)

  testSchemaClipping(//2所需结构的级别列表
    "2-level list of required struct",

    parquetSchema =
      s"""message root {
         |  required group f0 {
         |    required group f00 (LIST) {
         |      repeated group element {
         |        required int32 f000;
         |        optional int64 f001;
         |      }
         |    }
         |  }
         |}
       """.stripMargin,

    catalystSchema = {
      val f00ElementType =
        new StructType()
          .add("f001", LongType, nullable = true)
          .add("f002", DoubleType, nullable = false)

      val f00Type = ArrayType(f00ElementType, containsNull = false)
      val f0Type = new StructType().add("f00", f00Type, nullable = false)

      new StructType().add("f0", f0Type, nullable = false)
    },

    expectedSchema =
      s"""message root {
         |  required group f0 {
         |    required group f00 (LIST) {
         |      repeated group element {
         |        optional int64 f001;
         |        required double f002;
         |      }
         |    }
         |  }
         |}
       """.stripMargin)

  testSchemaClipping(
    "standard array",//标准数组

    parquetSchema =
      """message root {
        |  required group f0 {
        |    optional group f00 (LIST) {
        |      repeated group list {
        |        required binary element (UTF8);
        |      }
        |    }
        |
        |    optional group f01 (LIST) {
        |      repeated group list {
        |        required group element {
        |          optional int32 f010;
        |          optional double f011;
        |        }
        |      }
        |    }
        |  }
        |}
      """.stripMargin,

    catalystSchema = {
      val f01ElementType = new StructType()
        .add("f011", DoubleType, nullable = true)
        .add("f012", LongType, nullable = true)

      val f0Type = new StructType()
        .add("f00", ArrayType(StringType, containsNull = false), nullable = true)
        .add("f01", ArrayType(f01ElementType, containsNull = false), nullable = true)

      new StructType().add("f0", f0Type, nullable = false)
    },

    expectedSchema =
      """message root {
        |  required group f0 {
        |    optional group f00 (LIST) {
        |      repeated group list {
        |        required binary element (UTF8);
        |      }
        |    }
        |
        |    optional group f01 (LIST) {
        |      repeated group list {
        |        required group element {
        |          optional double f011;
        |          optional int64 f012;
        |        }
        |      }
        |    }
        |  }
        |}
      """.stripMargin)

  testSchemaClipping(
    "empty requested schema",

    parquetSchema =
      """message root {
        |  required group f0 {
        |    required int32 f00;
        |    required int64 f01;
        |  }
        |}
      """.stripMargin,

    catalystSchema = new StructType(),

    expectedSchema = "message root {}")

  testSchemaClipping(//不相交的字段集
    "disjoint field sets",

    parquetSchema =
      """message root {
        |  required group f0 {
        |    required int32 f00;
        |    required int64 f01;
        |  }
        |}
      """.stripMargin,

    catalystSchema =
      new StructType()
        .add(
          "f0",
          new StructType()
            .add("f02", FloatType, nullable = true)
            .add("f03", DoubleType, nullable = true),
          nullable = true),

    expectedSchema =
      """message root {
        |  required group f0 {
        |    optional float f02;
        |    optional double f03;
        |  }
        |}
      """.stripMargin)

  testSchemaClipping(
    "parquet-avro style map",

    parquetSchema =
      """message root {
        |  required group f0 (MAP) {
        |    repeated group map (MAP_KEY_VALUE) {
        |      required int32 key;
        |      required group value {
        |        required int32 value_f0;
        |        required int64 value_f1;
        |      }
        |    }
        |  }
        |}
      """.stripMargin,

    catalystSchema = {
      val valueType =
        new StructType()
          .add("value_f1", LongType, nullable = false)
          .add("value_f2", DoubleType, nullable = false)

      val f0Type = MapType(IntegerType, valueType, valueContainsNull = false)

      new StructType().add("f0", f0Type, nullable = false)
    },

    expectedSchema =
      """message root {
        |  required group f0 (MAP) {
        |    repeated group map (MAP_KEY_VALUE) {
        |      required int32 key;
        |      required group value {
        |        required int64 value_f1;
        |        required double value_f2;
        |      }
        |    }
        |  }
        |}
      """.stripMargin)

  testSchemaClipping(
    "standard map",

    parquetSchema =
      """message root {
        |  required group f0 (MAP) {
        |    repeated group key_value {
        |      required int32 key;
        |      required group value {
        |        required int32 value_f0;
        |        required int64 value_f1;
        |      }
        |    }
        |  }
        |}
      """.stripMargin,

    catalystSchema = {
      val valueType =
        new StructType()
          .add("value_f1", LongType, nullable = false)
          .add("value_f2", DoubleType, nullable = false)

      val f0Type = MapType(IntegerType, valueType, valueContainsNull = false)

      new StructType().add("f0", f0Type, nullable = false)
    },

    expectedSchema =
      """message root {
        |  required group f0 (MAP) {
        |    repeated group key_value {
        |      required int32 key;
        |      required group value {
        |        required int64 value_f1;
        |        required double value_f2;
        |      }
        |    }
        |  }
        |}
      """.stripMargin)

  testSchemaClipping(//带复杂键的标准映射
    "standard map with complex key",

    parquetSchema =
      """message root {
        |  required group f0 (MAP) {
        |    repeated group key_value {
        |      required group key {
        |        required int32 value_f0;
        |        required int64 value_f1;
        |      }
        |      required int32 value;
        |    }
        |  }
        |}
      """.stripMargin,

    catalystSchema = {
      val keyType =
        new StructType()
          .add("value_f1", LongType, nullable = false)
          .add("value_f2", DoubleType, nullable = false)

      val f0Type = MapType(keyType, IntegerType, valueContainsNull = false)

      new StructType().add("f0", f0Type, nullable = false)
    },

    expectedSchema =
      """message root {
        |  required group f0 (MAP) {
        |    repeated group key_value {
        |      required group key {
        |        required int64 value_f1;
        |        required double value_f2;
        |      }
        |      required int32 value;
        |    }
        |  }
        |}
      """.stripMargin)
}
