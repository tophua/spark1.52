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

import java.nio.ByteBuffer
import java.util.{List => JList, Map => JMap}

import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.collection.JavaConverters.mapAsJavaMapConverter

import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.parquet.test.avro._
import org.apache.spark.sql.test.SharedSQLContext
//Parquet Avro兼容性套件
class ParquetAvroCompatibilitySuite extends ParquetCompatibilityTest with SharedSQLContext {
  private def withWriter[T <: IndexedRecord]
      (path: String, schema: Schema)
      (f: AvroParquetWriter[T] => Unit): Unit = {
    logInfo(
        //使用以下Avro模式将Avro记录写入Parquet文件
      s"""Writing Avro records with the following Avro schema into Parquet file:
         |
         |${schema.toString(true)}
       """.stripMargin)

    val writer = new AvroParquetWriter[T](new Path(path), schema)
    try f(writer) finally writer.close()
  }

  test("required primitives") {//所需的原始
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      withWriter[AvroPrimitives](path, AvroPrimitives.getClassSchema) { writer =>
        (0 until 10).foreach { i =>
          writer.write(
            AvroPrimitives.newBuilder()
              .setBoolColumn(i % 2 == 0)
              .setIntColumn(i)
              .setLongColumn(i.toLong * 10)
              .setFloatColumn(i.toFloat + 0.1f)
              .setDoubleColumn(i.toDouble + 0.2d)
              .setBinaryColumn(ByteBuffer.wrap(s"val_$i".getBytes("UTF-8")))
              .setStringColumn(s"val_$i")
              .build())
        }
      }

      logParquetSchema(path)

      checkAnswer(sqlContext.read.parquet(path), (0 until 10).map { i =>
        Row(
          i % 2 == 0,
          i,
          i.toLong * 10,
          i.toFloat + 0.1f,
          i.toDouble + 0.2d,
          s"val_$i".getBytes("UTF-8"),
          s"val_$i")
      })
    }
  }

  test("optional primitives") {//可选的原始类型
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      withWriter[AvroOptionalPrimitives](path, AvroOptionalPrimitives.getClassSchema) { writer =>
        (0 until 10).foreach { i =>
          val record = if (i % 3 == 0) {
            AvroOptionalPrimitives.newBuilder()
              .setMaybeBoolColumn(null)
              .setMaybeIntColumn(null)
              .setMaybeLongColumn(null)
              .setMaybeFloatColumn(null)
              .setMaybeDoubleColumn(null)
              .setMaybeBinaryColumn(null)
              .setMaybeStringColumn(null)
              .build()
          } else {
            AvroOptionalPrimitives.newBuilder()
              .setMaybeBoolColumn(i % 2 == 0)
              .setMaybeIntColumn(i)
              .setMaybeLongColumn(i.toLong * 10)
              .setMaybeFloatColumn(i.toFloat + 0.1f)
              .setMaybeDoubleColumn(i.toDouble + 0.2d)
              .setMaybeBinaryColumn(ByteBuffer.wrap(s"val_$i".getBytes("UTF-8")))
              .setMaybeStringColumn(s"val_$i")
              .build()
          }

          writer.write(record)
        }
      }

      logParquetSchema(path)

      checkAnswer(sqlContext.read.parquet(path), (0 until 10).map { i =>
        if (i % 3 == 0) {
          Row.apply(Seq.fill(7)(null): _*)
        } else {
          Row(
            i % 2 == 0,
            i,
            i.toLong * 10,
            i.toFloat + 0.1f,
            i.toDouble + 0.2d,
            s"val_$i".getBytes("UTF-8"),
            s"val_$i")
        }
      })
    }
  }

  test("non-nullable arrays") {//非空数组
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      withWriter[AvroNonNullableArrays](path, AvroNonNullableArrays.getClassSchema) { writer =>
        (0 until 10).foreach { i =>
          val record = {
            val builder =
              AvroNonNullableArrays.newBuilder()
                .setStringsColumn(Seq.tabulate(3)(i => s"val_$i").asJava)

            if (i % 3 == 0) {
              builder.setMaybeIntsColumn(null).build()
            } else {
              builder.setMaybeIntsColumn(Seq.tabulate(3)(Int.box).asJava).build()
            }
          }

          writer.write(record)
        }
      }

      logParquetSchema(path)

      checkAnswer(sqlContext.read.parquet(path), (0 until 10).map { i =>
        Row(
          Seq.tabulate(3)(i => s"val_$i"),
          if (i % 3 == 0) null else Seq.tabulate(3)(identity))
      })
    }
  }
  //可空数组(parquet-avro 1.7.0不能正确支持这个)
  ignore("nullable arrays (parquet-avro 1.7.0 does not properly support this)") {
    // TODO Complete this test case after upgrading to parquet-mr 1.8+
  }

  test("SPARK-10136 array of primitive array") {//原始数组
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      withWriter[AvroArrayOfArray](path, AvroArrayOfArray.getClassSchema) { writer =>
        (0 until 10).foreach { i =>
          writer.write(AvroArrayOfArray.newBuilder()
            .setIntArraysColumn(
              Seq.tabulate(3, 3)((i, j) => i * 3 + j: Integer).map(_.asJava).asJava)
            .build())
        }
      }

      logParquetSchema(path)

      checkAnswer(sqlContext.read.parquet(path), (0 until 10).map { i =>
        Row(Seq.tabulate(3, 3)((i, j) => i * 3 + j))
      })
    }
  }

  test("map of primitive array") {//原始数组
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      withWriter[AvroMapOfArray](path, AvroMapOfArray.getClassSchema) { writer =>
        (0 until 10).foreach { i =>
          writer.write(AvroMapOfArray.newBuilder()
            .setStringToIntsColumn(
              Seq.tabulate(3) { i =>
                i.toString -> Seq.tabulate(3)(j => i + j: Integer).asJava
              }.toMap.asJava)
            .build())
        }
      }

      logParquetSchema(path)

      checkAnswer(sqlContext.read.parquet(path), (0 until 10).map { i =>
        Row(Seq.tabulate(3)(i => i.toString -> Seq.tabulate(3)(j => i + j)).toMap)
      })
    }
  }

  test("various complex types") {//各种复杂类型
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      //C:\Users\liushuhua\AppData\Local\Temp\spark-0315827e-7087-4666-b17a-ff0a263039f1
      //println(path)
      withWriter[ParquetAvroCompat](path, ParquetAvroCompat.getClassSchema) { writer =>
        (0 until 10).foreach(i => writer.write(makeParquetAvroCompat(i)))
      }

      logParquetSchema(path)
      /**
       *  +--------------------+--------------------+--------------------+
          |      strings_column|string_to_int_column|      complex_column|
          +--------------------+--------------------+--------------------+
          |[arr_0, arr_1, ar...|Map(0 -> 0, 1 -> ...|Map(0 -> WrappedA...|
          |[arr_1, arr_2, ar...|Map(0 -> 1, 1 -> ...|Map(1 -> WrappedA...|
          |[arr_2, arr_3, ar...|Map(0 -> 2, 1 -> ...|Map(2 -> WrappedA...|
          |[arr_3, arr_4, ar...|Map(0 -> 3, 1 -> ...|Map(3 -> WrappedA...|
          |[arr_4, arr_5, ar...|Map(0 -> 4, 1 -> ...|Map(4 -> WrappedA...|
          |[arr_5, arr_6, ar...|Map(0 -> 5, 1 -> ...|Map(5 -> WrappedA...|
          |[arr_6, arr_7, ar...|Map(0 -> 6, 1 -> ...|Map(6 -> WrappedA...|
          |[arr_7, arr_8, ar...|Map(0 -> 7, 1 -> ...|Map(7 -> WrappedA...|
          |[arr_8, arr_9, ar...|Map(0 -> 8, 1 -> ...|Map(8 -> WrappedA...|
          |[arr_9, arr_10, a...|Map(0 -> 9, 1 -> ...|Map(9 -> WrappedA...|
          +--------------------+--------------------+--------------------+
       */
      //sqlContext.read.parquet(path).show()
      checkAnswer(sqlContext.read.parquet(path), (0 until 10).map { i =>
        Row(
          Seq.tabulate(3)(n => s"arr_${i + n}"),
          Seq.tabulate(3)(n => n.toString -> (i + n: Integer)).toMap,
          Seq.tabulate(3) { n =>
            (i + n).toString -> Seq.tabulate(3) { m =>
              Row(Seq.tabulate(3)(j => i + j + m), s"val_${i + m}")
            }
          }.toMap)
      })
    }
  }

  def makeParquetAvroCompat(i: Int): ParquetAvroCompat = {
    def makeComplexColumn(i: Int): JMap[String, JList[Nested]] = {
      Seq.tabulate(3) { n =>
        (i + n).toString -> Seq.tabulate(3) { m =>
          Nested
            .newBuilder()
            .setNestedIntsColumn(Seq.tabulate(3)(j => i + j + m: Integer).asJava)
            .setNestedStringColumn(s"val_${i + m}")
            .build()
        }.asJava
      }.toMap.asJava
    }

    ParquetAvroCompat
      .newBuilder()
      .setStringsColumn(Seq.tabulate(3)(n => s"arr_${i + n}").asJava)
      .setStringToIntColumn(Seq.tabulate(3)(n => n.toString -> (i + n: Integer)).toMap.asJava)
      .setComplexColumn(makeComplexColumn(i))
      .build()
  }
  //下推涉及Parquet ENUM列的谓词
  test("SPARK-9407 Push down predicates involving Parquet ENUM columns") {
    import testImplicits._

    withTempPath { dir =>
      val path = dir.getCanonicalPath

      withWriter[ParquetEnum](path, ParquetEnum.getClassSchema) { writer =>
        (0 until 4).foreach { i =>
          writer.write(ParquetEnum.newBuilder().setSuit(Suit.values.apply(i)).build())
        }
      }

      checkAnswer(sqlContext.read.parquet(path).filter('suit === "SPADES"), Row("SPADES"))
    }
  }
}
