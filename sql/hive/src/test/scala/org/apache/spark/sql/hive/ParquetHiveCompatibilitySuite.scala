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

package org.apache.spark.sql.hive

import java.sql.Timestamp
import java.util.{Locale, TimeZone}

import org.apache.hadoop.hive.conf.HiveConf
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.execution.datasources.parquet.ParquetCompatibilityTest
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.{Row, SQLConf, SQLContext}

class ParquetHiveCompatibilitySuite extends ParquetCompatibilityTest with BeforeAndAfterAll {
  override def _sqlContext: SQLContext = TestHive
  private val sqlContext = _sqlContext

  /**
   * Set the staging directory (and hence path to ignore Parquet files under)
    * 设置暂存目录（因此，忽略Parquet文件的路径）
   * to that set by [[HiveConf.ConfVars.STAGINGDIR]].
   */
  private val stagingDir = new HiveConf().getVar(HiveConf.ConfVars.STAGINGDIR)

  private val originalTimeZone = TimeZone.getDefault
  private val originalLocale = Locale.getDefault

  protected override def beforeAll(): Unit = {
    TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"))
    Locale.setDefault(Locale.US)
  }

  override protected def afterAll(): Unit = {
    TimeZone.setDefault(originalTimeZone)
    Locale.setDefault(originalLocale)
  }

  override protected def logParquetSchema(path: String): Unit = {
    val schema = readParquetSchema(path, { path =>
      !path.getName.startsWith("_") && !path.getName.startsWith(stagingDir)
    })

    logInfo(
      s"""Schema of the Parquet file written by parquet-avro:
         |$schema
       """.stripMargin)
  }

  private def testParquetHiveCompatibility(row: Row, hiveTypes: String*): Unit = {
    withTable("parquet_compat") {
      withTempPath { dir =>
        val path = dir.getCanonicalPath

        // Hive columns are always nullable, so here we append a all-null row.
        //Hive列总是可空的，所以这里我们附加一个全空的行
        val rows = row :: Row(Seq.fill(row.length)(null): _*) :: Nil

        // Don't convert Hive metastore Parquet tables to let Hive write those Parquet files.
        //不要转换Hive转移的Parquet表,让Hive写这些Parquet文件
        withSQLConf(HiveContext.CONVERT_METASTORE_PARQUET.key -> "false") {
          withTempTable("data") {
            val fields = hiveTypes.zipWithIndex.map { case (typ, index) => s"  col_$index $typ" }

            val ddl =
              s"""CREATE TABLE parquet_compat(
                 |${fields.mkString(",\n")}
                 |)
                 |STORED AS PARQUET
                 |LOCATION '$path'
               """.stripMargin

            logInfo(
              s"""Creating testing Parquet table with the following DDL:
                 |$ddl
               """.stripMargin)

            sqlContext.sql(ddl)

            val schema = sqlContext.table("parquet_compat").schema
            val rowRDD = sqlContext.sparkContext.parallelize(rows).coalesce(1)
            sqlContext.createDataFrame(rowRDD, schema).registerTempTable("data")
            sqlContext.sql("INSERT INTO TABLE parquet_compat SELECT * FROM data")
          }
        }

        logParquetSchema(path)

        // Unfortunately parquet-hive doesn't add `UTF8` annotation to BINARY when writing strings.
        // Have to assume all BINARY values are strings here.
        //不幸的是，编写字符串时，parquet-hive不会将“UTF8”注释添加到BINARY。
        //必须假设所有BINARY值都是字符串。
        withSQLConf(SQLConf.PARQUET_BINARY_AS_STRING.key -> "true") {
          checkAnswer(sqlContext.read.parquet(path), rows)
        }
      }
    }
  }
  //简单的原始
  test("simple primitives") {
    testParquetHiveCompatibility(
      Row(true, 1.toByte, 2.toShort, 3, 4.toLong, 5.1f, 6.1d, "foo"),
      "BOOLEAN", "TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE", "STRING")
  }
  //时间戳
  test("SPARK-10177 timestamp") {
    testParquetHiveCompatibility(Row(Timestamp.valueOf("2015-08-24 00:31:00")), "TIMESTAMP")
  }

  test("array") {
    testParquetHiveCompatibility(
      Row(
        Seq[Integer](1: Integer, null, 2: Integer, null),
        Seq[String]("foo", null, "bar", null),
        Seq[Seq[Integer]](
          Seq[Integer](1: Integer, null),
          Seq[Integer](2: Integer, null))),
      "ARRAY<INT>",
      "ARRAY<STRING>",
      "ARRAY<ARRAY<INT>>")
  }

  test("map") {
    testParquetHiveCompatibility(
      Row(
        Map[Integer, String](
          (1: Integer) -> "foo",
          (2: Integer) -> null)),
      "MAP<INT, STRING>")
  }

  // HIVE-11625: Parquet map entries with null keys are dropped by Hive
  //使用空键映射条目
  ignore("map entries with null keys") {
    testParquetHiveCompatibility(
      Row(
        Map[Integer, String](
          null.asInstanceOf[Integer] -> "bar",
          null.asInstanceOf[Integer] -> null)),
      "MAP<INT, STRING>")
  }

  test("struct") {
    testParquetHiveCompatibility(
      Row(Row(1, Seq("foo", "bar", null))),
      "STRUCT<f0: INT, f1: ARRAY<STRING>>")
  }
}
