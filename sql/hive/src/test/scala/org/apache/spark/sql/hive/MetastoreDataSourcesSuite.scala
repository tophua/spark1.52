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

import java.io.{IOException, File}

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.Path
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.client.{HiveTable, ManagedTable}
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.hive.test.TestHive._
import org.apache.spark.sql.hive.test.TestHive.implicits._
import org.apache.spark.sql.execution.datasources.parquet.ParquetRelation
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

/**
 * Tests for persisting tables created though the data sources API into the metastore.
  *测试通过数据源API创建的持久化表到元数据存储
 */
class MetastoreDataSourcesSuite extends QueryTest with SQLTestUtils with BeforeAndAfterAll
  with Logging {
  override def _sqlContext: SQLContext = TestHive
  private val sqlContext = _sqlContext

  var jsonFilePath: String = _

  override def beforeAll(): Unit = {
    jsonFilePath = Utils.getSparkClassLoader.getResource("sample.json").getFile
  }
  //持久的JSON表
  test("persistent JSON table") {
    withTable("jsonTable") {
      sql(
        s"""CREATE TABLE jsonTable
           |USING org.apache.spark.sql.json.DefaultSource
           |OPTIONS (
           |  path '$jsonFilePath'
           |)
         """.stripMargin)

      checkAnswer(
        sql("SELECT * FROM jsonTable"),
        read.json(jsonFilePath).collect().toSeq)
    }
  }
  //具有用户指定模式的持久JSON表
  test("persistent JSON table with a user specified schema") {
    withTable("jsonTable") {
      sql(
        s"""CREATE TABLE jsonTable (
           |a string,
           |b String,
           |`c_!@(3)` int,
           |`<d>` Struct<`d!`:array<int>, `=`:array<struct<Dd2: boolean>>>)
           |USING org.apache.spark.sql.json.DefaultSource
           |OPTIONS (
           |  path '$jsonFilePath'
           |)
         """.stripMargin)

      withTempTable("expectedJsonTable") {
        read.json(jsonFilePath).registerTempTable("expectedJsonTable")
        checkAnswer(
          sql("SELECT a, b, `c_!@(3)`, `<d>`.`d!`, `<d>`.`=` FROM jsonTable"),
          sql("SELECT a, b, `c_!@(3)`, `<d>`.`d!`, `<d>`.`=` FROM expectedJsonTable"))
      }
    }
  }
  //具有用户指定模式的持久JSON表与一个字段的子集
  test("persistent JSON table with a user specified schema with a subset of fields") {
    withTable("jsonTable") {
      // This works because JSON objects are self-describing and JSONRelation can get needed
      // field values based on field names.
      //这是因为JSON对象是自描述的，JSONRelation可以被需要基于字段名称的字段值。
      sql(
        s"""CREATE TABLE jsonTable (`<d>` Struct<`=`:array<struct<Dd2: boolean>>>, b String)
           |USING org.apache.spark.sql.json.DefaultSource
           |OPTIONS (
           |  path '$jsonFilePath'
           |)
         """.stripMargin)

      val innerStruct = StructType(Seq(
        StructField("=", ArrayType(StructType(StructField("Dd2", BooleanType, true) :: Nil)))))

      val expectedSchema = StructType(Seq(
        StructField("<d>", innerStruct, true),
        StructField("b", StringType, true)))

      assert(expectedSchema === table("jsonTable").schema)

      withTempTable("expectedJsonTable") {
        read.json(jsonFilePath).registerTempTable("expectedJsonTable")
        checkAnswer(
          sql("SELECT b, `<d>`.`=` FROM jsonTable"),
          sql("SELECT b, `<d>`.`=` FROM expectedJsonTable"))
      }
    }
  }
  //解析缩短提供商名称
  test("resolve shortened provider names") {
    withTable("jsonTable") {
      sql(
        s"""
           |CREATE TABLE jsonTable
           |USING org.apache.spark.sql.json
           |OPTIONS (
           |  path '$jsonFilePath'
           |)
         """.stripMargin)

      checkAnswer(
        sql("SELECT * FROM jsonTable"),
        read.json(jsonFilePath).collect().toSeq)
    }
  }
  //删除表
  test("drop table") {
    withTable("jsonTable") {
      sql(
        s"""
           |CREATE TABLE jsonTable
           |USING org.apache.spark.sql.json
           |OPTIONS (
           |  path '$jsonFilePath'
           |)
         """.stripMargin)

      checkAnswer(
        sql("SELECT * FROM jsonTable"),
        read.json(jsonFilePath))

      sql("DROP TABLE jsonTable")

      intercept[Exception] {
        sql("SELECT * FROM jsonTable").collect()
      }

      assert(
        new File(jsonFilePath).exists(),
        "The table with specified path is considered as an external table, " +
          "its data should not deleted after DROP TABLE.")
    }
  }
  //无刷新检查更改
  test("check change without refresh") {
    withTempPath { tempDir =>
      withTable("jsonTable") {
        (("a", "b") :: Nil).toDF().toJSON.saveAsTextFile(tempDir.getCanonicalPath)

        sql(
          s"""CREATE TABLE jsonTable
             |USING org.apache.spark.sql.json
             |OPTIONS (
             |  path '${tempDir.getCanonicalPath}'
             |)
           """.stripMargin)

        checkAnswer(
          sql("SELECT * FROM jsonTable"),
          Row("a", "b"))

        Utils.deleteRecursively(tempDir)
        (("a1", "b1", "c1") :: Nil).toDF().toJSON.saveAsTextFile(tempDir.getCanonicalPath)

        // Schema is cached so the new column does not show. The updated values in existing columns
        // will show.
        //缓存模式,因此新列不显示,将显示现有列中的更新值。
        checkAnswer(
          sql("SELECT * FROM jsonTable"),
          Row("a1", "b1"))

        sql("REFRESH TABLE jsonTable")

        // Check that the refresh worked
        checkAnswer(
          sql("SELECT * FROM jsonTable"),
          Row("a1", "b1", "c1"))
      }
    }
  }
  //删除,改变,再创建
  test("drop, change, recreate") {
    withTempPath { tempDir =>
      (("a", "b") :: Nil).toDF().toJSON.saveAsTextFile(tempDir.getCanonicalPath)

      withTable("jsonTable") {
        sql(
          s"""CREATE TABLE jsonTable
             |USING org.apache.spark.sql.json
             |OPTIONS (
             |  path '${tempDir.getCanonicalPath}'
             |)
           """.stripMargin)

        checkAnswer(
          sql("SELECT * FROM jsonTable"),
          Row("a", "b"))

        Utils.deleteRecursively(tempDir)
        (("a", "b", "c") :: Nil).toDF().toJSON.saveAsTextFile(tempDir.getCanonicalPath)

        sql("DROP TABLE jsonTable")

        sql(
          s"""CREATE TABLE jsonTable
             |USING org.apache.spark.sql.json
             |OPTIONS (
             |  path '${tempDir.getCanonicalPath}'
             |)
           """.stripMargin)

        // New table should reflect new schema.
        //新表应反映新模式
        checkAnswer(
          sql("SELECT * FROM jsonTable"),
          Row("a", "b", "c"))
      }
    }
  }
  //使缓存无效并重新加载
  test("invalidate cache and reload") {
    withTable("jsonTable") {
      sql(
        s"""CREATE TABLE jsonTable (`c_!@(3)` int)
           |USING org.apache.spark.sql.json.DefaultSource
           |OPTIONS (
           |  path '$jsonFilePath'
           |)
         """.stripMargin)

      withTempTable("expectedJsonTable") {
        read.json(jsonFilePath).registerTempTable("expectedJsonTable")

        checkAnswer(
          sql("SELECT * FROM jsonTable"),
          sql("SELECT `c_!@(3)` FROM expectedJsonTable").collect().toSeq)

        // Discard the cached relation.
        //放弃缓存的关系
        invalidateTable("jsonTable")

        checkAnswer(
          sql("SELECT * FROM jsonTable"),
          sql("SELECT `c_!@(3)` FROM expectedJsonTable").collect().toSeq)

        invalidateTable("jsonTable")
        val expectedSchema = StructType(StructField("c_!@(3)", IntegerType, true) :: Nil)

        assert(expectedSchema === table("jsonTable").schema)
      }
    }
  }
  //create table as select （CTAS）
  test("CTAS") {
    withTempPath { tempPath =>
      withTable("jsonTable", "ctasJsonTable") {
        sql(
          s"""CREATE TABLE jsonTable
             |USING org.apache.spark.sql.json.DefaultSource
             |OPTIONS (
             |  path '$jsonFilePath'
             |)
           """.stripMargin)

        sql(
          s"""CREATE TABLE ctasJsonTable
             |USING org.apache.spark.sql.json.DefaultSource
             |OPTIONS (
             |  path '$tempPath'
             |) AS
             |SELECT * FROM jsonTable
           """.stripMargin)

        assert(table("ctasJsonTable").schema === table("jsonTable").schema)

        checkAnswer(
          sql("SELECT * FROM ctasJsonTable"),
          sql("SELECT * FROM jsonTable").collect())
      }
    }
  }
  //如果不存在,create table as select （CTAS）
  test("CTAS with IF NOT EXISTS") {
    withTempPath { path =>
      val tempPath = path.getCanonicalPath

      withTable("jsonTable", "ctasJsonTable") {
        sql(
          s"""CREATE TABLE jsonTable
             |USING org.apache.spark.sql.json.DefaultSource
             |OPTIONS (
             |  path '$jsonFilePath'
             |)
           """.stripMargin)

        sql(
          s"""CREATE TABLE ctasJsonTable
             |USING org.apache.spark.sql.json.DefaultSource
             |OPTIONS (
             |  path '$tempPath'
             |) AS
             |SELECT * FROM jsonTable
           """.stripMargin)

        // Create the table again should trigger a AnalysisException.
        //再次创建表应该触发一个AnalysisException
        val message = intercept[AnalysisException] {
          sql(
            s"""CREATE TABLE ctasJsonTable
               |USING org.apache.spark.sql.json.DefaultSource
               |OPTIONS (
               |  path '$tempPath'
               |) AS
               |SELECT * FROM jsonTable
             """.stripMargin)
        }.getMessage

        assert(
          message.contains("Table ctasJsonTable already exists."),
          "We should complain that ctasJsonTable already exists")

        // The following statement should be fine if it has IF NOT EXISTS.
        // It tries to create a table ctasJsonTable with a new schema.
        // The actual table's schema and data should not be changed.
        //如果有不存在的话,以下声明应该很好,它试图用一种新的模式创建一个表ctasjsontable。
        //实际表的架构和数据不应更改。
        sql(
          s"""CREATE TABLE IF NOT EXISTS ctasJsonTable
             |USING org.apache.spark.sql.json.DefaultSource
             |OPTIONS (
             |  path '$tempPath'
             |) AS
             |SELECT a FROM jsonTable
           """.stripMargin)

        // Discard the cached relation.
        //丢弃缓存关系
        invalidateTable("ctasJsonTable")

        // Schema should not be changed.模式不应更改
        assert(table("ctasJsonTable").schema === table("jsonTable").schema)
        // Table data should not be changed. 不应更改表数据
        checkAnswer(
          sql("SELECT * FROM ctasJsonTable"),
          sql("SELECT * FROM jsonTable").collect())
      }
    }
  }
  //CTAS管理表 (create table .. as select)
  test("CTAS a managed table") {
    withTable("jsonTable", "ctasJsonTable", "loadedTable") {
      sql(
        s"""CREATE TABLE jsonTable
           |USING org.apache.spark.sql.json.DefaultSource
           |OPTIONS (
           |  path '$jsonFilePath'
           |)
         """.stripMargin)

      val expectedPath = catalog.hiveDefaultTableFilePath("ctasJsonTable")
      val filesystemPath = new Path(expectedPath)
      val fs = filesystemPath.getFileSystem(sparkContext.hadoopConfiguration)
      if (fs.exists(filesystemPath)) fs.delete(filesystemPath, true)

      // It is a managed table when we do not specify the location.
      //当我们不指定位置时,它是一个托管表。
      sql(
        s"""CREATE TABLE ctasJsonTable
           |USING org.apache.spark.sql.json.DefaultSource
           |AS
           |SELECT * FROM jsonTable
         """.stripMargin)

      assert(fs.exists(filesystemPath), s"$expectedPath should exist after we create the table.")

      sql(
        s"""CREATE TABLE loadedTable
           |USING org.apache.spark.sql.json.DefaultSource
           |OPTIONS (
           |  path '$expectedPath'
           |)
         """.stripMargin)

      assert(table("ctasJsonTable").schema === table("loadedTable").schema)

      checkAnswer(
        sql("SELECT * FROM ctasJsonTable"),
        sql("SELECT * FROM loadedTable"))

      sql("DROP TABLE ctasJsonTable")
      assert(!fs.exists(filesystemPath), s"$expectedPath should not exist after we drop the table.")
    }
  }
  //在使用数据源API时未能删除无效表
  test("SPARK-5286 Fail to drop an invalid table when using the data source API") {
    withTable("jsonTable") {
      sql(
        s"""CREATE TABLE jsonTable
           |USING org.apache.spark.sql.json.DefaultSource
           |OPTIONS (
           |  path 'it is not a path at all!'
           |)
         """.stripMargin)

      sql("DROP TABLE jsonTable").collect().foreach(i => logInfo(i.toString))
    }
  }
  //使用数据源API时无法删除无效表
  test("SPARK-5839 HiveMetastoreCatalog does not recognize table aliases of data source tables.") {
    withTable("savedJsonTable") {
      // Save the df as a managed table (by not specifying the path).
      //将df保存为托管表（不指定路径）。
      (1 to 10)
        .map(i => i -> s"str$i")
        .toDF("a", "b")
        .write
        .format("json")
        .saveAsTable("savedJsonTable")

      checkAnswer(
        sql("SELECT * FROM savedJsonTable where savedJsonTable.a < 5"),
        (1 to 4).map(i => Row(i, s"str$i")))

      checkAnswer(
        sql("SELECT * FROM savedJsonTable tmp where tmp.a > 5"),
        (6 to 10).map(i => Row(i, s"str$i")))

      invalidateTable("savedJsonTable")

      checkAnswer(
        sql("SELECT * FROM savedJsonTable where savedJsonTable.a < 5"),
        (1 to 4).map(i => Row(i, s"str$i")))

      checkAnswer(
        sql("SELECT * FROM savedJsonTable tmp where tmp.a > 5"),
        (6 to 10).map(i => Row(i, s"str$i")))
    }
  }
//保存表
  test("save table") {
    withTempPath { path =>
      val tempPath = path.getCanonicalPath

      withTable("savedJsonTable") {
        val df = (1 to 10).map(i => i -> s"str$i").toDF("a", "b")

        withSQLConf(SQLConf.DEFAULT_DATA_SOURCE_NAME.key -> "json") {
          // Save the df as a managed table (by not specifying the path).
          //将df保存为托管表(不指定路径)
          df.write.saveAsTable("savedJsonTable")

          checkAnswer(sql("SELECT * FROM savedJsonTable"), df)

          // We can overwrite it.
          //我们可以覆盖它
          df.write.mode(SaveMode.Overwrite).saveAsTable("savedJsonTable")
          checkAnswer(sql("SELECT * FROM savedJsonTable"), df)

          // When the save mode is Ignore, we will do nothing when the table already exists.
          //当保存模式为Ignore时,当表已存在时,我们将不执行任何操作。
          df.select("b").write.mode(SaveMode.Ignore).saveAsTable("savedJsonTable")
          // TODO in ResolvedDataSource, will convert the schema into nullable = true
          // hence the df.schema is not exactly the same as table("savedJsonTable").schema
          // assert(df.schema === table("savedJsonTable").schema)
          //因此df.schema与表（“save Json Table”）不完全相同。schema // assert（df.schema === table（“save Json Table”）。schema）
          checkAnswer(sql("SELECT * FROM savedJsonTable"), df)

          // Drop table will also delete the data.
          // 删除表也将删除数据
          sql("DROP TABLE savedJsonTable")
          intercept[IOException] {
            read.json(catalog.hiveDefaultTableFilePath("savedJsonTable"))
          }
        }

        // Create an external table by specifying the path.
        //通过指定路径创建外部表。
        withSQLConf(SQLConf.DEFAULT_DATA_SOURCE_NAME.key -> "not a source name") {
          df.write
            .format("org.apache.spark.sql.json")
            .mode(SaveMode.Append)
            .option("path", tempPath.toString)
            .saveAsTable("savedJsonTable")
          checkAnswer(sql("SELECT * FROM savedJsonTable"), df)
        }

        // Data should not be deleted after we drop the table.
        //删除表后不应删除数据
        sql("DROP TABLE savedJsonTable")
        checkAnswer(read.json(tempPath.toString), df)
      }
    }
  }
  //创建外部表
  test("create external table") {
    withTempPath { tempPath =>
      withTable("savedJsonTable", "createdJsonTable") {
        val df = read.json(sparkContext.parallelize((1 to 10).map { i =>
          s"""{ "a": $i, "b": "str$i" }"""
        }))

        withSQLConf(SQLConf.DEFAULT_DATA_SOURCE_NAME.key -> "not a source name") {
          df.write
            .format("json")
            .mode(SaveMode.Append)
            .option("path", tempPath.toString)
            .saveAsTable("savedJsonTable")
        }

        withSQLConf(SQLConf.DEFAULT_DATA_SOURCE_NAME.key -> "json") {
          createExternalTable("createdJsonTable", tempPath.toString)
          assert(table("createdJsonTable").schema === df.schema)
          checkAnswer(sql("SELECT * FROM createdJsonTable"), df)

          assert(
            intercept[AnalysisException] {
              createExternalTable("createdJsonTable", jsonFilePath.toString)
            }.getMessage.contains("Table createdJsonTable already exists."),
            "We should complain that createdJsonTable already exists")
        }

        // Data should not be deleted.
        //不应删除数据
        sql("DROP TABLE createdJsonTable")
        checkAnswer(read.json(tempPath.toString), df)

        // Try to specify the schema.
        //尝试指定模式
        withSQLConf(SQLConf.DEFAULT_DATA_SOURCE_NAME.key -> "not a source name") {
          val schema = StructType(StructField("b", StringType, true) :: Nil)
          createExternalTable(
            "createdJsonTable",
            "org.apache.spark.sql.json",
            schema,
            Map("path" -> tempPath.toString))

          checkAnswer(
            sql("SELECT * FROM createdJsonTable"),
            sql("SELECT b FROM savedJsonTable"))

          sql("DROP TABLE createdJsonTable")

          assert(
            intercept[RuntimeException] {
              createExternalTable(
                "createdJsonTable",
                "org.apache.spark.sql.json",
                schema,
                Map.empty[String, String])
            }.getMessage.contains("key not found: path"),
            "We should complain that path is not specified.")
        }
      }
    }
  }
  //扫描通过CTAS语句创建的parquet表
  test("scan a parquet table created through a CTAS statement") {
    withSQLConf(HiveContext.CONVERT_METASTORE_PARQUET.key -> "true") {
      withTempTable("jt") {
        (1 to 10).map(i => i -> s"str$i").toDF("a", "b").registerTempTable("jt")

        withTable("test_parquet_ctas") {
          sql(
            """CREATE TABLE test_parquet_ctas STORED AS PARQUET
              |AS SELECT tmp.a FROM jt tmp WHERE tmp.a < 5
            """.stripMargin)

          checkAnswer(
            sql(s"SELECT a FROM test_parquet_ctas WHERE a > 2 "),
            Row(3) :: Row(4) :: Nil)

          table("test_parquet_ctas").queryExecution.optimizedPlan match {
            case LogicalRelation(p: ParquetRelation, _) => // OK
            case _ =>
              fail(s"test_parquet_ctas should have be converted to ${classOf[ParquetRelation]}")
          }
        }
      }
    }
  }
  //预插入可空性检查(ArrayType)
  test("Pre insert nullability check (ArrayType)") {
    withTable("arrayInParquet") {
      {
        val df = (Tuple1(Seq(Int.box(1), null: Integer)) :: Nil).toDF("a")
        val expectedSchema =
          StructType(
            StructField(
              "a",
              ArrayType(IntegerType, containsNull = true),
              nullable = true) :: Nil)

        assert(df.schema === expectedSchema)

        df.write
          .format("parquet")
          .mode(SaveMode.Overwrite)
          .saveAsTable("arrayInParquet")
      }

      {
        val df = (Tuple1(Seq(2, 3)) :: Nil).toDF("a")
        val expectedSchema =
          StructType(
            StructField(
              "a",
              ArrayType(IntegerType, containsNull = false),
              nullable = true) :: Nil)

        assert(df.schema === expectedSchema)

        df.write
          .format("parquet")
          .mode(SaveMode.Append)
          .insertInto("arrayInParquet")
      }

      (Tuple1(Seq(4, 5)) :: Nil).toDF("a")
        .write
        .mode(SaveMode.Append)
        //这个内部调用df2.insertInto
        .saveAsTable("arrayInParquet") // This one internally calls df2.insertInto.

      (Tuple1(Seq(Int.box(6), null: Integer)) :: Nil).toDF("a")
        .write
        .mode(SaveMode.Append)
        .saveAsTable("arrayInParquet")

      refreshTable("arrayInParquet")

      checkAnswer(
        sql("SELECT a FROM arrayInParquet"),
        Row(ArrayBuffer(1, null)) ::
          Row(ArrayBuffer(2, 3)) ::
          Row(ArrayBuffer(4, 5)) ::
          Row(ArrayBuffer(6, null)) :: Nil)
    }
  }
  //预插入可空性检查(MapType)
  test("Pre insert nullability check (MapType)") {
    withTable("mapInParquet") {
      {
        val df = (Tuple1(Map(1 -> (null: Integer))) :: Nil).toDF("a")
        val expectedSchema =
          StructType(
            StructField(
              "a",
              MapType(IntegerType, IntegerType, valueContainsNull = true),
              nullable = true) :: Nil)

        assert(df.schema === expectedSchema)

        df.write
          .format("parquet")
          .mode(SaveMode.Overwrite)
          .saveAsTable("mapInParquet")
      }

      {
        val df = (Tuple1(Map(2 -> 3)) :: Nil).toDF("a")
        val expectedSchema =
          StructType(
            StructField(
              "a",
              MapType(IntegerType, IntegerType, valueContainsNull = false),
              nullable = true) :: Nil)

        assert(df.schema === expectedSchema)

        df.write
          .format("parquet")
          .mode(SaveMode.Append)
          .insertInto("mapInParquet")
      }

      (Tuple1(Map(4 -> 5)) :: Nil).toDF("a")
        .write
        .format("parquet")
        .mode(SaveMode.Append)
        .saveAsTable("mapInParquet") // This one internally calls df2.insertInto.

      (Tuple1(Map(6 -> null.asInstanceOf[Integer])) :: Nil).toDF("a")
        .write
        .format("parquet")
        .mode(SaveMode.Append)
        .saveAsTable("mapInParquet")

      refreshTable("mapInParquet")

      checkAnswer(
        sql("SELECT a FROM mapInParquet"),
        Row(Map(1 -> null)) ::
          Row(Map(2 -> 3)) ::
          Row(Map(4 -> 5)) ::
          Row(Map(6 -> null)) :: Nil)
    }
  }
  //广泛的架构支持
  test("SPARK-6024 wide schema support") {
    withSQLConf(SQLConf.SCHEMA_STRING_LENGTH_THRESHOLD.key -> "4000") {
      withTable("wide_schema") {
        // We will need 80 splits for this schema if the threshold is 4000.
        //如果阈值为4000，我们将需要80个分割。
        val schema = StructType((1 to 5000).map(i => StructField(s"c_$i", StringType, true)))

        // Manually create a metastore data source table.
        //手动创建一个转移数据源表
        catalog.createDataSourceTable(
          tableName = "wide_schema",
          userSpecifiedSchema = Some(schema),
          partitionColumns = Array.empty[String],
          provider = "json",
          options = Map("path" -> "just a dummy path"),
          isExternal = false)

        invalidateTable("wide_schema")

        val actualSchema = table("wide_schema").schema
        assert(schema === actualSchema)
      }
    }
  }
  //仍然支持存储在spark.sql.sources.schema中的模式
  test("SPARK-6655 still support a schema stored in spark.sql.sources.schema") {
    val tableName = "spark6655"
    withTable(tableName) {
      val schema = StructType(StructField("int", IntegerType, true) :: Nil)
      val hiveTable = HiveTable(
        specifiedDatabase = Some("default"),
        name = tableName,
        schema = Seq.empty,
        partitionColumns = Seq.empty,
        properties = Map(
          "spark.sql.sources.provider" -> "json",
          "spark.sql.sources.schema" -> schema.json,
          "EXTERNAL" -> "FALSE"),
        tableType = ManagedTable,
        serdeProperties = Map(
          "path" -> catalog.hiveDefaultTableFilePath(tableName)))

      catalog.client.createTable(hiveTable)

      invalidateTable(tableName)
      val actualSchema = table(tableName).schema
      assert(schema === actualSchema)
    }
  }
  //保存分区列信息
  test("Saving partition columns information") {
    val df = (1 to 10).map(i => (i, i + 1, s"str$i", s"str${i + 1}")).toDF("a", "b", "c", "d")
    val tableName = s"partitionInfo_${System.currentTimeMillis()}"

    withTable(tableName) {
      df.write.format("parquet").partitionBy("d", "b").saveAsTable(tableName)
      invalidateTable(tableName)
      val metastoreTable = catalog.client.getTable("default", tableName)
      val expectedPartitionColumns = StructType(df.schema("d") :: df.schema("b") :: Nil)
      val actualPartitionColumns =
        StructType(
          metastoreTable.partitionColumns.map(c =>
            StructField(c.name, HiveMetastoreTypes.toDataType(c.hiveType))))
      // Make sure partition columns are correctly stored in metastore.
      //确保分区列正确存储在转移中
      assert(
        expectedPartitionColumns.sameType(actualPartitionColumns),
        s"Partitions columns stored in metastore $actualPartitionColumns is not the " +
          s"partition columns defined by the saveAsTable operation $expectedPartitionColumns.")

      // Check the content of the saved table.
      //检查保存的表的内容
      checkAnswer(
        table(tableName).select("c", "b", "d", "a"),
        df.select("c", "b", "d", "a"))
    }
  }
  //插入表中
  test("insert into a table") {
    def createDF(from: Int, to: Int): DataFrame = {
      (from to to).map(i => i -> s"str$i").toDF("c1", "c2")
    }

    withTable("insertParquet") {
      createDF(0, 9).write.format("parquet").saveAsTable("insertParquet")
      checkAnswer(
        sql("SELECT p.c1, p.c2 FROM insertParquet p WHERE p.c1 > 5"),
        (6 to 9).map(i => Row(i, s"str$i")))

      intercept[AnalysisException] {
        createDF(10, 19).write.format("parquet").saveAsTable("insertParquet")
      }

      createDF(10, 19).write.mode(SaveMode.Append).format("parquet").saveAsTable("insertParquet")
      checkAnswer(
        sql("SELECT p.c1, p.c2 FROM insertParquet p WHERE p.c1 > 5"),
        (6 to 19).map(i => Row(i, s"str$i")))

      createDF(20, 29).write.mode(SaveMode.Append).format("parquet").saveAsTable("insertParquet")
      checkAnswer(
        sql("SELECT p.c1, c2 FROM insertParquet p WHERE p.c1 > 5 AND p.c1 < 25"),
        (6 to 24).map(i => Row(i, s"str$i")))

      intercept[AnalysisException] {
        createDF(30, 39).write.saveAsTable("insertParquet")
      }

      createDF(30, 39).write.mode(SaveMode.Append).saveAsTable("insertParquet")
      checkAnswer(
        sql("SELECT p.c1, c2 FROM insertParquet p WHERE p.c1 > 5 AND p.c1 < 35"),
        (6 to 34).map(i => Row(i, s"str$i")))

      createDF(40, 49).write.mode(SaveMode.Append).insertInto("insertParquet")
      checkAnswer(
        sql("SELECT p.c1, c2 FROM insertParquet p WHERE p.c1 > 5 AND p.c1 < 45"),
        (6 to 44).map(i => Row(i, s"str$i")))

      createDF(50, 59).write.mode(SaveMode.Overwrite).saveAsTable("insertParquet")
      checkAnswer(
        sql("SELECT p.c1, c2 FROM insertParquet p WHERE p.c1 > 51 AND p.c1 < 55"),
        (52 to 54).map(i => Row(i, s"str$i")))
      createDF(60, 69).write.mode(SaveMode.Ignore).saveAsTable("insertParquet")
      checkAnswer(
        sql("SELECT p.c1, c2 FROM insertParquet p"),
        (50 to 59).map(i => Row(i, s"str$i")))

      createDF(70, 79).write.mode(SaveMode.Overwrite).insertInto("insertParquet")
      checkAnswer(
        sql("SELECT p.c1, c2 FROM insertParquet p"),
        (70 to 79).map(i => Row(i, s"str$i")))
    }
  }
  //通过'使用dbname'创建表到特定的数据库
  test("SPARK-8156:create table to specific database by 'use dbname' ") {

    val df = (1 to 3).map(i => (i, s"val_$i", i * 2)).toDF("a", "b", "c")
    sqlContext.sql("""create database if not exists testdb8156""")
    sqlContext.sql("""use testdb8156""")
    df.write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .saveAsTable("ttt3")

    checkAnswer(
      sqlContext.sql("show TABLES in testdb8156").filter("tableName = 'ttt3'"),
      Row("ttt3", false))
    sqlContext.sql("""use default""")
    sqlContext.sql("""drop database if exists testdb8156 CASCADE""")
  }
}
