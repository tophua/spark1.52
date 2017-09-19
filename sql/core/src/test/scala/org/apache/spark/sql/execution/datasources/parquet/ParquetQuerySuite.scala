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

import java.io.File

import org.apache.hadoop.fs.Path

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificMutableRow
import org.apache.spark.sql.execution.datasources.parquet.TestingUDT.{NestedStructUDT, NestedStruct}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

/**
 * A test suite that tests various Parquet queries. 
 * 测试各种Parquet查询的测试套件
 */
class ParquetQuerySuite extends QueryTest with ParquetTest with SharedSQLContext {

  test("simple select queries") {//简单的选择查询
    /**
      +---+
      | _1|
      +---+
      |  6|
      |  7|
      |  8|
      |  9|
      +---+*/
    withParquetTable((0 until 10).map(i => (i, i.toString)), "t") {
      //sql("SELECT _1 FROM t where t._1 > 5").show()
      checkAnswer(sql("SELECT _1 FROM t where t._1 > 5"), (6 until 10).map(Row.apply(_)))
      checkAnswer(sql("SELECT _1 FROM t as tmp where tmp._1 < 5"), (0 until 5).map(Row.apply(_)))
    }
  }

  test("appending") {//添加
    val data = (0 until 10).map(i => (i, i.toString))
    ctx.createDataFrame(data).toDF("c1", "c2").registerTempTable("tmp")
    /**
      +---+---+
      | _1| _2|
      +---+---+
      |  0|  0|
      |  1|  1|
      |  2|  2|
      |  3|  3|
      |  4|  4|
      |  5|  5|
      +---+---+*/
    withParquetTable(data, "t") {
      sql("INSERT INTO TABLE t SELECT * FROM tmp")
     // sql("INSERT INTO TABLE t SELECT * FROM tmp").show()
      //ctx.table("t").show()
      checkAnswer(ctx.table("t"), (data ++ data).map(Row.fromTuple))
    }
    ctx.catalog.unregisterTable(Seq("tmp"))
  }

  test("overwriting") {//覆盖
    val data = (0 until 10).map(i => (i, i.toString))
    ctx.createDataFrame(data).toDF("c1", "c2").registerTempTable("tmp")
    /**
      +---+---+
      | c1| c2|
      +---+---+
      |  0|  0|
      |  1|  1|
      |  2|  2|
      |  3|  3|
      |  4|  4|
      |  5|  5|
      |  6|  6|
      |  7|  7|
      |  8|  8|
      |  9|  9|
      +---+---+*/
     sql("SELECT * FROM tmp").show()
     
    withParquetTable(data, "t") {
      sql("INSERT OVERWRITE TABLE t SELECT * FROM tmp")
      checkAnswer(ctx.table("t"), data.map(Row.fromTuple))
    }
    ctx.catalog.unregisterTable(Seq("tmp"))
  }

  test("self-join") {//自连接
    // 4 rows, cells of column 1 of row 2 and row 4 are null
    //4行,行2和行4的列1的单元为空
    val data = (1 to 4).map { i =>
      val maybeInt = if (i % 2 == 0) None else Some(i)
      (maybeInt, i.toString)
    }

    withParquetTable(data, "t") {
      val selfJoin = sql("SELECT * FROM t x JOIN t y WHERE x._1 = y._1")
      val queryOutput = selfJoin.queryExecution.analyzed.output

      assertResult(4, "Field count mismatches")(queryOutput.size)
      assertResult(2, "Duplicated expression ID in query plan:\n $selfJoin") {
        queryOutput.filter(_.name == "_1").map(_.exprId).size
      }

      checkAnswer(selfJoin, List(Row(1, "1", 1, "1"), Row(3, "3", 3, "3")))
    }
  }

  test("nested data - struct with array field") {//嵌套数据结构数组的字段
    val data = (1 to 10).map(i => Tuple1((i, Seq("val_$i"))))
    //data.to
    /**
      +------+
      |   _c0|
      +------+
      |val_$i|
      |val_$i|
      |val_$i|
      |val_$i|
      |val_$i|
      |val_$i|
      |val_$i|
      |val_$i|
      |val_$i|
      |val_$i|
      +------+*/
    withParquetTable(data, "t") {
      //sql("SELECT _1._2[0] FROM t").show()
      checkAnswer(sql("SELECT _1._2[0] FROM t"), data.map {
        case Tuple1((_, Seq(string))) => Row(string)
      })
    }
  }
  //嵌套数组列结构
  test("nested data - array of struct") {
    /**
    +------+
    |   _c0|
    +------+
    |val_$i|
    |val_$i|
    |val_$i|
    |val_$i|
    |val_$i|
    |val_$i|
    |val_$i|
    |val_$i|
    |val_$i|
    |val_$i|
    +------+*/
    val data = (1 to 10).map(i => Tuple1(Seq(i -> "val_$i")))
    withParquetTable(data, "t") {
      sql("SELECT _1[0]._2 FROM t").show()
      checkAnswer(sql("SELECT _1[0]._2 FROM t"), data.map {
        case Tuple1(Seq((_, string))) => Row(string)
      })
    }
  }
  //仅由下推过滤器引用的列应保留
  test("SPARK-1913 regression: columns only referenced by pushed down filters should remain") {
    /**
      +---+
      | _1|
      +---+
      |  1|
      |  2|
      |  3|
      |  4|
      |  5|
      |  6|
      |  7|
      |  8|
      |  9|
      +---+*/
    withParquetTable((1 to 10).map(Tuple1.apply), "t") {
      //sql("SELECT _1 FROM t WHERE _1 < 10").show()
      checkAnswer(sql("SELECT _1 FROM t WHERE _1 < 10"), (1 to 9).map(Row.apply(_)))
    }
  }
  //使用字典压缩在拼版中存储的字符串
  test("SPARK-5309 strings stored using dictionary compression in parquet") {
    /**
     * sql("SELECT _1, _2, SUM(_3) FROM t GROUP BY _1, _2").show()
      +----+-----+---+
      |  _1|   _2|_c2|
      +----+-----+---+
      |same|run_1|100|
      |same|run_6|100|
      |same|run_2|100|
      |same|run_7|100|
      |same|run_3|100|
      |same|run_8|100|
      |same|run_4|100|
      |same|run_9|100|
      |same|run_0|100|
      |same|run_5|100|
      +----+-----+---+*/
    withParquetTable((0 until 1000).map(i => ("same", "run_" + i /100, 1)), "t") {
    
      checkAnswer(sql("SELECT _1, _2, SUM(_3) FROM t GROUP BY _1, _2"),
        (0 until 10).map(i => Row("same", "run_" + i, 100)))
      checkAnswer(sql("SELECT _1, _2, SUM(_3) FROM t WHERE _2 = 'run_5' GROUP BY _1, _2"),
        List(Row("same", "run_5", 100)))
    }
  }
  //十进制类型应与非本地类型一起工作
  test("SPARK-6917 DecimalType should work with non-native types") {
    val data = (1 to 10).map(i => Row(Decimal(i, 18, 0), new java.sql.Timestamp(i)))
    val schema = StructType(List(StructField("d", DecimalType(18, 0), false),
      StructField("time", TimestampType, false)).toArray)
    withTempPath { file =>
      /**
       *+---+--------------------+
        |  d|                time|
        +---+--------------------+
        |  1|1969-12-31 16:00:...|
        |  2|1969-12-31 16:00:...|
        |  3|1969-12-31 16:00:...|
        |  4|1969-12-31 16:00:...|
        |  5|1969-12-31 16:00:...|
        |  6|1969-12-31 16:00:...|
        |  7|1969-12-31 16:00:...|
        |  8|1969-12-31 16:00:...|
        |  9|1969-12-31 16:00:...|
        | 10|1969-12-31 16:00:...|
        +---+--------------------+*/
      val df = ctx.createDataFrame(ctx.sparkContext.parallelize(data), schema)
      //df.show()
      df.write.parquet(file.getCanonicalPath)
      val df2 = ctx.read.parquet(file.getCanonicalPath)
      checkAnswer(df2, df.collect().toSeq)
    }
  }
  //启用/禁用合并partfiles合并时parquet模式
  test("Enabling/disabling merging partfiles when merging parquet schema") {
    def testSchemaMerging(expectedColumnNumber: Int): Unit = {
      withTempDir { dir =>
        val basePath = dir.getCanonicalPath
        ctx.range(0, 10).toDF("a").write.parquet(new Path(basePath, "foo=1").toString)
        ctx.range(0, 10).toDF("b").write.parquet(new Path(basePath, "foo=2").toString)
        // delete summary files, so if we don't merge part-files, one column will not be included.
        Utils.deleteRecursively(new File(basePath + "/foo=1/_metadata"))
        Utils.deleteRecursively(new File(basePath + "/foo=1/_common_metadata"))
        assert(ctx.read.parquet(basePath).columns.length === expectedColumnNumber)
      }
    }

    withSQLConf(SQLConf.PARQUET_SCHEMA_MERGING_ENABLED.key -> "true",
      SQLConf.PARQUET_SCHEMA_RESPECT_SUMMARIES.key -> "true") {
      testSchemaMerging(2)
    }

    withSQLConf(SQLConf.PARQUET_SCHEMA_MERGING_ENABLED.key -> "true",
      SQLConf.PARQUET_SCHEMA_RESPECT_SUMMARIES.key -> "false") {
      testSchemaMerging(3)
    }
  }
  //启用/禁用模式合并
  test("Enabling/disabling schema merging") {
    def testSchemaMerging(expectedColumnNumber: Int): Unit = {
      withTempDir { dir =>
        val basePath = dir.getCanonicalPath
        ctx.range(0, 10).toDF("a").write.parquet(new Path(basePath, "foo=1").toString)
        ctx.range(0, 10).toDF("b").write.parquet(new Path(basePath, "foo=2").toString)
        assert(ctx.read.parquet(basePath).columns.length === expectedColumnNumber)
      }
    }

    withSQLConf(SQLConf.PARQUET_SCHEMA_MERGING_ENABLED.key -> "true") {
      testSchemaMerging(3)
    }

    withSQLConf(SQLConf.PARQUET_SCHEMA_MERGING_ENABLED.key -> "false") {
      testSchemaMerging(2)
    }
  }
  //应该尊重用户指定的选项
  test("SPARK-8990 DataFrameReader.parquet() should respect user specified options") {
    withTempPath { dir =>
      val basePath = dir.getCanonicalPath
      ctx.range(0, 10).toDF("a").write.parquet(new Path(basePath, "foo=1").toString)
      ctx.range(0, 10).toDF("b").write.parquet(new Path(basePath, "foo=a").toString)

      // Disables the global SQL option for schema merging
      //禁用模式合并全局SQL选项
      withSQLConf(SQLConf.PARQUET_SCHEMA_MERGING_ENABLED.key -> "false") {
        assertResult(2) {
          // Disables schema merging via data source option
          //通过数据源选项禁用架构合并
          ctx.read.option("mergeSchema", "false").parquet(basePath).columns.length
        }

        assertResult(3) {
          // Enables schema merging via data source option
          //通过数据源选项实现架构合并
          ctx.read.option("mergeSchema", "true").parquet(basePath).columns.length
        }
      }
    }
  }
  //小数应正确写入parquet
  test("SPARK-9119 Decimal should be correctly written into parquet") {
    withTempPath { dir =>
      val basePath = dir.getCanonicalPath
      val schema = StructType(Array(StructField("name", DecimalType(10, 5), false)))
      val rowRDD = sqlContext.sparkContext.parallelize(Array(Row(Decimal("67123.45"))))
      val df = sqlContext.createDataFrame(rowRDD, schema)
      /**+-----------+
        |       name|
        +-----------+
        |67123.45000|
        +-----------+**/
      //df.show()
      df.write.parquet(basePath)

      val decimal = sqlContext.read.parquet(basePath).first().getDecimal(0)
      assert(Decimal("67123.45") === Decimal(decimal))
    }
  }
  //嵌套结构的模式合并
  test("SPARK-10005 Schema merging for nested struct") {
    val sqlContext = _sqlContext
    import sqlContext.implicits._

    withTempPath { dir =>
      val path = dir.getCanonicalPath

      def append(df: DataFrame): Unit = {
      //追加模式
        df.write.mode(SaveMode.Append).parquet(path)
      }

      // Note that both the following two DataFrames contain a single struct column with multiple
      // nested fields.
      //请注意,以下两个数据集都包含具有多个嵌套字段的单个结构列
      append((1 to 2).map(i => Tuple1((i, i))).toDF())
      append((1 to 2).map(i => Tuple1((i, i, i))).toDF())

      withSQLConf(SQLConf.PARQUET_BINARY_AS_STRING.key -> "true") {
        checkAnswer(
          sqlContext.read.option("mergeSchema", "true").parquet(path),
          Seq(
            Row(Row(1, 1, null)),
            Row(Row(2, 2, null)),
            Row(Row(1, 1, 1)),
            Row(Row(2, 2, 2))))
      }
    }
  }
  //请求模式剪辑 - 相同的模式
  test("SPARK-10301 requested schema clipping - same schema") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      val df = sqlContext.range(1).selectExpr("NAMED_STRUCT('a', id, 'b', id + 1) AS s").coalesce(1)
      df.write.parquet(path)

      val userDefinedSchema =
        new StructType()
          .add(
            "s",
            new StructType()
              .add("a", LongType, nullable = true)
              .add("b", LongType, nullable = true),
            nullable = true)

      checkAnswer(
        sqlContext.read.schema(userDefinedSchema).parquet(path),
        Row(Row(0L, 1L)))
    }
  }

  // This test case is ignored because of parquet-mr bug PARQUET-370
  //请求模式剪辑 - 具有不相交字段集的模式
  ignore("SPARK-10301 requested schema clipping - schemas with disjoint sets of fields") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      val df = sqlContext.range(1).selectExpr("NAMED_STRUCT('a', id, 'b', id + 1) AS s").coalesce(1)
      df.write.parquet(path)

      val userDefinedSchema =
        new StructType()
          .add(
            "s",
            new StructType()
              .add("c", LongType, nullable = true)
              .add("d", LongType, nullable = true),
            nullable = true)

      checkAnswer(
        sqlContext.read.schema(userDefinedSchema).parquet(path),
        Row(Row(null, null)))
    }
  }
  //请求模式剪辑 - 请求模式包含物理模式
  test("SPARK-10301 requested schema clipping - requested schema contains physical schema") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      val df = sqlContext.range(1).selectExpr("NAMED_STRUCT('a', id, 'b', id + 1) AS s").coalesce(1)
      df.write.parquet(path)

      val userDefinedSchema =
        new StructType()
          .add(
            "s",
            new StructType()
              .add("a", LongType, nullable = true)
              .add("b", LongType, nullable = true)
              .add("c", LongType, nullable = true)
              .add("d", LongType, nullable = true),
            nullable = true)

      checkAnswer(
        sqlContext.read.schema(userDefinedSchema).parquet(path),
        Row(Row(0L, 1L, null, null)))
    }

    withTempPath { dir =>
      val path = dir.getCanonicalPath
      val df = sqlContext.range(1).selectExpr("NAMED_STRUCT('a', id, 'd', id + 3) AS s").coalesce(1)
      df.write.parquet(path)

      val userDefinedSchema =
        new StructType()
          .add(
            "s",
            new StructType()
              .add("a", LongType, nullable = true)
              .add("b", LongType, nullable = true)
              .add("c", LongType, nullable = true)
              .add("d", LongType, nullable = true),
            nullable = true)

      checkAnswer(
        sqlContext.read.schema(userDefinedSchema).parquet(path),
        Row(Row(0L, null, null, 3L)))
    }
  }
  //请求模式剪辑 - 物理模式包含请求的模式
  test("SPARK-10301 requested schema clipping - physical schema contains requested schema") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      val df = sqlContext
        .range(1)
        .selectExpr("NAMED_STRUCT('a', id, 'b', id + 1, 'c', id + 2, 'd', id + 3) AS s")
        .coalesce(1)

      df.write.parquet(path)

      val userDefinedSchema =
        new StructType()
          .add(
            "s",
            new StructType()
              .add("a", LongType, nullable = true)
              .add("b", LongType, nullable = true),
            nullable = true)

      checkAnswer(
        sqlContext.read.schema(userDefinedSchema).parquet(path),
        Row(Row(0L, 1L)))
    }

    withTempPath { dir =>
      val path = dir.getCanonicalPath
      val df = sqlContext
        .range(1)
        .selectExpr("NAMED_STRUCT('a', id, 'b', id + 1, 'c', id + 2, 'd', id + 3) AS s")
        .coalesce(1)

      df.write.parquet(path)

      val userDefinedSchema =
        new StructType()
          .add(
            "s",
            new StructType()
              .add("a", LongType, nullable = true)
              .add("d", LongType, nullable = true),
            nullable = true)

      checkAnswer(
        sqlContext.read.schema(userDefinedSchema).parquet(path),
        Row(Row(0L, 3L)))
    }
  }
  //请求模式限制 - 模式重叠但不包含对方
  test("SPARK-10301 requested schema clipping - schemas overlap but don't contain each other") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      val df = sqlContext
        .range(1)
        .selectExpr("NAMED_STRUCT('a', id, 'b', id + 1, 'c', id + 2) AS s")
        .coalesce(1)

      df.write.parquet(path)

      val userDefinedSchema =
        new StructType()
          .add(
            "s",
            new StructType()
              .add("b", LongType, nullable = true)
              .add("c", LongType, nullable = true)
              .add("d", LongType, nullable = true),
            nullable = true)

      checkAnswer(
        sqlContext.read.schema(userDefinedSchema).parquet(path),
        Row(Row(1L, 2L, null)))
    }
  }
  //请求模式剪辑 - 深层嵌套结构
  test("SPARK-10301 requested schema clipping - deeply nested struct") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      val df = sqlContext
        .range(1)
        .selectExpr("NAMED_STRUCT('a', ARRAY(NAMED_STRUCT('b', id, 'c', id))) AS s")
        .coalesce(1)

      df.write.parquet(path)

      val userDefinedSchema = new StructType()
        .add("s",
          new StructType()
            .add(
              "a",
              ArrayType(
                new StructType()
                  .add("b", LongType, nullable = true)
                  .add("d", StringType, nullable = true),
                containsNull = true),
              nullable = true),
          nullable = true)

      checkAnswer(
        sqlContext.read.schema(userDefinedSchema).parquet(path),
        Row(Row(Seq(Row(0, null)))))
    }
  }
  //请求模式修剪 - 乱序
  test("SPARK-10301 requested schema clipping - out of order") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      val df1 = sqlContext
        .range(1)
        .selectExpr("NAMED_STRUCT('a', id, 'b', id + 1, 'c', id + 2) AS s")
        .coalesce(1)

      val df2 = sqlContext
        .range(1, 2)
        .selectExpr("NAMED_STRUCT('c', id + 2, 'b', id + 1, 'd', id + 3) AS s")
        .coalesce(1)

      df1.write.parquet(path)
       //追加模式
      df2.write.mode(SaveMode.Append).parquet(path)

      val userDefinedSchema = new StructType()
        .add("s",
          new StructType()
            .add("a", LongType, nullable = true)
            .add("b", LongType, nullable = true)
            .add("d", LongType, nullable = true),
          nullable = true)

      checkAnswer(
        sqlContext.read.schema(userDefinedSchema).parquet(path),
        Seq(
          Row(Row(0, 1, null)),
          Row(Row(null, 2, 4))))
    }
  }
  //请求模式剪辑 - 模式合并
  test("SPARK-10301 requested schema clipping - schema merging") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      val df1 = sqlContext
        .range(1)
        .selectExpr("NAMED_STRUCT('a', id, 'c', id + 2) AS s")
        .coalesce(1)

      val df2 = sqlContext
        .range(1, 2)
        .selectExpr("NAMED_STRUCT('a', id, 'b', id + 1, 'c', id + 2) AS s")
        .coalesce(1)
	 //追加模式
      df1.write.mode(SaveMode.Append).parquet(path)
      df2.write.mode(SaveMode.Append).parquet(path)

      checkAnswer(
        sqlContext
          .read
          .option("mergeSchema", "true")
          .parquet(path)
          .selectExpr("s.a", "s.b", "s.c"),
        Seq(
          Row(0, null, 2),
          Row(1, 2, 3)))
    }
  }
  //请求模式剪辑 - UDT
  test("SPARK-10301 requested schema clipping - UDT") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      val df = sqlContext
        .range(1)
        .selectExpr(
          """NAMED_STRUCT(
            |  'f0', CAST(id AS STRING),
            |  'f1', NAMED_STRUCT(
            |    'a', CAST(id + 1 AS INT),
            |    'b', CAST(id + 2 AS LONG),
            |    'c', CAST(id + 3.5 AS DOUBLE)
            |  )
            |) AS s
          """.stripMargin)
        .coalesce(1)
	 //追加模式
      df.write.mode(SaveMode.Append).parquet(path)

      val userDefinedSchema =
        new StructType()
          .add(
            "s",
            new StructType()
              .add("f1", new NestedStructUDT, nullable = true),
            nullable = true)

      checkAnswer(
        sqlContext.read.schema(userDefinedSchema).parquet(path),
        Row(Row(NestedStruct(1, 2L, 3.5D))))
    }
  }
}

object TestingUDT {
  @SQLUserDefinedType(udt = classOf[NestedStructUDT])
  case class NestedStruct(a: Integer, b: Long, c: Double)

  class NestedStructUDT extends UserDefinedType[NestedStruct] {
    override def sqlType: DataType =
      new StructType()
        .add("a", IntegerType, nullable = true)
        .add("b", LongType, nullable = false)
        .add("c", DoubleType, nullable = false)

    override def serialize(obj: Any): Any = {
      val row = new SpecificMutableRow(sqlType.asInstanceOf[StructType].map(_.dataType))
      obj match {
        case n: NestedStruct =>
          row.setInt(0, n.a)
          row.setLong(1, n.b)
          row.setDouble(2, n.c)
      }
    }

    override def userClass: Class[NestedStruct] = classOf[NestedStruct]

    override def deserialize(datum: Any): NestedStruct = {
      datum match {
        case row: InternalRow =>
          NestedStruct(row.getInt(0), row.getLong(1), row.getDouble(2))
      }
    }
  }
}
