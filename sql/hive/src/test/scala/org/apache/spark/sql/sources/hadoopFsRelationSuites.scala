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

package org.apache.spark.sql.sources

import scala.collection.JavaConversions._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
import org.apache.parquet.hadoop.ParquetOutputCommitter

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types._


abstract class HadoopFsRelationTest extends QueryTest with SQLTestUtils {
  override def _sqlContext: SQLContext = TestHive
  protected val sqlContext = _sqlContext
  import sqlContext.implicits._

  val dataSourceName: String

  protected def supportsDataType(dataType: DataType): Boolean = true

  val dataSchema =
    StructType(
      Seq(
        StructField("a", IntegerType, nullable = false),
        StructField("b", StringType, nullable = false)))

  lazy val testDF = (1 to 3).map(i => (i, s"val_$i")).toDF("a", "b")

  lazy val partitionedTestDF1 = (for {
    i <- 1 to 3
    p2 <- Seq("foo", "bar")
  } yield (i, s"val_$i", 1, p2)).toDF("a", "b", "p1", "p2")

  lazy val partitionedTestDF2 = (for {
    i <- 1 to 3
    p2 <- Seq("foo", "bar")
  } yield (i, s"val_$i", 2, p2)).toDF("a", "b", "p1", "p2")

  lazy val partitionedTestDF = partitionedTestDF1.unionAll(partitionedTestDF2)

  def checkQueries(df: DataFrame): Unit = {
    // Selects everything 选择一切
    checkAnswer(
      df,
      for (i <- 1 to 3; p1 <- 1 to 2; p2 <- Seq("foo", "bar")) yield Row(i, s"val_$i", p1, p2))

    // Simple filtering and partition pruning 简单的过滤和分区修剪
    checkAnswer(
      df.filter('a > 1 && 'p1 === 2),
      for (i <- 2 to 3; p2 <- Seq("foo", "bar")) yield Row(i, s"val_$i", 2, p2))

    // Simple projection and filtering 简单的投影和过滤
    checkAnswer(
      df.filter('a > 1).select('b, 'a + 1),
      for (i <- 2 to 3; _ <- 1 to 2; _ <- Seq("foo", "bar")) yield Row(s"val_$i", i + 1))

    // Simple projection and partition pruning 简单的投影和分割修剪
    checkAnswer(
      df.filter('a > 1 && 'p1 < 2).select('b, 'p1),
      for (i <- 2 to 3; _ <- Seq("foo", "bar")) yield Row(s"val_$i", 1))

    // Project many copies of columns with different types (reproduction for SPARK-7858)
    //投影许多不同类型的列（SPARK-7858的复制品）
    checkAnswer(
      df.filter('a > 1 && 'p1 < 2).select('b, 'b, 'b, 'b, 'p1, 'p1, 'p1, 'p1),
      for (i <- 2 to 3; _ <- Seq("foo", "bar"))
        yield Row(s"val_$i", s"val_$i", s"val_$i", s"val_$i", 1, 1, 1, 1))

    // Self-join
    df.registerTempTable("t")
    withTempTable("t") {
      checkAnswer(
        sql(
          """SELECT l.a, r.b, l.p1, r.p2
            |FROM t l JOIN t r
            |ON l.a = r.a AND l.p1 = r.p1 AND l.p2 = r.p2
          """.stripMargin),
        for (i <- 1 to 3; p1 <- 1 to 2; p2 <- Seq("foo", "bar")) yield Row(i, s"val_$i", p1, p2))
    }
  }

  private val supportedDataTypes = Seq(
    StringType, BinaryType,
    NullType, BooleanType,
    ByteType, ShortType, IntegerType, LongType,
    FloatType, DoubleType, DecimalType(25, 5), DecimalType(6, 5),
    DateType, TimestampType,
    ArrayType(IntegerType),
    MapType(StringType, LongType),
    new StructType()
      .add("f1", FloatType, nullable = true)
      .add("f2", ArrayType(BooleanType, containsNull = true), nullable = true),
    new MyDenseVectorUDT()
  ).filter(supportsDataType)

  for (dataType <- supportedDataTypes) {
    test(s"test all data types - $dataType") {
      withTempPath { file =>
        val path = file.getCanonicalPath

        val dataGenerator = RandomDataGenerator.forType(
          dataType = dataType,
          nullable = true,
          seed = Some(System.nanoTime())
        ).getOrElse {
          fail(s"Failed to create data generator for schema $dataType")
        }

        // Create a DF for the schema with random data. The index field is used to sort the
        // DataFrame.  This is a workaround for SPARK-10591.
      //为具有随机数据的模式创建一个DF。 索引字段用于排序DataFrame 这是SPARK-10591的解决方法。
        val schema = new StructType()
          .add("index", IntegerType, nullable = false)
          .add("col", dataType, nullable = true)
        val rdd = sqlContext.sparkContext.parallelize((1 to 10).map(i => Row(i, dataGenerator())))
        val df = sqlContext.createDataFrame(rdd, schema).orderBy("index").coalesce(1)

        df.write
          .mode("overwrite")
          .format(dataSourceName)
          .option("dataSchema", df.schema.json)
          .save(path)

        val loadedDF = sqlContext
          .read
          .format(dataSourceName)
          .option("dataSchema", df.schema.json)
          .schema(df.schema)
          .load(path)
          .orderBy("index")

        checkAnswer(loadedDF, df)
      }
    }
  }
  //save（）/ load（） - 非分区表 - 覆盖
  test("save()/load() - non-partitioned table - Overwrite") {
    withTempPath { file =>
      testDF.write.mode(SaveMode.Overwrite).format(dataSourceName).save(file.getCanonicalPath)
      testDF.write.mode(SaveMode.Overwrite).format(dataSourceName).save(file.getCanonicalPath)

      checkAnswer(
        sqlContext.read.format(dataSourceName)
          .option("path", file.getCanonicalPath)
          .option("dataSchema", dataSchema.json)
          .load(),
        testDF.collect())
    }
  }
    //Save（）/ load（） - 非分区表 - 附加
  test("save()/load() - non-partitioned table - Append") {
    withTempPath { file =>
      testDF.write.mode(SaveMode.Overwrite).format(dataSourceName).save(file.getCanonicalPath)
      testDF.write.mode(SaveMode.Append).format(dataSourceName).save(file.getCanonicalPath)

      checkAnswer(
        sqlContext.read.format(dataSourceName)
          .option("dataSchema", dataSchema.json)
          .load(file.getCanonicalPath).orderBy("a"),
        testDF.unionAll(testDF).orderBy("a").collect())
    }
  }
  //save（）/ load（） - 非分区表 - ErrorIfExists
  test("save()/load() - non-partitioned table - ErrorIfExists") {
    withTempDir { file =>
      intercept[AnalysisException] {
        testDF.write.format(dataSourceName).mode(SaveMode.ErrorIfExists).save(file.getCanonicalPath)
      }
    }
  }
  //save（）/ load（） - 非分区表 - 忽略
  test("save()/load() - non-partitioned table - Ignore") {
    withTempDir { file =>
      testDF.write.mode(SaveMode.Ignore).format(dataSourceName).save(file.getCanonicalPath)

      val path = new Path(file.getCanonicalPath)
      val fs = path.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
      assert(fs.listStatus(path).isEmpty)
    }
  }
  //save（）/ load（） - 分区表 - 简单查询
  test("save()/load() - partitioned table - simple queries") {
    withTempPath { file =>
      partitionedTestDF.write
        .format(dataSourceName)
        .mode(SaveMode.ErrorIfExists)
        .partitionBy("p1", "p2")
        .save(file.getCanonicalPath)

      checkQueries(
        sqlContext.read.format(dataSourceName)
          .option("dataSchema", dataSchema.json)
          .load(file.getCanonicalPath))
    }
  }
  //save（）/ load（） - 分区表 - 覆盖
  test("save()/load() - partitioned table - Overwrite") {
    withTempPath { file =>
      partitionedTestDF.write
        .format(dataSourceName)
        .mode(SaveMode.Overwrite)
        .partitionBy("p1", "p2")
        .save(file.getCanonicalPath)

      partitionedTestDF.write
        .format(dataSourceName)
        .mode(SaveMode.Overwrite)
        .partitionBy("p1", "p2")
        .save(file.getCanonicalPath)

      checkAnswer(
        sqlContext.read.format(dataSourceName)
          .option("dataSchema", dataSchema.json)
          .load(file.getCanonicalPath),
        partitionedTestDF.collect())
    }
  }
  //save（）/ load（） - 分区表 - 附加
  test("save()/load() - partitioned table - Append") {
    withTempPath { file =>
      partitionedTestDF.write
        .format(dataSourceName)
        .mode(SaveMode.Overwrite)
        .partitionBy("p1", "p2")
        .save(file.getCanonicalPath)

      partitionedTestDF.write
        .format(dataSourceName)
        .mode(SaveMode.Append)
        .partitionBy("p1", "p2")
        .save(file.getCanonicalPath)

      checkAnswer(
        sqlContext.read.format(dataSourceName)
          .option("dataSchema", dataSchema.json)
          .load(file.getCanonicalPath),
        partitionedTestDF.unionAll(partitionedTestDF).collect())
    }
  }
  //save（）/ load（） - 分区表 - 附加 - 新的分区值
  test("save()/load() - partitioned table - Append - new partition values") {
    withTempPath { file =>
      partitionedTestDF1.write
        .format(dataSourceName)
        .mode(SaveMode.Overwrite)
        .partitionBy("p1", "p2")
        .save(file.getCanonicalPath)

      partitionedTestDF2.write
        .format(dataSourceName)
        .mode(SaveMode.Append)
        .partitionBy("p1", "p2")
        .save(file.getCanonicalPath)

      checkAnswer(
        sqlContext.read.format(dataSourceName)
          .option("dataSchema", dataSchema.json)
          .load(file.getCanonicalPath),
        partitionedTestDF.collect())
    }
  }
  //save（）/ load（） - 分区表 - ErrorIfExists
  test("save()/load() - partitioned table - ErrorIfExists") {
    withTempDir { file =>
      intercept[AnalysisException] {
        partitionedTestDF.write
          .format(dataSourceName)
          .mode(SaveMode.ErrorIfExists)
          .partitionBy("p1", "p2")
          .save(file.getCanonicalPath)
      }
    }
  }
  //save（）/ load（） - 分区表 - 忽略
  test("save()/load() - partitioned table - Ignore") {
    withTempDir { file =>
      partitionedTestDF.write
        .format(dataSourceName).mode(SaveMode.Ignore).save(file.getCanonicalPath)

      val path = new Path(file.getCanonicalPath)
      val fs = path.getFileSystem(SparkHadoopUtil.get.conf)
      assert(fs.listStatus(path).isEmpty)
    }
  }
  //saveAsTable（）/ load（） - 非分区表 - 覆盖
  test("saveAsTable()/load() - non-partitioned table - Overwrite") {
    testDF.write.format(dataSourceName).mode(SaveMode.Overwrite)
      .option("dataSchema", dataSchema.json)
      .saveAsTable("t")

    withTable("t") {
      checkAnswer(sqlContext.table("t"), testDF.collect())
    }
  }
  //saveAsTable（）/ load（） - 非分区表 - 附加
  test("saveAsTable()/load() - non-partitioned table - Append") {
    testDF.write.format(dataSourceName).mode(SaveMode.Overwrite).saveAsTable("t")
    testDF.write.format(dataSourceName).mode(SaveMode.Append).saveAsTable("t")

    withTable("t") {
      checkAnswer(sqlContext.table("t"), testDF.unionAll(testDF).orderBy("a").collect())
    }
  }
  //saveAsTable（）/ load（） - 非分区表 - ErrorIfExists
  test("saveAsTable()/load() - non-partitioned table - ErrorIfExists") {
    Seq.empty[(Int, String)].toDF().registerTempTable("t")

    withTempTable("t") {
      intercept[AnalysisException] {
        testDF.write.format(dataSourceName).mode(SaveMode.ErrorIfExists).saveAsTable("t")
      }
    }
  }
  //saveAsTable（）/ load（） - 非分区表 - 忽略
  test("saveAsTable()/load() - non-partitioned table - Ignore") {
    Seq.empty[(Int, String)].toDF().registerTempTable("t")

    withTempTable("t") {
      testDF.write.format(dataSourceName).mode(SaveMode.Ignore).saveAsTable("t")
      assert(sqlContext.table("t").collect().isEmpty)
    }
  }
  //saveAsTable（）/ load（） - 分区表 - 简单查询
  test("saveAsTable()/load() - partitioned table - simple queries") {
    partitionedTestDF.write.format(dataSourceName)
      .mode(SaveMode.Overwrite)
      .option("dataSchema", dataSchema.json)
      .saveAsTable("t")

    withTable("t") {
      checkQueries(sqlContext.table("t"))
    }
  }
  //saveAsTable（）/ load（） - 分区表 - 覆盖
  test("saveAsTable()/load() - partitioned table - Overwrite") {
    partitionedTestDF.write
      .format(dataSourceName)
      .mode(SaveMode.Overwrite)
      .option("dataSchema", dataSchema.json)
      .partitionBy("p1", "p2")
      .saveAsTable("t")

    partitionedTestDF.write
      .format(dataSourceName)
      .mode(SaveMode.Overwrite)
      .option("dataSchema", dataSchema.json)
      .partitionBy("p1", "p2")
      .saveAsTable("t")

    withTable("t") {
      checkAnswer(sqlContext.table("t"), partitionedTestDF.collect())
    }
  }
  //saveAsTable（）/ load（） - 分区表 - 附加
  test("saveAsTable()/load() - partitioned table - Append") {
    partitionedTestDF.write
      .format(dataSourceName)
      .mode(SaveMode.Overwrite)
      .option("dataSchema", dataSchema.json)
      .partitionBy("p1", "p2")
      .saveAsTable("t")

    partitionedTestDF.write
      .format(dataSourceName)
      .mode(SaveMode.Append)
      .option("dataSchema", dataSchema.json)
      .partitionBy("p1", "p2")
      .saveAsTable("t")

    withTable("t") {
      checkAnswer(sqlContext.table("t"), partitionedTestDF.unionAll(partitionedTestDF).collect())
    }
  }
  //saveAsTable（）/ load（） - 分区表 - 附加 - 新的分区值
  test("saveAsTable()/load() - partitioned table - Append - new partition values") {
    partitionedTestDF1.write
      .format(dataSourceName)
      .mode(SaveMode.Overwrite)
      .option("dataSchema", dataSchema.json)
      .partitionBy("p1", "p2")
      .saveAsTable("t")

    partitionedTestDF2.write
      .format(dataSourceName)
      .mode(SaveMode.Append)
      .option("dataSchema", dataSchema.json)
      .partitionBy("p1", "p2")
      .saveAsTable("t")

    withTable("t") {
      checkAnswer(sqlContext.table("t"), partitionedTestDF.collect())
    }
  }
  //saveAsTable（）/ load（） - 分区表 - 附加 - 不匹配的分区列
  test("saveAsTable()/load() - partitioned table - Append - mismatched partition columns") {
    partitionedTestDF1.write
      .format(dataSourceName)
      .mode(SaveMode.Overwrite)
      .option("dataSchema", dataSchema.json)
      .partitionBy("p1", "p2")
      .saveAsTable("t")

    // Using only a subset of all partition columns
    //仅使用所有分区列的一个子集
    intercept[Throwable] {
      partitionedTestDF2.write
        .format(dataSourceName)
        .mode(SaveMode.Append)
        .option("dataSchema", dataSchema.json)
        .partitionBy("p1")
        .saveAsTable("t")
    }
  }
  //saveAsTable（）/ load（） - 分区表 - ErrorIfExists
  test("saveAsTable()/load() - partitioned table - ErrorIfExists") {
    Seq.empty[(Int, String)].toDF().registerTempTable("t")

    withTempTable("t") {
      intercept[AnalysisException] {
        partitionedTestDF.write
          .format(dataSourceName)
          .mode(SaveMode.ErrorIfExists)
          .option("dataSchema", dataSchema.json)
          .partitionBy("p1", "p2")
          .saveAsTable("t")
      }
    }
  }
  //saveAsTable（）/ load（） - 分区表 - 忽略
  test("saveAsTable()/load() - partitioned table - Ignore") {
    Seq.empty[(Int, String)].toDF().registerTempTable("t")

    withTempTable("t") {
      partitionedTestDF.write
        .format(dataSourceName)
        .mode(SaveMode.Ignore)
        .option("dataSchema", dataSchema.json)
        .partitionBy("p1", "p2")
        .saveAsTable("t")

      assert(sqlContext.table("t").collect().isEmpty)
    }
  }
  //Hadoop风格
  test("Hadoop style globbing") {
    withTempPath { file =>
      partitionedTestDF.write
        .format(dataSourceName)
        .mode(SaveMode.Overwrite)
        .partitionBy("p1", "p2")
        .save(file.getCanonicalPath)

      val df = sqlContext.read
        .format(dataSourceName)
        .option("dataSchema", dataSchema.json)
        .load(s"${file.getCanonicalPath}/p1=*/p2=???")

      val expectedPaths = Set(
        s"${file.getCanonicalFile}/p1=1/p2=foo",
        s"${file.getCanonicalFile}/p1=2/p2=foo",
        s"${file.getCanonicalFile}/p1=1/p2=bar",
        s"${file.getCanonicalFile}/p1=2/p2=bar"
      ).map { p =>
        val path = new Path(p)
        val fs = path.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
        path.makeQualified(fs.getUri, fs.getWorkingDirectory).toString
      }

      val actualPaths = df.queryExecution.analyzed.collectFirst {
        case LogicalRelation(relation: HadoopFsRelation, _) =>
          relation.paths.toSet
      }.getOrElse {
        fail("Expect an FSBasedRelation, but none could be found")
      }

      assert(actualPaths === expectedPaths)
      checkAnswer(df, partitionedTestDF.collect())
    }
  }

  // HadoopFsRelation.discoverPartitions() called by refresh(), which will ignore
  // the given partition data type.
  ////通过refresh（）调用的HadoopFsRelation.discoverPartitions（），它将被忽略
  //给定的分区数据类型。
  ignore("Partition column type casting") {
    withTempPath { file =>
      val input = partitionedTestDF.select('a, 'b, 'p1.cast(StringType).as('ps), 'p2)

      input
        .write
        .format(dataSourceName)
        .mode(SaveMode.Overwrite)
        .partitionBy("ps", "p2")
        .saveAsTable("t")

      withTempTable("t") {
        checkAnswer(sqlContext.table("t"), input.collect())
      }
    }
  }
  //保存分区表时，相应调整列名称
  test("SPARK-7616: adjust column name order accordingly when saving partitioned table") {
    val df = (1 to 3).map(i => (i, s"val_$i", i * 2)).toDF("a", "b", "c")

    df.write
      .format(dataSourceName)
      .mode(SaveMode.Overwrite)
      .partitionBy("c", "a")
      .saveAsTable("t")

    withTable("t") {
      checkAnswer(sqlContext.table("t"), df.select('b, 'c, 'a).collect())
    }
  }

  // NOTE: This test suite is not super deterministic.  On nodes with only relatively few cores
  // (4 or even 1), it's hard to reproduce the data loss issue.  But on nodes with for example 8 or
  // more cores, the issue can be reproduced steadily.  Fortunately our Jenkins builder meets this
  // requirement.  We probably want to move this test case to spark-integration-tests or spark-perf
  // later.
  //在写文件时避免名称冲突
  test("SPARK-8406: Avoids name collision while writing files") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      sqlContext
        .range(10000)
        .repartition(250)
        .write
        .mode(SaveMode.Overwrite)
        .format(dataSourceName)
        .save(path)

      assertResult(10000) {
        sqlContext
          .read
          .format(dataSourceName)
          .option("dataSchema", StructType(StructField("id", LongType) :: Nil).json)
          .load(path)
          .count()
      }
    }
  }
  //指定的自定义输出提交者不会用于附加数据
  test("SPARK-8578 specified custom output committer will not be used to append data") {
    val clonedConf = new Configuration(configuration)
    try {
      val df = sqlContext.range(1, 10).toDF("i")
      withTempPath { dir =>
        df.write.mode("append").format(dataSourceName).save(dir.getCanonicalPath)
        configuration.set(
          SQLConf.OUTPUT_COMMITTER_CLASS.key,
          classOf[AlwaysFailOutputCommitter].getName)
        // Since Parquet has its own output committer setting, also set it
        // to AlwaysFailParquetOutputCommitter at here.
        //由于Parquet有自己的输出提交者设置，也设置它到AlwaysFailParquetOutputCommitter在这里。
        configuration.set("spark.sql.parquet.output.committer.class",
          classOf[AlwaysFailParquetOutputCommitter].getName)
        // Because there data already exists,
        // this append should succeed because we will use the output committer associated
        // with file format and AlwaysFailOutputCommitter will not be used.
        //因为数据已经存在,这个append应该成功，因为我们将使用输出提交者关联与文件格式和AlwaysFailOutputCommitter将不被使用。
        df.write.mode("append").format(dataSourceName).save(dir.getCanonicalPath)
        checkAnswer(
          sqlContext.read
            .format(dataSourceName)
            .option("dataSchema", df.schema.json)
            .load(dir.getCanonicalPath),
          df.unionAll(df))

        // This will fail because AlwaysFailOutputCommitter is used when we do append.
        //这将失败，因为在附加时使用AlwaysFailOutputCommitter
        intercept[Exception] {
          df.write.mode("overwrite").format(dataSourceName).save(dir.getCanonicalPath)
        }
      }
      withTempPath { dir =>
        configuration.set(
          SQLConf.OUTPUT_COMMITTER_CLASS.key,
          classOf[AlwaysFailOutputCommitter].getName)
        // Since Parquet has its own output committer setting, also set it
        // to AlwaysFailParquetOutputCommitter at here.
        //由于Parquet有自己的输出提交者设置，也设置它
        //到AlwaysFailParquetOutputCommitter在这里。
        configuration.set("spark.sql.parquet.output.committer.class",
          classOf[AlwaysFailParquetOutputCommitter].getName)
        // Because there is no existing data,
        // this append will fail because AlwaysFailOutputCommitter is used when we do append
        // and there is no existing data.
        //因为没有现有的数据,这个附件会失败，因为在附加时使用AlwaysFailOutputCommitter并没有现有的数据
        intercept[Exception] {
          df.write.mode("append").format(dataSourceName).save(dir.getCanonicalPath)
        }
      }
    } finally {
      // Hadoop 1 doesn't have `Configuration.unset`
      //Hadoop 1没有`Configuration.unset`
      configuration.clear()
      clonedConf.foreach(entry => configuration.set(entry.getKey, entry.getValue))
    }
  }
  //当投机开启时，禁用自定义输出提交者
  test("SPARK-9899 Disable customized output committer when speculation is on") {
    val clonedConf = new Configuration(configuration)
    val speculationEnabled =
      sqlContext.sparkContext.conf.getBoolean("spark.speculation", defaultValue = false)

    try {
      withTempPath { dir =>
        // Enables task speculation
        //启用任务推测
        sqlContext.sparkContext.conf.set("spark.speculation", "true")

        // Uses a customized output committer which always fails
        //使用始终失败的自定义输出提交程序
        configuration.set(
          SQLConf.OUTPUT_COMMITTER_CLASS.key,
          classOf[AlwaysFailOutputCommitter].getName)

        // Code below shouldn't throw since customized output committer should be disabled.
        //下面的代码不应该被抛出，因为定制输出提交者应被禁用
        val df = sqlContext.range(10).coalesce(1)
        df.write.format(dataSourceName).save(dir.getCanonicalPath)
        checkAnswer(
          sqlContext
            .read
            .format(dataSourceName)
            .option("dataSchema", df.schema.json)
            .load(dir.getCanonicalPath),
          df)
      }
    } finally {
      // Hadoop 1 doesn't have `Configuration.unset`
      // Hadoop 1没有`Configuration.unset`
      configuration.clear()
      clonedConf.foreach(entry => configuration.set(entry.getKey, entry.getValue))
      sqlContext.sparkContext.conf.set("spark.speculation", speculationEnabled.toString)
    }
  }
}

// This class is used to test SPARK-8578. We should not use any custom output committer when
// we actually append data to an existing dir.
//这个类用于测试SPARK-8578。 何时不得使用任何自定义输出提交者
//我们实际上将数据附加到现有的目录。
class AlwaysFailOutputCommitter(
    outputPath: Path,
    context: TaskAttemptContext)
  extends FileOutputCommitter(outputPath, context) {

  override def commitJob(context: JobContext): Unit = {
    sys.error("Intentional job commitment failure for testing purpose.")
  }
}

// This class is used to test SPARK-8578. We should not use any custom output committer when
// we actually append data to an existing dir.
//这个类用于测试SPARK-8578。 何时不得使用任何自定义输出提交者
//我们实际上将数据附加到现有的目录。
class AlwaysFailParquetOutputCommitter(
    outputPath: Path,
    context: TaskAttemptContext)
  extends ParquetOutputCommitter(outputPath, context) {

  override def commitJob(context: JobContext): Unit = {
    sys.error("Intentional job commitment failure for testing purpose.")
  }
}
