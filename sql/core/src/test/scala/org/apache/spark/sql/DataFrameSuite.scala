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

package org.apache.spark.sql

import java.io.File

import scala.language.postfixOps
import scala.util.Random

import org.apache.spark.sql.catalyst.plans.logical.OneRowRelation
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.test.{SharedSQLContext}
/**
 * DataFrame是一个分布式的,按照命名列的形式组织的数据集合,与关系型数据库中的数据库表类似
 */
class DataFrameSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("analysis error should be eagerly reported") {//分析错误应急切的报告
    // Eager analysis.
    withSQLConf(SQLConf.DATAFRAME_EAGER_ANALYSIS.key -> "true") {
      intercept[Exception] { testData.select('nonExistentName) }
      intercept[Exception] {//df.agg() 求聚合用的相关函数
        testData.groupBy('key).agg(Map("nonExistentName" -> "sum"))
      }
      intercept[Exception] {
        testData.groupBy("nonExistentName").agg(Map("key" -> "sum"))//df.agg() 求聚合用的相关函数
      }
      intercept[Exception] {
        testData.groupBy($"abcd").agg(Map("key" -> "sum"))
      }
    }

    // No more eager analysis once the flag is turned off
    //一旦关闭标志不再进行更多的分析
    withSQLConf(SQLConf.DATAFRAME_EAGER_ANALYSIS.key -> "false") {
      testData.select('nonExistentName)
    }
  }

  test("dataframe toString") {//dataframe转换字符串
    assert(testData.toString === "[key: int, value: string]")
    //列名获取方式
    assert(testData("key").toString === "key")
    assert($"test".toString === "test")
  }

  test("rename nested groupby") {//重命名嵌套查询
    val df = Seq((1, (1, 1))).toDF()
    /**
     *+---+-----+
      | _1|   _2|
      +---+-----+
      |  1|[1,1]|
      +---+-----+
     */
    //df.show()
    checkAnswer(
        //第一列分组,合计第二列,第一个值
      df.groupBy("_1").agg(sum("_2._1")).toDF("key", "total"),
      Row(1, 1) :: Nil)
  }

  test("invalid plan toString, debug mode") {//无效的计划方法,调试模式
    // Turn on debug mode so we can see invalid query plans.
    //打开调试模式,这样我们就可以看到无效的查询计划。
    import org.apache.spark.sql.execution.debug._

    withSQLConf(SQLConf.DATAFRAME_EAGER_ANALYSIS.key -> "true") {
      sqlContext.debug()

      val badPlan = testData.select('badColumn)

      assert(badPlan.toString contains badPlan.queryExecution.toString,
        "toString on bad query plans should include the query execution but was:\n" +
          badPlan.toString)
    }
  }

  test("access complex data") {//访问复杂的数据
    /**
     *+-----------+-----+---------+-----+
      |          m|    s|        a|    b|
      +-----------+-----+---------+-----+
      |Map(1 -> 1)|[1,1]|[1, 1, 1]| true|
      |Map(2 -> 2)|[2,2]|[2, 2, 2]|false|
      +-----------+-----+---------+-----+
     */
    complexData.show()
    //过虑a列,getItem(0)数组第一个值
    assert(complexData.filter(complexData("a").getItem(0) === 2).count() == 1)
     //过虑m列,getItem(1)数组第二个值
    assert(complexData.filter(complexData("m").getItem("1") === 1).count() == 1)
     //过虑s列,取字段Key的值,
    assert(complexData.filter(complexData("s").getField("key") === 1).count() == 1)
  }

  test("table scan") {//表扫描
    //默认数据集testData,
    checkAnswer(
      testData,
      testData.collect().toSeq)
  }

  test("empty data frame") {//空数据集
    assert(sqlContext.emptyDataFrame.columns.toSeq === Seq.empty[String])
    assert(sqlContext.emptyDataFrame.count() === 0)
  }

  test("head and take") {//头和take取值相等
    assert(testData.take(2) === testData.collect().take(2))
    assert(testData.head(2) === testData.collect().take(2))
    assert(testData.head(2).head.schema === testData.schema)
  }

  test("simple explode") {//简单的把字符串分割为数组
    //元组形式
    val df = Seq(Tuple1("a b c"), Tuple1("d e")).toDF("words")
    /**
     *+-----+
      |words|
      +-----+
      |a b c|
      |  d e|
      +-----+
     */
    df.show()
    checkAnswer(
       //简单的把字符串分割为数组
      df.explode("words", "word") { word: String => word.split(" ").toSeq }.select('word),
      Row("a") :: Row("b") :: Row("c") :: Row("d") ::Row("e") :: Nil
    )
  }

  test("explode") {//把字符串分割为数组
    val df = Seq((1, "a b c"), (2, "a b"), (3, "a")).toDF("number", "letters")
    val df2 =
      df.explode('letters) {
        //使用case Row形式分隔,注意Tuple1元组形式
        case Row(letters: String) => letters.split(" ").map(Tuple1(_)).toSeq
      }
   /**
    * +------+-------+---+
      |number|letters| _1|
      +------+-------+---+
      |     1|  a b c|  a|
      |     1|  a b c|  b|
      |     1|  a b c|  c|
      |     2|    a b|  a|
      |     2|    a b|  b|
      |     3|      a|  a|
      +------+-------+---+
    */
     df2.select('_1 as 'letter, 'number).groupBy('letter).agg(countDistinct('number)).show()
  /** +------+----------------------+
      |letter|COUNT(DISTINCT number)|
      +------+----------------------+
      |     a|                     3|
      |     b|                     2|
      |     c|                     1|
      +------+----------------------+**/
    checkAnswer(
      df2
        .select('_1 as 'letter, 'number)
        .groupBy('letter)
        //countDistinct去重统计
        .agg(countDistinct('number)),//df.agg() 求聚合用的相关函数
      Row("a", 3) :: Row("b", 2) :: Row("c", 1) :: Nil
    )
  }
   //explode函数把字符串分割为数组
  test("SPARK-8930: explode should fail with a meaningful message if it takes a star") {
    val df = Seq(("1", "1,2"), ("2", "4"), ("3", "7,8,9")).toDF("prefix", "csv")
    /**
    df.show()
    +------+-----+
    |prefix|  csv|
    +------+-----+
    |     1|  1,2|
    |     2|    4|
    |     3|7,8,9|
    +------+-----+*/    
    val e = intercept[AnalysisException] {
      df.explode($"*") { case Row(prefix: String, csv: String) =>
        csv.split(",").map(v => Tuple1(prefix + ":" + v)).toSeq
      }.queryExecution.assertAnalyzed()
    }
    assert(e.getMessage.contains(
      "Cannot explode *, explode can only be applied on a specific column."))

    df.explode('prefix, 'csv) { case Row(prefix: String, csv: String) =>
      csv.split(",").map(v => Tuple1(prefix + ":" + v)).toSeq
    }.queryExecution.assertAnalyzed()
  }

  test("explode alias and star") {//把字符串分割为数组的别名和星号
    val df = Seq((Array("a"), 1)).toDF("a", "b")

    checkAnswer(
      df.select(explode($"a").as("a"), $"*"),
      Row("a", Seq("a"), 1) :: Nil)
  }

  test("selectExpr") {//选择表达式
    
    /**
     *testData.show()
      +---+-----+
      |  1|    1|
      |  2|    2|
      |  3|    3|
      |  4|    4|
      |  5|    5|
      | 16|   16|
      | 17|   17|
      | 18|   18|
      | 19|   19|
      | 20|   20|
      +---+-----+ */
     //testData.selectExpr("abs(key)", "value").show()
    checkAnswer(        
      testData.selectExpr("abs(key)", "value"),
      testData.collect().map(row => Row(math.abs(row.getInt(0)), row.getString(1))).toSeq)
  }

  test("selectExpr with alias") {//选择表达式的别名
    /**
     *+---+
      |  k|
      +---+
      |  1|
      |  2|
      |  3|
      +---+*/
    testData.selectExpr("key as k").select("k").show()
    
    checkAnswer(
      //表达式使用别名
      testData.selectExpr("key as k").select("k"),
      testData.select("key").collect().toSeq)
  }

  test("filterExpr") {//过滤操作
    checkAnswer(
      testData.filter("key > 90"),
      //使用集合过虑
      testData.collect().filter(_.getInt(0) > 90).toSeq)
  }

  test("filterExpr using where") {//使用where过滤操作
    checkAnswer(
      testData.where("key > 50"),
      testData.collect().filter(_.getInt(0) > 50).toSeq)
  }

  test("repartition") {//重新分配分区
    checkAnswer(
      testData.select('key).repartition(10).select('key),
      testData.select('key).collect().toSeq)
  }

  test("coalesce") {//合并分区,引用字段使用
    assert(testData.select('key).coalesce(1).rdd.partitions.size === 1)

    checkAnswer(
      testData.select('key).coalesce(1).select('key),
      testData.select('key).collect().toSeq)
  }

  test("convert $\"attribute name\" into unresolved attribute") {//转换 为解决属性
    checkAnswer(
      testData.where($"key" === lit(1)).select($"value"),
      Row("1"))
  }
  //将Scala Symbol'attrname转换为未解析的属性
  test("convert Scala Symbol 'attrname into unresolved attribute") {
    checkAnswer(
      testData.where('key === lit(1)).select('value),
      Row("1"))
  }

  test("select *") {
    checkAnswer(
      testData.select($"*"),
      testData.collect().toSeq)
  }

  test("simple select") {//简单的选择
    checkAnswer(
      testData.where('key === lit(1)).select('value),
      Row("1"))
  }

  test("select with functions") {//选择功能
    checkAnswer(
      testData.select(sum('value), avg('value), count(lit(1))),
      Row(5050.0, 50.5, 100))

    checkAnswer(
      testData2.select('a + 'b, 'a < 'b),
      Seq(
        Row(2, false),
        Row(3, true),
        Row(3, false),
        Row(4, false),
        Row(4, false),
        Row(5, false)))

    checkAnswer(
      testData2.select(sumDistinct('a)),
      Row(6))
  }

  test("global sorting") {//全局排序
    checkAnswer(
      //使用字段排序
      testData2.orderBy('a.asc, 'b.asc),
      Seq(Row(1, 1), Row(1, 2), Row(2, 1), Row(2, 2), Row(3, 1), Row(3, 2)))

    checkAnswer(//使用函数排序
      testData2.orderBy(asc("a"), desc("b")),
      Seq(Row(1, 2), Row(1, 1), Row(2, 2), Row(2, 1), Row(3, 2), Row(3, 1)))

    checkAnswer(
      testData2.orderBy('a.asc, 'b.desc),
      Seq(Row(1, 2), Row(1, 1), Row(2, 2), Row(2, 1), Row(3, 2), Row(3, 1)))

    checkAnswer(
      testData2.orderBy('a.desc, 'b.desc),
      Seq(Row(3, 2), Row(3, 1), Row(2, 2), Row(2, 1), Row(1, 2), Row(1, 1)))

    checkAnswer(
      testData2.orderBy('a.desc, 'b.asc),
      Seq(Row(3, 1), Row(3, 2), Row(2, 1), Row(2, 2), Row(1, 1), Row(1, 2)))

    /**
     *+---------+--------------------+
      |     data|          nestedData|
      +---------+--------------------+
      |[1, 2, 3]|[WrappedArray(1, ...|
      |[2, 3, 4]|[WrappedArray(2, ...|
      +---------+--------------------+
     */
    arrayData.toDF().show()      
    arrayData.toDF().orderBy('data.getItem(0).desc).show()
    checkAnswer(
      //使用data数组字段第一个值排序(即1)
      arrayData.toDF().orderBy('data.getItem(0).asc),
      arrayData.toDF().collect().sortBy(_.getAs[Seq[Int]](0)(0)).toSeq)

    checkAnswer(
      //使用data数组字段第一个值排序(即1),降序
      arrayData.toDF().orderBy('data.getItem(0).desc),
      arrayData.toDF().collect().sortBy(_.getAs[Seq[Int]](0)(0)).reverse.toSeq)

    checkAnswer(
      arrayData.toDF().orderBy('data.getItem(1).asc),
      arrayData.toDF().collect().sortBy(_.getAs[Seq[Int]](0)(1)).toSeq)

    checkAnswer(
      arrayData.toDF().orderBy('data.getItem(1).desc),
      arrayData.toDF().collect().sortBy(_.getAs[Seq[Int]](0)(1)).reverse.toSeq)
  }

  test("limit") {//限制
    //取10条数据
    testData.limit(10).show()
    checkAnswer(
      testData.limit(10),
      testData.take(10).toSeq)

    checkAnswer(
      arrayData.toDF().limit(1),
      arrayData.take(1).map(r => Row.fromSeq(r.productIterator.toSeq)))

    checkAnswer(
      mapData.toDF().limit(1),
      mapData.take(1).map(r => Row.fromSeq(r.productIterator.toSeq)))
  }

  test("except") {//返回两个结果集的差(即从左查询中返回右查询没有找到的所有非重复值)
    lowerCaseData.except(upperCaseData).show()
    checkAnswer(
      lowerCaseData.except(upperCaseData),
      Row(1, "a") ::
      Row(2, "b") ::
      Row(3, "c") ::
      Row(4, "d") :: Nil)
    checkAnswer(lowerCaseData.except(lowerCaseData), Nil)
    checkAnswer(upperCaseData.except(upperCaseData), Nil)
  }

  test("intersect") {//返回 两个结果集的交集(即两个查询都返回的所有非重复值)
    checkAnswer(
      lowerCaseData.intersect(lowerCaseData),
      Row(1, "a") ::
      Row(2, "b") ::
      Row(3, "c") ::
      Row(4, "d") :: Nil)
    checkAnswer(lowerCaseData.intersect(upperCaseData), Nil)
  }

  test("udf") {//自定义方法
    val foo = udf((a: Int, b: String) => a.toString + b)
    checkAnswer(
      //SELECT *, foo(key, value) FROM testData
      testData.select($"*", foo('key, 'value)).limit(3),
      Row(1, "1", "11") :: Row(2, "2", "22") :: Row(3, "3", "33") :: Nil
    )
  }

  test("deprecated callUdf in SQLContext") {//不赞成使用callUdf
    val df = Seq(("id1", 1), ("id2", 4), ("id3", 5)).toDF("id", "value")
    val sqlctx = df.sqlContext
    sqlctx.udf.register("simpleUdf", (v: Int) => v * v)
    checkAnswer(
      df.select($"id", callUdf("simpleUdf", $"value")),
      Row("id1", 1) :: Row("id2", 16) :: Row("id3", 25) :: Nil)
  }

  test("callUDF in SQLContext") {//
    val df = Seq(("id1", 1), ("id2", 4), ("id3", 5)).toDF("id", "value")
    val sqlctx = df.sqlContext
    sqlctx.udf.register("simpleUDF", (v: Int) => v * v)
    checkAnswer(
      df.select($"id", callUDF("simpleUDF", $"value")),
      Row("id1", 1) :: Row("id2", 16) :: Row("id3", 25) :: Nil)
  }

  test("withColumn") {//使用列
    //使用新列
    val df = testData.toDF().withColumn("newCol", col("key") + 1)
     //df.show()
    checkAnswer(
      df,
      testData.collect().map { case Row(key: Int, value: String) =>
        Row(key, value, key + 1)
      }.toSeq)
    assert(df.schema.map(_.name) === Seq("key", "value", "newCol"))
  }

  test("replace column using withColumn") {//替换使用的列
    val df2 = sqlContext.sparkContext.parallelize(Array(1, 2, 3)).toDF("x")
    val df3 = df2.withColumn("x", df2("x") + 1)
    checkAnswer(
      df3.select("x"),
      Row(2) :: Row(3) :: Row(4) :: Nil)
  }

  test("drop column using drop") {//删除使用列
    val df = testData.drop("key")
    checkAnswer(
      df,
      testData.collect().map(x => Row(x.getString(1))).toSeq)
    assert(df.schema.map(_.name) === Seq("value"))
  }

  test("drop unknown column (no-op)") {//删除未知的列
    /**
     *+---+-----+
      |key|value|
      +---+-----+
      |  1|    1|
      |  2|    2|
      +---+-----+
     */
    testData.show()
    //删除未知的列
    val df = testData.drop("random")
    checkAnswer(
      df,
      testData.collect().toSeq)
    assert(df.schema.map(_.name) === Seq("key", "value"))
  }
  //删除列使用列的引用
  test("drop column using drop with column reference") {
    val col = testData("key")
    //删除引用的列
    val df = testData.drop(col)
    df.show()
    checkAnswer(
      df,
      testData.collect().map(x => Row(x.getString(1))).toSeq)
    assert(df.schema.map(_.name) === Seq("value"))
  }
   //删除未知列使用的列
  test("drop unknown column (no-op) with column reference") {
    val col = Column("random")
    val df = testData.drop(col)
    checkAnswer(
      df,
      testData.collect().toSeq)
    assert(df.schema.map(_.name) === Seq("key", "value"))
  }
  //使用列引用将具有相同名称（no-op）的未知列删除
  test("drop unknown column with same name (no-op) with column reference") {
    val col = Column("key")//删除未知的列,但列没有引用
    val df = testData.drop(col)
    checkAnswer(
      df,
      testData.collect().toSeq)
    //查找列名的方法
    assert(df.schema.map(_.name) === Seq("key", "value"))
  }
  //使用列引用加入重复列后删除列
  test("drop column after join with duplicate columns using column reference") {
    /**
     *+--------+------+
      |personId|salary|
      +--------+------+
      |       0|2000.0|
      |       1|1000.0|
      +--------+------+
     */
    salary.show()//重命名列
    val newSalary = salary.withColumnRenamed("personId", "id")
    /**
     *+---+------+
      | id|salary|
      +---+------+
      |  0|2000.0|
      |  1|1000.0|
      +---+------+
     */
    newSalary.show()
    val col = newSalary("id")
    // this join will result in duplicate "id" columns
    //此连接将导致重复的“ID”列
    val joinedDf = person.join(newSalary,
      person("id") === newSalary("id"), "inner")
    // remove only the "id" column that was associated with newSalary
      //只删除“ID”列,与薪酬
    val df = joinedDf.drop(col)
    checkAnswer(
      df,
      joinedDf.collect().map {
        case Row(id: Int, name: String, age: Int, idToDrop: Int, salary: Double) =>
          Row(id, name, age, salary)
      }.toSeq)
     //查找列名的方法
    assert(df.schema.map(_.name) === Seq("id", "name", "age", "salary"))
    assert(df("id") == person("id"))
  }

  test("withColumnRenamed") {//列的重命名
    val df = testData.toDF().withColumn("newCol", col("key") + 1)
      .withColumnRenamed("value", "valueRenamed")
    checkAnswer(
      df,
      testData.collect().map { case Row(key: Int, value: String) =>
        Row(key, value, key + 1)
      }.toSeq)
    assert(df.schema.map(_.name) === Seq("key", "valueRenamed", "newCol"))
  }

  test("describe") {//描述
    val describeTestData = Seq(
      ("Bob", 16, 176),
      ("Alice", 32, 164),
      ("David", 60, 192),
      ("Amy", 24, 180)).toDF("name", "age", "height")

    val describeResult = Seq(
      Row("count", "4", "4"),
      Row("mean", "33.0", "178.0"),
      Row("stddev", "16.583123951777", "10.0"),
      Row("min", "16", "164"),
      Row("max", "60", "192"))

    val emptyDescribeResult = Seq(
      Row("count", "0", "0"),
      Row("mean", null, null),
      Row("stddev", null, null),
      Row("min", null, null),
      Row("max", null, null))
     //定义一个方法,获得列名字的序列,参数DataFrame
    def getSchemaAsSeq(df: DataFrame): Seq[String] = df.schema.map(_.name)
    //获得两列的描述DataFrame
    val describeTwoCols = describeTestData.describe("age", "height")
    //getSchemaAsSeq(describeTwoCols).foreach {println }
    assert(getSchemaAsSeq(describeTwoCols) === Seq("summary", "age", "height"))
    checkAnswer(describeTwoCols, describeResult)
    // All aggregate value should have been cast to string
    describeTwoCols.collect().foreach { row =>
      assert(row.get(1).isInstanceOf[String], "expected string but found " + row.get(1).getClass)
      assert(row.get(2).isInstanceOf[String], "expected string but found " + row.get(2).getClass)
    }

    val describeAllCols = describeTestData.describe()
    //为什么多一列summary
    assert(getSchemaAsSeq(describeAllCols) === Seq("summary", "age", "height"))
    checkAnswer(describeAllCols, describeResult)

    val describeOneCol = describeTestData.describe("age")
    assert(getSchemaAsSeq(describeOneCol) === Seq("summary", "age"))
    checkAnswer(describeOneCol, describeResult.map { case Row(s, d, _) => Row(s, d)} )

    val describeNoCol = describeTestData.select("name").describe()
    assert(getSchemaAsSeq(describeNoCol) === Seq("summary"))
    checkAnswer(describeNoCol, describeResult.map { case Row(s, _, _) => Row(s)} )

    val emptyDescription = describeTestData.limit(0).describe()
    assert(getSchemaAsSeq(emptyDescription) === Seq("summary", "age", "height"))
    checkAnswer(emptyDescription, emptyDescribeResult)
  }

  test("apply on query results (SPARK-5462)") {//应用查询结果
    val df = testData.sqlContext.sql("select key from testData")
    checkAnswer(df.select(df("key")), testData.select('key).collect().toSeq)
  }

  test("inputFiles") {//输入文件
    withTempDir { dir =>
      val df = Seq((1, 22)).toDF("a", "b")
      //parquet目录
      val parquetDir = new File(dir, "parquet").getCanonicalPath
      //println(parquetDir)
      //保存parquet格式文件
      //df.write.json(parquetDir)
      df.write.parquet(parquetDir)
      //读取数据自动转换DataFrame
      val parquetDF = sqlContext.read.parquet(parquetDir)
      //判断输入文件不为空nonEmpty,nonEmpty测试可遍历迭代器是不是为空
      //读取数据成功
      //println(parquetDF.inputFiles.nonEmpty)
      assert(parquetDF.inputFiles.nonEmpty)

      val jsonDir = new File(dir, "json").getCanonicalPath
      println(jsonDir)
      //写Json文件
      df.write.json(jsonDir)
      //读取保存的json文件
      val jsonDF = sqlContext.read.json(jsonDir)
      //判断读取json文件是否成功
      assert(parquetDF.inputFiles.nonEmpty)

      val unioned = jsonDF.unionAll(parquetDF).inputFiles.sorted
      val allFiles = (jsonDF.inputFiles ++ parquetDF.inputFiles).toSet.toArray.sorted
      assert(unioned === allFiles)
    }
  }

  ignore("show") {
    // This test case is intended ignored, but to make sure it compiles correctly
    //这个测试用例的目的是忽略,但要确保它正确编译
    //默认显示20行数据
    testData.select($"*").show()
    //show显示数据行数
    testData.select($"*").show(1000)
  }

  test("showString: truncate = [true, false]") {//截断
    val longString = Array.fill(21)("1").mkString    
    val df = sqlContext.sparkContext.parallelize(Seq("1", longString)).toDF()
    df.show()
    val expectedAnswerForFalse = """+---------------------+
                                   ||_1                   |
                                   |+---------------------+
                                   ||1                    |
                                   ||111111111111111111111|
                                   |+---------------------+
                                   |""".stripMargin
   // assert(df.showString(10, false) === expectedAnswerForFalse)
    val expectedAnswerForTrue = """+--------------------+
                                  ||                  _1|
                                  |+--------------------+
                                  ||                   1|
                                  ||11111111111111111...|
                                  |+--------------------+
                                  |""".stripMargin
  //  assert(df.showString(10, true) === expectedAnswerForTrue)
  }

  test("showString(negative)") {//负的
    val expectedAnswer = """+---+-----+
                           ||key|value|
                           |+---+-----+
                           |+---+-----+
                           |only showing top 0 rows
                           |""".stripMargin
  //  assert(testData.select($"*").showString(-1) === expectedAnswer)
  }

  test("showString(0)") {
    val expectedAnswer = """+---+-----+
                           ||key|value|
                           |+---+-----+
                           |+---+-----+
                           |only showing top 0 rows
                           |""".stripMargin
   // assert(testData.select($"*").showString(0) === expectedAnswer)
  }

  test("showString: array") {//数组
    val df = Seq(
      (Array(1, 2, 3), Array(1, 2, 3)),
      (Array(2, 3, 4), Array(2, 3, 4))
    ).toDF()
    val expectedAnswer = """+---------+---------+
                           ||       _1|       _2|
                           |+---------+---------+
                           ||[1, 2, 3]|[1, 2, 3]|
                           ||[2, 3, 4]|[2, 3, 4]|
                           |+---------+---------+
                           |""".stripMargin
  //  assert(df.showString(10) === expectedAnswer)
  }

  test("showString: minimum column width") {//最小的列宽度
    val df = Seq(
      (1, 1),
      (2, 2)
    ).toDF()
    val expectedAnswer = """+---+---+
                           || _1| _2|
                           |+---+---+
                           ||  1|  1|
                           ||  2|  2|
                           |+---+---+
                           |""".stripMargin
   //assert(df.showString(10) === expectedAnswer)
  }

  test("SPARK-7319 showString") {
    val expectedAnswer = """+---+-----+
                           ||key|value|
                           |+---+-----+
                           ||  1|    1|
                           |+---+-----+
                           |only showing top 1 row
                           |""".stripMargin
  //  assert(testData.select($"*").showString(1) === expectedAnswer)
  }

  test("SPARK-7327 show with empty dataFrame") {//显示空的dataFrame
    val expectedAnswer = """+---+-----+
                           ||key|value|
                           |+---+-----+
                           |+---+-----+
                           |""".stripMargin
  //  assert(testData.select($"*").filter($"key" < 0).showString(1) === expectedAnswer)
  }
//StructType代表一张表,StructField代表一个字段
 /* test("createDataFrame(RDD[Row], StructType) should convert UDTs (SPARK-6672)") {
    val rowRDD = sqlContext.sparkContext.parallelize(Seq(Row(new ExamplePoint(1.0, 2.0))))
    //StructType代表一张表,StructField代表一个字段
    val schema = StructType(Array(StructField("point", new ExamplePointUDT(), false)))
    //schema方式创建RDD
    val df = sqlContext.createDataFrame(rowRDD, schema)
    df.rdd.collect()
  }*/

  test("SPARK-6899: type should match when using codegen") {//配合使用时代码生成
    withSQLConf(SQLConf.CODEGEN_ENABLED.key -> "true") {
      checkAnswer(
        decimalData.agg(avg('a)),//df.agg() 求聚合用的相关函数
        Row(new java.math.BigDecimal(2.0)))
    }
  }

  test("SPARK-7133: Implement struct, array, and map field accessor") {//实现结构,数组,和Map字段的访问
    assert(complexData.filter(complexData("a")(0) === 2).count() == 1)
    assert(complexData.filter(complexData("m")("1") === 1).count() == 1)
    assert(complexData.filter(complexData("s")("key") === 1).count() == 1)
    assert(complexData.filter(complexData("m")(complexData("s")("value")) === 1).count() == 1)
    assert(complexData.filter(complexData("a")(complexData("s")("key")) === 1).count() == 1)
  }
  //支持引号属性
  test("SPARK-7551: support backticks for DataFrame attribute resolution") {
    val df = sqlContext.read.json(sqlContext.sparkContext.makeRDD(
      """{"a.b": {"c": {"d..e": {"f": 1}}}}""" :: Nil))
    checkAnswer(
      df.select(df("`a.b`.c.`d..e`.`f`")),
      Row(1)
    )

    val df2 = sqlContext.read.json(sqlContext.sparkContext.makeRDD(
      """{"a  b": {"c": {"d  e": {"f": 1}}}}""" :: Nil))
    checkAnswer(
      df2.select(df2("`a  b`.c.d  e.f")),
      Row(1)
    )

    def checkError(testFun: => Unit): Unit = {
      val e = intercept[org.apache.spark.sql.AnalysisException] {
        testFun
      }
      assert(e.getMessage.contains("syntax error in attribute name:"))
    }
    checkError(df("`abc.`c`"))
    checkError(df("`abc`..d"))
    checkError(df("`a`.b."))
    checkError(df("`a.b`.c.`d"))
  }

  test("SPARK-7324 dropDuplicates") {//
    val testData = sqlContext.sparkContext.parallelize(
      (2, 1, 2) :: (1, 1, 1) ::
      (1, 2, 1) :: (2, 1, 2) ::
      (2, 2, 2) :: (2, 2, 1) ::
      (2, 1, 1) :: (1, 1, 2) ::
      (1, 2, 2) :: (1, 2, 1) :: Nil).toDF("key", "value1", "value2")
      /**
       *+---+------+------+
        |key|value1|value2|
        +---+------+------+
        |  2|     1|     2|
        |  1|     1|     1|
        |  1|     2|     1|
        |  2|     1|     2|
        |  2|     2|     2|
        |  2|     2|     1|
        |  2|     1|     1|
        |  1|     1|     2|
        |  1|     2|     2|
        |  1|     2|     1|
        +---+------+------+ */
   testData.show()
   /**
    * +---+------+------+
      |key|value1|value2|
      +---+------+------+
      |  1|     2|     1|
      |  2|     2|     2|
      |  1|     2|     2|
      |  2|     1|     1|
      |  2|     1|     2|
      |  1|     1|     1|
      |  2|     2|     1|
      |  1|     1|     2|
      +---+------+------+
    */
   //删除重复的记录
   testData.dropDuplicates().show()
    checkAnswer(
      testData.dropDuplicates(),
      Seq(Row(2, 1, 2), Row(1, 1, 1), Row(1, 2, 1),
        Row(2, 2, 2), Row(2, 1, 1), Row(2, 2, 1),
        Row(1, 1, 2), Row(1, 2, 2)))

    checkAnswer(
        //删除重复的指定列
      testData.dropDuplicates(Seq("key", "value1")),
      Seq(Row(2, 1, 2), Row(1, 2, 1), Row(1, 1, 1), Row(2, 2, 2)))

    checkAnswer(
      testData.dropDuplicates(Seq("value1", "value2")),
      Seq(Row(2, 1, 2), Row(1, 2, 1), Row(1, 1, 1), Row(2, 2, 2)))

    checkAnswer(
      testData.dropDuplicates(Seq("key")),
      Seq(Row(2, 1, 2), Row(1, 1, 1)))

    checkAnswer(
      testData.dropDuplicates(Seq("value1")),
      Seq(Row(2, 1, 2), Row(1, 2, 1)))

    checkAnswer(
      testData.dropDuplicates(Seq("value2")),
      Seq(Row(2, 1, 2), Row(1, 1, 1)))
  }

  test("SPARK-7150 range api") {//范围
    // numSlice is greater than length numslice大于长度
    val res1 = sqlContext.range(0, 10, 1, 15).select("id")
    assert(res1.count == 10)//df.agg() 求聚合用的相关函数
    assert(res1.agg(sum("id")).as("sumid").collect() === Seq(Row(45)))

    val res2 = sqlContext.range(3, 15, 3, 2).select("id")
    assert(res2.count == 4)
    assert(res2.agg(sum("id")).as("sumid").collect() === Seq(Row(30)))

    val res3 = sqlContext.range(1, -2).select("id")
    assert(res3.count == 0)

    // start is positive, end is negative, step is negative
    //开始是正的,结束是负数,一步是负数
    val res4 = sqlContext.range(1, -2, -2, 6).select("id")
    assert(res4.count == 2)//df.agg() 求聚合用的相关函数
    assert(res4.agg(sum("id")).as("sumid").collect() === Seq(Row(0)))

    // start, end, step are negative
    //开始,结束,一步都是否定的
    val res5 = sqlContext.range(-3, -8, -2, 1).select("id")
    assert(res5.count == 3)
    assert(res5.agg(sum("id")).as("sumid").collect() === Seq(Row(-15)))

    // start, end are negative, step is positive
    //开始,结束是负的,步骤是正的
    val res6 = sqlContext.range(-8, -4, 2, 1).select("id")
    assert(res6.count == 2)
    assert(res6.agg(sum("id")).as("sumid").collect() === Seq(Row(-14)))

    val res7 = sqlContext.range(-10, -9, -20, 1).select("id")
    assert(res7.count == 0)

    val res8 = sqlContext.range(Long.MinValue, Long.MaxValue, Long.MaxValue, 100).select("id")
    assert(res8.count == 3)//df.agg() 求聚合用的相关函数
    assert(res8.agg(sum("id")).as("sumid").collect() === Seq(Row(-3)))

    val res9 = sqlContext.range(Long.MaxValue, Long.MinValue, Long.MinValue, 100).select("id")
    assert(res9.count == 2)//df.agg() 求聚合用的相关函数
    assert(res9.agg(sum("id")).as("sumid").collect() === Seq(Row(Long.MaxValue - 1)))

    // only end provided as argument
    // 只提供作为参数的结束
    val res10 = sqlContext.range(10).select("id")
    assert(res10.count == 10)//df.agg() 求聚合用的相关函数
    assert(res10.agg(sum("id")).as("sumid").collect() === Seq(Row(45)))

    val res11 = sqlContext.range(-1).select("id")
    assert(res11.count == 0)
  }

  test("SPARK-8621: support empty string column name") {//支持空字符串列名称
    val df = Seq(Tuple1(1)).toDF("").as("t")
    // We should allow empty string as column name
    //我们应该允许空字符串作为列名称
    df.col("")
    df.col("t.``")
  }
   //NaN 是Not-a-Number的缩写,某些float或double类型不符合标准浮点数语义
   //NaN == NaN,即：NaN和NaN总是相等
   //在聚合函数中,所有NaN分到同一组
   //NaN在join操作中可以当做一个普通的join key
   //NaN在升序排序中排到最后,比任何其他数值都大
  test("SPARK-8797: sort by float column containing NaN should not crash") {
    //排序float列包含NaN,不应该崩溃
    val inputData = Seq.fill(10)(Tuple1(Float.NaN)) ++ (1 to 20).map(x => Tuple1(x.toFloat))
    inputData.foreach(println)
    val df = Random.shuffle(inputData).toDF("a")
    /**
    +----+
    |   a|
    +----+
    | 2.0|
    |14.0|
    | 7.0|
    | 1.0|
    | NaN|
    | NaN|
    |10.0|
    |11.0|
    | NaN|
    |15.0|
    +----+*/
    df.show(10)
    //排序包括NaN,没有值,即不能排序操作
    //df.orderBy("a").collect().foreach { x => println _}
  }
  //排序double列包含NaN,不应该崩溃
  test("SPARK-8797: sort by double column containing NaN should not crash") {
    val inputData = Seq.fill(10)(Tuple1(Double.NaN)) ++ (1 to 1000).map(x => Tuple1(x.toDouble))
    val df = Random.shuffle(inputData).toDF("a")
    df.orderBy("a").collect()
  }
   //NaN 是Not-a-Number的缩写,某些float或double类型不符合标准浮点数语义
   //NaN == NaN,即：NaN和NaN总是相等
   //在聚合函数中,所有NaN分到同一组
   //NaN在join操作中可以当做一个普通的join key
   //NaN在升序排序中排到最后,比任何其他数值都大
  test("NaN is greater than all other non-NaN numeric values") {
    val maxDouble = Seq(Double.NaN, Double.PositiveInfinity, Double.MaxValue)
      .map(Tuple1.apply).toDF("a").selectExpr("max(a)").first()
    assert(java.lang.Double.isNaN(maxDouble.getDouble(0)))
    val maxFloat = Seq(Float.NaN, Float.PositiveInfinity, Float.MaxValue)
      .map(Tuple1.apply).toDF("a").selectExpr("max(a)").first()
    assert(java.lang.Float.isNaN(maxFloat.getFloat(0)))
  }
  //重复列的更好的异常
  test("SPARK-8072: Better Exception for Duplicate Columns") {
    // only one duplicate column present 只有一个重复的列
    val e = intercept[org.apache.spark.sql.AnalysisException] {
      Seq((1, 2, 3), (2, 3, 4), (3, 4, 5)).toDF("column1", "column2", "column1")
        .write.format("parquet").save("temp")
    }
    assert(e.getMessage.contains("Duplicate column(s)"))
    assert(e.getMessage.contains("parquet"))
    assert(e.getMessage.contains("column1"))
    assert(!e.getMessage.contains("column2"))

    // multiple duplicate columns present 多个重复列
    val f = intercept[org.apache.spark.sql.AnalysisException] {
      Seq((1, 2, 3, 4, 5), (2, 3, 4, 5, 6), (3, 4, 5, 6, 7))
        .toDF("column1", "column2", "column3", "column1", "column3")
        .write.format("json").save("temp")
    }
    assert(f.getMessage.contains("Duplicate column(s)"))
    assert(f.getMessage.contains("JSON"))
    assert(f.getMessage.contains("column1"))
    assert(f.getMessage.contains("column3"))
    assert(!f.getMessage.contains("column2"))
  }
  //插入的表法更好的错误消息
  test("SPARK-6941: Better error message for inserting into RDD-based Table") {
    withTempDir { dir =>

      val tempParquetFile = new File(dir, "tmp_parquet")
      val tempJsonFile = new File(dir, "tmp_json")

      val df = Seq(Tuple1(1)).toDF()
      val insertion = Seq(Tuple1(2)).toDF("col")

      // pass case: parquet table (HadoopFsRelation)
      //当数据输出的位置已存在时,重写
      df.write.mode(SaveMode.Overwrite).parquet(tempParquetFile.getCanonicalPath)
      val pdf = sqlContext.read.parquet(tempParquetFile.getCanonicalPath)
      pdf.registerTempTable("parquet_base")
      insertion.write.insertInto("parquet_base")

      // pass case: json table (InsertableRelation)
      //当数据输出的位置已存在时,重写
      df.write.mode(SaveMode.Overwrite).json(tempJsonFile.getCanonicalPath)
      val jdf = sqlContext.read.json(tempJsonFile.getCanonicalPath)
      jdf.registerTempTable("json_base")
       //当数据输出的位置已存在时,重写
      insertion.write.mode(SaveMode.Overwrite).insertInto("json_base")

      // error cases: insert into an RDD
      //错误例:插入法
      df.registerTempTable("rdd_base")
      val e1 = intercept[AnalysisException] {
        insertion.write.insertInto("rdd_base")
      }
      assert(e1.getMessage.contains("Inserting into an RDD-based table is not allowed."))

      // error case: insert into a logical plan that is not a LeafNode
      //错误案例:插入一个合乎逻辑的计划,不是叶结点
      val indirectDS = pdf.select("_1").filter($"_1" > 5)
      indirectDS.registerTempTable("indirect_ds")
      val e2 = intercept[AnalysisException] {
        insertion.write.insertInto("indirect_ds")
      }
      assert(e2.getMessage.contains("Inserting into an RDD-based table is not allowed."))

      // error case: insert into an OneRowRelation
      new DataFrame(sqlContext, OneRowRelation).registerTempTable("one_row")
      val e3 = intercept[AnalysisException] {
        insertion.write.insertInto("one_row")
      }
      assert(e3.getMessage.contains("Inserting into an RDD-based table is not allowed."))
    }
  }
  //返回相同的值
  test("SPARK-8608: call `show` on local DataFrame with random columns should return same value") {
    // Make sure we can pass this test for both codegen mode and interpreted mode.
    withSQLConf(SQLConf.CODEGEN_ENABLED.key -> "true") {
      val df = testData.select(rand(33))
      assert(df.showString(5) == df.showString(5))
    }

    withSQLConf(SQLConf.CODEGEN_ENABLED.key -> "false") {
      val df = testData.select(rand(33))
      assert(df.showString(5) == df.showString(5))
    }

    // We will reuse the same Expression object for LocalRelation.
    val df = (1 to 10).map(Tuple1.apply).toDF().select(rand(33))
    assert(df.showString(5) == df.showString(5))
  }
  //本地数据框任意列排序后应该返回相同的值
  test("SPARK-8609: local DataFrame with random columns should return same value after sort") {
    // Make sure we can pass this test for both codegen mode and interpreted mode.
    //确保我们可以通过代码生成模式和解释模式,这两个测试
    withSQLConf(SQLConf.CODEGEN_ENABLED.key -> "true") {
      checkAnswer(testData.sort(rand(33)), testData.sort(rand(33)))
    }

    withSQLConf(SQLConf.CODEGEN_ENABLED.key -> "false") {
      checkAnswer(testData.sort(rand(33)), testData.sort(rand(33)))
    }

    // We will reuse the same Expression object for LocalRelation.
    //我们将使用相同的表达对象的地方
    val df = (1 to 10).map(Tuple1.apply).toDF()
    checkAnswer(df.sort(rand(33)), df.sort(rand(33)))
  }
  //具有非确定性表达式的排序
  test("SPARK-9083: sort with non-deterministic expressions") {
    import org.apache.spark.util.random.XORShiftRandom

    val seed = 33
    val df = (1 to 100).map(Tuple1.apply).toDF("i")
    val random = new XORShiftRandom(seed)
    val expected = (1 to 100).map(_ -> random.nextDouble()).sortBy(_._2).map(_._1)
    val actual = df.sort(rand(seed)).collect().map(_.getInt(0))
    assert(expected === actual)
  }
  //DataFrame.orderBy应该支持嵌套列名称
  test("SPARK-9323: DataFrame.orderBy should support nested column name") {
    val df = sqlContext.read.json(sqlContext.sparkContext.makeRDD(
      """{"a": {"b": 1}}""" :: Nil))
    checkAnswer(df.orderBy("a.b"), Row(Row(1)))
  }
  //正确分析分组/聚集在结构领域
  test("SPARK-9950: correctly analyze grouping/aggregating on struct fields") {
    val df = Seq(("x", (1, 1)), ("y", (2, 2))).toDF("a", "b")//df.agg() 求聚合用的相关函数
    checkAnswer(df.groupBy("b._1").agg(sum("b._2")), Row(1, 1) :: Row(2, 2) :: Nil)
  }
  //避免转换的执行器
  test("SPARK-10093: Avoid transformations on executors") {
    val df = Seq((1, 1)).toDF("a", "b")
    df.where($"a" === 1)
      .select($"a", $"b", struct($"b"))
      .orderBy("a")
      .select(struct($"b"))
      .collect()
  }
  //项目不应被推倒通过交叉或除
  test("SPARK-10539: Project should not be pushed down through Intersect or Except") {
    val df1 = (1 to 100).map(Tuple1.apply).toDF("i")
    val df2 = (1 to 30).map(Tuple1.apply).toDF("i")
    val intersect = df1.intersect(df2)
    val except = df1.except(df2)
    assert(intersect.count() === 30)
    assert(except.count() === 70)
  }
  //正确处理非确定性表达式集合运算
  test("SPARK-10740: handle nondeterministic expressions correctly for set operations") {
    val df1 = (1 to 20).map(Tuple1.apply).toDF("i")
    val df2 = (1 to 10).map(Tuple1.apply).toDF("i")

    // When generating expected results at here, we need to follow the implementation of
    //当在这里产生预期的结果,我们需要遵循兰德的表达
    // Rand expression.
    def expected(df: DataFrame): Seq[Row] = {
      df.rdd.collectPartitions().zipWithIndex.flatMap {
        case (data, index) =>
          val rng = new org.apache.spark.util.random.XORShiftRandom(7 + index)
          data.filter(_.getInt(0) < rng.nextDouble() * 10)
      }
    }

    val union = df1.unionAll(df2)
    checkAnswer(
      union.filter('i < rand(7) * 10),
      expected(union)
    )
    checkAnswer(
      union.select(rand(7)),
      union.rdd.collectPartitions().zipWithIndex.flatMap {
        case (data, index) =>
          val rng = new org.apache.spark.util.random.XORShiftRandom(7 + index)
          data.map(_ => rng.nextDouble()).map(i => Row(i))
      }
    )

    val intersect = df1.intersect(df2)
    checkAnswer(
      intersect.filter('i < rand(7) * 10),
      expected(intersect)
    )

    val except = df1.except(df2)
    checkAnswer(
      except.filter('i < rand(7) * 10),
      expected(except)
    )
  }
  //在分区的列固定大小敏感的筛选器
  test("SPARK-11301: fix case sensitivity for filter on partitioned columns") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      withTempPath { path =>
        Seq(2012 -> "a").toDF("year", "val").write.partitionBy("year").parquet(path.getAbsolutePath)
        val df = sqlContext.read.parquet(path.getAbsolutePath)
        checkAnswer(df.filter($"yEAr" > 2000).select($"val"), Row("a"))
      }
    }
  }
}
