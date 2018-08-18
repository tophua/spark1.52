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

import java.math.MathContext
import java.sql.Timestamp

import org.apache.spark.AccumulatorSuite
import org.apache.spark.sql.catalyst.DefaultParserDialect
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.errors.DialectException
import org.apache.spark.sql.execution.aggregate
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SQLTestData._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._

/** 
 *  A SQL Dialect for testing purpose, and it can not be nested type
 *  用于测试目的的SQL方言,它不能嵌套类型
 *  */
class MyDialect extends DefaultParserDialect
//SQL查询测试套件
class SQLQuerySuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  setupTestData()

  test("having clause") {//HAVING子句   
    Seq(("one", 1), ("two", 2), ("three", 3), ("one", 5)).toDF("k", "v").registerTempTable("hav")
    /**
     * sql("SELECT * FROM hav ").show()
     	  +-----+---+
        |    k|  v|
        +-----+---+
        |  one|  1|
        |  two|  2|
        |three|  3|
        |  one|  5|
        +-----+---+*/    
    checkAnswer(
      sql("SELECT k, sum(v) FROM hav GROUP BY k HAVING sum(v) > 2"),
      Row("one", 6) :: Row("three", 3) :: Nil)
  }

  test("SPARK-8010: promote numeric to string") {//数字转换字符
    val df = Seq((1, 1)).toDF("key", "value")
    df.registerTempTable("src")
    // case when 使用
    /**
     * +---+
     * |_c0|
     * +---+
     * |1.0|
     * +---+
     */
    sql("select case when true then 1.0 else '1' end from src ").show()

    val queryCaseWhen = sql("select case when true then 1.0 else '1' end from src ")

    //coalesce函数可以接受一系列的值,如果第一个为null,使用第二个值,如果第二个值为null,使用第三个值,以此类推
    val queryCoalesce = sql("select coalesce(null, 1, '1') from src ")
    /**
     +---+---+---+
     |_c0|_c1|_c2|
     +---+---+---+
     |  1|  2|  3|
     +---+---+---+
     */
    val queryCoalescec = sql("select coalesce(key, 2, '3'),coalesce(null, 2, '3'),coalesce(null, null, '3') from src ").show()
    checkAnswer(queryCaseWhen, Row("1.0") :: Nil)
    checkAnswer(queryCoalesce, Row("1") :: Nil)
  }

  test("show functions") {//显示内置函数
    sql("SHOW functions").show()
    checkAnswer(sql("SHOW functions"),
      FunctionRegistry.builtin.listFunction().sorted.map(Row(_)))
  }

  test("describe functions") { //描述函数
    checkExistence(sql("describe function extended upper"), true,
      "Function: upper",
      "Class: org.apache.spark.sql.catalyst.expressions.Upper",
      "Usage: upper(str) - Returns str with all characters changed to uppercase",
      "Extended Usage:",
      "> SELECT upper('SparkSql');",
      "'SPARKSQL'")

    checkExistence(sql("describe functioN Upper"), true,
      "Function: upper",
      "Class: org.apache.spark.sql.catalyst.expressions.Upper",
      "Usage: upper(str) - Returns str with all characters changed to uppercase")

    checkExistence(sql("describe functioN Upper"), false,
      "Extended Usage")

    checkExistence(sql("describe functioN abcadf"), true,
      "Function: abcadf is not found.")
  }

  test("SPARK-6743: no columns from cache") {//没有缓存的列
    Seq(
      (83, 0, 38),//第一行数据
      (26, 0, 79),//第二行数据
      (43, 81, 24)//第三行数据
    ).toDF("a", "b", "c").registerTempTable("cachedData")
 
   //缓存列     
    sqlContext.cacheTable("cachedData")
      
    /**
     * +---+---+---+
       |  a|  b|  c|
       +---+---+---+
       | 83|  0| 38|
       | 26|  0| 79|
       | 43| 81| 24|
       +---+---+---+
     */
     sql("SELECT * FROM cachedData").show()
   
     /**
      * +---+
      *  |  b|
      *  +---+
      *  | 81|
      *  |  0|
      *  +---+
      */
    checkAnswer(//分组b列,
      sql("SELECT t1.b FROM  cachedData t1 GROUP BY t1.b"),
      Row(0) :: Row(81) :: Nil)
  }

  test("self join with aliases") {//自连接列的别名    
    Seq(1, 2, 3).map(i => (i, i.toString)).toDF("int", "str").registerTempTable("df")
    //sql("SELECT * from df").show()
    /**
     *+---+---+
      |int|str|
      +---+---+
      |  1|  1|
      |  2|  2|
      |  3|  3|
      +---+---+
     */
    checkAnswer(
      sql(
        """
          |SELECT x.str, COUNT(*)
          |FROM df x JOIN df y ON x.str = y.str
          |GROUP BY x.str
        """.stripMargin),
      Row("1", 1) :: Row("2", 1) :: Row("3", 1) :: Nil)
  }

  test("support table.star") {//支持表星号
    /**
     *+---+-----+
      |key|value|
      +---+-----+
      |  1|    1|
      |  2|    2|
      |  3|    3|
      |  4|    4|
      |  5|    5|
      |  6|    6|
      |  7|    7|
      |  8|    8|
      |  9|    9|
      | 10|   10|
      +---+-----+
     */
    sql("select * from testData").show()
    /**
     *+---+---+
      |  a|  b|
      +---+---+
      |  1|  1|
      |  1|  2|
      |  2|  1|
      |  2|  2|
      |  3|  1|
      |  3|  2|
      +---+---+
     */
    sql("select * from testData2").show()
    checkAnswer(
      sql(
        """
          |SELECT r.*
          |FROM testData l join testData2 r on (l.key = r.a)
        """.stripMargin),
      Row(1, 1) :: Row(1, 2) :: Row(2, 1) :: Row(2, 2) :: Row(3, 1) :: Row(3, 2) :: Nil)
  }

  test("self join with alias in agg") {//自加入别名聚合函数
      Seq(1, 2, 3)
        .map(i => (i, i.toString))
        .toDF("int", "str")
        .groupBy("str")//df.agg() 求聚合用的相关函数
        .agg($"str", count("str").as("strCount"))//DataFrame中的内置函数操作的结果是返回一个Column对象
        .registerTempTable("df")
        
        
         Seq(1, 2, 3)
        .map(i => (i, i.toString))
        .toDF("int", "str").registerTempTable("df1")
        /**
         *+---+---+--------+
          |str|str|strCount|
          +---+---+--------+
          |  2|  2|       1|
          |  3|  3|       1|
          |  1|  1|       1|
          +---+---+--------+
         */
    //sql("select a.* from df1 a").show()
    /**
      +---+---+
      |str|_c1|
      +---+---+
      |  2|  1|
      |  3|  1|
      |  1|  1|
      +---+---+*/
/*    sql(
        """
          |SELECT x.str, SUM(x.strCount)
          |FROM df x JOIN df y ON x.str = y.str
          |GROUP BY x.str
        """.stripMargin).show()*/        
    checkAnswer(
      sql(
        """
          |SELECT x.str, SUM(x.strCount)
          |FROM df x JOIN df y ON x.str = y.str
          |GROUP BY x.str
        """.stripMargin),
      Row("1", 1) :: Row("2", 1) :: Row("3", 1) :: Nil)
  }

  test("SPARK-8668 expr function") {//表达式函数
    checkAnswer(Seq((1, "Bobby G."))
      .toDF("id", "name")//使用函数表达式
      //length返回字符串长度,abs求绝对值
      .select(expr("length(name)"), expr("abs(id)")), Row(8, 1))

    checkAnswer(Seq((1, "building burrito tunnels"), (1, "major projects"))
      .toDF("id", "saying")
      //表达式函数,以saying长度分组
      .groupBy(expr("length(saying)"))
      .count(), Row(24, 1) :: Row(14, 1) :: Nil)
  }

  test("SQL Dialect Switching to a new SQL parser") {//SQL方言切换到一个新的SQL解析器
    val newContext = new SQLContext(sqlContext.sparkContext)
    //getCanonicalName返回更容易理解的类名org.apache.spark.sql.MyDialect
    //println(classOf[MyDialect].getCanonicalName())
    newContext.setConf("spark.sql.dialect", classOf[MyDialect].getCanonicalName())
    assert(newContext.getSQLDialect().getClass === classOf[MyDialect])
    assert(newContext.sql("SELECT 1").collect() === Array(Row(1)))
  }

  test("SQL Dialect Switch to an invalid parser with alias") {//SQL方言开关无效的解析器的别名
    val newContext = new SQLContext(sqlContext.sparkContext)
    newContext.sql("SET spark.sql.dialect=MyTestClass")
    intercept[DialectException] {
      newContext.sql("SELECT 1")
    }
    // test if the dialect set back to DefaultSQLDialect
    //测试如果方言重新设置为默认的SQL方言
    assert(newContext.getSQLDialect().getClass === classOf[DefaultParserDialect])
  }

  test("SPARK-4625 support SORT BY in SimpleSQLParser & DSL") {//通过简单的SQL解析器支持排序    
    checkAnswer(
      //TestData2(a: Int, b: Int)
      sql("SELECT a FROM testData2 SORT BY a"),
      Seq(1, 1, 2, 2, 3, 3).map(Row(_))
    )
  }
  //返回不同结果的集合
  test("SPARK-7158 collect and take return different results") {
    import java.util.UUID

    val df = Seq(Tuple1(1), Tuple1(2), Tuple1(3)).toDF("index")
    // we except the id is materialized once
    //我们除了ID被物化了一次
    val idUDF = org.apache.spark.sql.functions.udf(() => UUID.randomUUID().toString)
    //注意自定义方法
    val dfWithId = df.withColumn("id", idUDF())
    // Make a new DataFrame (actually the same reference to the old one)
    //标记一个新DataFrame,(实际上是相同的旧参考)
    val cached = dfWithId.cache()
    // Trigger the cache
    //触发缓存
    val d0 = dfWithId.collect()
    val d1 = cached.collect()
    val d2 = cached.collect()

    // Since the ID is only materialized once, then all of the records
    // should come from the cache, not by re-computing. Otherwise, the ID
    // will be different
    //由于ID只实现一次,所有的记录应该来自缓存,而不是重新计算, 否则 ID会不同
    assert(d0.map(_(0)) === d2.map(_(0)))
    assert(d0.map(_(1)) === d2.map(_(1)))

    assert(d1.map(_(0)) === d2.map(_(0)))
    assert(d1.map(_(1)) === d2.map(_(1)))
  }

  test("grouping on nested fields") {//嵌套字段上的分组
    //读取Json
    sqlContext.read.json(sqlContext.sparkContext.parallelize(
      """{"nested": {"attribute": 1}, "value": 2}""" :: Nil))
     .registerTempTable("rows")
     /**
      * +------+-----+
        |nested|value|
        +------+-----+
        |   [1]|    2|
        +------+-----+
      */
     //stripMargin默认是"|"作为出来连接符,在多行换行的行头前面加一个"|"符号即可
     /**
      * +---------+---+
        |attribute|cnt|
        +---------+---+
        |        1|  1|
        +---------+---+
      */
     sql( """
          | select nested.attribute, count(*) as cnt
          |  from rows
          |  group by nested.attribute""".stripMargin).show()
    sql(
      """
        |select attribute, sum(cnt)
        |from (
        |  select nested.attribute, count(*) as cnt
        |  from rows
        |  group by nested.attribute) a
        |group by attribute
      """.stripMargin).show()
    checkAnswer(
        //分组嵌套
      sql(
        """
          |select attribute, sum(cnt)
          |from (
          |  select nested.attribute, count(*) as cnt
          |  from rows
          |  group by nested.attribute) a
          |group by attribute
        """.stripMargin),
      Row(1, 1) :: Nil)
  }

  test("SPARK-6201 IN type conversion") {//类型转换
    sqlContext.read.json(
        //json类型转换,共一列a,三行数据
      sqlContext.sparkContext.parallelize(
        Seq("{\"a\": \"1\"}}", "{\"a\": \"2\"}}", "{\"a\": \"3\"}}")))
      .registerTempTable("d")
      
    checkAnswer(
      sql("select * from d where d.a in (1,2)"),
      Seq(Row("1"), Row("2")))
  }

  test("SPARK-8828 sum should return null if all input values are null") {//如果所有的输入值为空,则应返回null
    withSQLConf(SQLConf.USE_SQL_AGGREGATE2.key -> "true") {
      withSQLConf(SQLConf.CODEGEN_ENABLED.key -> "true") {
        checkAnswer(
          sql("select sum(a), avg(a) from allNulls"),
          Seq(Row(null, null))
        )
      }
      withSQLConf(SQLConf.CODEGEN_ENABLED.key -> "false") {
        checkAnswer(
          sql("select sum(a), avg(a) from allNulls"),
          Seq(Row(null, null))
        )
      }
    }
    withSQLConf(SQLConf.USE_SQL_AGGREGATE2.key -> "false") {
      withSQLConf(SQLConf.CODEGEN_ENABLED.key -> "true") {
        checkAnswer(
          sql("select sum(a), avg(a) from allNulls"),
          Seq(Row(null, null))
        )
      }
      withSQLConf(SQLConf.CODEGEN_ENABLED.key -> "false") {
        checkAnswer(
          sql("select sum(a), avg(a) from allNulls"),
          Seq(Row(null, null))
        )
      }
    }
  }

  private def testCodeGen(sqlText: String, expectedResults: Seq[Row]): Unit = {
    val df = sql(sqlText)
    // First, check if we have GeneratedAggregate.
    //首先,检查是否生成聚合
    val hasGeneratedAgg = df.queryExecution.executedPlan
      .collect { case _: aggregate.TungstenAggregate => true }
      .nonEmpty
    if (!hasGeneratedAgg) {
      fail(
        s"""
           |Codegen is enabled, but query $sqlText does not have TungstenAggregate in the plan.
           |${df.queryExecution.simpleString}
         """.stripMargin)
    }
    // Then, check results.
    //然后检查结果
    checkAnswer(df, expectedResults)
  }

  test("aggregation with codegen") {//聚集与代码生成
    val originalValue = sqlContext.conf.codegenEnabled
    sqlContext.setConf(SQLConf.CODEGEN_ENABLED, true)
    // Prepare a table that we can group some rows.
    //准备一个表,我们可以组一些行
    sqlContext.table("testData")
      .unionAll(sqlContext.table("testData"))
      .unionAll(sqlContext.table("testData"))
      .registerTempTable("testData3x")

    try {
      // Just to group rows.
      testCodeGen(
        "SELECT key FROM testData3x GROUP BY key",
        (1 to 100).map(Row(_)))
      // COUNT 计数
      testCodeGen(
        "SELECT key, count(value) FROM testData3x GROUP BY key",
        (1 to 100).map(i => Row(i, 3)))
      testCodeGen(
        "SELECT count(key) FROM testData3x",
        Row(300) :: Nil)
      // COUNT DISTINCT ON int
        //计数不同的int
      testCodeGen(
        "SELECT value, count(distinct key) FROM testData3x GROUP BY value",
        (1 to 100).map(i => Row(i.toString, 1)))
      testCodeGen(
        "SELECT count(distinct key) FROM testData3x",
        Row(100) :: Nil)
      // SUM 求和
      testCodeGen(
        "SELECT value, sum(key) FROM testData3x GROUP BY value",
        (1 to 100).map(i => Row(i.toString, 3 * i)))
      testCodeGen(
          //CAST 强制转换 Double
        "SELECT sum(key), SUM(CAST(key as Double)) FROM testData3x",
        Row(5050 * 3, 5050 * 3.0) :: Nil)
      // AVERAGE
      testCodeGen(
          //求平均值
        "SELECT value, avg(key) FROM testData3x GROUP BY value",
        (1 to 100).map(i => Row(i.toString, i)))
      testCodeGen(
        "SELECT avg(key) FROM testData3x",
        Row(50.5) :: Nil)
      // MAX
      testCodeGen(
          //最大值
        "SELECT value, max(key) FROM testData3x GROUP BY value",
        (1 to 100).map(i => Row(i.toString, i)))
      testCodeGen(
        "SELECT max(key) FROM testData3x",
        Row(100) :: Nil)
      // MIN 最小值
      testCodeGen(
        "SELECT value, min(key) FROM testData3x GROUP BY value",
        (1 to 100).map(i => Row(i.toString, i)))
      testCodeGen(
        "SELECT min(key) FROM testData3x",
        Row(1) :: Nil)
      // Some combinations.
      //组合
      testCodeGen(
        """
          |SELECT
          |  value,
          |  sum(key),
          |  max(key),
          |  min(key),
          |  avg(key),
          |  count(key),
          |  count(distinct key)
          |FROM testData3x
          |GROUP BY value
        """.stripMargin,
        (1 to 100).map(i => Row(i.toString, i*3, i, i, i, 3, 1)))
      testCodeGen(
        "SELECT max(key), min(key), avg(key), count(key), count(distinct key) FROM testData3x",
        Row(100, 1, 50.5, 300, 100) :: Nil)
      // Aggregate with Code generation handling all null values
      //用代码生成处理所有空值的集合
      testCodeGen(
        "SELECT  sum('a'), avg('a'), count(null) FROM testData",
        Row(null, null, 0) :: Nil)
    } finally {
      //删除临时表testData3x
      sqlContext.dropTempTable("testData3x")
      //恢复配置设置
      sqlContext.setConf(SQLConf.CODEGEN_ENABLED, originalValue)
    }
  }

  test("Add Parser of SQL COALESCE()") {//添加解析SQL合并
    /**
     * sql("""SELECT COALESCE(1, 2)""").show()
        +---+
        |_c0|
        +---+
        |  1|
        +---+*/
    checkAnswer(
        //依次参考各参数表达式,遇到非null值即停止并返回该值
      sql("""SELECT COALESCE(1, 2)"""),
      Row(1))
      /**
       *sql("SELECT COALESCE(null, 1, 1.5)").show()
        +---+
        |_c0|
        +---+
        |1.0|
        +---+*/     
    checkAnswer(
        //coalesce函数可以接受一系列的值,如果第一个为null,使用第二个值,如果第二个值为null,使用第三个值,以此类推
      sql("SELECT COALESCE(null, 1, 1.5)"),
      Row(BigDecimal(1)))
    checkAnswer(
      sql("SELECT COALESCE(null, null, null)"),
      Row(null))
  }

  test("SPARK-3176 Added Parser of SQL LAST()") {//添加解析SQL LAST函数
    /**
     *+---+
      |  n|
      +---+
      |  1|
      |  2|
      |  3|
      |  4|
      +---+
     */
    sql("SELECT n FROM lowerCaseData").show()
    checkAnswer(
       //返回指定的列中最后一个记录的值
      sql("SELECT LAST(n) FROM lowerCaseData"),
      Row(4))
  }

  test("SPARK-2041 column name equals tablename") {//表的列名称等于
    checkAnswer(
      sql("SELECT tableName FROM tableName"),
      Row("test"))
  }

  test("SQRT") {//平方根函数
    /**
     *+------------------+---+
      |              SQRT|key|
      +------------------+---+
      |               1.0|  1|
      |1.4142135623730951|  2|
      |1.7320508075688772|  3|
      |               2.0|  4|
      |  2.23606797749979|  5|
      | 2.449489742783178|  6|
      |2.6457513110645907|  7|
      |2.8284271247461903|  8|
      |               3.0|  9|
      +----------------------+
     */
    sql("SELECT SQRT(key) as SQRT ,key FROM testData").show()
    checkAnswer(      
      sql("SELECT SQRT(key) FROM testData"),
      (1 to 100).map(x => Row(math.sqrt(x.toDouble))).toSeq
    )
  }

  test("SQRT with automatic string casts") {//平方根函数 cast强转换字符类型
    //sql("SELECT SQRT(CAST(key AS STRING)) FROM testData").show()
    checkAnswer(
      sql("SELECT SQRT(CAST(key AS STRING)) FROM testData"),
      (1 to 100).map(x => Row(math.sqrt(x.toDouble))).toSeq
    )
  }

  test("SPARK-2407 Added Parser of SQL SUBSTR()") {//substr()添加SQL解析器
    /**
     *+---------+
      |tableName|
      +---------+
      |     test|
      +---------+
     */
     sql("SELECT tableName FROM tableName").show()
    checkAnswer(
        //substr字符串中抽取从 start 下标开始的指定数目的字符
      sql("SELECT substr(tableName, 1, 2) FROM tableName"),
      Row("te"))
    checkAnswer(//从第三个位置开始截取
      sql("SELECT substr(tableName, 3) FROM tableName"),
      Row("st"))
    checkAnswer(//从第一个位置开始截取2个
      sql("SELECT substring(tableName, 1, 2) FROM tableName"),
      Row("te"))
    checkAnswer(
      sql("SELECT substring(tableName, 3) FROM tableName"),
      Row("st"))
  }

  test("SPARK-3173 Timestamp support in the parser") {//支持解析Timestamp类型
    //用数据集创建一个time对象的DataFrame,将DataFrame注册为一个表
    //DataFrame 与关系型数据库中的数据库表类似
    /**
      sql("SELECT time FROM timestamps").show()
      +--------------------+
      |                time|
      +--------------------+
      |1969-12-31 16:00:...|
      |1969-12-31 16:00:...|
      |1969-12-31 16:00:...|
      |1969-12-31 16:00:...|
      +--------------------+*/
    (0 to 3).map(i => Tuple1(new Timestamp(i))).toDF("time").registerTempTable("timestamps")
   
    checkAnswer(sql(
      "SELECT time FROM timestamps WHERE time='1969-12-31 16:00:00.0'"),
      Row(java.sql.Timestamp.valueOf("1969-12-31 16:00:00")))

    checkAnswer(sql(//cast强转换TIMESTAMP类型
      "SELECT time FROM timestamps WHERE time=CAST('1969-12-31 16:00:00.001' AS TIMESTAMP)"),
      Row(java.sql.Timestamp.valueOf("1969-12-31 16:00:00.001")))

    checkAnswer(sql(
      "SELECT time FROM timestamps WHERE time='1969-12-31 16:00:00.001'"),
      Row(java.sql.Timestamp.valueOf("1969-12-31 16:00:00.001")))

    checkAnswer(sql(
      "SELECT time FROM timestamps WHERE '1969-12-31 16:00:00.001'=time"),
      Row(java.sql.Timestamp.valueOf("1969-12-31 16:00:00.001")))

    checkAnswer(sql(
        //时间戳段查询
      """SELECT time FROM timestamps WHERE time<'1969-12-31 16:00:00.003'
          AND time>'1969-12-31 16:00:00.001'"""),
      Row(java.sql.Timestamp.valueOf("1969-12-31 16:00:00.002")))
      // in操作
    checkAnswer(sql(
      """
        |SELECT time FROM timestamps
        |WHERE time IN ('1969-12-31 16:00:00.001','1969-12-31 16:00:00.002')
      """.stripMargin),//时间戳段查询
      Seq(Row(java.sql.Timestamp.valueOf("1969-12-31 16:00:00.001")),
        Row(java.sql.Timestamp.valueOf("1969-12-31 16:00:00.002"))))

    checkAnswer(sql(
      "SELECT time FROM timestamps WHERE time='123'"),
      Nil)
  }

  test("index into array") {//数组的索引
    /**
     *+---------+---+---+---+
      |     data|_c1|_c2|_c3|
      +---------+---+---+---+
      |[1, 2, 3]|  1|  3|  2|
      |[2, 3, 4]|  2|  5|  3|
      +---------+---+---+---+ */
    sql("SELECT data, data[0], data[0] + data[1], data[0 + 1] FROM arrayData").show()    
    checkAnswer(
      sql("SELECT data, data[0], data[0] + data[1], data[0 + 1] FROM arrayData"),
      arrayData.map(d => Row(d.data, d.data(0), d.data(0) + d.data(1), d.data(1))).collect())
  }

  test("left semi greater than predicate") {//左半
    /**
     *+---+---+
      |  a|  b|
      +---+---+
      |  1|  1|
      |  1|  2|
      |  2|  1|
      |  2|  2|
      |  3|  1|
      |  3|  2|
      +---+---+
     */
    //sql("SELECT * FROM testData2").show()
    checkAnswer(// IN/EXISTS 子查询的一种更高效的实现
      sql("SELECT * FROM testData2 x LEFT SEMI JOIN testData2 y ON x.a >= y.a + 2"),
      Seq(Row(3, 1), Row(3, 2))
    )
  }
  //左半大于和相等操作
  test("left semi greater than predicate and equal operator") {
    checkAnswer(
      sql("SELECT * FROM testData2 x LEFT SEMI JOIN testData2 y ON x.b = y.b and x.a >= y.a + 2"),
      Seq(Row(3, 1), Row(3, 2))
    )

    checkAnswer(
      sql("SELECT * FROM testData2 x LEFT SEMI JOIN testData2 y ON x.b = y.a and x.a >= y.b + 1"),
      Seq(Row(2, 1), Row(2, 2), Row(3, 1), Row(3, 2))
    )
  }

  test("index into array of arrays") {//数组数组的索引
    /**
     *+--------------------+---+---+
      |          nestedData|_c1|_c2|
      +--------------------+---+---+
      |[WrappedArray(1, ...|  1|  3|
      |[WrappedArray(2, ...|  2|  5|
      +--------------------+---+---+
     */
     sql(
        "SELECT nestedData, nestedData[0][0], nestedData[0][0] + nestedData[0][1] FROM arrayData").show()
    checkAnswer(
      sql(
        "SELECT nestedData, nestedData[0][0], nestedData[0][0] + nestedData[0][1] FROM arrayData"),
      arrayData.map(d =>
        Row(d.nestedData,
         d.nestedData(0)(0),
         d.nestedData(0)(0) + d.nestedData(0)(1))).collect().toSeq)
  }

  test("agg") {//分组求和函数
    /**
     *+---+---+
      |  a|  b|
      +---+---+
      |  1|  1|
      |  1|  2|
      |  2|  1|
      |  2|  2|
      |  3|  1|
      |  3|  2|
      +---+---+
     */
    //sql("SELECT * FROM testData2 ").show()
    checkAnswer(
        //分组求和
      sql("SELECT a, SUM(b) FROM testData2 GROUP BY a"),
      Seq(Row(1, 3), Row(2, 3), Row(3, 3)))
  }

  test("literal in agg grouping expressions") {//分组表达式
    def literalInAggTest(): Unit = {
      /**
     *+---+---+
      |  a|  b|
      +---+---+
      |  1|  1|
      |  1|  2|
      |  2|  1|
      |  2|  2|
      |  3|  1|
      |  3|  2|
      +---+---+
     */
      checkAnswer(
        sql("SELECT a, count(1) FROM testData2 GROUP BY a, 1"),
        Seq(Row(1, 2), Row(2, 2), Row(3, 2)))
      checkAnswer(
        sql("SELECT a, count(2) FROM testData2 GROUP BY a, 2"),
        Seq(Row(1, 2), Row(2, 2), Row(3, 2)))

      checkAnswer(
        sql("SELECT a, 1, sum(b) FROM testData2 GROUP BY a, 1"),
        sql("SELECT a, 1, sum(b) FROM testData2 GROUP BY a"))
      checkAnswer(
        sql("SELECT a, 1, sum(b) FROM testData2 GROUP BY a, 1 + 2"),
        sql("SELECT a, 1, sum(b) FROM testData2 GROUP BY a"))
      checkAnswer(
        sql("SELECT 1, 2, sum(b) FROM testData2 GROUP BY 1, 2"),
        sql("SELECT 1, 2, sum(b) FROM testData2"))
    }

    literalInAggTest()
    withSQLConf(SQLConf.USE_SQL_AGGREGATE2.key -> "false") {
      literalInAggTest()
    }
  }
  //聚合操作
  test("aggregates with nulls") {
    /**
     *+----+
      |   a|
      +----+
      |   1|
      |   2|
      |   3|
      |null|
      +----+
     */
    sql("SELECT * FROM nullInts").show()
    checkAnswer(
      //最小,最大,平均,求和,计数
      sql("SELECT MIN(a), MAX(a), AVG(a), SUM(a), COUNT(a) FROM nullInts"),
      Row(1, 3, 2, 6, 3)
    )
  }

  test("select *") {//星号
    checkAnswer(
      sql("SELECT * FROM testData"),
      testData.collect().toSeq)
  }

  test("simple select") {//简单的选择字段
    checkAnswer(
      sql("SELECT value FROM testData WHERE key = 1"),
      Row("1"))
  }

  def sortTest(): Unit = {//排序测试
    checkAnswer(
      sql("SELECT * FROM testData2 ORDER BY a ASC, b ASC"),
      Seq(Row(1, 1), Row(1, 2), Row(2, 1), Row(2, 2), Row(3, 1), Row(3, 2)))

    checkAnswer(
      sql("SELECT * FROM testData2 ORDER BY a ASC, b DESC"),
      Seq(Row(1, 2), Row(1, 1), Row(2, 2), Row(2, 1), Row(3, 2), Row(3, 1)))

    checkAnswer(
      sql("SELECT * FROM testData2 ORDER BY a DESC, b DESC"),
      Seq(Row(3, 2), Row(3, 1), Row(2, 2), Row(2, 1), Row(1, 2), Row(1, 1)))

    checkAnswer(
      sql("SELECT * FROM testData2 ORDER BY a DESC, b ASC"),
      Seq(Row(3, 1), Row(3, 2), Row(2, 1), Row(2, 2), Row(1, 1), Row(1, 2)))

    checkAnswer(
      sql("SELECT b FROM binaryData ORDER BY a ASC"),
      (1 to 5).map(Row(_)))

    checkAnswer(
      sql("SELECT b FROM binaryData ORDER BY a DESC"),
      (1 to 5).map(Row(_)).toSeq.reverse)

    checkAnswer(
      sql("SELECT * FROM arrayData ORDER BY data[0] ASC"),
      arrayData.collect().sortBy(_.data(0)).map(Row.fromTuple).toSeq)

    checkAnswer(
      sql("SELECT * FROM arrayData ORDER BY data[0] DESC"),
      arrayData.collect().sortBy(_.data(0)).reverse.map(Row.fromTuple).toSeq)

    checkAnswer(
      sql("SELECT * FROM mapData ORDER BY data[1] ASC"),
      mapData.collect().sortBy(_.data(1)).map(Row.fromTuple).toSeq)

    checkAnswer(
      sql("SELECT * FROM mapData ORDER BY data[1] DESC"),
      mapData.collect().sortBy(_.data(1)).reverse.map(Row.fromTuple).toSeq)
  }

  test("sorting") {//外部排序方式
    withSQLConf(SQLConf.EXTERNAL_SORT.key -> "false") {
      sortTest()
    }
  }

  test("external sorting") {//扩展外部排序
    withSQLConf(SQLConf.EXTERNAL_SORT.key -> "true") {
      sortTest()
    }
  }

  test("SPARK-6927 sorting with codegen on") {//排序与代码生成
    withSQLConf(SQLConf.EXTERNAL_SORT.key -> "false",
      SQLConf.CODEGEN_ENABLED.key -> "true") {
      sortTest()
    }
  }

  test("SPARK-6927 external sorting with codegen on") {//外部排序与代码生成
    withSQLConf(SQLConf.EXTERNAL_SORT.key -> "true",
      SQLConf.CODEGEN_ENABLED.key -> "true") {
      sortTest()
    }
  }

  test("limit") {//限制
    checkAnswer(//前10行
      sql("SELECT * FROM testData LIMIT 10"),
      testData.take(10).toSeq)

    checkAnswer(
      sql("SELECT * FROM arrayData LIMIT 1"),//前1行
      arrayData.collect().take(1).map(Row.fromTuple).toSeq)

    checkAnswer(
      sql("SELECT * FROM mapData LIMIT 1"),//前1行
      mapData.collect().take(1).map(Row.fromTuple).toSeq)
  }

  test("CTE feature") {
    //WITH AS 子查询部,可以定义一个SQL片断
    checkAnswer(
      sql("with q1 as (select * from testData limit 10) select * from q1"),
      testData.take(10).toSeq)

    checkAnswer(
      sql("""
        |with q1 as (select * from testData where key= '5'),
        |q2 as (select * from testData where key = '4')
        |select * from q1 union all select * from q2""".stripMargin),
      Row(5, "5") :: Row(4, "4") :: Nil)

  }

  test("Allow only a single WITH clause per query") {//只允许一个查询子句
    intercept[RuntimeException] {
      sql(
        "with q1 as (select * from testData) with q2 as (select * from q1) select * from q2")
    }
  }

  test("date row") {//日期行
    checkAnswer(sql(//cast强制转换日期类型
      """select cast("2015-01-28" as date) from testData limit 1"""),
      Row(java.sql.Date.valueOf("2015-01-28"))
    )
  }

  test("from follow multiple brackets") {//跟随多个括号
    checkAnswer(sql(
      """
        |select key from ((select * from testData limit 1)
        |  union all (select * from testData limit 1)) x limit 1
      """.stripMargin),
      Row(1)
    )

    checkAnswer(sql(
      "select key from (select * from testData) x limit 1"),
      Row(1)
    )

    checkAnswer(sql(
      """
        |select key from
        |  (select * from testData limit 1 union all select * from testData limit 1) x
        |  limit 1
      """.stripMargin),
      Row(1)
    )
  }

  test("average") {//平均数
    checkAnswer(
      //平均
      sql("SELECT AVG(a) FROM testData2"),
      Row(2.0))
  }

  test("average overflow") {//平均数溢出
    /**
     *sql("SELECT * FROM largeAndSmallInts").show()
     *+----------+---+
      |         a|  b|
      +----------+---+
      |2147483644|  1|
      |         1|  2|
      |2147483645|  1|
      |         2|  2|
      |2147483646|  1|
      |         3|  2|
      +----------+---+
     */    
    checkAnswer(
      sql("SELECT AVG(a),b FROM largeAndSmallInts group by b"),
      Seq(Row(2147483645.0, 1), Row(2.0, 2)))
  }

  test("count") {//计数
    //sql("SELECT COUNT(*) FROM testData2").show()
    checkAnswer(
      sql("SELECT COUNT(*) FROM testData2"),
      Row(testData2.count()))
  }

  test("count distinct") {//重复计数
    checkAnswer(
      sql("SELECT COUNT(DISTINCT b) FROM testData2"),
      Row(2))
  }

  test("approximate count distinct") {//相似计数
    sql("SELECT APPROXIMATE COUNT(DISTINCT a) FROM testData2").show()
    checkAnswer(
        //相似计算
      sql("SELECT APPROXIMATE COUNT(DISTINCT a) FROM testData2"),
      Row(3))
  }
  //用户提供的标准偏差的近似计数
  test("approximate count distinct with user provided standard deviation") {//
    checkAnswer(
      sql("SELECT APPROXIMATE(0.04) COUNT(DISTINCT a) FROM testData2"),
      Row(3))
  }

  test("null count") {//空的计数
    /**
     *+---+----+
      |  a|   b|
      +---+----+
      |  1|null|
      |  2|   2|
      +---+----+
     */
    sql("SELECT * FROM testData3 ").show()
    checkAnswer(
      sql("SELECT a, COUNT(b) FROM testData3 GROUP BY a"),
      Seq(Row(1, 0), Row(2, 1)))

    checkAnswer(
      sql(
        "SELECT COUNT(a), COUNT(b), COUNT(1), COUNT(DISTINCT a), COUNT(DISTINCT b) FROM testData3"),
      Row(2, 1, 2, 2, 1))
  }

  test("count of empty table") {//空表计数
    withTempTable("t") {
      Seq.empty[(Int, Int)].toDF("a", "b").registerTempTable("t")
      checkAnswer(
        sql("select count(a) from t"),
        Row(0))
    }
  }

  test("inner join where, one match per row") {//内部连接,每行一个匹配
    /**
     *+---+---+---+---+
      |  N|  L|  n|  l|
      +---+---+---+---+
      |  1|  A|  1|  a|
      |  1|  A|  2|  b|
      |  2|  B|  1|  a|
      |  6|  F|  2|  b|
      |  4|  D|  3|  c|
      |  4|  D|  4|  d|
      +---+---+---+---+
     */
     sql("SELECT a.*,b.* FROM upperCaseData a,lowerCaseData b where a.N=b.n").show()
    checkAnswer(
      //大写小写匹配
      sql("SELECT * FROM upperCaseData JOIN lowerCaseData WHERE n = N"),
      Seq(
        Row(1, "A", 1, "a"),
        Row(2, "B", 2, "b"),
        Row(3, "C", 3, "c"),
        Row(4, "D", 4, "d")))
  }

  test("inner join ON, one match per row") {//内部连接,每行一个匹配
    /**
      sql("SELECT * FROM upperCaseData JOIN lowerCaseData ON n = N").show()
      +---+---+---+---+
      |  N|  L|  n|  l|
      +---+---+---+---+
      |  1|  A|  1|  a|
      |  2|  B|  2|  b|
      |  3|  C|  3|  c|
      |  4|  D|  4|  d|
      +---+---+---+---+*/    
    checkAnswer(
      sql("SELECT * FROM upperCaseData JOIN lowerCaseData ON n = N"),
      Seq(
        Row(1, "A", 1, "a"),
        Row(2, "B", 2, "b"),
        Row(3, "C", 3, "c"),
        Row(4, "D", 4, "d")))
  }

  test("inner join, where, multiple matches") {//内部连接,多个匹配
    checkAnswer(
      sql("""
        |SELECT * FROM
        |  (SELECT * FROM testData2 WHERE a = 1) x JOIN
        |  (SELECT * FROM testData2 WHERE a = 1) y
        |WHERE x.a = y.a""".stripMargin),
      Row(1, 1, 1, 1) ::
      Row(1, 1, 1, 2) ::
      Row(1, 2, 1, 1) ::
      Row(1, 2, 1, 2) :: Nil)
  }

  test("inner join, no matches") {//内部连接,不匹配
    checkAnswer(
      sql(
        """
          |SELECT * FROM
          |  (SELECT * FROM testData2 WHERE a = 1) x JOIN
          |  (SELECT * FROM testData2 WHERE a = 2) y
          |WHERE x.a = y.a""".stripMargin),
      Nil)
  }

  test("big inner join, 4 matches per row") {//大内部连接,4个匹配行
    checkAnswer(
      sql(
        """
          |SELECT * FROM
          |  (SELECT * FROM testData UNION ALL
          |   SELECT * FROM testData UNION ALL
          |   SELECT * FROM testData UNION ALL
          |   SELECT * FROM testData) x JOIN
          |  (SELECT * FROM testData UNION ALL
          |   SELECT * FROM testData UNION ALL
          |   SELECT * FROM testData UNION ALL
          |   SELECT * FROM testData) y
          |WHERE x.key = y.key""".stripMargin),
      testData.rdd.flatMap(
        row => Seq.fill(16)(Row.merge(row, row))).collect().toSeq)
  }

  test("cartesian product join") {//笛卡尔积连接
    checkAnswer(
      testData3.join(testData3),
      Row(1, null, 1, null) ::
      Row(1, null, 2, 2) ::
      Row(2, 2, 1, null) ::
      Row(2, 2, 2, 2) :: Nil)
  }

  test("left outer join") {//左外连接
    checkAnswer(
      sql("SELECT * FROM upperCaseData LEFT OUTER JOIN lowerCaseData ON n = N"),
      Row(1, "A", 1, "a") ::
      Row(2, "B", 2, "b") ::
      Row(3, "C", 3, "c") ::
      Row(4, "D", 4, "d") ::
      Row(5, "E", null, null) ::
      Row(6, "F", null, null) :: Nil)
  }

  test("right outer join") {//右外连接
    checkAnswer(
      sql("SELECT * FROM lowerCaseData RIGHT OUTER JOIN upperCaseData ON n = N"),
      Row(1, "a", 1, "A") ::
      Row(2, "b", 2, "B") ::
      Row(3, "c", 3, "C") ::
      Row(4, "d", 4, "D") ::
      Row(null, null, 5, "E") ::
      Row(null, null, 6, "F") :: Nil)
  }

  test("full outer join") {//全外连接
    checkAnswer(
      sql(
        """
          |SELECT * FROM
          |  (SELECT * FROM upperCaseData WHERE N <= 4) leftTable FULL OUTER JOIN
          |  (SELECT * FROM upperCaseData WHERE N >= 3) rightTable
          |    ON leftTable.N = rightTable.N
        """.stripMargin),
      Row(1, "A", null, null) ::
      Row(2, "B", null, null) ::
      Row(3, "C", 3, "C") ::
      Row (4, "D", 4, "D") ::
      Row(null, null, 5, "E") ::
      Row(null, null, 6, "F") :: Nil)
  }

  test("SPARK-3349 partitioning after limit") {//分割后的限制
    sql("SELECT DISTINCT n FROM lowerCaseData ORDER BY n DESC")
      .limit(2)
      .registerTempTable("subset1")
    sql("SELECT DISTINCT n FROM lowerCaseData")
      .limit(2)
      .registerTempTable("subset2")
    checkAnswer(
      sql("SELECT * FROM lowerCaseData INNER JOIN subset1 ON subset1.n = lowerCaseData.n"),
      Row(3, "c", 3) ::
      Row(4, "d", 4) :: Nil)
    checkAnswer(
      sql("SELECT * FROM lowerCaseData INNER JOIN subset2 ON subset2.n = lowerCaseData.n"),
      Row(1, "a", 1) ::
      Row(2, "b", 2) :: Nil)
  }

  test("mixed-case keywords") {//混合的情况下关键词
    checkAnswer(
      sql(
        """
          |SeleCT * from
          |  (select * from upperCaseData WherE N <= 4) leftTable fuLL OUtER joiN
          |  (sElEcT * FROM upperCaseData whERe N >= 3) rightTable
          |    oN leftTable.N = rightTable.N
        """.stripMargin),
      Row(1, "A", null, null) ::
      Row(2, "B", null, null) ::
      Row(3, "C", 3, "C") ::
      Row(4, "D", 4, "D") ::
      Row(null, null, 5, "E") ::
      Row(null, null, 6, "F") :: Nil)
  }

  test("select with table name as qualifier") {//选择与表名称作为限定符
    checkAnswer(
      sql("SELECT testData.value FROM testData WHERE testData.key = 1"),
      Row("1"))
  }

  test("inner join ON with table name as qualifier") {//内部连接与表名作为限定符
    checkAnswer(
      sql("SELECT * FROM upperCaseData JOIN lowerCaseData ON lowerCaseData.n = upperCaseData.N"),
      Seq(
        Row(1, "A", 1, "a"),
        Row(2, "B", 2, "b"),
        Row(3, "C", 3, "c"),
        Row(4, "D", 4, "d")))
  }
  //具有内部连接的合格选择与表名作为限定符
  test("qualified select with inner join ON with table name as qualifier") {
    checkAnswer(
      sql("SELECT upperCaseData.N, upperCaseData.L FROM upperCaseData JOIN lowerCaseData " +
        "ON lowerCaseData.n = upperCaseData.N"),
      Seq(
        Row(1, "A"),
        Row(2, "B"),
        Row(3, "C"),
        Row(4, "D")))
  }

  test("system function upper()") {//系统函数upper
    /**
      sql("SELECT * FROM lowerCaseData").show()
      +---+---+
      |  n|  l|
      +---+---+
      |  1|  a|
      |  2|  b|
      |  3|  c|
      |  4|  d|
      +---+---+
     */   
    checkAnswer(//转换大写,UPPER列名
      sql("SELECT n,UPPER(l) FROM lowerCaseData"),
      Seq(
        Row(1, "A"),
        Row(2, "B"),
        Row(3, "C"),
        Row(4, "D")))

    checkAnswer(
      sql("SELECT n, UPPER(s) FROM nullStrings"),
      Seq(
        Row(1, "ABC"),
        Row(2, "ABC"),
        Row(3, null)))
  }

  test("system function lower()") {//系统函数lower
    checkAnswer(
      sql("SELECT N,LOWER(L) FROM upperCaseData"),
      Seq(
        Row(1, "a"),
        Row(2, "b"),
        Row(3, "c"),
        Row(4, "d"),
        Row(5, "e"),
        Row(6, "f")))

    checkAnswer(
      sql("SELECT n, LOWER(s) FROM nullStrings"),
      Seq(
        Row(1, "abc"),
        Row(2, "abc"),
        Row(3, null)))
  }

  test("UNION") {//联合
    checkAnswer(
      sql("SELECT * FROM lowerCaseData UNION SELECT * FROM upperCaseData"),
      Row(1, "A") :: Row(1, "a") :: Row(2, "B") :: Row(2, "b") :: Row(3, "C") :: Row(3, "c") ::
      Row(4, "D") :: Row(4, "d") :: Row(5, "E") :: Row(6, "F") :: Nil)
    checkAnswer(
      sql("SELECT * FROM lowerCaseData UNION SELECT * FROM lowerCaseData"),
      Row(1, "a") :: Row(2, "b") :: Row(3, "c") :: Row(4, "d") :: Nil)
    checkAnswer(
      sql("SELECT * FROM lowerCaseData UNION ALL SELECT * FROM lowerCaseData"),
      Row(1, "a") :: Row(1, "a") :: Row(2, "b") :: Row(2, "b") :: Row(3, "c") :: Row(3, "c") ::
      Row(4, "d") :: Row(4, "d") :: Nil)
  }

  test("UNION with column mismatches") {//与列不匹配的联合
    // Column name mismatches are allowed.
    //允许列名称不匹配
    checkAnswer(
      sql("SELECT n,l FROM lowerCaseData UNION SELECT N as x1, L as x2 FROM upperCaseData"),
      Row(1, "A") :: Row(1, "a") :: Row(2, "B") :: Row(2, "b") :: Row(3, "C") :: Row(3, "c") ::
      Row(4, "D") :: Row(4, "d") :: Row(5, "E") :: Row(6, "F") :: Nil)
    // Column type mismatches are not allowed, forcing a type coercion.
    //不允许列类型不匹配,强制类型强制
    checkAnswer(
      sql("SELECT n FROM lowerCaseData UNION SELECT L FROM upperCaseData"),
      ("1" :: "2" :: "3" :: "4" :: "A" :: "B" :: "C" :: "D" :: "E" :: "F" :: Nil).map(Row(_)))
    // Column type mismatches where a coercion is not possible, in this case between integer
    // and array types, trigger a TreeNodeException.
    //列类型不匹配的强制是不可能,在整数数组类型之间的这种情况下,引发treenodeexception。
    intercept[AnalysisException] {
      sql("SELECT data FROM arrayData UNION SELECT 1 FROM arrayData").collect()
    }
  }

  test("EXCEPT") {//EXCEPT 返回两个结果集的差(即从左查询中返回右查询没有找到的所有非重复值)
    /**
     *+---+---+
      |  n|  l|
      +---+---+
      |  1|  a|
      |  2|  b|
      |  3|  c|
      |  4|  d|
      +---+---+
     */
    //sql("SELECT a.* FROM lowerCaseData a").show()
    /**
     *+---+---+
      |  N|  L|
      +---+---+
      |  1|  A|
      |  2|  B|
      |  3|  C|
      |  4|  D|
      |  5|  E|
      |  6|  F|
      +---+---+
     */
    //sql("SELECT b.* FROM upperCaseData b").show()
    //EXCEPT 返回两个结果集的差
    checkAnswer(
      sql("SELECT * FROM lowerCaseData EXCEPT SELECT * FROM upperCaseData"),
      Row(1, "a") ::
      Row(2, "b") ::
      Row(3, "c") ::
      Row(4, "d") :: Nil)
    checkAnswer(
      sql("SELECT * FROM lowerCaseData EXCEPT SELECT * FROM lowerCaseData"), Nil)
    checkAnswer(
      sql("SELECT * FROM upperCaseData EXCEPT SELECT * FROM upperCaseData"), Nil)
  }

  test("INTERSECT") {//相交
    
    checkAnswer(
      sql("SELECT * FROM lowerCaseData INTERSECT SELECT * FROM lowerCaseData"),
      Row(1, "a") ::
      Row(2, "b") ::
      Row(3, "c") ::
      Row(4, "d") :: Nil)
    checkAnswer(
      sql("SELECT * FROM lowerCaseData INTERSECT SELECT * FROM upperCaseData"), Nil)
  }

  test("SET commands semantics using sql()") {//使用SQL命令集语义
    sqlContext.conf.clear()
    val testKey = "test.key.0"
    val testVal = "test.val.0"
    val nonexistentKey = "nonexistent"

    // "set" itself returns all config variables currently specified in SQLConf.
    assert(sql("SET").collect().size == 0)

    // "set key=val"
    sql(s"SET $testKey=$testVal")
    checkAnswer(
      sql("SET"),
      Row(testKey, testVal)
    )

    sql(s"SET ${testKey + testKey}=${testVal + testVal}")
    checkAnswer(
      sql("set"),
      Seq(
        Row(testKey, testVal),
        Row(testKey + testKey, testVal + testVal))
    )

    // "set key"
    checkAnswer(
      sql(s"SET $testKey"),
      Row(testKey, testVal)
    )
    checkAnswer(
      sql(s"SET $nonexistentKey"),
      Row(nonexistentKey, "<undefined>")
    )
    sqlContext.conf.clear()
  }

  test("SET commands with illegal or inappropriate argument") {//用非法或不适当的参数设置命令
    sqlContext.conf.clear()
    // Set negative mapred.reduce.tasks for automatically determing
    // the number of reducers is not supported
    intercept[IllegalArgumentException](sql(s"SET mapred.reduce.tasks=-1"))
    intercept[IllegalArgumentException](sql(s"SET mapred.reduce.tasks=-01"))
    intercept[IllegalArgumentException](sql(s"SET mapred.reduce.tasks=-2"))
    sqlContext.conf.clear()
  }

  test("apply schema") {//应用模式
  //StructType代表一张表,StructField代表一个字段
    val schema1 = StructType(
      StructField("f1", IntegerType, false) ::
      StructField("f2", StringType, false) ::
      StructField("f3", BooleanType, false) ::
      StructField("f4", IntegerType, true) :: Nil)

    val rowRDD1 = unparsedStrings.map { r =>
      val values = r.split(",").map(_.trim)
      val v4 = try values(3).toInt catch {
        case _: NumberFormatException => null
      }
      Row(values(0).toInt, values(1), values(2).toBoolean, v4)
    }

    val df1 = sqlContext.createDataFrame(rowRDD1, schema1)
    df1.registerTempTable("applySchema1")
    checkAnswer(
      sql("SELECT * FROM applySchema1"),
      Row(1, "A1", true, null) ::
      Row(2, "B2", false, null) ::
      Row(3, "C3", true, null) ::
      Row(4, "D4", true, 2147483644) :: Nil)

    checkAnswer(
      sql("SELECT f1, f4 FROM applySchema1"),
      Row(1, null) ::
      Row(2, null) ::
      Row(3, null) ::
      Row(4, 2147483644) :: Nil)
    //StructType代表一张表,StructField代表一个字段
    val schema2 = StructType(
      StructField("f1", StructType(
        StructField("f11", IntegerType, false) ::
        StructField("f12", BooleanType, false) :: Nil), false) ::
      StructField("f2", MapType(StringType, IntegerType, true), false) :: Nil)

    val rowRDD2 = unparsedStrings.map { r =>
      val values = r.split(",").map(_.trim)
      val v4 = try values(3).toInt catch {
        case _: NumberFormatException => null
      }
      Row(Row(values(0).toInt, values(2).toBoolean), Map(values(1) -> v4))
    }

    val df2 = sqlContext.createDataFrame(rowRDD2, schema2)
    df2.registerTempTable("applySchema2")
    /**
      +---------+--------------------+
      |       f1|                  f2|
      +---------+--------------------+
      | [1,true]|     Map(A1 -> null)|
      |[2,false]|     Map(B2 -> null)|
      | [3,true]|     Map(C3 -> null)|
      | [4,true]|Map(D4 -> 2147483...|
      +---------+--------------------+*/
    //df2.show()
    checkAnswer(
      sql("SELECT * FROM applySchema2"),
      Row(Row(1, true), Map("A1" -> null)) ::
      Row(Row(2, false), Map("B2" -> null)) ::
      Row(Row(3, true), Map("C3" -> null)) ::
      Row(Row(4, true), Map("D4" -> 2147483644)) :: Nil)

    checkAnswer(
      sql("SELECT f1.f11, f2['D4'] FROM applySchema2"),
      Row(1, null) ::
      Row(2, null) ::
      Row(3, null) ::
      Row(4, 2147483644) :: Nil)

    // The value of a MapType column can be a mutable map.
    //一个map类型列中的值可以是一个可变的Map
    val rowRDD3 = unparsedStrings.map { r =>
      val values = r.split(",").map(_.trim)
      val v4 = try values(3).toInt catch {
        case _: NumberFormatException => null
      }
      Row(Row(values(0).toInt, values(2).toBoolean), scala.collection.mutable.Map(values(1) -> v4))
    }

    val df3 = sqlContext.createDataFrame(rowRDD3, schema2)
    df3.registerTempTable("applySchema3")

    checkAnswer(
      sql("SELECT f1.f11, f2['D4'] FROM applySchema3"),
      Row(1, null) ::
      Row(2, null) ::
      Row(3, null) ::
      Row(4, 2147483644) :: Nil)
  }

  test("SPARK-3423 BETWEEN") {//在…之间
    checkAnswer(
      sql("SELECT key, value FROM testData WHERE key BETWEEN 5 and 7"),
      Seq(Row(5, "5"), Row(6, "6"), Row(7, "7"))
    )

    checkAnswer(
      sql("SELECT key, value FROM testData WHERE key BETWEEN 7 and 7"),
      Row(7, "7")
    )

    checkAnswer(
      sql("SELECT key, value FROM testData WHERE key BETWEEN 9 and 7"),
      Nil
    )
  }

  test("cast boolean to string") {//强制布尔值转换字符串
    // TODO Ensure true/false string letter casing is consistent with Hive in all cases.
    checkAnswer(
        //强制转换布字符串类型,
      sql("SELECT CAST(TRUE AS STRING), CAST(FALSE AS STRING) FROM testData LIMIT 1"),
      Row("true", "false"))
  }

  test("metadata is propagated correctly") {//元数据正确传递
    sql("SELECT * FROM person").show()
    /**
     *+---+----+---+
      | id|name|age|
      +---+----+---+
      |  0|mike| 30|
      |  1| jim| 20|
      +---+----+---+*/
    val person: DataFrame = sql("SELECT * FROM person")
    /**
     * StructType(
     * 		StructField(id,IntegerType,false), 
     * 		StructField(name,StringType,true), 
     * 		StructField(age,IntegerType,false))
     */
    val schema = person.schema
    //println(schema)
    val docKey = "doc"
    val docValue = "first name"
    val metadata = new MetadataBuilder()
      .putString(docKey, docValue)
      .build()
    val schemaWithMeta = new StructType(Array(
      schema("id"), schema("name").copy(metadata = metadata), schema("age")))
    val personWithMeta = sqlContext.createDataFrame(person.rdd, schemaWithMeta)
    def validateMetadata(rdd: DataFrame): Unit = {
      assert(rdd.schema("name").metadata.getString(docKey) == docValue)
    }
    personWithMeta.registerTempTable("personWithMeta")
    validateMetadata(personWithMeta.select($"name"))
    validateMetadata(personWithMeta.select($"name"))
    validateMetadata(personWithMeta.select($"id", $"name"))
    validateMetadata(sql("SELECT * FROM personWithMeta"))
    validateMetadata(sql("SELECT id, name FROM personWithMeta"))
    validateMetadata(sql("SELECT * FROM personWithMeta JOIN salary ON id = personId"))
    validateMetadata(sql(
      "SELECT name, salary FROM personWithMeta JOIN salary ON id = personId"))
  }
  //重命名功能表达与组给出错误
  test("SPARK-3371 Renaming a function expression with group by gives error") {
    //重命名
    sqlContext.udf.register("len", (s: String) => s.length)
    checkAnswer(
      sql("SELECT len(value) as temp FROM testData WHERE key = 1 group by len(value)"),
      Row(1))
  }
  //case when语句,用于计算条件列表并返回多个可能结果表达式之
  test("SPARK-3813 CASE a WHEN b THEN c [WHEN d THEN e]* [ELSE f] END") {
    checkAnswer(
      sql("SELECT CASE key WHEN 1 THEN 1 ELSE 0 END FROM testData WHERE key = 1 group by key"),
      Row(1))
  }

  test("SPARK-3813 CASE WHEN a THEN b [WHEN c THEN d]* [ELSE e] END") {
    checkAnswer(
      sql("SELECT CASE WHEN key = 1 THEN 1 ELSE 2 END FROM testData WHERE key = 1 group by key"),
      Row(1))
  }
  //聚合属性的非聚合属性的抛出错误
  test("throw errors for non-aggregate attributes with aggregation") {
    def checkAggregation(query: String, isInvalidQuery: Boolean = true) {
      if (isInvalidQuery) {
        val e = intercept[AnalysisException](sql(query).queryExecution.analyzed)
        assert(e.getMessage contains "group by")
      } else {
        // Should not throw
        //不应该抛出异常
        sql(query).queryExecution.analyzed
      }
    }

    checkAggregation("SELECT key, COUNT(*) FROM testData")
    checkAggregation("SELECT COUNT(key), COUNT(*) FROM testData", isInvalidQuery = false)

    checkAggregation("SELECT value, COUNT(*) FROM testData GROUP BY key")
    checkAggregation("SELECT COUNT(value), SUM(key) FROM testData GROUP BY key", false)

    checkAggregation("SELECT key + 2, COUNT(*) FROM testData GROUP BY key + 1")
    checkAggregation("SELECT key + 1 + 1, COUNT(*) FROM testData GROUP BY key + 1", false)
  }
  //测试可以使用Long.MinValue最大值
  test("Test to check we can use Long.MinValue") {
    checkAnswer(
      sql(s"SELECT ${Long.MinValue} FROM testData ORDER BY key LIMIT 1"), Row(Long.MinValue)
    )

    checkAnswer(
      sql(s"SELECT key FROM testData WHERE key > ${Long.MinValue}"),
      (1 to 100).map(Row(_)).toSeq
    )
  }
  //浮点数格式
  test("Floating point number format") {
    checkAnswer(
      sql("SELECT 0.3"), Row(BigDecimal(0.3).underlying())
    )

    checkAnswer(//单独使用SELECT语句
      sql("SELECT -0.8"), Row(BigDecimal(-0.8).underlying())
    )

    checkAnswer(
      sql("SELECT .5"), Row(BigDecimal(0.5))
    )

    checkAnswer(
      sql("SELECT -.18"), Row(BigDecimal(-0.18))
    )
  }
  //自动整数类型
  test("Auto cast integer type") {
    checkAnswer(
      sql(s"SELECT ${Int.MaxValue + 1L}"), Row(Int.MaxValue + 1L)
    )

    checkAnswer(
      sql(s"SELECT ${Int.MinValue - 1L}"), Row(Int.MinValue - 1L)
    )

    checkAnswer(
      sql("SELECT 9223372036854775808"), Row(new java.math.BigDecimal("9223372036854775808"))
    )

    checkAnswer(
      sql("SELECT -9223372036854775809"), Row(new java.math.BigDecimal("-9223372036854775809"))
    )
  }
  //测试检查我们可以应用符号表达式
  test("Test to check we can apply sign to expression") {

    checkAnswer(//测试使用负数
      sql("SELECT -100"), Row(-100)
    )

    checkAnswer(//测试使用正数
      sql("SELECT +230"), Row(230)
    )

    checkAnswer(
      sql("SELECT -5.2"), Row(BigDecimal(-5.2))
    )

    checkAnswer(
      sql("SELECT +6.8"), Row(BigDecimal(6.8))
    )

    checkAnswer(
      sql("SELECT -key FROM testData WHERE key = 2"), Row(-2)
    )

    checkAnswer(
      sql("SELECT +key FROM testData WHERE key = 3"), Row(3)
    )

    checkAnswer(
      sql("SELECT -(key + 1) FROM testData WHERE key = 1"), Row(-2)
    )

    checkAnswer(
      sql("SELECT - key + 1 FROM testData WHERE key = 10"), Row(-9)
    )

    checkAnswer(
      sql("SELECT +(key + 5) FROM testData WHERE key = 5"), Row(10)
    )

    checkAnswer(
      sql("SELECT -MAX(key) FROM testData"), Row(-100)
    )

    checkAnswer(
      sql("SELECT +MAX(key) FROM testData"), Row(100)
    )

    checkAnswer(
      sql("SELECT - (-10)"), Row(10)
    )

    checkAnswer(
      sql("SELECT + (-key) FROM testData WHERE key = 32"), Row(-32)
    )

    checkAnswer(
      sql("SELECT - (+Max(key)) FROM testData"), Row(-100)
    )

    checkAnswer(
      sql("SELECT - - 3"), Row(3)
    )

    checkAnswer(
      sql("SELECT - + 20"), Row(-20)
    )

    checkAnswer(
      sql("SELEcT - + 45"), Row(-45)
    )

    checkAnswer(
      sql("SELECT + + 100"), Row(100)
    )

    checkAnswer(
      sql("SELECT - - Max(key) FROM testData"), Row(100)
    )

    checkAnswer(
      sql("SELECT + - key FROM testData WHERE key = 33"), Row(-33)
    )
  }
  //多个连接
  test("Multiple join") {
    checkAnswer(
      sql(
        """SELECT a.key, b.key, c.key
          |FROM testData a
          |JOIN testData b ON a.key = b.key
          |JOIN testData c ON a.key = c.key
        """.stripMargin),
      (1 to 100).map(i => Row(i, i, i)))
  }
  //在列名上特殊字符
  test("SPARK-3483 Special chars in column names") {
    val data = sqlContext.sparkContext.parallelize(
      Seq("""{"key?number1": "value1", "key.number2": "value2"}"""))
      //使用JSON读取RDD[String],使用单引号名特殊字符
    sqlContext.read.json(data).registerTempTable("records")
    sql("SELECT `key?number1`, `key.number2` FROM records")
  }
  //支持按位与运算符
  test("SPARK-3814 Support Bitwise & operator") {
    checkAnswer(sql("SELECT key&1 FROM testData WHERE key = 1 "), Row(1))
  }
  //支持按位或运算符
  test("SPARK-3814 Support Bitwise | operator") {
    checkAnswer(sql("SELECT key|0 FROM testData WHERE key = 1 "), Row(1))
  }
  //
  test("SPARK-3814 Support Bitwise ^ operator") {
    checkAnswer(sql("SELECT key^0 FROM testData WHERE key = 1 "), Row(1))
  }

  test("SPARK-3814 Support Bitwise ~ operator") {
    checkAnswer(sql("SELECT ~key FROM testData WHERE key = 1 "), Row(-2))
  }
  //多表连接不工作sparksql
  test("SPARK-4120 Join of multiple tables does not work in SparkSQL") {
    checkAnswer(
      sql(
        """SELECT a.key, b.key, c.key
          |FROM testData a,testData b,testData c
          |where a.key = b.key and a.key = c.key
        """.stripMargin),
      (1 to 100).map(i => Row(i, i, i)))
  }
  //查询是否已经不在work,如果它不是在SQL和HQL之间的Spark
  test("SPARK-4154 Query does not work if it has 'not between' in Spark SQL and HQL") {
    checkAnswer(sql("SELECT key FROM testData WHERE key not between 0 and 10 order by key"),
        (11 to 100).map(i => Row(i)))
  }

  test("SPARK-4207 Query which has syntax like 'not like' is not working in Spark SQL") {
    checkAnswer(sql("SELECT key FROM testData WHERE value not like '100%' order by key"),
        (1 to 99).map(i => Row(i)))
  }
  //随着结构字段作为分组字段的子表达式
  test("SPARK-4322 Grouping field with struct field as sub expression") {
    sqlContext.read.json(sqlContext.sparkContext.makeRDD("""{"a": {"b": [{"c": 1}]}}""" :: Nil))
      .registerTempTable("data")
    checkAnswer(sql("SELECT a.b[0].c FROM data GROUP BY a.b[0].c"), Row(1))
    sqlContext.dropTempTable("data")

    sqlContext.read.json(
      sqlContext.sparkContext.makeRDD("""{"a": {"b": 1}}""" :: Nil)).registerTempTable("data")
    checkAnswer(sql("SELECT a.b + 1 FROM data GROUP BY a.b + 1"), Row(2))
    sqlContext.dropTempTable("data")
  }
  //固定属性引用解析错误时使用顺序
  test("SPARK-4432 Fix attribute reference resolution error when using ORDER BY") {
    checkAnswer(
      sql("SELECT a + b FROM testData2 ORDER BY a"),
      Seq(2, 3, 3, 4, 4, 5).map(Row(_))
    )
  }
  //排序ASC默认时,不指定升序和降序
  test("oder by asc by default when not specify ascending and descending") {
    checkAnswer(
      sql("SELECT a, b FROM testData2 ORDER BY a desc, b"),
      Seq(Row(3, 1), Row(3, 2), Row(2, 1), Row(2, 2), Row(1, 1), Row(1, 2))
    )
  }

  test("Supporting relational operator '<=>' in Spark SQL") {//支持关系运算符
    val nullCheckData1 = TestData(1, "1") :: TestData(2, null) :: Nil
    val rdd1 = sqlContext.sparkContext.parallelize((0 to 1).map(i => nullCheckData1(i)))
    rdd1.toDF().registerTempTable("nulldata1")
    val nullCheckData2 = TestData(1, "1") :: TestData(2, null) :: Nil
    val rdd2 = sqlContext.sparkContext.parallelize((0 to 1).map(i => nullCheckData2(i)))
    rdd2.toDF().registerTempTable("nulldata2")
    checkAnswer(sql("SELECT nulldata1.key FROM nulldata1 join " +
      "nulldata2 on nulldata1.value <=> nulldata2.value"),
        (1 to 2).map(i => Row(i)))
  }

  test("Multi-column COUNT(DISTINCT ...)") {//多列计数
    //数组对象
    val data = TestData(1, "val_1") :: TestData(2, "val_2") :: Nil
    //数组对象转换成RDD对象
    val rdd = sqlContext.sparkContext.parallelize((0 to 1).map(i => data(i)))
    rdd.toDF().registerTempTable("distinctData")
    
    checkAnswer(sql("SELECT COUNT(DISTINCT key,value) FROM distinctData"), Row(2))
  }

  test("SPARK-4699 case sensitivity SQL query") {//敏感的SQL查询
    //设置大小写敏感为false
    sqlContext.setConf(SQLConf.CASE_SENSITIVE, false)
    val data = TestData(1, "val_1") :: TestData(2, "val_2") :: Nil
    val rdd = sqlContext.sparkContext.parallelize((0 to 1).map(i => data(i)))
    rdd.toDF().registerTempTable("testTable1")
    checkAnswer(sql("SELECT VALUE FROM TESTTABLE1 where KEY = 1"), Row("val_1"))
    //设置大小写敏感为true
    sqlContext.setConf(SQLConf.CASE_SENSITIVE, true)
  }

  test("SPARK-6145: ORDER BY test for nested fields") {//测试嵌套字段排序
    sqlContext.read.json(sqlContext.sparkContext.makeRDD(
        """{"a": {"b": 1, "a": {"a": 1}}, "c": [{"d": 1}]}""" :: Nil))
      .registerTempTable("nestedOrder")

    checkAnswer(sql("SELECT 1 FROM nestedOrder ORDER BY a.b"), Row(1))
    checkAnswer(sql("SELECT a.b FROM nestedOrder ORDER BY a.b"), Row(1))
    checkAnswer(sql("SELECT 1 FROM nestedOrder ORDER BY a.a.a"), Row(1))
    checkAnswer(sql("SELECT a.a.a FROM nestedOrder ORDER BY a.a.a"), Row(1))
    checkAnswer(sql("SELECT 1 FROM nestedOrder ORDER BY c[0].d"), Row(1))
    checkAnswer(sql("SELECT c[0].d FROM nestedOrder ORDER BY c[0].d"), Row(1))
  }

  test("SPARK-6145: special cases") {//特殊情况
    //makeRDD直接把字符串转换RDD
    sqlContext.read.json(sqlContext.sparkContext.makeRDD(
      """{"a": {"b": [1]}, "b": [{"a": 1}], "_c0": {"a": 1}}""" :: Nil)).registerTempTable("t")
    checkAnswer(sql("SELECT a.b[0] FROM t ORDER BY _c0.a"), Row(1))
    checkAnswer(sql("SELECT b[0].a FROM t ORDER BY _c0.a"), Row(1))
  }
  //在列名称的特殊字符的完整支持
  test("SPARK-6898: complete support for special chars in column names") {
    sqlContext.read.json(sqlContext.sparkContext.makeRDD(
      """{"a": {"c.b": 1}, "b.$q": [{"a@!.q": 1}], "q.w": {"w.i&": [1]}}""" :: Nil))
      .registerTempTable("t")

    checkAnswer(sql("SELECT a.`c.b`, `b.$q`[0].`a@!.q`, `q.w`.`w.i&`[0] FROM t"), Row(1, 1, 1))
  }
  //排序的聚合函数
  test("SPARK-6583 order by aggregated function") {
    //序列Map转换DataFrame
    Seq("1" -> 3, "1" -> 4, "2" -> 7, "2" -> 8, "3" -> 5, "3" -> 6, "4" -> 1, "4" -> 2)
    //DF(DataFrame)缩写,数据库表类似
      .toDF("a", "b").registerTempTable("orderByData")
      /**
       *+---+---+
        |  a|  b|
        +---+---+
        |  1|  3|
        |  1|  4|
        |  2|  7|
        |  2|  8|
        |  3|  5|
        |  3|  6|
        |  4|  1|
        |  4|  2|
        +---+---+*/
       sql(
        """
          |SELECT a,sum(b)
          |FROM orderByData
          |GROUP BY a
          |ORDER BY sum(b)
        """.stripMargin).show()
    checkAnswer(
       //以a分组,合计b进行排序
        /**
         *+---+---+
          |  a|_c1|
          +---+---+
          |  4|  3|
          |  1|  7|
          |  3| 11|
          |  2| 15|
          +---+---+*/
      sql(
        """
          |SELECT a
          |FROM orderByData
          |GROUP BY a
          |ORDER BY sum(b)
        """.stripMargin),
       
      Row("4") :: Row("1") :: Row("3") :: Row("2") :: Nil)
         
    checkAnswer(
      sql(
        """
          |SELECT sum(b)
          |FROM orderByData
          |GROUP BY a
          |ORDER BY sum(b)
        """.stripMargin),
      Row(3) :: Row(7) :: Row(11) :: Row(15) :: Nil)

    checkAnswer(
        //按合计的升序排序
        /**
         *+---+---+
          |  a|_c1|
          +---+---+
          |  4|  3|
          |  1|  7|
          |  3| 11|
          |  2| 15|
          +---+---+*/
      sql(
        """
          |SELECT a, sum(b)
          |FROM orderByData
          |GROUP BY a
          |ORDER BY sum(b)
        """.stripMargin),
      Row("4", 3) :: Row("1", 7) :: Row("3", 11) :: Row("2", 15) :: Nil)

    checkAnswer(
      sql(
        """
            |SELECT a, sum(b)
            |FROM orderByData
            |GROUP BY a
            |ORDER BY sum(b) + 1
          """.stripMargin),
      Row("4", 3) :: Row("1", 7) :: Row("3", 11) :: Row("2", 15) :: Nil)
  }
  //检查固定相等布尔值和数字类型之间
  test("SPARK-7952: fix the equality check between boolean and numeric types") {
    //调用临时表后删除
    withTempTable("t") {
      // numeric field i, boolean field j, result of i = j, result of i <=> j
      Seq[(Integer, java.lang.Boolean, java.lang.Boolean, java.lang.Boolean)](
        (1, true, true, true),
        (0, false, true, true),
        (2, true, false, false),
        (2, false, false, false),
        (null, true, null, false),
        (null, false, null, false),
        (0, null, null, false),
        (1, null, null, false),
        (null, null, null, true)
      ).toDF("i", "b", "r1", "r2").registerTempTable("t")

      checkAnswer(sql("select i = b from t"), sql("select r1 from t"))
      checkAnswer(sql("select i <=> b from t"), sql("select r2 from t"))
    }
  }
  //排序通过对复杂提取链
  test("SPARK-7067: order by queries for complex ExtractValue chain") {
    //调用临时表后删除
    withTempTable("t") {
      //json字符串数据读入
      sqlContext.read.json(sqlContext.sparkContext.makeRDD(
        """{"a": {"b": [{"c": 1}]}, "b": [{"d": 1}]}""" :: Nil)).registerTempTable("t")
      checkAnswer(sql("SELECT a.b FROM t ORDER BY b[0].d"), Row(Seq(Row(1))))
    }
  }

  test("SPARK-8782: ORDER BY NULL") {//空值的排序
    withTempTable("t") {
      Seq((1, 2), (1, 2)).toDF("a", "b").registerTempTable("t")
      checkAnswer(sql("SELECT * FROM t ORDER BY NULL"), Seq(Row(1, 2), Row(1, 2)))
    }
  }

  test("SPARK-8837: use keyword in column name") {//在列名称中使用关键字
    withTempTable("t") {
      val df = Seq(1 -> "a").toDF("count", "sort")
      checkAnswer(df.filter("count > 0"), Row(1, "a"))
      df.registerTempTable("t")
      checkAnswer(sql("select count, sort from t"), Row(1, "a"))
    }
  }

  test("SPARK-8753: add interval type") {//添加间隔类型
    import org.apache.spark.unsafe.types.CalendarInterval

    val df = sql("select interval 3 years -3 month 7 week 123 microseconds")
    df.show()
    checkAnswer(df, Row(new CalendarInterval(12 * 3 - 3, 7L * 1000 * 1000 * 3600 * 24 * 7 + 123 )))
    withTempPath(f => {
      // Currently we don't yet support saving out values of interval data type.
      //目前,我们还不支持保存区间数据类型的值
      val e = intercept[AnalysisException] {
        df.write.json(f.getCanonicalPath)
      }
      e.message.contains("Cannot save interval data type into external storage")
    })

    def checkIntervalParseError(s: String): Unit = {
      val e = intercept[AnalysisException] {
        sql(s)
      }
      e.message.contains("at least one time unit should be given for interval literal")
    }

    checkIntervalParseError("select interval")
    // Currently we don't yet support nanosecond
    //目前,我们还没有支持纳秒
    checkIntervalParseError("select interval 23 nanosecond")
  }
  //加法和减法表达式区间型
  test("SPARK-8945: add and subtract expressions for interval type") {
    import org.apache.spark.unsafe.types.CalendarInterval
    import org.apache.spark.unsafe.types.CalendarInterval.MICROS_PER_WEEK

    val df = sql("select interval 3 years -3 month 7 week 123 microseconds as i")
    checkAnswer(df, Row(new CalendarInterval(12 * 3 - 3, 7L * MICROS_PER_WEEK + 123)))

    checkAnswer(df.select(df("i") + new CalendarInterval(2, 123)),
      Row(new CalendarInterval(12 * 3 - 3 + 2, 7L * MICROS_PER_WEEK + 123 + 123)))

    checkAnswer(df.select(df("i") - new CalendarInterval(2, 123)),
      Row(new CalendarInterval(12 * 3 - 3 - 2, 7L * MICROS_PER_WEEK + 123 - 123)))

    // unary minus
    checkAnswer(df.select(-df("i")),
      Row(new CalendarInterval(-(12 * 3 - 3), -(7L * MICROS_PER_WEEK + 123))))
  }
    //用代码生成更新执行内存聚合
  test("aggregation with codegen updates peak execution memory") {
    withSQLConf((SQLConf.CODEGEN_ENABLED.key, "true")) {
      val sc = sqlContext.sparkContext
      AccumulatorSuite.verifyPeakExecutionMemorySet(sc, "aggregation with codegen") {
        testCodeGen(
          "SELECT key, count(value) FROM testData GROUP BY key",
          (1 to 100).map(i => Row(i, 1)))
      }
    }
  }
  //十进制小数乘法/除法.
  test("decimal precision with multiply/division") {
    //两个数相乘
    checkAnswer(sql("select 10.3 * 3.0"), Row(BigDecimal("30.90")))
    //两个数相乘
    checkAnswer(sql("select 10.3000 * 3.0"), Row(BigDecimal("30.90000")))
    checkAnswer(sql("select 10.30000 * 30.0"), Row(BigDecimal("309.000000")))
    checkAnswer(sql("select 10.300000000000000000 * 3.000000000000000000"),
      Row(BigDecimal("30.900000000000000000000000000000000000", new MathContext(38))))
    checkAnswer(sql("select 10.300000000000000000 * 3.0000000000000000000"),
      Row(null))
     //两个数相除
    checkAnswer(sql("select 10.3 / 3.0"), Row(BigDecimal("3.433333")))
    checkAnswer(sql("select 10.3000 / 3.0"), Row(BigDecimal("3.4333333")))
    checkAnswer(sql("select 10.30000 / 30.0"), Row(BigDecimal("0.343333333")))
    checkAnswer(sql("select 10.300000000000000000 / 3.00000000000000000"),
      Row(BigDecimal("3.433333333333333333333333333", new MathContext(38))))
    checkAnswer(sql("select 10.3000000000000000000 / 3.00000000000000000"),
      Row(BigDecimal("3.4333333333333333333333333333", new MathContext(38))))
  }
  //相除的数返回
  test("SPARK-10215 Div of Decimal returns null") {
    val d = Decimal(1.12321)
    val df = Seq((d, 1)).toDF("a", "b")
    checkAnswer(
      df.selectExpr("b * a / b"),
      Seq(Row(d.toBigDecimal)))
    checkAnswer(
      df.selectExpr("b * a / b / b"),
      Seq(Row(d.toBigDecimal)))
    checkAnswer(
        //数字表达式
      df.selectExpr("b * a + b"),
      Seq(Row(BigDecimal(2.12321))))
    checkAnswer(
      df.selectExpr("b * a - b"),
      Seq(Row(BigDecimal(0.12321))))
    checkAnswer(
      df.selectExpr("b * a * b"),
      Seq(Row(d.toBigDecimal)))
  }
  //大规模高精度
  test("precision smaller than scale") {
    checkAnswer(sql("select 10.00"), Row(BigDecimal("10.00")))
    checkAnswer(sql("select 1.00"), Row(BigDecimal("1.00")))
    checkAnswer(sql("select 0.10"), Row(BigDecimal("0.10")))
    checkAnswer(sql("select 0.01"), Row(BigDecimal("0.01")))
    checkAnswer(sql("select 0.001"), Row(BigDecimal("0.001")))
    checkAnswer(sql("select -0.01"), Row(BigDecimal("-0.01")))
    checkAnswer(sql("select -0.001"), Row(BigDecimal("-0.001")))
  } 
  //外部排序更新执行内存值
  test("external sorting updates peak execution memory") {
    withSQLConf((SQLConf.EXTERNAL_SORT.key, "true")) {
      val sc = sqlContext.sparkContext
      AccumulatorSuite.verifyPeakExecutionMemorySet(sc, "external sort") {
        sortTest()
      }
    }
  }
  //从数字表开始的错误
  test("SPARK-9511: error with table starting with number") {
    withTempTable("1one") {
      sqlContext.sparkContext.parallelize(1 to 10).map(i => (i, i.toString))
        .toDF("num", "str")
        .registerTempTable("1one")
      checkAnswer(sql("select count(num) from 1one"), Row(10))
    }
  }
 //不允许指定临时表的数据库名称
  test("specifying database name for a temporary table is not allowed") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      val df =
        sqlContext.sparkContext.parallelize(1 to 10).map(i => (i, i.toString)).toDF("num", "str")
      df
        .write
        //格式化parquet
        .format("parquet")
        //保存数据
        .save(path)

      val message = intercept[AnalysisException] {
        sqlContext.sql(
          s"""
          |CREATE TEMPORARY TABLE db.t
          |USING parquet
          |OPTIONS (
          |  path '$path'
          |)
        """.stripMargin)
      }.getMessage
      //指定数据库的名称或其他限定词是不允许的
      assert(message.contains("Specifying database name or other qualifiers are not allowed"))

      // If you use backticks to quote the name of a temporary table having dot in it.
      //如果你使用引号引用一个有点临时表的名称
      sqlContext.sql(
        //USING使用驱动类型
         //path 引用文件的路径
        s"""
          |CREATE TEMPORARY TABLE `db.t`
          |USING parquet
          |OPTIONS (
          |  path '$path'
          |)
        """.stripMargin)
        /**
         *+---+---+
          |num|str|
          +---+---+
          |  1|  1|
          |  2|  2|
          |  3|  3|
          |  4|  4|
          |  5|  5|
          |  6|  6|
          |  7|  7|
          |  8|  8|
          |  9|  9|
          | 10| 10|
          +---+---+*/
        //第一种查询方式        
        sqlContext.sql("select * from `db.t`").show()
        //第二种查询方式
        sqlContext.table("`db.t`").show()
      checkAnswer(sqlContext.table("`db.t`"), df)
    }
  }
  //类型强制转换为,如果应该子类先解决
  test("SPARK-10130 type coercion for IF should have children resolved first") {
    val df = Seq((1, 1), (-1, 1)).toDF("key", "value")
    df.registerTempTable("src")
    checkAnswer(
        //select 语句条件判断,子查询
      sql("SELECT IF(a > 0, a, 0) FROM (SELECT key a FROM src) temp"), Seq(Row(1), Row(0)))
  }
  //当使用unsaferows返回错误的结果
  test("SortMergeJoin returns wrong results when using UnsafeRows") {
    // This test is for the fix of https://issues.apache.org/jira/browse/SPARK-10737.
    // This bug will be triggered when Tungsten is enabled and there are multiple
    // SortMergeJoin operators executed in the same task.
    val confs =
      SQLConf.SORTMERGE_JOIN.key -> "true" ::
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "1" ::
        SQLConf.TUNGSTEN_ENABLED.key -> "true" :: Nil
    withSQLConf(confs: _*) {
      val df1 = (1 to 50).map(i => (s"str_$i", i)).toDF("i", "j")
      val df2 =
        df1
          .join(df1.select(df1("i")), "i")
          .select(df1("i"), df1("j"))

      val df3 = df2.withColumnRenamed("i", "i1").withColumnRenamed("j", "j1")
      val df4 =
        df2
          .join(df3, df2("i") === df3("i1"))
          .withColumn("diff", $"j" - $"j1")
          .select(df2("i"), df2("j"), $"diff")

      checkAnswer(
        df4,
        df1.withColumn("diff", lit(0)))
    }
  }
  //顺序非属性分组表达式的聚合
  test("SPARK-10389: order by non-attribute grouping expression on Aggregate") {
    withTempTable("src") {
      Seq((1, 1), (-1, 1)).toDF("key", "value").registerTempTable("src")
      checkAnswer(sql("SELECT MAX(value) FROM src GROUP BY key + 1 ORDER BY key + 1"),
        Seq(Row(1), Row(1)))
      checkAnswer(sql("SELECT MAX(value) FROM src GROUP BY key + 1 ORDER BY (key + 1) * 2"),
        Seq(Row(1), Row(1)))
    }
  }
  //过滤器不应被推送到样品
  test("SPARK-11303: filter should not be pushed down into sample") {
    val df = sqlContext.range(100)
    List(true, false).foreach { withReplacement =>
      val sampled = df.sample(withReplacement, 0.1, 1)
      val sampledOdd = sampled.filter("id % 2 != 0")
      val sampledEven = sampled.filter("id % 2 = 0")
      assert(sampled.count() == sampledOdd.count() + sampledEven.count())
    }
  }
  //解析正确的having
  test("SPARK-11032: resolve having correctly") {
    withTempTable("src") {
      Seq(1 -> "a").toDF("i", "j").registerTempTable("src")
       sql("SELECT MIN(t.i) FROM (SELECT * FROM src WHERE i > 0) t HAVING(COUNT(1) > 0)").show()
       
      checkAnswer(
        sql("SELECT MIN(t.i) FROM (SELECT * FROM src WHERE i > 0) t HAVING(COUNT(1) > 0)"),
        Row(1))
    }
  }
}
