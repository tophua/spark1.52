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

package org.apache.spark.sql.jdbc

import java.math.BigDecimal
import java.sql.DriverManager
import java.util.{Calendar, GregorianCalendar, Properties}

import org.h2.jdbc.JdbcSQLException
import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils
//JDBC测试套件
class JDBCSuite extends SparkFunSuite with BeforeAndAfter with SharedSQLContext {
  import testImplicits._

  val url = "jdbc:h2:mem:TEST0"
  //val url = "jdbc:h2:tcp://localhost/~/TEST0"
  ///  val url = "jdbc:testUserql://192.168.10.198:3306/testUser"
   val urlWithUserAndPass = "jdbc:h2:mem:TEST0;user=testUser;password=testPass"
  //val urlWithUserAndPass = "jdbc:h2:tcp://localhost/~/TEST0;user=testUser;password=testPass"
  // val urlWithUserAndPass = "jdbc:testUserql://192.168.10.198:3306/testUser?user=testUser&password=testPass"
  var conn: java.sql.Connection = null

  val testBytes = Array[Byte](99.toByte, 134.toByte, 135.toByte, 200.toByte, 205.toByte)

  val testH2Dialect = new JdbcDialect {
    override def canHandle(url: String) : Boolean = url.startsWith("jdbc:h2")
   // override def canHandle(url: String) : Boolean = url.startsWith("jdbc:testUserql")
    override def getCatalystType(
        sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] =
      Some(StringType)
  }

  before {
    Utils.classForName("org.h2.Driver")
     //Utils.classForName("org.testUserql.Driver")
    // Extra properties that will be specified for our database. We need these to test
    // usage of parameters from OPTIONS clause in queries.
    //将为我们的数据库指定额外的属性,我们需要这些来测试查询选项子句中的参数的用法
    val properties = new Properties()
    properties.setProperty("user", "testUser")
    properties.setProperty("password", "testPass")
    properties.setProperty("rowId", "false")
    //properties.setProperty("user", "testUser")
    //properties.setProperty("password", "testPass")

    conn = DriverManager.getConnection(url, properties)
    //级别删除       
    conn.prepareStatement("create schema IF NOT EXISTS test").executeUpdate()
    conn.prepareStatement("DROP TABLE  IF EXISTS TEST.PEOPLE").executeUpdate()
    conn.prepareStatement("DROP TABLE  IF EXISTS TEST.FLTTYPES").executeUpdate()
    conn.prepareStatement("DROP TABLE  IF EXISTS TEST.INTTYPES").executeUpdate()
    conn.prepareStatement("DROP TABLE  IF EXISTS TEST.NULLTYPES").executeUpdate()
    conn.prepareStatement("DROP TABLE  IF EXISTS TEST.PEOPLE").executeUpdate()
    conn.prepareStatement("DROP TABLE  IF EXISTS TEST.STRTYPES").executeUpdate()
    conn.prepareStatement("DROP TABLE  IF EXISTS TEST.TIMETYPES").executeUpdate()
    conn.prepareStatement(
      "create table test.people (name TEXT(32) NOT NULL, theid INTEGER NOT NULL)").executeUpdate()
    conn.prepareStatement("insert into test.people values ('fred', 1)").executeUpdate()
    conn.prepareStatement("insert into test.people values ('mary', 2)").executeUpdate()
    conn.prepareStatement(
      "insert into test.people values ('joe ''foo'' \"bar\"', 3)").executeUpdate()
    conn.commit()
 

    sql(
      //TEST.PEOPLE表名,对应的foobar临时表
      //注意使用jdbc方式
	    //dbtable需要读取的JDBC表,任何在From子句中的元素都可以,例如表或者子查询等
      s"""
        |CREATE TEMPORARY TABLE foobar
        |USING org.apache.spark.sql.jdbc
        |OPTIONS (url '$url', dbtable 'TEST.PEOPLE', user 'testUser', password 'testPass')
      """.stripMargin.replaceAll("\n", " "))
     //决定了每次数据取多少行 fetchSize 2参数
     //dbtable需要读取的JDBC表,任何在From子句中的元素都可以,例如表或者子查询等
    sql(
      s"""
        |CREATE TEMPORARY TABLE fetchtwo
        |USING org.apache.spark.sql.jdbc
        |OPTIONS (url '$url', dbtable 'TEST.PEOPLE', user 'testUser', password 'testPass',
        |         fetchSize '2')
      """.stripMargin.replaceAll("\n", " "))
    //dbtable需要读取的JDBC表,任何在From子句中的元素都可以,例如表或者子查询等
    sql(
      s"""
        |CREATE TEMPORARY TABLE parts
        |USING org.apache.spark.sql.jdbc
        |OPTIONS (url '$url', dbtable 'TEST.PEOPLE', user 'testUser', password 'testPass',
        |         partitionColumn 'THEID', lowerBound '1', upperBound '4', numPartitions '3')
      """.stripMargin.replaceAll("\n", " "))

    conn.prepareStatement("create table test.inttypes (a INT, b BOOLEAN, c TINYINT, "
      + "d SMALLINT, e BIGINT)").executeUpdate()
    conn.prepareStatement("insert into test.inttypes values (1, false, 3, 4, 1234567890123)"
        ).executeUpdate()
    //插入一条null数据
    conn.prepareStatement("insert into test.inttypes values (null, null, null, null, null)"
        ).executeUpdate()
    conn.commit()
    //dbtable需要读取的JDBC表。任何在From子句中的元素都可以,例如表或者子查询等
    sql(
      s"""
        |CREATE TEMPORARY TABLE inttypes
        |USING org.apache.spark.sql.jdbc
        |OPTIONS (url '$url', dbtable 'TEST.INTTYPES', user 'testUser', password 'testPass')
      """.stripMargin.replaceAll("\n", " "))

    conn.prepareStatement("create table test.strtypes (a BINARY(20), b VARCHAR(20), "
      + "c VARCHAR_IGNORECASE(20), d CHAR(20), e BLOB, f CLOB)").executeUpdate()
    val stmt = conn.prepareStatement("insert into test.strtypes values (?, ?, ?, ?, ?, ?)")
    stmt.setBytes(1, testBytes)
    stmt.setString(2, "Sensitive")
    stmt.setString(3, "Insensitive")
    stmt.setString(4, "Twenty-byte CHAR")//20字符
    stmt.setBytes(5, testBytes)
    stmt.setString(6, "I am a clob!")
    stmt.executeUpdate()
    //dbtable需要读取的JDBC表。任何在From子句中的元素都可以,例如表或者子查询等
    sql(
      s"""
        |CREATE TEMPORARY TABLE strtypes
        |USING org.apache.spark.sql.jdbc
        |OPTIONS (url '$url', dbtable 'TEST.STRTYPES', user 'testUser', password 'testPass')
      """.stripMargin.replaceAll("\n", " "))

    conn.prepareStatement("create table test.timetypes (a TIME, b DATE, c TIMESTAMP)"
        ).executeUpdate()
    conn.prepareStatement("insert into test.timetypes values ('12:34:56', "
      + "'1996-01-01', '2002-02-20 11:22:33.543543543')").executeUpdate()
    conn.prepareStatement("insert into test.timetypes values ('12:34:56', "
      + "null, '2002-02-20 11:22:33.543543543')").executeUpdate()
    conn.commit()
    //dbtable需要读取的JDBC表。任何在From子句中的元素都可以,例如表或者子查询等
    sql(
      s"""
        |CREATE TEMPORARY TABLE timetypes
        |USING org.apache.spark.sql.jdbc
        |OPTIONS (url '$url', dbtable 'TEST.TIMETYPES', user 'testUser', password 'testPass')
      """.stripMargin.replaceAll("\n", " "))


    conn.prepareStatement("create table test.flttypes (a DOUBLE, b REAL, c DECIMAL(38, 18))"
        ).executeUpdate()
    conn.prepareStatement("insert into test.flttypes values ("
      + "1.0000000000000002220446049250313080847263336181640625, "
      + "1.00000011920928955078125, "
      + "123456789012345.543215432154321)").executeUpdate()
    conn.commit()
    //dbtable需要读取的JDBC表。任何在From子句中的元素都可以,例如表或者子查询等
    sql(
      s"""
        |CREATE TEMPORARY TABLE flttypes
        |USING org.apache.spark.sql.jdbc
        |OPTIONS (url '$url', dbtable 'TEST.FLTTYPES', user 'testUser', password 'testPass')
      """.stripMargin.replaceAll("\n", " "))

    conn.prepareStatement(
      s"""
        |create table test.nulltypes (a INT, b BOOLEAN, c TINYINT, d BINARY(20), e VARCHAR(20),
        |f VARCHAR_IGNORECASE(20), g CHAR(20), h BLOB, i CLOB, j TIME, k DATE, l TIMESTAMP,
        |m DOUBLE, n REAL, o DECIMAL(38, 18))
      """.stripMargin.replaceAll("\n", " ")).executeUpdate()
    conn.prepareStatement("insert into test.nulltypes values ("
      + "null, null, null, null, null, null, null, null, null, "
      + "null, null, null, null, null, null)").executeUpdate()
    conn.commit()
    //dbtable需要读取的JDBC表。任何在From子句中的元素都可以,例如表或者子查询等
    sql(
      s"""
         |CREATE TEMPORARY TABLE nulltypes
         |USING org.apache.spark.sql.jdbc
         |OPTIONS (url '$url', dbtable 'TEST.NULLTYPES', user 'testUser', password 'testPass')
      """.stripMargin.replaceAll("\n", " "))

    // Untested: IDENTITY, OTHER, UUID, ARRAY, and GEOMETRY types.
  }

  after {
    conn.close()
  }

  test("SELECT *") {
    //foobar对应表PEOPLE
    assert(sql("SELECT * FROM foobar").collect().size === 3)
  }

  test("SELECT * WHERE (simple predicates)") {
    //没有数据记录
    assert(sql("SELECT * FROM foobar WHERE THEID != 2").collect().size === 2)
    //二条记录
    assert(sql("SELECT * FROM foobar WHERE NAME = 'fred'").collect().size === 1)
    //一条记录
    assert(sql("SELECT * FROM foobar WHERE NAME != 'fred'").collect().size === 2)
    //字符串比较    
    assert(sql("SELECT * FROM foobar WHERE NAME = 'fred'").collect().size === 1)
    assert(sql("SELECT * FROM foobar WHERE NAME > 'fred'").collect().size === 2)
    assert(sql("SELECT * FROM foobar WHERE NAME != 'fred'").collect().size === 2)
  }

  test("SELECT * WHERE (quoted strings)") {//引号字符串
    
    assert(sql("select * from foobar").where('NAME === "joe 'foo' \"bar\"").collect().size === 1)
  }

  test("SELECT first field") {//选择一个字段
   
    /**
      +---------------+
      |           NAME|
      +---------------+
      |           fred|
      |           mary|
      |joe 'foo' "bar"|
			+---------------+*/
    //sql("SELECT NAME FROM foobar").show()
     //升序
    val names = sql("SELECT NAME FROM foobar").collect().map(x => x.getString(0)).sortWith(_ < _)
    assert(names.size === 3)
    assert(names(0).equals("fred"))
    assert(names(1).equals("joe 'foo' \"bar\""))
    assert(names(2).equals("mary"))
  }

  test("SELECT first field when fetchSize is two") {//批量获取fetchSize 2
    val names = sql("SELECT NAME FROM fetchtwo").collect().map(x => x.getString(0)).sortWith(_ < _)
    assert(names.size === 3)
    assert(names(0).equals("fred"))
    assert(names(1).equals("joe 'foo' \"bar\""))
    assert(names(2).equals("mary"))
  }

  test("SELECT second field") {//选择第二个字段
    val ids = sql("SELECT THEID FROM foobar").collect().map(x => x.getInt(0)).sortWith(_ < _)
    assert(ids.size === 3)
    assert(ids(0) === 1)
    assert(ids(1) === 2)
    assert(ids(2) === 3)
  }

  test("SELECT second field when fetchSize is two") {
    val ids = sql("SELECT THEID FROM fetchtwo").collect().map(x => x.getInt(0)).sortWith(_ < _)
    assert(ids.size === 3)
    assert(ids(0) === 1)
    assert(ids(1) === 2)
    assert(ids(2) === 3)
  }

  test("SELECT * partitioned") {//选择分区
    //sql("SELECT * FROM parts").show()
    /**
      +---------------+-----+
      |           NAME|THEID|
      +---------------+-----+
      |           fred|    1|
      |           mary|    2|
      |joe 'foo' "bar"|    3|
      +---------------+-----+
     */
    assert(sql("SELECT * FROM parts").collect().size == 3)
  }

  test("SELECT WHERE (simple predicates) partitioned") {//简单的计算分区
    assert(sql("SELECT * FROM parts WHERE THEID < 1").collect().size === 0)
    assert(sql("SELECT * FROM parts WHERE THEID != 2").collect().size === 2)
    assert(sql("SELECT THEID FROM parts WHERE THEID = 1").collect().size === 1)
  }

  test("SELECT second field partitioned") {
    val ids = sql("SELECT THEID FROM parts").collect().map(x => x.getInt(0)).sortWith(_ < _)
    assert(ids.size === 3)
    assert(ids(0) === 1)
    assert(ids(1) === 2)
    assert(ids(2) === 3)
  }
//dbtable需要读取的JDBC表,任何在From子句中的元素都可以,例如表或者子查询等
  test("Register JDBC query with renamed fields") {//注册JDBC查询重命名字段
    // Regression test for bug SPARK-7345
    sql(
      s"""
        |CREATE TEMPORARY TABLE renamed
        |USING org.apache.spark.sql.jdbc
        |OPTIONS (url '$url', dbtable '(select NAME as NAME1, NAME as NAME2 from TEST.PEOPLE)',
        |user 'testUser', password 'testPass')
      """.stripMargin.replaceAll("\n", " "))

    val df = sql("SELECT * FROM renamed")
    /**
    df.show()
    +---------------+---------------+
    |          NAME1|          NAME2|
    +---------------+---------------+
    |           fred|           fred|
    |           mary|           mary|
    |joe 'foo' "bar"|joe 'foo' "bar"|
    +---------------+---------------+*/
   
    val fields=df.schema.fields
    assert(df.schema.fields.size == 2)
    assert(df.schema.fields(0).name == "NAME1")
    assert(df.schema.fields(1).name == "NAME2")
  }

  test("Basic API") {//JDBC基本API读取方式
    assert(ctx.read.jdbc(
      urlWithUserAndPass, "TEST.PEOPLE", new Properties).collect().length === 3)
  }

  test("Basic API with FetchSize") {//基础API设置属性获取大小
    val properties = new Properties
    //fetchSize决定了每次数据取多少行
    properties.setProperty("fetchSize", "2")
    assert(ctx.read.jdbc(
      urlWithUserAndPass, "TEST.PEOPLE", properties).collect().length === 3)
  }

  test("Partitioning via JDBCPartitioningInfo API") {//通过划分分区
    assert(
        /**
         * 参数theid分区的字段,将用于分区的整数类型的列的名称
         * 参数0分区的下界数
         *     4分区的上界数
         *     3分区的个数
         */
      ctx.read.jdbc(urlWithUserAndPass, "TEST.PEOPLE", "theid", 0, 4, 3, new Properties)
      .collect().length === 3)
  }

  test("Partitioning via list-of-where-clauses API") {//
    val parts = Array[String]("THEID < 2", "THEID >= 2")
    assert(ctx.read.jdbc(urlWithUserAndPass, "TEST.PEOPLE", parts, new Properties)
      .collect().length === 3)
  }

  test("H2 integral types") {//H2的整数类型
    val rows = sql("SELECT * FROM inttypes WHERE A IS NOT NULL").collect()
    assert(rows.length === 1)
    assert(rows(0).getInt(0) === 1)
    assert(rows(0).getBoolean(1) === false)
    assert(rows(0).getInt(2) === 3)
    assert(rows(0).getInt(3) === 4)
    assert(rows(0).getLong(4) === 1234567890123L)
  }

  test("H2 null entries") {//H2空项
    //查找字段a为null
    val rows = sql("SELECT * FROM inttypes WHERE A IS NULL").collect()
    assert(rows.length === 1)
    //按序数读取列的值判断是否为空 isNullAt
    assert(rows(0).isNullAt(0))
    assert(rows(0).isNullAt(1))
    assert(rows(0).isNullAt(2))
    assert(rows(0).isNullAt(3))
    assert(rows(0).isNullAt(4))
  }

  test("H2 string types") {//H2字符串类型
    val rows = sql("SELECT * FROM strtypes").collect()
    assert(rows(0).getAs[Array[Byte]](0).sameElements(testBytes))
    assert(rows(0).getString(1).equals("Sensitive"))
    assert(rows(0).getString(2).equals("Insensitive"))
    assert(rows(0).getString(3).trim().equals("Twenty-byte CHAR"))//注意char最大长度20,长度不够空格填充
    assert(rows(0).getAs[Array[Byte]](4).sameElements(testBytes))
    assert(rows(0).getString(5).equals("I am a clob!"))
  }

  test("H2 time types") {//H2时间类型
    //sql("SELECT * FROM timetypes").show
     /**
      +--------------------+----------+--------------------+
      |                   A|         B|                   C|
      +--------------------+----------+--------------------+
      |1970-01-01 12:34:...|1996-01-01|2002-02-20 11:22:...|
      |1970-01-01 12:34:...|      null|2002-02-20 11:22:...|
      +--------------------+----------+--------------------+*/
    val rows = sql("SELECT * FROM timetypes").collect()

    //格林日期
    val cal = new GregorianCalendar(java.util.Locale.ROOT)
    //设置12:34:56时间 获出第一行第一列的值将数据转换Timestamp
    cal.setTime(rows(0).getAs[java.sql.Timestamp](0))
    //小时
    assert(cal.get(Calendar.HOUR_OF_DAY) === 12)
    //分钟
    assert(cal.get(Calendar.MINUTE) === 34)
    //秒
    assert(cal.get(Calendar.SECOND) === 56)
    //设置1996-01-01转换Timestamp  (1)第二列数据转换Timestamp
    cal.setTime(rows(0).getAs[java.sql.Timestamp](1))
    //获得年
    assert(cal.get(Calendar.YEAR) === 1996)
    //月
    assert(cal.get(Calendar.MONTH) === 0)
    //日
    assert(cal.get(Calendar.DAY_OF_MONTH) === 1)
    //设置2002-02-20 11:22:33.543544,(2)第三列数据转换Timestamp
    cal.setTime(rows(0).getAs[java.sql.Timestamp](2))
    //年
    assert(cal.get(Calendar.YEAR) === 2002)
    //月
    assert(cal.get(Calendar.MONTH) === 1)
    //日
    assert(cal.get(Calendar.DAY_OF_MONTH) === 20)
    //小时
    assert(cal.get(Calendar.HOUR) === 11)
    //分
    assert(cal.get(Calendar.MINUTE) === 22)
    //秒 
    assert(cal.get(Calendar.SECOND) === 33)
    //毫微秒nanos
    assert(rows(0).getAs[java.sql.Timestamp](2).getNanos === 543543000)
  }
  test("test DATE types") {//H2日期类型
    /**
     *+--------------------+----------+--------------------+
      |                   A|         B|                   C|
      +--------------------+----------+--------------------+
      |1970-01-01 12:34:...|1996-01-01|2002-02-20 11:22:...|
      |1970-01-01 12:34:...|      null|2002-02-20 11:22:...|
      +--------------------+----------+--------------------+*/
  /*    ctx.read.jdbc(
      urlWithUserAndPass, "TEST.TIMETYPES", new Properties).show()*/
    val rows = ctx.read.jdbc(
      urlWithUserAndPass, "TEST.TIMETYPES", new Properties).collect()
       //缓存数据行
    val cachedRows = ctx.read.jdbc(urlWithUserAndPass, "TEST.TIMETYPES", new Properties)
      .cache().collect()
       //第一行第二列数据1996-01-01
    assert(rows(0).getAs[java.sql.Date](1) === java.sql.Date.valueOf("1996-01-01"))
      //第二行第二列数据null
    assert(rows(1).getAs[java.sql.Date](1) === null)
     //带缓存的数据
    assert(cachedRows(0).getAs[java.sql.Date](1) === java.sql.Date.valueOf("1996-01-01"))
  }


  test("test DATE types in cache") {//缓存中的测试日期类型
    /**
      +--------------------+----------+--------------------+
      |                   A|         B|                   C|
      +--------------------+----------+--------------------+
      |1970-01-01 12:34:...|1996-01-01|2002-02-20 11:22:...|
      |1970-01-01 12:34:...|      null|2002-02-20 11:22:...|
      +--------------------+----------+--------------------+*/
     //ctx.read.jdbc(urlWithUserAndPass, "TEST.TIMETYPES", new Properties).show()
    val rows = ctx.read.jdbc(urlWithUserAndPass, "TEST.TIMETYPES", new Properties).collect()
    //注册临时表
    ctx.read.jdbc(urlWithUserAndPass, "TEST.TIMETYPES", new Properties).cache().registerTempTable("mycached_date")
    //使用注意的注意临时表
    val cachedRows = sql("select * from mycached_date").collect()
    //第一行第二列数据1996-01-01
    assert(rows(0).getAs[java.sql.Date](1) === java.sql.Date.valueOf("1996-01-01"))
     //使用缓存数据第一行第二列数据1996-01-01
    assert(cachedRows(0).getAs[java.sql.Date](1) === java.sql.Date.valueOf("1996-01-01"))
  }

  test("test types for null value") {//空值的测试类型
    /**
      +----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+
      |   A|   B|   C|   D|   E|   F|   G|   H|   I|   J|   K|   L|   M|   N|   O|
      +----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+
      |null|null|null|null|null|null|null|null|null|null|null|null|null|null|null|
      +----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+*/
    ctx.read.jdbc(
      urlWithUserAndPass, "TEST.NULLTYPES", new Properties).show()
    val rows = ctx.read.jdbc(
      urlWithUserAndPass, "TEST.NULLTYPES", new Properties).collect()
    assert((0 to 14).forall(i => rows(0).isNullAt(i)))
  }

  test("H2 floating-point types") {//H2的浮点类型
    /**
      +------------------+------------------+--------------------+
      |                 A|                 B|                   C|
      +------------------+------------------+--------------------+
      |1.0000000000000002|1.0000001192092896|123456789012345.5...|
      +------------------+------------------+--------------------+*/
     //sql("SELECT * FROM flttypes").show()
    val rows = sql("SELECT * FROM flttypes").collect()
    assert(rows(0).getDouble(0) === 1.00000000000000022)
    assert(rows(0).getDouble(1) === 1.00000011920928955)
     assert(rows(0).getAs[BigDecimal](2) ===
      new BigDecimal("123456789012345.543215432154321000"))
    //强制转大数据类型
    assert(rows(0).getAs[BigDecimal](2)===
      new BigDecimal("123456789012345.543215432154321000"))
      //获取字段类型及长度
    assert(rows(0).schema.fields(2).dataType === DecimalType(38, 18))
    
    val result = sql("SELECT C FROM flttypes where C > C - 1").collect()
    assert(result(0).getAs[BigDecimal](0) ===
      new BigDecimal("123456789012345.543215432154321000"))
  }
//dbtable需要读取的JDBC表。任何在From子句中的元素都可以,例如表或者子查询等
  test("SQL query as table name") {//SQL查询的表名
    sql(
      s"""
        |CREATE TEMPORARY TABLE hack
        |USING org.apache.spark.sql.jdbc
        |OPTIONS (url '$url', dbtable '(SELECT B, B*B FROM TEST.FLTTYPES)',
        |         user 'testUser', password 'testPass')
      """.stripMargin.replaceAll("\n", " "))
      /**
        +------------------+-----------------+
        |                 B|            B * B|
        +------------------+-----------------+
        |1.0000001192092896|1.000000238418579|
        +------------------+-----------------+*/
      //sql("SELECT * FROM hack").show()
    val rows = sql("SELECT * FROM hack").collect()
    assert(rows(0).getDouble(0) === 1.00000011920928955) // Yes, I meant ==.
    // For some reason, H2 computes this square incorrectly...
    //因为某种原因,H2计算这方不正确
    //assert(math.abs(rows(0).getDouble(1) - 1.00000023841859331) < 1e-12)
  }

  test("Pass extra properties via OPTIONS") {//通过选项传递额外的属性
    // We set rowId to false during setup, which means that _ROWID_ column should be absent from
    // 我们设置ROWID虚似安装过程中,RowID列应用在所有表
    // all tables. If rowId is true (default), the query below doesn't throw an exception.
    //如果rowId为true,查询条件不会抛出异常
    intercept[JdbcSQLException] {
      sql(
        s"""
          |CREATE TEMPORARY TABLE abc
          |USING org.apache.spark.sql.jdbc
          |OPTIONS (url '$url', dbtable '(SELECT _ROWID_ FROM test.people)',
          |         user 'testUser', password 'testPass')
        """.stripMargin.replaceAll("\n", " "))
    }
  }

  test("Remap types via JdbcDialects") {//通过jdbcdialects映射类型
    JdbcDialects.registerDialect(testH2Dialect)//注意方言
    val df = ctx.read.jdbc(urlWithUserAndPass, "TEST.PEOPLE", new Properties)
    /**
      +---------------+-----+
      |           NAME|THEID|
      +---------------+-----+
      |           fred|    1|
      |           mary|    2|
      |joe 'foo' "bar"|    3|
      +---------------+-----+*/
    //df.show()
    assert(df.schema.filter(_.dataType != org.apache.spark.sql.types.StringType).isEmpty)
    val rows = df.collect()
    assert(rows(0).get(0).isInstanceOf[String])
    assert(rows(0).get(1).isInstanceOf[String])
    JdbcDialects.unregisterDialect(testH2Dialect)//注销方言
  }

  test("Default jdbc dialect registration") {//默认注册JDBC方言
    assert(JdbcDialects.get("jdbc:mysql://127.0.0.1/db") == MySQLDialect)
    assert(JdbcDialects.get("jdbc:postgresql://127.0.0.1/db") == PostgresDialect)
    assert(JdbcDialects.get("test.invalid") == NoopDialect)
  }

  test("quote column names by jdbc dialect") {//引用列名JDBC方言
    val MySQL = JdbcDialects.get("jdbc:mysql://127.0.0.1/db")    
    val Postgres = JdbcDialects.get("jdbc:postgresql://127.0.0.1/db")
    
    val columns = Seq("abc", "key")
    val MySQLColumns = columns.map(MySQL.quoteIdentifier(_))
    val PostgresColumns = columns.map(Postgres.quoteIdentifier(_))
    
    assert(MySQLColumns === Seq("`abc`", "`key`"))
    assert(PostgresColumns === Seq(""""abc"""", """"key""""))
  }

  test("Dialect unregister") {//方言注销
    JdbcDialects.registerDialect(testH2Dialect)
    JdbcDialects.unregisterDialect(testH2Dialect)
    assert(JdbcDialects.get(urlWithUserAndPass) == NoopDialect)
  }

  test("Aggregated dialects") {//聚合的方言
    val agg = new AggregatedDialect(List(new JdbcDialect {
      override def canHandle(url: String) : Boolean = url.startsWith("jdbc:h2:")
      override def getCatalystType(
          sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] =
        if (sqlType % 2 == 0) {
          Some(LongType)
        } else {
          None
        }
    }, testH2Dialect))
    assert(agg.canHandle("jdbc:h2:xxx"))
    assert(!agg.canHandle("jdbc:h2"))
    assert(agg.getCatalystType(0, "", 1, null) === Some(LongType))
    assert(agg.getCatalystType(1, "", 1, null) === Some(StringType))
  }

  test("Test DataFrame.where for Date and Timestamp") {//测试日期和时间戳
    // Regression test for bug SPARK-11788
    //回归测试
    val timestamp = java.sql.Timestamp.valueOf("2001-02-20 11:22:33.543543");
    val date = java.sql.Date.valueOf("1995-01-01")
    val jdbcDf = sqlContext.read.jdbc(urlWithUserAndPass, "TEST.TIMETYPES", new Properties)
    /**
      jdbcDf.show()
      +--------------------+----------+--------------------+
      |                   A|         B|                   C|
      +--------------------+----------+--------------------+
      |1970-01-01 12:34:...|1996-01-01|2002-02-20 11:22:...|
      |1970-01-01 12:34:...|      null|2002-02-20 11:22:...|
      +--------------------+----------+--------------------+
     */        
    val rows = jdbcDf.where($"B" > date && $"C" > timestamp).collect()
    assert(rows(0).getAs[java.sql.Date](1) === java.sql.Date.valueOf("1996-01-01"))
    assert(rows(0).getAs[java.sql.Timestamp](2)
      === java.sql.Timestamp.valueOf("2002-02-20 11:22:33.543543"))
  }
}
