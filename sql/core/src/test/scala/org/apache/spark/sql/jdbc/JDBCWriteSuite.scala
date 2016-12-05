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

import java.sql.DriverManager
import java.util.Properties

import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

class JDBCWriteSuite extends SparkFunSuite with BeforeAndAfter with SharedSQLContext {

  //val url1 = "jdbc:h2:mem:testdb2"
  val url1 = "jdbc:h2:tcp://localhost/~/testdb2"
  //val url = "jdbc:postgresql://192.168.10.198:3306/postgres?user=postgres&password=admin"
  //val url = "jdbc:postgresql://192.168.10.198:3306/postgres?user=postgres&password=admin"
  var conn: java.sql.Connection = null
   val url = "jdbc:h2:mem:testdb3"
  //  val url = "jdbc:h2:tcp://localhost/~/testdb3?user=testUser&password=testPass"
  //val url1 = "jdbc:postgresql://192.168.10.198:3306/postgres"
  
  var conn1: java.sql.Connection = null
  val properties = new Properties()
   properties.setProperty("user", "testUser")
   properties.setProperty("password", "testPass")
   //properties.setProperty("user", "postgres")
   //properties.setProperty("password", "admin")
   properties.setProperty("rowId", "false")

  before {
    Utils.classForName("org.h2.Driver")
    
    //Utils.classForName("org.postgresql.Driver")
    
    conn = DriverManager.getConnection(url)    
     conn.prepareStatement("create schema test").executeUpdate()

    conn1 = DriverManager.getConnection(url1, properties)
    conn1.prepareStatement("create schema IF NOT EXISTS TEST").executeUpdate()
    conn1.prepareStatement("drop table if exists TEST.people").executeUpdate()
    conn1.prepareStatement("drop table if exists TEST.DROPTEST").executeUpdate()
    conn1.prepareStatement("drop table if exists TEST.TRUNCATETEST").executeUpdate()
    
    conn1.prepareStatement(
      "create table test.people (name TEXT(32) NOT NULL, theid INTEGER NOT NULL)").executeUpdate()
    conn1.prepareStatement("insert into test.people values ('fred', 1)").executeUpdate()
    conn1.prepareStatement("insert into test.people values ('mary', 2)").executeUpdate()
    conn1.prepareStatement("drop table if exists test.people1").executeUpdate()
    conn1.prepareStatement(
      "create table test.people1 (name TEXT(32) NOT NULL, theid INTEGER NOT NULL)").executeUpdate()
    conn1.commit()
    conn.commit()
//dbtable需要读取的JDBC表。任何在From子句中的元素都可以，例如表或者子查询等
    sql(
      s"""
        |CREATE TEMPORARY TABLE PEOPLE
        |USING org.apache.spark.sql.jdbc
        |OPTIONS (url '$url1', dbtable 'TEST.PEOPLE', user 'testUser', password 'testPass')
      """.stripMargin.replaceAll("\n", " "))
//dbtable需要读取的JDBC表。任何在From子句中的元素都可以，例如表或者子查询等
    sql(
      s"""
        |CREATE TEMPORARY TABLE PEOPLE1
        |USING org.apache.spark.sql.jdbc
        |OPTIONS (url '$url1', dbtable 'TEST.PEOPLE1', user 'testUser', password 'testPass')
      """.stripMargin.replaceAll("\n", " "))
  }

  after {
    conn.close()
    conn1.close()
  }

  private lazy val sc = ctx.sparkContext

  private lazy val arr2x2 = Array[Row](Row.apply("dave", 42), Row.apply("mary", 222))
  private lazy val arr1x2 = Array[Row](Row.apply("fred", 3))
  //StructType代表一张表,StructField代表一个字段
  private lazy val schema2 = StructType(
      StructField("name", StringType) ::
      StructField("id", IntegerType) :: Nil)

  private lazy val arr2x3 = Array[Row](Row.apply("dave", 42, 1), Row.apply("mary", 222, 2))
  //StructType代表一张表,StructField代表一个字段
  private lazy val schema3 = StructType(
      StructField("name", StringType) ::
      StructField("id", IntegerType) ::
      StructField("seq", IntegerType) :: Nil)

  test("Basic CREATE") {//基本创建
    val df = ctx.createDataFrame(sc.parallelize(arr2x2), schema2)
    //创建表插入二行数据,根据schema2数据类型自动创建表构造
    /**
     * "dave", 42
     * "mary", 222
     */
    df.write.jdbc(url, "TEST.BASICCREATETEST", new Properties)
    //读取数据库的数据
    assert(2 === ctx.read.jdbc(url, "TEST.BASICCREATETEST", new Properties).count)
    //取出第一行数据,leng代表几列数据
    assert(2 === ctx.read.jdbc(url, "TEST.BASICCREATETEST", new Properties).collect()(0).length)
  }

  test("CREATE with overwrite") {//创建覆盖
    val df = ctx.createDataFrame(sc.parallelize(arr2x3), schema3)
    val df2 = ctx.createDataFrame(sc.parallelize(arr1x2), schema2)

    df.write.jdbc(url1, "TEST.DROPTEST", properties)
    //总数据
    assert(2 === ctx.read.jdbc(url1, "TEST.DROPTEST", properties).count)
    //获得第1行,3列
    assert(3 === ctx.read.jdbc(url1, "TEST.DROPTEST", properties).collect()(0).length)
    //覆盖数据模式,即原来的数据删除
    //当数据输出的位置已存在时,重写
    df2.write.mode(SaveMode.Overwrite).jdbc(url1, "TEST.DROPTEST", properties)
    assert(1 === ctx.read.jdbc(url1, "TEST.DROPTEST", properties).count)
    //表构造2列
    assert(2 === ctx.read.jdbc(url1, "TEST.DROPTEST", properties).collect()(0).length)
  }

  test("CREATE then INSERT to append") {//创建然后插入到追加
    val df = ctx.createDataFrame(sc.parallelize(arr2x2), schema2)
    val df2 = ctx.createDataFrame(sc.parallelize(arr1x2), schema2)
    //插入2行2列数据,
    df.write.jdbc(url, "TEST.APPENDTEST", new Properties)
     assert(2 === ctx.read.jdbc(url, "TEST.APPENDTEST", new Properties).count)
    //当数据输出的位置已存在时,在文件后面追加
    df2.write.mode(SaveMode.Append).jdbc(url, "TEST.APPENDTEST", new Properties)
    //插入1行2列数据,
    assert(3 === ctx.read.jdbc(url, "TEST.APPENDTEST", new Properties).count)
    assert(2 === ctx.read.jdbc(url, "TEST.APPENDTEST", new Properties).collect()(0).length)
  }

  test("CREATE then INSERT to truncate") {//创建并插入截断
    val df = ctx.createDataFrame(sc.parallelize(arr2x2), schema2)
    val df2 = ctx.createDataFrame(sc.parallelize(arr1x2), schema2)

    df.write.jdbc(url1, "TEST.TRUNCATETEST", properties)
    //2行数据
    assert(2 === ctx.read.jdbc(url1, "TEST.TRUNCATETEST", properties).count)
    //当数据输出的位置已存在时,重写
    df2.write.mode(SaveMode.Overwrite).jdbc(url1, "TEST.TRUNCATETEST", properties)
    //1行数据
    assert(1 === ctx.read.jdbc(url1, "TEST.TRUNCATETEST", properties).count)
    assert(2 === ctx.read.jdbc(url1, "TEST.TRUNCATETEST", properties).collect()(0).length)
  }

  test("Incompatible INSERT to append") {//不匹配插入与追加
    //二列数据
    val df = ctx.createDataFrame(sc.parallelize(arr2x2), schema2)
    //三列数据
    val df2 = ctx.createDataFrame(sc.parallelize(arr2x3), schema3)

    df.write.jdbc(url, "TEST.INCOMPATIBLETEST", new Properties)
    intercept[org.apache.spark.SparkException] {//追加三列数据报告
    //当数据输出的位置已存在时，在文件后面追加
      df2.write.mode(SaveMode.Append).jdbc(url, "TEST.INCOMPATIBLETEST", new Properties)
    }
  }

  test("INSERT to JDBC Datasource") {//插入JDBC数据源
    sql("INSERT INTO TABLE PEOPLE1 SELECT * FROM PEOPLE")
    assert(2 === ctx.read.jdbc(url1, "TEST.PEOPLE1", properties).count)
    assert(2 === ctx.read.jdbc(url1, "TEST.PEOPLE1", properties).collect()(0).length)
  }

  test("INSERT to JDBC Datasource with overwrite") {//插入与改写JDBC数据源
    sql("INSERT INTO TABLE PEOPLE1 SELECT * FROM PEOPLE")
    sql("INSERT OVERWRITE TABLE PEOPLE1 SELECT * FROM PEOPLE")
    assert(2 === ctx.read.jdbc(url1, "TEST.PEOPLE1", properties).count)
    assert(2 === ctx.read.jdbc(url1, "TEST.PEOPLE1", properties).collect()(0).length)
  }
}
