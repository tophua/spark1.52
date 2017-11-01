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

import java.io.{File, IOException}

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.execution.datasources.DDLException
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.Utils

//创建表作为选择套件
class CreateTableAsSelectSuite extends DataSourceTest with SharedSQLContext with BeforeAndAfter {
  protected override lazy val sql = caseInsensitiveContext.sql _
  private lazy val sparkContext = caseInsensitiveContext.sparkContext
  private var path: File = null

  override def beforeAll(): Unit = {
    super.beforeAll()
    path = Utils.createTempDir()
    val rdd = sparkContext.parallelize((1 to 10).map(i => s"""{"a":$i, "b":"str${i}"}"""))
    //读取RDD[String]类型
    caseInsensitiveContext.read.json(rdd).registerTempTable("jt")
  }

  override def afterAll(): Unit = {
    try {
      //删除临时表
      caseInsensitiveContext.dropTempTable("jt")
    } finally {
      super.afterAll()
    }
  }

  after {
    //递归删除目录文件
    Utils.deleteRecursively(path)
  }

  test("CREATE TEMPORARY TABLE AS SELECT") {//创建临时表作为选择的字段
    sql(
        //注意USING json格式
      s"""
        |CREATE TEMPORARY TABLE jsonTable
        |USING json
        |OPTIONS (
        |  path '${path.toString}'
        |) AS
        |SELECT a, b FROM jt
      """.stripMargin)

    checkAnswer(
      sql("SELECT a, b FROM jsonTable"),
      sql("SELECT a, b FROM jt").collect())

    caseInsensitiveContext.dropTempTable("jsonTable")
  }
  //创建临时表,作为未写权限的文件的基础上的选择
  test("CREATE TEMPORARY TABLE AS SELECT based on the file without write permission") {
    val childPath = new File(path.toString, "child")
    path.mkdir()
    childPath.createNewFile()
    //设置只读权限
    path.setWritable(false)
    //如果文件夹不能写,报错
    val e = intercept[IOException] {
      sql(
        s"""
           |CREATE TEMPORARY TABLE jsonTable
           |USING json
           |OPTIONS (
           |  path '${path.toString}'
           |) AS
           |SELECT a, b FROM jt
        """.stripMargin)
      sql("SELECT a, b FROM jsonTable").collect()
    }
    assert(e.getMessage().contains("Unable to clear output directory"))

    path.setWritable(true)
  }
  //创建一个表,并创建一个具有相同名称的另一个表
  test("create a table, drop it and create another one with the same name") {
    sql(
      s"""
        |CREATE TEMPORARY TABLE jsonTable
        |USING json
        |OPTIONS (
        |  path '${path.toString}'
        |) AS
        |SELECT a, b FROM jt
      """.stripMargin)

    checkAnswer(
      sql("SELECT a, b FROM jsonTable"),
      sql("SELECT a, b FROM jt").collect())

    val message = intercept[DDLException]{
      //IF NOT EXISTS 不充许覆盖临时表
      sql(
        s"""
        |CREATE TEMPORARY TABLE IF NOT EXISTS jsonTable
        |USING json
        |OPTIONS (
        |  path '${path.toString}'
        |) AS
        |SELECT a * 4 FROM jt
      """.stripMargin)
    }.getMessage
    assert(
      message.contains(s"a CREATE TEMPORARY TABLE statement does not allow IF NOT EXISTS clause."),
      "CREATE TEMPORARY TABLE IF NOT EXISTS should not be allowed.")

    // Overwrite the temporary table.
    //覆盖临时表
    sql(
      s"""
        |CREATE TEMPORARY TABLE jsonTable
        |USING json
        |OPTIONS (
        |  path '${path.toString}'
        |) AS
        |SELECT a * 4 FROM jt
      """.stripMargin)
    checkAnswer(
      sql("SELECT * FROM jsonTable"),
      sql("SELECT a * 4 FROM jt").collect())

    caseInsensitiveContext.dropTempTable("jsonTable")
    // Explicitly delete the data.
    //显式删除数据
    if (path.exists()) Utils.deleteRecursively(path)

    sql(
      s"""
        |CREATE TEMPORARY TABLE jsonTable
        |USING json
        |OPTIONS (
        |  path '${path.toString}'
        |) AS
        |SELECT b FROM jt
      """.stripMargin)     
    checkAnswer(
      sql("SELECT * FROM jsonTable"),
      sql("SELECT b FROM jt").collect())

    caseInsensitiveContext.dropTempTable("jsonTable")
  }
  //创建临时表作为选择,如果不存在的话是不允许的
  test("CREATE TEMPORARY TABLE AS SELECT with IF NOT EXISTS is not allowed") {
    val message = intercept[DDLException]{
      sql(
          // IF NOT EXISTS 如果不存在
        s"""
        |CREATE TEMPORARY TABLE IF NOT EXISTS jsonTable
        |USING json
        |OPTIONS (
        |  path '${path.toString}'
        |) AS
        |SELECT b FROM jt
      """.stripMargin)
    }.getMessage
    assert(
      message.contains("a CREATE TEMPORARY TABLE statement does not allow IF NOT EXISTS clause."),
      "CREATE TEMPORARY TABLE IF NOT EXISTS should not be allowed.")
  }
  //一个列定义CTAS语句是不允许的,create table as select （CTAS）
  test("a CTAS statement with column definitions is not allowed") {
    intercept[DDLException]{
      sql(
        s"""
        |CREATE TEMPORARY TABLE jsonTable (a int, b string)
        |USING json
        |OPTIONS (
        |  path '${path.toString}'
        |) AS
        |SELECT a, b FROM jt
      """.stripMargin)
    }
  }
  //在查询时,不允许在表上写入
  test("it is not allowed to write to a table while querying it.") {
    //path注意path使用单引号,引入路径
    sql(
      s"""
        |CREATE TEMPORARY TABLE jsonTable
        |USING json
        |OPTIONS (
        |  path '${path.toString}'
        |) AS
        |SELECT a, b FROM jt
      """.stripMargin)

    val message = intercept[AnalysisException] {
      sql(
        s"""
        |CREATE TEMPORARY TABLE jsonTable
        |USING json
        |OPTIONS (
        |  path '${path.toString}'
        |) AS
        |SELECT a, b FROM jsonTable
      """.stripMargin)
    }.getMessage
    assert(
      message.contains("Cannot overwrite table "),
      "Writing to a table while querying it should not be allowed.")
  }
}
