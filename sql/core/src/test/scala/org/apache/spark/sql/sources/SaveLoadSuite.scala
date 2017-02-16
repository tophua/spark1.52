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

import java.io.File

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.{AnalysisException, SaveMode, SQLConf, DataFrame}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils
//保存加载测试套件
class SaveLoadSuite extends DataSourceTest with SharedSQLContext with BeforeAndAfter {
  protected override lazy val sql = caseInsensitiveContext.sql _
  private lazy val sparkContext = caseInsensitiveContext.sparkContext
  private var originalDefaultSource: String = null
  private var path: File = null
  private var df: DataFrame = null

  override def beforeAll(): Unit = {
    super.beforeAll()
    //创建一个不区分大小写的数据源,获得默认数据源名称
    originalDefaultSource = caseInsensitiveContext.conf.defaultDataSourceName
    //创建Json数据源
    path = Utils.createTempDir()
    //删除临时文件
    path.delete()

    val rdd = sparkContext.parallelize((1 to 10).map(i => s"""{"a":$i, "b":"str${i}"}"""))
    df = caseInsensitiveContext.read.json(rdd)
    df.registerTempTable("jsonTable")
  }

  override def afterAll(): Unit = {
    try {
      caseInsensitiveContext.conf.setConf(SQLConf.DEFAULT_DATA_SOURCE_NAME, originalDefaultSource)
    } finally {
      super.afterAll()
    }
  }

  after {
    Utils.deleteRecursively(path)
  }
  //检查加载数据表
  def checkLoad(expectedDF: DataFrame = df, tbl: String = "jsonTable"): Unit = {
    caseInsensitiveContext.conf.setConf(
      SQLConf.DEFAULT_DATA_SOURCE_NAME, "org.apache.spark.sql.json")
    checkAnswer(caseInsensitiveContext.read.load(path.toString), expectedDF.collect())

    // Test if we can pick up the data source name passed in load.
    //测试,如果我们可以拿起数据源名称传递的负载
    caseInsensitiveContext.conf.setConf(SQLConf.DEFAULT_DATA_SOURCE_NAME, "not a source name")
    checkAnswer(caseInsensitiveContext.read.format("json").load(path.toString),
      expectedDF.collect())
    checkAnswer(caseInsensitiveContext.read.format("json").load(path.toString),
      expectedDF.collect())//StructType代表一张表,StructField代表一个字段
    val schema = StructType(StructField("b", StringType, true) :: Nil)
    checkAnswer(
      caseInsensitiveContext.read.format("json").schema(schema).load(path.toString),
      sql(s"SELECT b FROM $tbl").collect())
  }

  test("save with path and load") {//保存路径和加载
    caseInsensitiveContext.conf.setConf(
      SQLConf.DEFAULT_DATA_SOURCE_NAME, "org.apache.spark.sql.json")
    df.write.save(path.toString)
    checkLoad()
  }

  test("save with string mode and path, and load") {//保存字符串模式和路径,并加载
    caseInsensitiveContext.conf.setConf(
      SQLConf.DEFAULT_DATA_SOURCE_NAME, "org.apache.spark.sql.json")
    path.createNewFile()
    df.write.mode("overwrite").save(path.toString)
    checkLoad()
  }

  test("save with path and datasource, and load") {//路径和数据源保存和加载
    caseInsensitiveContext.conf.setConf(SQLConf.DEFAULT_DATA_SOURCE_NAME, "not a source name")
    df.write.json(path.toString)
    checkLoad()
  }

  test("save with data source and options, and load") {//保存与数据源和选项,并加载
    caseInsensitiveContext.conf.setConf(SQLConf.DEFAULT_DATA_SOURCE_NAME, "not a source name")
    //当数据输出的位置已存在时,抛出此异常
    df.write.mode(SaveMode.ErrorIfExists).json(path.toString)
    checkLoad()
  }

  test("save and save again") {//再次保存和保存
    df.write.json(path.toString)

    val message = intercept[AnalysisException] {
      df.write.json(path.toString)
    }.getMessage

    assert(
      message.contains("already exists"),
      "We should complain that the path already exists.")

    if (path.exists()) Utils.deleteRecursively(path)

    df.write.json(path.toString)
    checkLoad()
    //当数据输出的位置已存在时,重写
    df.write.mode(SaveMode.Overwrite).json(path.toString)
    checkLoad()

    // verify the append mode
    //当数据输出的位置已存在时,在文件后面追加
    df.write.mode(SaveMode.Append).json(path.toString)
    val df2 = df.unionAll(df)
    df2.registerTempTable("jsonTable2")

    checkLoad(df2, "jsonTable2")
  }
}
