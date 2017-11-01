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

package org.apache.spark.sql.hive.client

import java.io.File

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkFunSuite}
import org.apache.spark.sql.catalyst.expressions.{NamedExpression, Literal, AttributeReference, EqualTo}
import org.apache.spark.sql.catalyst.util.quietly
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.util.Utils

/**
 * A simple set of tests that call the methods of a hive ClientInterface, loading different version
 * of hive from maven central.  These tests are simple in that they are mostly just testing to make
 * sure that reflective calls are not throwing NoSuchMethod error, but the actually functionality
 * is not fully tested.
  * 一套简单的测试,称为hive 群clientinterface方法,从Maven中央蜂巢加载不同的版本,
  * 这些测试是简单的,他们大多只是测试确保反射调用不扔nosuchmethod误差,但实际功能不完全测试。
 */
class VersionsSuite extends SparkFunSuite with Logging {

  // Do not use a temp path here to speed up subsequent executions of the unit test during
  // development.
  //不要在这里使用临时路径来加快开发过程中单元测试的后续执行
  private val ivyPath = Some(
    new File(sys.props("java.io.tmpdir"), "hive-ivy-cache").getAbsolutePath())

  private def buildConf() = {
    lazy val warehousePath = Utils.createTempDir()
    lazy val metastorePath = Utils.createTempDir()
    metastorePath.delete()
    Map(
      "javax.jdo.option.ConnectionURL" -> s"jdbc:derby:;databaseName=$metastorePath;create=true",
      "hive.metastore.warehouse.dir" -> warehousePath.toString)
  }
  //sanity 正常
  test("success sanity check") {
    val badClient = IsolatedClientLoader.forVersion(HiveContext.hiveExecutionVersion,
      buildConf(),
      ivyPath).client
    val db = new HiveDatabase("default", "")
    badClient.createDatabase(db)
  }

  private def getNestedMessages(e: Throwable): String = {
    var causes = ""
    var lastException = e
    while (lastException != null) {
      causes += lastException.toString + "\n"
      lastException = lastException.getCause
    }
    causes
  }

  private val emptyDir = Utils.createTempDir().getCanonicalPath

  private def partSpec = {
    val hashMap = new java.util.LinkedHashMap[String, String]
    hashMap.put("key", "1")
    hashMap
  }

  // Its actually pretty easy to mess things up and have all of your tests "pass" by accidentally
  // connecting to an auto-populated, in-process metastore.  Let's make sure we are getting the
  // versions right by forcing a known compatibility failure.
  //它实际上很容易把事情搞砸了,你的所有的测试的“通行证”的意外连接到自动填充,中间元数据。
  // 让我们确保通过强迫已知的兼容性失败来获得正确的版本。
  // TODO: currently only works on mysql where we manually create the schema...
  //破坏性检查
  ignore("failure sanity check") {
    val e = intercept[Throwable] {
      val badClient = quietly {
        IsolatedClientLoader.forVersion("13", buildConf(), ivyPath).client
      }
    }
    assert(getNestedMessages(e) contains "Unknown column 'A0.OWNER_NAME' in 'field list'")
  }

  private val versions = Seq("12", "13", "14", "1.0.0", "1.1.0", "1.2.0")

  private var client: ClientInterface = null

  versions.foreach { version =>
    //创建客户端
    test(s"$version: create client") {
      client = null
      //Hack为了避免一些JVM版本segv。
      System.gc() // Hack to avoid SEGV on some JVM versions.
      client = IsolatedClientLoader.forVersion(version, buildConf(), ivyPath).client
    }
  //创建数据库
    test(s"$version: createDatabase") {
      val db = HiveDatabase("default", "")
      client.createDatabase(db)
    }
  //创建表
    test(s"$version: createTable") {
      val table =
        HiveTable(
          specifiedDatabase = Option("default"),
          name = "src",
          schema = Seq(HiveColumn("key", "int", "")),
          partitionColumns = Seq.empty,
          properties = Map.empty,
          serdeProperties = Map.empty,
          tableType = ManagedTable,
          location = None,
          inputFormat =
            Some(classOf[org.apache.hadoop.mapred.TextInputFormat].getName),
          outputFormat =
            Some(classOf[org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat[_, _]].getName),
          serde =
            Some(classOf[org.apache.hadoop.hive.serde2.`lazy`.LazySimpleSerDe].getName()))

      client.createTable(table)
    }
    //获得表
    test(s"$version: getTable") {
      client.getTable("default", "src")
    }
    //获得列表
    test(s"$version: listTables") {
      assert(client.listTables("default") === Seq("src"))
    }
    //当前数据库
    test(s"$version: currentDatabase") {
      assert(client.currentDatabase === "default")
    }
    //当前数据库
    test(s"$version: getDatabase") {
      client.getDatabase("default")
    }
    //修改表
    test(s"$version: alterTable") {
      client.alterTable(client.getTable("default", "src"))
    }
    //设置命令
    test(s"$version: set command") {
      client.runSqlHive("SET spark.sql.test.key=1")
    }
    //创建分区表的DDL
    test(s"$version: create partitioned table DDL") {
      client.runSqlHive("CREATE TABLE src_part (value INT) PARTITIONED BY (key INT)")
      client.runSqlHive("ALTER TABLE src_part ADD PARTITION (key = '1')")
    }
    //得到的分区
    test(s"$version: getPartitions") {
      client.getAllPartitions(client.getTable("default", "src_part"))
    }
    //通过过滤器获取分区
    test(s"$version: getPartitionsByFilter") {
      client.getPartitionsByFilter(client.getTable("default", "src_part"), Seq(EqualTo(
        AttributeReference("key", IntegerType, false)(NamedExpression.newExprId),
        Literal(1))))
    }
    //加载分区
    test(s"$version: loadPartition") {
      client.loadPartition(
        emptyDir,
        "default.src_part",
        partSpec,
        false,
        false,
        false,
        false)
    }
    //加载表
    test(s"$version: loadTable") {
      client.loadTable(
        emptyDir,
        "src",
        false,
        false)
    }
    //加载动态分区
    test(s"$version: loadDynamicPartitions") {
      client.loadDynamicPartitions(
        emptyDir,
        "default.src_part",
        partSpec,
        false,
        1,
        false,
        false)
    }
    //创建索引并重置
    test(s"$version: create index and reset") {
      client.runSqlHive("CREATE TABLE indexed_table (key INT)")
      client.runSqlHive("CREATE INDEX index_1 ON TABLE indexed_table(key) " +
        "as 'COMPACT' WITH DEFERRED REBUILD")
      client.reset()
    }
  }
}
