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

import org.scalatest.BeforeAndAfterAll

import scala.reflect.ClassTag

import org.apache.spark.sql.{Row, SQLConf, QueryTest}
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.hive.execution._

class StatisticsSuite extends QueryTest with BeforeAndAfterAll {

  private lazy val ctx: HiveContext = {
    val ctx = org.apache.spark.sql.hive.test.TestHive
    ctx.reset()
    ctx.cacheTables = false
    ctx
  }

  import ctx.sql
  //解析分析命令
  test("parse analyze commands") {
    def assertAnalyzeCommand(analyzeCommand: String, c: Class[_]) {
      val parsed = HiveQl.parseSql(analyzeCommand)
      val operators = parsed.collect {
        case a: AnalyzeTable => a
        case o => o
      }

      assert(operators.size === 1)
      if (operators(0).getClass() != c) {
        fail(
          s"""$analyzeCommand expected command: $c, but got ${operators(0)}
             |parsed command:
             |$parsed
           """.stripMargin)
      }
    }

    // Ensure session state is initialized.
    //确保会话状态被初始化
    ctx.parseSql("use default")

    assertAnalyzeCommand(
      "ANALYZE TABLE Table1 COMPUTE STATISTICS",
      classOf[HiveNativeCommand])
    assertAnalyzeCommand(
      "ANALYZE TABLE Table1 PARTITION(ds='2008-04-09', hr=11) COMPUTE STATISTICS",
      classOf[HiveNativeCommand])
    assertAnalyzeCommand(
      "ANALYZE TABLE Table1 PARTITION(ds='2008-04-09', hr=11) COMPUTE STATISTICS noscan",
      classOf[HiveNativeCommand])
    assertAnalyzeCommand(
      "ANALYZE TABLE Table1 PARTITION(ds, hr) COMPUTE STATISTICS",
      classOf[HiveNativeCommand])
    assertAnalyzeCommand(
      "ANALYZE TABLE Table1 PARTITION(ds, hr) COMPUTE STATISTICS noscan",
      classOf[HiveNativeCommand])

    assertAnalyzeCommand(
      "ANALYZE TABLE Table1 COMPUTE STATISTICS nOscAn",
      classOf[AnalyzeTable])
  }
  //分析Metastore关系
  test("analyze MetastoreRelations") {
    def queryTotalSize(tableName: String): BigInt =
      ctx.catalog.lookupRelation(Seq(tableName)).statistics.sizeInBytes

    // Non-partitioned table 非分区表
    sql("CREATE TABLE analyzeTable (key STRING, value STRING)").collect()
    sql("INSERT INTO TABLE analyzeTable SELECT * FROM src").collect()
    sql("INSERT INTO TABLE analyzeTable SELECT * FROM src").collect()

    sql("ANALYZE TABLE analyzeTable COMPUTE STATISTICS noscan")

    assert(queryTotalSize("analyzeTable") === BigInt(11624))

    sql("DROP TABLE analyzeTable").collect()

    // Partitioned table 分区表
    sql(
      """
        |CREATE TABLE analyzeTable_part (key STRING, value STRING) PARTITIONED BY (ds STRING)
      """.stripMargin).collect()
    sql(
      """
        |INSERT INTO TABLE analyzeTable_part PARTITION (ds='2010-01-01')
        |SELECT * FROM src
      """.stripMargin).collect()
    sql(
      """
        |INSERT INTO TABLE analyzeTable_part PARTITION (ds='2010-01-02')
        |SELECT * FROM src
      """.stripMargin).collect()
    sql(
      """
        |INSERT INTO TABLE analyzeTable_part PARTITION (ds='2010-01-03')
        |SELECT * FROM src
      """.stripMargin).collect()

    assert(queryTotalSize("analyzeTable_part") === ctx.conf.defaultSizeInBytes)

    sql("ANALYZE TABLE analyzeTable_part COMPUTE STATISTICS noscan")

    assert(queryTotalSize("analyzeTable_part") === BigInt(17436))

    sql("DROP TABLE analyzeTable_part").collect()

    // Try to analyze a temp table 尝试分析一个临时表
    sql("""SELECT * FROM src""").registerTempTable("tempTable")
    intercept[UnsupportedOperationException] {
      ctx.analyze("tempTable")
    }
    ctx.catalog.unregisterTable(Seq("tempTable"))
  }
  //估计MetastoreRelation测试的大小
  test("estimates the size of a test MetastoreRelation") {
    val df = sql("""SELECT * FROM src""")
    val sizes = df.queryExecution.analyzed.collect { case mr: MetastoreRelation =>
      mr.statistics.sizeInBytes
    }
    assert(sizes.size === 1, s"Size wrong for:\n ${df.queryExecution}")
    assert(sizes(0).equals(BigInt(5812)),
      s"expected exact size 5812 for test table 'src', got: ${sizes(0)}")
  }
  //通过关系的大小估计自动转换为广播散列连接
 test("auto converts to broadcast hash join, by size estimate of a relation") {
    def mkTest(
        before: () => Unit,
        after: () => Unit,
        query: String,
        expectedAnswer: Seq[Row],
        ct: ClassTag[_]): Unit = {
      before()

      var df = sql(query)

      // Assert src has a size smaller than the threshold.
      //Assert src的大小小于阈值
      val sizes = df.queryExecution.analyzed.collect {
        case r if ct.runtimeClass.isAssignableFrom(r.getClass) => r.statistics.sizeInBytes
      }
      assert(sizes.size === 2 && sizes(0) <= ctx.conf.autoBroadcastJoinThreshold
        && sizes(1) <= ctx.conf.autoBroadcastJoinThreshold,
        s"query should contain two relations, each of which has size smaller than autoConvertSize")

      // Using `sparkPlan` because for relevant patterns in HashJoin to be
      // matched, other strategies need to be applied.
      //使用`sparkPlan`，因为HashJoin中的相关模式是匹配，其他策略需要应用。
      var bhj = df.queryExecution.sparkPlan.collect { case j: BroadcastHashJoin => j }
      assert(bhj.size === 1,
        s"actual query plans do not contain broadcast join: ${df.queryExecution}")
    //检查输出的正确性
      checkAnswer(df, expectedAnswer) // check correctness of output

      ctx.conf.settings.synchronized {
        val tmp = ctx.conf.autoBroadcastJoinThreshold

        sql(s"""SET ${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key}=-1""")
        df = sql(query)
        bhj = df.queryExecution.sparkPlan.collect { case j: BroadcastHashJoin => j }
        assert(bhj.isEmpty, "BroadcastHashJoin still planned even though it is switched off")

        val shj = df.queryExecution.sparkPlan.collect { case j: SortMergeJoin => j }
        assert(shj.size === 1,
          "ShuffledHashJoin should be planned when BroadcastHashJoin is turned off")

        sql(s"""SET ${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key}=$tmp""")
      }

      after()
    }

    /** Tests for MetastoreRelation  MetastoreRelation测试*/
    val metastoreQuery = """SELECT * FROM src a JOIN src b ON a.key = 238 AND a.key = b.key"""
    val metastoreAnswer = Seq.fill(4)(Row(238, "val_238", 238, "val_238"))
    mkTest(
      () => (),
      () => (),
      metastoreQuery,
      metastoreAnswer,
      implicitly[ClassTag[MetastoreRelation]]
    )
  }
  //根据关系的大小估计自动转换为广播左半连接
test("auto converts to broadcast left semi join, by size estimate of a relation") {
    val leftSemiJoinQuery =
      """SELECT * FROM src a
        |left semi JOIN src b ON a.key=86 and a.key = b.key""".stripMargin
    val answer = Row(86, "val_86")

    var df = sql(leftSemiJoinQuery)

    // Assert src has a size smaller than the threshold.
    //Assert src的大小小于阈值
    val sizes = df.queryExecution.analyzed.collect {
      case r if implicitly[ClassTag[MetastoreRelation]].runtimeClass
        .isAssignableFrom(r.getClass) =>
        r.statistics.sizeInBytes
    }
    assert(sizes.size === 2 && sizes(1) <= ctx.conf.autoBroadcastJoinThreshold
      && sizes(0) <= ctx.conf.autoBroadcastJoinThreshold,
      s"query should contain two relations, each of which has size smaller than autoConvertSize")

    // Using `sparkPlan` because for relevant patterns in HashJoin to be
    // matched, other strategies need to be applied.
    //使用sparkplan因为有关HashJoin图案相配合,需要运用其他策略
    var bhj = df.queryExecution.sparkPlan.collect {
      case j: BroadcastLeftSemiJoinHash => j
    }
    assert(bhj.size === 1,
      s"actual query plans do not contain broadcast join: ${df.queryExecution}")
    //检查输出的正确性
    checkAnswer(df, answer) // check correctness of output

    ctx.conf.settings.synchronized {
      val tmp = ctx.conf.autoBroadcastJoinThreshold

      sql(s"SET ${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key}=-1")
      df = sql(leftSemiJoinQuery)
      bhj = df.queryExecution.sparkPlan.collect {
        case j: BroadcastLeftSemiJoinHash => j
      }
      //broadcasthashjoin仍计划即使是关闭
      assert(bhj.isEmpty, "BroadcastHashJoin still planned even though it is switched off")

      val shj = df.queryExecution.sparkPlan.collect {
        case j: LeftSemiJoinHash => j
      }
      //leftsemijoinhash应计划当BroadcastHashJoin关闭
      assert(shj.size === 1,
        "LeftSemiJoinHash should be planned when BroadcastHashJoin is turned off")

      sql(s"SET ${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key}=$tmp")
    }

  }
}
