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

package org.apache.spark.sql.hive.test

import java.io.File
import java.util.{Set => JavaSet}

import scala.collection.mutable
import scala.language.implicitConversions

import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.ql.exec.FunctionRegistry
import org.apache.hadoop.hive.ql.processors._
import org.apache.hadoop.hive.serde2.`lazy`.LazySimpleSerDe

import org.apache.spark.sql.SQLConf
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.CacheTableCommand
import org.apache.spark.sql.hive._
import org.apache.spark.sql.hive.execution.HiveNativeCommand
import org.apache.spark.util.{ShutdownHookManager, Utils}
import org.apache.spark.{SparkConf, SparkContext}

/* Implicit conversions */
import scala.collection.JavaConversions._

// SPARK-3729: Test key required to check for initialization errors with config.
//使用config检查初始化错误所需的测试键
object TestHive
  extends TestHiveContext(
    new SparkContext(
      System.getProperty("spark.sql.test.master", "local[32]"),
      "TestSQLContext",
      new SparkConf()
        .set("spark.sql.test", "")
        .set("spark.sql.hive.metastore.barrierPrefixes",
          "org.apache.spark.sql.hive.execution.PairSerDe")
        // SPARK-8910
        .set("spark.ui.enabled", "false")))

/**
 * A locally running test instance of Spark's Hive execution engine.
 * Spark的Hive执行引擎的本地运行测试实例
  *
 * Data from testTables will be automatically loaded whenever a query is run over those tables.
  * 每当查询运行在这些表上时,将自动加载来自testTable的数据。
 * Calling reset will delete all tables and other state in the database, leaving the database
 * in a "clean" state.
  * 调用重置将删除数据库中的所有表和其他状态,使数据库处于“清理”状态。
 *
 * TestHive is singleton object version of this class because instantiating multiple copies of the
 * hive metastore seems to lead to weird non-deterministic failures.  Therefore, the execution of
 * test cases that rely on TestHive must be serialized.
  *
  * TestHive是这个类的单例对象版本,因为实例化多个拷贝的hive转移似乎导致了奇怪的非确定性失败,
  * 因此,依赖于TestHive的测试用例的执行必须被序列化。
 */
class TestHiveContext(sc: SparkContext) extends HiveContext(sc) {
  self =>

  import HiveContext._

  // By clearing the port we force Spark to pick a new one.  This allows us to rerun tests
  // without restarting the JVM.
  //通过清理端口,我们强制Spark选择一个新的,这样我们就可以重新运行测试而不重新启动JVM。
  System.clearProperty("spark.hostPort")
  CommandProcessorFactory.clean(hiveconf)

  hiveconf.set("hive.plan.serialization.format", "javaXML")

  lazy val warehousePath = Utils.createTempDir(namePrefix = "warehouse-")

  lazy val scratchDirPath = {
    val dir = Utils.createTempDir(namePrefix = "scratch-")
    dir.delete()
    dir
  }

  private lazy val temporaryConfig = newTemporaryConfiguration()

  /**
    * Sets up the system initially or after a RESET command
    * 初始或RESET命令后设置系统
    * */
  protected override def configure(): Map[String, String] = {
    super.configure() ++ temporaryConfig ++ Map(
      ConfVars.METASTOREWAREHOUSE.varname -> warehousePath.toURI.toString,
      ConfVars.METASTORE_INTEGER_JDO_PUSHDOWN.varname -> "true",
      ConfVars.SCRATCHDIR.varname -> scratchDirPath.toURI.toString,
      ConfVars.METASTORE_CLIENT_CONNECT_RETRY_DELAY.varname -> "1"
    )
  }

  val testTempDir = Utils.createTempDir()

  // For some hive test case which contain ${system:test.tmp.dir}
  //对于一些包含$ {system：test.tmp.dir}的hive测试用例
  System.setProperty("test.tmp.dir", testTempDir.getCanonicalPath)

  /** The location of the compiled hive distribution
    * 编译的配置单元的位置*/
  lazy val hiveHome = envVarToFile("HIVE_HOME")
  /** The location of the hive source code.
    * hive源代码的位置。*/
  lazy val hiveDevHome = envVarToFile("HIVE_DEV_HOME")

  // Override so we can intercept relative paths and rewrite them to point at hive.
  //覆盖,所以我们可以拦截相对路径并重写它们以指向hive
  override def runSqlHive(sql: String): Seq[String] =
    super.runSqlHive(rewritePaths(substitutor.substitute(this.hiveconf, sql)))

  override def executePlan(plan: LogicalPlan): this.QueryExecution =
    new this.QueryExecution(plan)

  override protected[sql] def createSession(): SQLSession = {
    new this.SQLSession()
  }

  protected[hive] class SQLSession extends super.SQLSession {
    /** Fewer partitions to speed up testing.
      * 更少的分区可以加快测试速度*/
    protected[sql] override lazy val conf: SQLConf = new SQLConf {
      override def numShufflePartitions: Int = getConf(SQLConf.SHUFFLE_PARTITIONS, 5)
      // TODO as in unit test, conf.clear() probably be called, all of the value will be cleared.
      // The super.getConf(SQLConf.DIALECT) is "sql" by default, we need to set it as "hiveql"
      //默认情况下，super.getConf（SQLConf.DIALECT）是“sql”,我们需要将其设置为“hiveql”
      override def dialect: String = super.getConf(SQLConf.DIALECT, "hiveql")
      //区分大小写
      override def caseSensitiveAnalysis: Boolean = getConf(SQLConf.CASE_SENSITIVE, false)
    }
  }

  /**
   * Returns the value of specified environmental variable as a [[java.io.File]] after checking
   * to ensure it exists
    * 在检查以确保它存在之后,返回指定环境变量的值作为[[java.io.File]]
   */
  private def envVarToFile(envVar: String): Option[File] = {
    Option(System.getenv(envVar)).map(new File(_))
  }

  /**
   * Replaces relative paths to the parent directory "../" with hiveDevHome since this is how the
   * hive test cases assume the system is set up.
    * 用hiveDevHome代替父目录“../”的相对路径,因为这是hive测试用例假定系统设置的方式
   */
  private def rewritePaths(cmd: String): String =
    if (cmd.toUpperCase contains "LOAD DATA") {
      val testDataLocation =
        hiveDevHome.map(_.getCanonicalPath).getOrElse(inRepoTests.getCanonicalPath)
      cmd.replaceAll("\\.\\./\\.\\./", testDataLocation + "/")
    } else {
      cmd
    }

  val hiveFilesTemp = File.createTempFile("catalystHiveFiles", "")
  hiveFilesTemp.delete()
  hiveFilesTemp.mkdir()
  ShutdownHookManager.registerShutdownDeleteDir(hiveFilesTemp)

  val inRepoTests = if (System.getProperty("user.dir").endsWith("sql" + File.separator + "hive")) {
    new File("src" + File.separator + "test" + File.separator + "resources" + File.separator)
  } else {
    new File("sql" + File.separator + "hive" + File.separator + "src" + File.separator + "test" +
      File.separator + "resources")
  }

  def getHiveFile(path: String): File = {
    val stripped = path.replaceAll("""\.\.\/""", "").replace('/', File.separatorChar)
    hiveDevHome
      .map(new File(_, stripped))
      .filter(_.exists)
      .getOrElse(new File(inRepoTests, stripped))
  }

  val describedTable = "DESCRIBE (\\w+)".r

  /**
   * Override QueryExecution with special debug workflow.
    * 使用特殊调试工作流覆盖QueryExecution
   */
  class QueryExecution(logicalPlan: LogicalPlan)
    extends super.QueryExecution(logicalPlan) {
    def this(sql: String) = this(parseSql(sql))
    override lazy val analyzed = {
      val describedTables = logical match {
        case HiveNativeCommand(describedTable(tbl)) => tbl :: Nil
        case CacheTableCommand(tbl, _, _) => tbl :: Nil
        case _ => Nil
      }

      // Make sure any test tables referenced are loaded.
      //确保所有引用的测试表都被加载
      val referencedTables =
        describedTables ++
        logical.collect { case UnresolvedRelation(tableIdent, _) => tableIdent.last }
      val referencedTestTables = referencedTables.filter(testTables.contains)
      logDebug(s"Query references test tables: ${referencedTestTables.mkString(", ")}")
      referencedTestTables.foreach(loadTestTable)
      // Proceed with analysis.
      //继续进行分析
      analyzer.execute(logical)
    }
  }

  case class TestTable(name: String, commands: (() => Unit)*)

  protected[hive] implicit class SqlCmd(sql: String) {
    def cmd: () => Unit = {
      () => new QueryExecution(sql).stringResult(): Unit
    }
  }

  /**
   * A list of test tables and the DDL required to initialize them.  A test table is loaded on
   * demand when a query are run against it.
    * 测试表和DDL初始化所需的列表,当对它执行查询时,需要加载测试表
   */
  @transient
  lazy val testTables = new mutable.HashMap[String, TestTable]()

  def registerTestTable(testTable: TestTable): Unit = {
    testTables += (testTable.name -> testTable)
  }

  // The test tables that are defined in the Hive QTestUtil.
  // /itests/util/src/main/java/org/apache/hadoop/hive/ql/QTestUtil.java
  // https://github.com/apache/hive/blob/branch-0.13/data/scripts/q_test_init.sql
  @transient
  val hiveQTestUtilTables = Seq(
    TestTable("src",
      "CREATE TABLE src (key INT, value STRING)".cmd,
      s"LOAD DATA LOCAL INPATH '${getHiveFile("data/files/kv1.txt")}' INTO TABLE src".cmd),
    TestTable("src1",
      "CREATE TABLE src1 (key INT, value STRING)".cmd,
      s"LOAD DATA LOCAL INPATH '${getHiveFile("data/files/kv3.txt")}' INTO TABLE src1".cmd),
    TestTable("srcpart", () => {
      runSqlHive(
        "CREATE TABLE srcpart (key INT, value STRING) PARTITIONED BY (ds STRING, hr STRING)")
      for (ds <- Seq("2008-04-08", "2008-04-09"); hr <- Seq("11", "12")) {
        runSqlHive(
          s"""LOAD DATA LOCAL INPATH '${getHiveFile("data/files/kv1.txt")}'
             |OVERWRITE INTO TABLE srcpart PARTITION (ds='$ds',hr='$hr')
           """.stripMargin)
      }
    }),
    TestTable("srcpart1", () => {
      runSqlHive("CREATE TABLE srcpart1 (key INT, value STRING) PARTITIONED BY (ds STRING, hr INT)")
      for (ds <- Seq("2008-04-08", "2008-04-09"); hr <- 11 to 12) {
        runSqlHive(
          s"""LOAD DATA LOCAL INPATH '${getHiveFile("data/files/kv1.txt")}'
             |OVERWRITE INTO TABLE srcpart1 PARTITION (ds='$ds',hr='$hr')
           """.stripMargin)
      }
    }),
    TestTable("src_thrift", () => {
      import org.apache.hadoop.hive.serde2.thrift.ThriftDeserializer
      import org.apache.hadoop.mapred.{SequenceFileInputFormat, SequenceFileOutputFormat}
      import org.apache.thrift.protocol.TBinaryProtocol

      runSqlHive(
        s"""
         |CREATE TABLE src_thrift(fake INT)
         |ROW FORMAT SERDE '${classOf[ThriftDeserializer].getName}'
         |WITH SERDEPROPERTIES(
         |  'serialization.class'='org.apache.spark.sql.hive.test.Complex',
         |  'serialization.format'='${classOf[TBinaryProtocol].getName}'
         |)
         |STORED AS
         |INPUTFORMAT '${classOf[SequenceFileInputFormat[_, _]].getName}'
         |OUTPUTFORMAT '${classOf[SequenceFileOutputFormat[_, _]].getName}'
        """.stripMargin)

      runSqlHive(
        s"LOAD DATA LOCAL INPATH '${getHiveFile("data/files/complex.seq")}' INTO TABLE src_thrift")
    }),
    TestTable("serdeins",
      s"""CREATE TABLE serdeins (key INT, value STRING)
         |ROW FORMAT SERDE '${classOf[LazySimpleSerDe].getCanonicalName}'
         |WITH SERDEPROPERTIES ('field.delim'='\\t')
       """.stripMargin.cmd,
      "INSERT OVERWRITE TABLE serdeins SELECT * FROM src".cmd),
    TestTable("episodes",
      s"""CREATE TABLE episodes (title STRING, air_date STRING, doctor INT)
         |STORED AS avro
         |TBLPROPERTIES (
         |  'avro.schema.literal'='{
         |    "type": "record",
         |    "name": "episodes",
         |    "namespace": "testing.hive.avro.serde",
         |    "fields": [
         |      {
         |          "name": "title",
         |          "type": "string",
         |          "doc": "episode title"
         |      },
         |      {
         |          "name": "air_date",
         |          "type": "string",
         |          "doc": "initial date"
         |      },
         |      {
         |          "name": "doctor",
         |          "type": "int",
         |          "doc": "main actor playing the Doctor in episode"
         |      }
         |    ]
         |  }'
         |)
       """.stripMargin.cmd,
      s"LOAD DATA LOCAL INPATH '${getHiveFile("data/files/episodes.avro")}' INTO TABLE episodes".cmd
    ),
    // THIS TABLE IS NOT THE SAME AS THE HIVE TEST TABLE episodes_partitioned AS DYNAMIC PARITIONING
    // IS NOT YET SUPPORTED
    //该表不是同样的,因为动态分区没有被支持的HIVE测试表分区
    TestTable("episodes_part",
      s"""CREATE TABLE episodes_part (title STRING, air_date STRING, doctor INT)
         |PARTITIONED BY (doctor_pt INT)
         |STORED AS avro
         |TBLPROPERTIES (
         |  'avro.schema.literal'='{
         |    "type": "record",
         |    "name": "episodes",
         |    "namespace": "testing.hive.avro.serde",
         |    "fields": [
         |      {
         |          "name": "title",
         |          "type": "string",
         |          "doc": "episode title"
         |      },
         |      {
         |          "name": "air_date",
         |          "type": "string",
         |          "doc": "initial date"
         |      },
         |      {
         |          "name": "doctor",
         |          "type": "int",
         |          "doc": "main actor playing the Doctor in episode"
         |      }
         |    ]
         |  }'
         |)
       """.stripMargin.cmd,
      // WORKAROUND: Required to pass schema to SerDe for partitioned tables.
      //替代方法：必须将模式传递给SerDe用于分区表
      // TODO: Pass this automatically from the table to partitions.
      s"""
         |ALTER TABLE episodes_part SET SERDEPROPERTIES (
         |  'avro.schema.literal'='{
         |    "type": "record",
         |    "name": "episodes",
         |    "namespace": "testing.hive.avro.serde",
         |    "fields": [
         |      {
         |          "name": "title",
         |          "type": "string",
         |          "doc": "episode title"
         |      },
         |      {
         |          "name": "air_date",
         |          "type": "string",
         |          "doc": "initial date"
         |      },
         |      {
         |          "name": "doctor",
         |          "type": "int",
         |          "doc": "main actor playing the Doctor in episode"
         |      }
         |    ]
         |  }'
         |)
        """.stripMargin.cmd,
      s"""
        INSERT OVERWRITE TABLE episodes_part PARTITION (doctor_pt=1)
        SELECT title, air_date, doctor FROM episodes
      """.cmd
      ),
    TestTable("src_json",
      s"""CREATE TABLE src_json (json STRING) STORED AS TEXTFILE
       """.stripMargin.cmd,
      s"LOAD DATA LOCAL INPATH '${getHiveFile("data/files/json.txt")}' INTO TABLE src_json".cmd)
  )

  hiveQTestUtilTables.foreach(registerTestTable)

  private val loadedTables = new collection.mutable.HashSet[String]

  var cacheTables: Boolean = false
  def loadTestTable(name: String) {
    if (!(loadedTables contains name)) {
      // Marks the table as loaded first to prevent infinite mutually recursive table loading.
      //首先标记表的加载,以防止无限的相互递归表加载
      loadedTables += name
      logDebug(s"Loading test table $name")
      val createCmds =
        testTables.get(name).map(_.commands).getOrElse(sys.error(s"Unknown test table $name"))
      createCmds.foreach(_())

      if (cacheTables) {
        cacheTable(name)
      }
    }
  }

  /**
   * Records the UDFs present when the server starts, so we can delete ones that are created by
   * tests.
    * 记录服务器启动时存在的UDF,所以我们可以删除由服务器启动时存在的UDF创建的UDF,所以我们可以删除由测试创建的UDF
   */
  protected val originalUDFs: JavaSet[String] = FunctionRegistry.getFunctionNames

  /**
   * Resets the test instance by deleting any tables that have been created.
    * 通过删除已创建的任何表来重置测试实例
   * TODO: also clear out UDFs, views, etc.
   */
  def reset() {
    try {
      // HACK: Hive is too noisy by default.
      org.apache.log4j.LogManager.getCurrentLoggers.foreach { log =>
        log.asInstanceOf[org.apache.log4j.Logger].setLevel(org.apache.log4j.Level.WARN)
      }

      cacheManager.clearCache()
      loadedTables.clear()
      catalog.cachedDataSourceTables.invalidateAll()
      catalog.client.reset()
      catalog.unregisterAllTables()

      FunctionRegistry.getFunctionNames.filterNot(originalUDFs.contains(_)).foreach { udfName =>
        FunctionRegistry.unregisterTemporaryUDF(udfName)
      }

      // Some tests corrupt this value on purpose, which breaks the RESET call below.
      //某些测试会故意破坏此值,这会打破下面的RESET调用
      hiveconf.set("fs.default.name", new File(".").toURI.toString)
      // It is important that we RESET first as broken hooks that might have been set could break
      // other sql exec here.
      //重要的是,我们首先重置可能已设置的断线钩可能会在其中破坏其他sql exec。
      executionHive.runSqlHive("RESET")
      metadataHive.runSqlHive("RESET")
      // For some reason, RESET does not reset the following variables...
      //由于某些原因,RESET不会重置以下变量...
      // https://issues.apache.org/jira/browse/HIVE-9004
      runSqlHive("set hive.table.parameters.default=")
      runSqlHive("set datanucleus.cache.collections=true")
      runSqlHive("set datanucleus.cache.collections.lazy=true")
      // Lots of tests fail if we do not change the partition whitelist from the default.
      //如果我们不将分区白名单从默认值更改为很多测试将失败
      runSqlHive("set hive.metastore.partition.name.whitelist.pattern=.*")

      configure().foreach {
        case (k, v) =>
          metadataHive.runSqlHive(s"SET $k=$v")
      }
      defaultOverrides()

      runSqlHive("USE default")

      // Just loading src makes a lot of tests pass.  This is because some tests do something like
      // drop an index on src at the beginning.  Since we just pass DDL to hive this bypasses our
      // Analyzer and thus the test table auto-loading mechanism.
      // Remove after we handle more DDL operations natively.
      //只需加载src就可以通过很多测试,这是因为一些测试开始时就像在src上放一个索引,
      // 由于我们只是通过DDL来使hive这个绕过我们的分析器,因此也是测试表的自动加载机制,在我们处理更多的DDL操作之后,我们去掉了。
      loadTestTable("src")
      loadTestTable("srcpart")
    } catch {
      case e: Exception =>
        logError("FATAL ERROR: Failed to reset TestDB state.", e)
    }
  }
}
