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

package org.apache.spark.sql.hive.execution

import java.io._

import scala.util.control.NonFatal

import org.scalatest.{BeforeAndAfterAll, GivenWhenThen}

import org.apache.spark.{Logging, SparkFunSuite}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.{SetCommand, ExplainCommand}
import org.apache.spark.sql.execution.datasources.DescribeCommand
import org.apache.spark.sql.hive.test.TestHive

/**
 * Allows the creations of tests that execute the same query against both hive
 * and catalyst, comparing the results.
  * 允许创建对hive和catalyst执行相同查询的测试,比较结果。
 * The "golden" results from Hive are cached in an retrieved both from the classpath and
 * [answerCache] to speed up testing.
 * Hive的“golden”结果被缓存在从类路径和[answerCache]中检索出来,以加速测试
 * See the documentation of public vals in this class for information on how test execution can be
 * configured using system properties.
  * 有关如何使用系统属性配置测试执行的信息,请参阅此类中的公共文档的文档
  * 看到这类公共vals类信息如何测试执行可以配置使用系统属性
  * Hive比较测试
 */
abstract class HiveComparisonTest
  extends SparkFunSuite with BeforeAndAfterAll with GivenWhenThen with Logging {

  /**
   * When set, any cache files that result in test failures will be deleted.  Used when the test
   * harness or hive have been updated thus requiring new golden answers to be computed for some
   * tests. Also prevents the classpath being used when looking for golden answers as these are
   * usually stale.
    *
    *设置时,导致测试失败的任何缓存文件都将被删除,当测试线束或蜂巢已更新时使用,
    * 因此需要为某些测试计算新的golden答案,同时防止在寻找golden答案时使用类路径,因为这些通常是陈旧的。
   */
  val recomputeCache = System.getProperty("spark.hive.recomputeCache") != null
  //testshard=0:4
  protected val shardRegEx = "(\\d+):(\\d+)".r
  /**
   * Allows multiple JVMs to be run in parallel, each responsible for portion of all test cases.
    * 允许并行运行多个JVM,每个JVM负责所有测试用例的一部分
   * Format `shardId:numShards`. Shard ids should be zero indexed.  E.g. -Dspark.hive.testshard=0:4.
    * 格式为`shardId：numShards`,碎片应该被零索引,例如-Dspark.hive.testshard=0:4
   */
  val shardInfo = Option(System.getProperty("spark.hive.shard")).map {
    case shardRegEx(id, total) => (id.toInt, total.toInt)
  }

  protected val targetDir = new File("target")

  /**
   * When set, this comma separated list is defines directories that contain the names of test cases
   * that should be skipped.
    * 设置时,此逗号分隔列表定义包含应该跳过的测试用例名称的目录
   *
   * For example when `-Dspark.hive.skiptests=passed,hiveFailed` is specified and test cases listed
   * in [[passedDirectory]] or [[hiveFailedDirectory]] will be skipped.
    * 例如`-Dspark.hive.skip tests = passed，hive Failed`被指定,
    * 并且将跳过[[passedDirectory]]或[[hiveFailedDirectory]]中列出的测试用例。
   */
  val skipDirectories =
    Option(System.getProperty("spark.hive.skiptests"))
      .toSeq
      .flatMap(_.split(","))
      .map(name => new File(targetDir, s"$suiteName.$name"))

  val runOnlyDirectories =
    Option(System.getProperty("spark.hive.runonlytests"))
      .toSeq
      .flatMap(_.split(","))
      .map(name => new File(targetDir, s"$suiteName.$name"))

  /** The local directory with cached golden answer will be stored.
    * 具有缓存的golden答案的本地目录将被存储。*/
  protected val answerCache = new File("src" + File.separator + "test" +
    File.separator + "resources" + File.separator + "golden")
  if (!answerCache.exists) {
    answerCache.mkdir()
  }

  /** The [[ClassLoader]] that contains test dependencies.  Used to look for golden answers.
    * 包含测试依赖关系的[[ClassLoader]],用来寻找golden答案。*/
  protected val testClassLoader = this.getClass.getClassLoader

  /**
    * Directory containing a file for each test case that passes.
    * 目录包含每个测试用例的文件
    * */
  val passedDirectory = new File(targetDir, s"$suiteName.passed")
  if (!passedDirectory.exists()) {
    passedDirectory.mkdir() // Not atomic!
  }

  /** Directory containing output of tests that fail to execute with Catalyst.
    * 包含无法使用Catalyst执行的测试输出的目录
    *  */
  val failedDirectory = new File(targetDir, s"$suiteName.failed")
  if (!failedDirectory.exists()) {
    failedDirectory.mkdir() // Not atomic!
  }

  /** Directory containing output of tests where catalyst produces the wrong answer.
    * 目录包含催化剂产生错误答案的测试输出 */
  val wrongDirectory = new File(targetDir, s"$suiteName.wrong")
  if (!wrongDirectory.exists()) {
    wrongDirectory.mkdir() // Not atomic!
  }

  /** Directory containing output of tests where we fail to generate golden output with Hive.
    * 包含测试输出的目录,我们无法使用Hive生成golden输出。*/
  val hiveFailedDirectory = new File(targetDir, s"$suiteName.hiveFailed")
  if (!hiveFailedDirectory.exists()) {
    hiveFailedDirectory.mkdir() // Not atomic!
  }

  /**
    * All directories that contain per-query output files
    * 包含每个查询输出文件的所有目录
    * */
  val outputDirectories = Seq(
    passedDirectory,
    failedDirectory,
    wrongDirectory,
    hiveFailedDirectory)


  protected val cacheDigest = java.security.MessageDigest.getInstance("MD5")
  protected def getMd5(str: String): String = {
    val digest = java.security.MessageDigest.getInstance("MD5")
    digest.update(str.replaceAll(System.lineSeparator(), "\n").getBytes("utf-8"))
    new java.math.BigInteger(1, digest.digest).toString(16)
  }

  protected def prepareAnswer(
    hiveQuery: TestHive.type#QueryExecution,
    answer: Seq[String]): Seq[String] = {

    def isSorted(plan: LogicalPlan): Boolean = plan match {
      case _: Join | _: Aggregate | _: Generate | _: Sample | _: Distinct => false
      case PhysicalOperation(_, _, Sort(_, true, _)) => true
      case _ => plan.children.iterator.exists(isSorted)
    }

    val orderedAnswer = hiveQuery.analyzed match {
      // Clean out non-deterministic time schema info.
      //清除非确定性时间模式信息
      // Hack: Hive simply prints the result of a SET command to screen,
      // and does not return it as a query answer.
        //Hack：Hive只需将SET命令的结果打印到屏幕上,而不会将其作为查询答案返回。
      case _: SetCommand => Seq("0")
      case HiveNativeCommand(c) if c.toLowerCase.contains("desc") =>
        answer
          .filterNot(nonDeterministicLine)
          .map(_.replaceAll("from deserializer", ""))
          .map(_.replaceAll("None", ""))
          .map(_.trim)
          .filterNot(_ == "")
      case _: HiveNativeCommand => answer.filterNot(nonDeterministicLine).filterNot(_ == "")
      case _: ExplainCommand => answer
      case _: DescribeCommand =>
        // Filter out non-deterministic lines and lines which do not have actual results but
        // can introduce problems because of the way Hive formats these lines.
        //滤除不具有实际结果的非确定性行和行,但由于Hive格式化这些行的方式,可能会引入问题。
        // Then, remove empty lines. Do not sort the results.
        //然后,删除空行,不排序结果。
        answer
          .filterNot(r => nonDeterministicLine(r) || ignoredLine(r))
          .map(_.replaceAll("from deserializer", ""))
          .map(_.replaceAll("None", ""))
          .map(_.trim)
          .filterNot(_ == "")
      case plan => if (isSorted(plan)) answer else answer.sorted
    }
    orderedAnswer.map(cleanPaths)
  }

  // TODO: Instead of filtering we should clean to avoid accidentally ignoring actual results.
  //非确定性线指标
  lazy val nonDeterministicLineIndicators = Seq(
    "CreateTime",
    "transient_lastDdlTime",
    "grantTime",
    "lastUpdateTime",
    "last_modified_by",
    "last_modified_time",
    "Owner:",
    "COLUMN_STATS_ACCURATE",
    // The following are hive specific schema parameters which we do not need to match exactly.
    //以下是hive特定的模式参数,我们不需要完全匹配。
    "numFiles",
    "numRows",
    "rawDataSize",
    "totalSize",
    "totalNumberFiles",
    "maxFileSize",
    "minFileSize"
  )
  protected def nonDeterministicLine(line: String) =
    nonDeterministicLineIndicators.exists(line contains _)

  // This list contains indicators for those lines which do not have actual results and we
  // want to ignore.
  //这个列表包含了那些没有实际结果和我们想要忽略索引的行
  lazy val ignoredLineIndicators = Seq(
    "# Partition Information",
    "# col_name"
  )

  protected def ignoredLine(line: String) =
    ignoredLineIndicators.exists(line contains _)

  /**
   * Removes non-deterministic paths from `str` so cached answers will compare correctly.
    *从“str”中删除非确定性路径，因此缓存的答案将正确比较。
   */
  protected def cleanPaths(str: String): String = {
    str.replaceAll("file:\\/.*\\/", "<PATH>")
  }

  val installHooksCommand = "(?i)SET.*hooks".r
  def createQueryTest(testCaseName: String, sql: String, reset: Boolean = true) {
    // testCaseName must not contain ':', which is not allowed to appear in a filename of Windows
    //testCaseName不能包含'：'，它不允许出现在Windows的文件名中
    assert(!testCaseName.contains(":"))

    // If test sharding is enable, skip tests that are not in the correct shard.
    //如果测试分片已启用,请跳过不在正确分片中的测试.
    shardInfo.foreach {
      case (shardId, numShards) if testCaseName.hashCode % numShards != shardId => return
      case (shardId, _) => logDebug(s"Shard $shardId includes test '$testCaseName'")
    }

    // Skip tests found in directories specified by user.
    // 在用户指定的目录中跳过测试
    skipDirectories
      .map(new File(_, testCaseName))
      .filter(_.exists)
      .foreach(_ => return)

    // If runonlytests is set, skip this test unless we find a file in one of the specified
    // directories.
    //如果设置了runonlytests,请跳过此测试,除非我们在指定的目录之一中找到一个文件.
    val runIndicators =
      runOnlyDirectories
        .map(new File(_, testCaseName))
        .filter(_.exists)
    if (runOnlyDirectories.nonEmpty && runIndicators.isEmpty) {
      logDebug(
        s"Skipping test '$testCaseName' not found in ${runOnlyDirectories.map(_.getCanonicalPath)}")
      return
    }

    test(testCaseName) {
      logDebug(s"=== HIVE TEST: $testCaseName ===")

      // Clear old output for this testcase.
      //这个测试用例清除旧的输出
      outputDirectories.map(new File(_, testCaseName)).filter(_.exists()).foreach(_.delete())

      val sqlWithoutComment =
        sql.split("\n").filterNot(l => l.matches("--.*(?<=[^\\\\]);")).mkString("\n")
      val allQueries =
        sqlWithoutComment.split("(?<=[^\\\\]);").map(_.trim).filterNot(q => q == "").toSeq

      // TODO: DOCUMENT UNSUPPORTED
      val queryList =
        allQueries
          // In hive, setting the hive.outerjoin.supports.filters flag to "false" essentially tells
          // the system to return the wrong answer.  Since we have no intention of mirroring their
          // previously broken behavior we simply filter out changes to this setting.
          //在hive中,将hive.outerjoin.supports.filters标志设置为“false”基本上告诉系统返回错误的答案。
          // 由于我们无意反映以前破坏的行为,所以我们只需将此更改过滤掉。
          .filterNot(_ contains "hive.outerjoin.supports.filters")
          .filterNot(_ contains "hive.exec.post.hooks")

      if (allQueries != queryList) {
        logWarning(s"Simplifications made on unsupported operations for test $testCaseName")
      }

      lazy val consoleTestCase = {
        val quotes = "\"\"\""
        queryList.zipWithIndex.map {
          case (query, i) =>
            s"""val q$i = sql($quotes$query$quotes); q$i.collect()"""
        }.mkString("\n== Console version of this test ==\n", "\n", "\n")
      }

      try {
        if (reset) {
          TestHive.reset()
        }

        val hiveCacheFiles = queryList.zipWithIndex.map {
          case (queryString, i) =>
            val cachedAnswerName = s"$testCaseName-$i-${getMd5(queryString)}"
            new File(answerCache, cachedAnswerName)
        }

        val hiveCachedResults = hiveCacheFiles.flatMap { cachedAnswerFile =>
          logDebug(s"Looking for cached answer file $cachedAnswerFile.")
          if (cachedAnswerFile.exists) {
            Some(fileToString(cachedAnswerFile))
          } else {
            logDebug(s"File $cachedAnswerFile not found")
            None
          }
        }.map {
          case "" => Nil
          case "\n" => Seq("")
          case other => other.split("\n").toSeq
        }

        val hiveResults: Seq[Seq[String]] =
          if (hiveCachedResults.size == queryList.size) {
            logInfo(s"Using answer cache for test: $testCaseName")
            hiveCachedResults
          } else {

            val hiveQueries = queryList.map(new TestHive.QueryExecution(_))
            // Make sure we can at least parse everything before attempting hive execution.
            // Note this must only look at the logical plan as we might not be able to analyze if
            // other DDL has not been executed yet.
            //确保我们至少可以在尝试配置单元执行之前解析所有内容
            //请注意,只能查看逻辑计划,因为我们可能无法分析其他DDL尚未执行
            hiveQueries.foreach(_.logical)
            val computedResults = (queryList.zipWithIndex, hiveQueries, hiveCacheFiles).zipped.map {
              case ((queryString, i), hiveQuery, cachedAnswerFile) =>
                try {
                  // Hooks often break the harness and don't really affect our test anyway, don't
                  // even try running them.
                  //钩子经常打破线束,不要真的影响我们的测试,甚至不要尝试运行它们
                  if (installHooksCommand.findAllMatchIn(queryString).nonEmpty) {
                    sys.error("hive exec hooks not supported for tests.")
                  }

                  logWarning(s"Running query ${i + 1}/${queryList.size} with hive.")
                  // Analyze the query with catalyst to ensure test tables are loaded.
                  //使用catalyst分析查询,以确保加载测试表。
                  val answer = hiveQuery.analyzed match {
                    case _: ExplainCommand =>
                      // No need to execute EXPLAIN queries as we don't check the output.
                      //不需要执行EXPLAIN查询,因为我们不检查输出
                      Nil
                    case _ => TestHive.runSqlHive(queryString)
                  }

                  // We need to add a new line to non-empty answers so we can differentiate Seq()
                  // from Seq("").
                  //我们需要在非空答案中添加一行,所以我们可以区分Seq（）和Seq（“”）
                  stringToFile(
                    cachedAnswerFile, answer.mkString("\n") + (if (answer.nonEmpty) "\n" else ""))
                  answer
                } catch {
                  case e: Exception =>
                    val errorMessage =
                      s"""
                        |Failed to generate golden answer for query:
                        |Error: ${e.getMessage}
                        |${stackTraceToString(e)}
                        |$queryString
                      """.stripMargin
                    stringToFile(
                      new File(hiveFailedDirectory, testCaseName),
                      errorMessage + consoleTestCase)
                    fail(errorMessage)
                }
            }.toSeq
            if (reset) { TestHive.reset() }

            computedResults
          }

        // Run w/ catalyst
        val catalystResults = queryList.zip(hiveResults).map { case (queryString, hive) =>
          val query = new TestHive.QueryExecution(queryString)
          try { (query, prepareAnswer(query, query.stringResult())) } catch {
            case e: Throwable =>
              val errorMessage =
                s"""
                  |Failed to execute query using catalyst:
                  |Error: ${e.getMessage}
                  |${stackTraceToString(e)}
                  |$query
                  |== HIVE - ${hive.size} row(s) ==
                  |${hive.mkString("\n")}
                """.stripMargin
              stringToFile(new File(failedDirectory, testCaseName), errorMessage + consoleTestCase)
              fail(errorMessage)
          }
        }.toSeq

        (queryList, hiveResults, catalystResults).zipped.foreach {
          case (query, hive, (hiveQuery, catalyst)) =>
            // Check that the results match unless its an EXPLAIN query.
            //检查结果是否匹配,除非它的EXPLAIN查询。
            val preparedHive = prepareAnswer(hiveQuery, hive)

            // We will ignore the ExplainCommand, ShowFunctions, DescribeFunction
            //我们将忽略ExplainCommand，ShowFunctions，DescribeFunction
            if ((!hiveQuery.logical.isInstanceOf[ExplainCommand]) &&
                (!hiveQuery.logical.isInstanceOf[ShowFunctions]) &&
                (!hiveQuery.logical.isInstanceOf[DescribeFunction]) &&
                preparedHive != catalyst) {

              val hivePrintOut = s"== HIVE - ${preparedHive.size} row(s) ==" +: preparedHive
              val catalystPrintOut = s"== CATALYST - ${catalyst.size} row(s) ==" +: catalyst

              val resultComparison = sideBySide(hivePrintOut, catalystPrintOut).mkString("\n")

              if (recomputeCache) {
                logWarning(s"Clearing cache files for failed test $testCaseName")
                hiveCacheFiles.foreach(_.delete())
              }

              // If this query is reading other tables that were created during this test run
              // also print out the query plans and results for those.
              //如果此查询正在读取在此测试运行期间创建的其他表,还会打印出查询计划和结果。
              val computedTablesMessages: String = try {
                val tablesRead = new TestHive.QueryExecution(query).executedPlan.collect {
                  case ts: HiveTableScan => ts.relation.tableName
                }.toSet

                TestHive.reset()
                val executions = queryList.map(new TestHive.QueryExecution(_))
                executions.foreach(_.toRdd)
                val tablesGenerated = queryList.zip(executions).flatMap {
                  case (q, e) => e.executedPlan.collect {
                    case i: InsertIntoHiveTable if tablesRead contains i.table.tableName =>
                      (q, e, i)
                  }
                }

                tablesGenerated.map { case (hiveql, execution, insert) =>
                  s"""
                     |=== Generated Table ===
                     |$hiveql
                     |$execution
                     |== Results ==
                     |${insert.child.execute().collect().mkString("\n")}
                   """.stripMargin
                }.mkString("\n")

              } catch {
                case NonFatal(e) =>
                  logError("Failed to compute generated tables", e)
                  s"Couldn't compute dependent tables: $e"
              }

              val errorMessage =
                s"""
                  |Results do not match for $testCaseName:
                  |$hiveQuery\n${hiveQuery.analyzed.output.map(_.name).mkString("\t")}
                  |$resultComparison
                  |$computedTablesMessages
                """.stripMargin

              stringToFile(new File(wrongDirectory, testCaseName), errorMessage + consoleTestCase)
              fail(errorMessage)
            }
        }

        // Touch passed file.
        new FileOutputStream(new File(passedDirectory, testCaseName)).close()
      } catch {
        case tf: org.scalatest.exceptions.TestFailedException => throw tf
        case originalException: Exception =>
          if (System.getProperty("spark.hive.canarytest") != null) {
            // When we encounter an error we check to see if the environment is still
            // okay by running a simple query. If this fails then we halt testing since
            // something must have gone seriously wrong.
            //当我们遇到错误时,我们通过运行一个简单的查询来检查环境是否还行。
            // 如果这样做失败了,那么我们停止测试,因为某些事情肯定是错误的。
            try {
              new TestHive.QueryExecution("SELECT key FROM src").stringResult()
              TestHive.runSqlHive("SELECT key FROM src")
            } catch {
              case e: Exception =>
                logError(s"FATAL ERROR: Canary query threw $e This implies that the " +
                  "testing environment has likely been corrupted.")
                // The testing setup traps exits so wait here for a long time so the developer
                // can see when things started to go wrong.
                //测试设置陷阱退出,等待这里很长一段时间,所以开发人员可以看到什么时候出现错误
                Thread.sleep(1000000)
            }
          }

          // If the canary query didn't fail then the environment is still okay,
          // so just throw the original exception.
          //如果canary查询没有失败,那么环境还是可以的,所以只是抛出原来的异常。
          throw originalException
      }
    }
  }
}
