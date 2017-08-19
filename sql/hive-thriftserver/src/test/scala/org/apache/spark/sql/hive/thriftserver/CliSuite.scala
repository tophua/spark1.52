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

package org.apache.spark.sql.hive.thriftserver

import java.io._
import java.sql.Timestamp
import java.util.Date

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.concurrent.{Await, Promise}

import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.test.ProcessTestUtils.ProcessOutputCapturer
import org.apache.spark.util.Utils
import org.apache.spark.{Logging, SparkFunSuite}

/**
 * A test suite for the `spark-sql` CLI tool.  Note that all test cases share the same temporary
 * Hive metastore and warehouse.
  * 用于“spark-sql”CLI工具的测试套件,请注意,所有测试用例共享相同的临时表Hive转移和仓库。
 */
class CliSuite extends SparkFunSuite with BeforeAndAfter with Logging {
  val warehousePath = Utils.createTempDir()
  val metastorePath = Utils.createTempDir()
  val scratchDirPath = Utils.createTempDir()

  before {
    warehousePath.delete()
    metastorePath.delete()
    scratchDirPath.delete()
  }

  after {
    warehousePath.delete()
    metastorePath.delete()
    scratchDirPath.delete()
  }

  /**
   * Run a CLI operation and expect all the queries and expected answers to be returned.
    * 运行CLI操作,并期望返回所有查询和预期答案
   * @param timeout maximum time for the commands to complete命令完成的最长时间
   * @param extraArgs any extra arguments 任何额外的参数
   * @param errorResponses a sequence of strings whose presence in the stdout of the forked process
   *                       is taken as an immediate error condition. That is: if a line containing
   *                       with one of these strings is found, fail the test immediately.
   *                       The default value is `Seq("Error:")`
    *                      在分叉进程的标准中存在的字符串序列被视为立即错误条件,
    *                      也就是说：如果找到包含这些字符串之一的行，则立即失败。默认值为“Seq（”Error：“）
   *
   * @param queriesAndExpectedAnswers one or more tupes of query + answer
   */
  def runCliWithin(
      timeout: FiniteDuration,
      extraArgs: Seq[String] = Seq.empty,
      errorResponses: Seq[String] = Seq("Error:"))(
      queriesAndExpectedAnswers: (String, String)*): Unit = {

    val (queries, expectedAnswers) = queriesAndExpectedAnswers.unzip
    // Explicitly adds ENTER for each statement to make sure they are actually entered into the CLI.
    //明确地为每个语句添加ENTER，以确保它们实际输入到CLI中。
    val queriesString = queries.map(_ + "\n").mkString

    val command = {
      val cliScript = "../../bin/spark-sql".split("/").mkString(File.separator)
      val jdbcUrl = s"jdbc:derby:;databaseName=$metastorePath;create=true"
      s"""$cliScript
         |  --master local
         |  --hiveconf ${ConfVars.METASTORECONNECTURLKEY}=$jdbcUrl
         |  --hiveconf ${ConfVars.METASTOREWAREHOUSE}=$warehousePath
         |  --hiveconf ${ConfVars.SCRATCHDIR}=$scratchDirPath
       """.stripMargin.split("\\s+").toSeq ++ extraArgs
    }

    var next = 0
    val foundAllExpectedAnswers = Promise.apply[Unit]()
    val buffer = new ArrayBuffer[String]()
    val lock = new Object

    def captureOutput(source: String)(line: String): Unit = lock.synchronized {
      // This test suite sometimes gets extremely slow out of unknown reason on Jenkins.  Here we
      // add a timestamp to provide more diagnosis information.
      //这个测试套件有时会因为杰金斯的不明原因而变得非常慢,这里我们添加一个时间戳来提供更多的诊断信息
      buffer += s"${new Timestamp(new Date().getTime)} - $source> $line"

      // If we haven't found all expected answers and another expected answer comes up...
      //如果我们没有找到所有预期的答案，另一个预期的答案出现...
      if (next < expectedAnswers.size && line.startsWith(expectedAnswers(next))) {
        next += 1
        // If all expected answers have been found...
        //如果已经找到所有预期的答案...
        if (next == expectedAnswers.size) {
          foundAllExpectedAnswers.trySuccess(())
        }
      } else {
        errorResponses.foreach { r =>
          if (line.contains(r)) {
            foundAllExpectedAnswers.tryFailure(
              new RuntimeException(s"Failed with error line '$line'"))
          }
        }
      }
    }

    val process = new ProcessBuilder(command: _*).start()

    val stdinWriter = new OutputStreamWriter(process.getOutputStream)
    stdinWriter.write(queriesString)
    stdinWriter.flush()
    stdinWriter.close()

    new ProcessOutputCapturer(process.getInputStream, captureOutput("stdout")).start()
    new ProcessOutputCapturer(process.getErrorStream, captureOutput("stderr")).start()

    try {
      Await.result(foundAllExpectedAnswers.future, timeout)
    } catch { case cause: Throwable =>
      val message =
        s"""
           |=======================
           |CliSuite failure output
           |=======================
           |Spark SQL CLI command line: ${command.mkString(" ")}
           |Exception: $cause
           |Executed query $next "${queries(next)}",
           |But failed to capture expected output "${expectedAnswers(next)}" within $timeout.
           |
           |${buffer.mkString("\n")}
           |===========================
           |End CliSuite failure output
           |===========================
         """.stripMargin
      logError(message, cause)
      fail(message, cause)
    } finally {
      process.destroy()
    }
  }
  //简单的命令
  test("Simple commands") {
    val dataFilePath =
      Thread.currentThread().getContextClassLoader.getResource("data/files/small_kv.txt")

    runCliWithin(3.minute)(
      "CREATE TABLE hive_test(key INT, val STRING);"
        -> "OK",
      "SHOW TABLES;"
        -> "hive_test",
      s"LOAD DATA LOCAL INPATH '$dataFilePath' OVERWRITE INTO TABLE hive_test;"
        -> "OK",
      "CACHE TABLE hive_test;"
        -> "Time taken: ",
      "SELECT COUNT(*) FROM hive_test;"
        -> "5",
      "DROP TABLE hive_test;"
        -> "OK"
    )
  }
  //单命令与-e
  test("Single command with -e") {
    runCliWithin(2.minute, Seq("-e", "SHOW DATABASES;"))("" -> "OK")
  }
  //单一命令 - 数据库
  test("Single command with --database") {
    runCliWithin(2.minute)(
      "CREATE DATABASE hive_test_db;"
        -> "OK",
      "USE hive_test_db;"
        -> "OK",
      "CREATE TABLE hive_test(key INT, val STRING);"
        -> "OK",
      "SHOW TABLES;"
        -> "Time taken: "
    )

    runCliWithin(2.minute, Seq("--database", "hive_test_db", "-e", "SHOW TABLES;"))(
      ""
        -> "OK",
      ""
        -> "hive_test"
    )
  }
  //使用-jars中提供的SerDe的命令
  test("Commands using SerDe provided in --jars") {
    val jarFile =
      "../hive/src/test/resources/hive-hcatalog-core-0.13.1.jar"
        .split("/")
        .mkString(File.separator)

    val dataFilePath =
      Thread.currentThread().getContextClassLoader.getResource("data/files/small_kv.txt")

    runCliWithin(3.minute, Seq("--jars", s"$jarFile"))(
      """CREATE TABLE t1(key string, val string)
        |ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe';
      """.stripMargin
        -> "OK",
      "CREATE TABLE sourceTable (key INT, val STRING);"
        -> "OK",
      s"LOAD DATA LOCAL INPATH '$dataFilePath' OVERWRITE INTO TABLE sourceTable;"
        -> "OK",
      "INSERT INTO TABLE t1 SELECT key, val FROM sourceTable;"
        -> "Time taken:",
      "SELECT count(key) FROM t1;"
        -> "5",
      "DROP TABLE t1;"
        -> "OK",
      "DROP TABLE sourceTable;"
        -> "OK"
    )
  }
  //分析错误报告
  test("SPARK-11188 Analysis error reporting") {
    runCliWithin(timeout = 2.minute,
      errorResponses = Seq("AnalysisException"))(
      "select * from nonexistent_table;"
        -> "Error in query: no such table nonexistent_table;"
    )
  }
}
