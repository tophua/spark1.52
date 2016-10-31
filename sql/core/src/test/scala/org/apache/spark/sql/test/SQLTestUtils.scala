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

package org.apache.spark.sql.test

import java.io.File
import java.util.UUID

import scala.util.Try
import scala.language.implicitConversions

import org.apache.hadoop.conf.Configuration
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{DataFrame, SQLContext, SQLImplicits}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.util.Utils

/**
 * Helper trait that should be extended by all SQL test suites.
 * 辅助性的特点,应该对所有的SQL测试套件的扩展
 * This allows subclasses to plugin a custom [[SQLContext]]. It comes with test data
 * 这使得子类可以自定义[[SQLContext]]插件,它的测试数据准备以及所有隐式转换广泛使用的dataframes
 * prepared in advance as well as all implicit conversions used extensively by dataframes.
 * To use implicit methods, import `testImplicits._` instead of through the [[SQLContext]].
 * 使用隐式方法,
 *
 * Subclasses should *not* create [[SQLContext]]s in the test suite constructor, which is
 * prone to leaving multiple overlapping [[org.apache.spark.SparkContext]]s in the same JVM.
 */
private[sql] trait SQLTestUtils
  extends SparkFunSuite
  with BeforeAndAfterAll
  with SQLTestData { self =>

  protected def _sqlContext: SQLContext

  // Whether to materialize all test data before the first test is run
  //是否在运行第一个测试之前实现所有的测试数据
  private var loadTestDataBeforeTests = false

  // Shorthand for running a query using our SQLContext
  //使用SQLContext简写的运行查询
  protected lazy val sql = _sqlContext.sql _

  /**
   * A helper object for importing SQL implicits.
   * 导入SQL隐含着一个帮助对象
   *
   * Note that the alternative of importing `sqlContext.implicits._` is not possible here.
   * 注意:代替导入'sqlContext.implicits._`不可能在这里
   * This is because we create the [[SQLContext]] immediately before the first test is run,
   * 这是因为我们创建 SQLContext之前先测试运行
   * but the implicits import is needed in the constructor.
   * 但隐式在构造函数需要导入
   */
  protected object testImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = self._sqlContext
  }

  /**
   * Materialize the test data immediately after the [[SQLContext]] is set up.
   * 实现测试数据后立即设置[SQLContext]
   * This is necessary if the data is accessed by name but not through direct reference.
   * 这是必要的,如果数据访问的名称,但不通过直接引用
   */
  protected def setupTestData(): Unit = {
    loadTestDataBeforeTests = true
  }

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    if (loadTestDataBeforeTests) {
      loadTestData()
    }
  }

  /**
   * The Hadoop configuration used by the active [[SQLContext]].
   * 使用Hadoop的配置激活[SQLContext]
   */
  protected def configuration: Configuration = {
    _sqlContext.sparkContext.hadoopConfiguration
  }

  /**
   * Sets all SQL configurations specified in `pairs`, calls `f`, and then restore all SQL
   * configurations.
   * 将所有的SQL配置指定`对`,调用` F `，然后恢复所有SQL配置
   * 
   * @todo Probably this method should be moved to a more general place
   */
  protected def withSQLConf(pairs: (String, String)*)(f: => Unit): Unit = {
    val (keys, values) = pairs.unzip
    val currentValues = keys.map(key => Try(_sqlContext.conf.getConfString(key)).toOption)
    (keys, values).zipped.foreach(_sqlContext.conf.setConfString)
    try f finally {
      keys.zip(currentValues).foreach {
        case (key, Some(value)) => _sqlContext.conf.setConfString(key, value)
        case (key, None) => _sqlContext.conf.unsetConf(key)
      }
    }
  }

  /**
   * Generates a temporary path without creating the actual file/directory, then pass it to `f`. If
   * a file/directory is created there by `f`, it will be delete after `f` returns.
   *
   * @todo Probably this method should be moved to a more general place
   */
  protected def withTempPath(f: File => Unit): Unit = {
    val path = Utils.createTempDir()
    path.delete()
    try f(path) finally Utils.deleteRecursively(path)
  }

  /**
   * Creates a temporary directory, which is then passed to `f` and will be deleted after `f`
   * returns.
   *
   * @todo Probably this method should be moved to a more general place
   */
  protected def withTempDir(f: File => Unit): Unit = {
    val dir = Utils.createTempDir().getCanonicalFile
    try f(dir) finally Utils.deleteRecursively(dir)
  }

  /**
   * Drops temporary table `tableName` after calling `f`.
   */
  protected def withTempTable(tableNames: String*)(f: => Unit): Unit = {
    try f finally tableNames.foreach(_sqlContext.dropTempTable)
  }

  /**
   * Drops table `tableName` after calling `f`.
   */
  protected def withTable(tableNames: String*)(f: => Unit): Unit = {
    try f finally {
      tableNames.foreach { name =>
        _sqlContext.sql(s"DROP TABLE IF EXISTS $name")
      }
    }
  }

  /**
   * Creates a temporary database and switches current database to it before executing `f`.  This
   * database is dropped after `f` returns.
   */
  protected def withTempDatabase(f: String => Unit): Unit = {
    val dbName = s"db_${UUID.randomUUID().toString.replace('-', '_')}"

    try {
      _sqlContext.sql(s"CREATE DATABASE $dbName")
    } catch { case cause: Throwable =>
      fail("Failed to create temporary database", cause)
    }

    try f(dbName) finally _sqlContext.sql(s"DROP DATABASE $dbName CASCADE")
  }

  /**
   * Activates database `db` before executing `f`, then switches back to `default` database after
   * `f` returns.
   */
  protected def activateDatabase(db: String)(f: => Unit): Unit = {
    _sqlContext.sql(s"USE $db")
    try f finally _sqlContext.sql(s"USE default")
  }

  /**
   * Turn a logical plan into a [[DataFrame]]. This should be removed once we have an easier
   * way to construct [[DataFrame]] directly out of local data without relying on implicits.
   */
  protected implicit def logicalPlanToSparkQuery(plan: LogicalPlan): DataFrame = {
    DataFrame(_sqlContext, plan)
  }
}
