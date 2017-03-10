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
  /**
   * =>符号可以看做是创建函数实例的语法糖,
   * val sql: String => DataFrame 函数类型
   * 例如： String => DataFrame 表示一个函数的输入参数类型是String,返回值类型是DataFrame
   */
  
  
  protected lazy val sql = _sqlContext.sql _

  /**
   * A helper object for importing SQL implicits.
   * 用于导入SQL隐式的帮助程序对象
   *
   * Note that the alternative of importing `sqlContext.implicits._` is not possible here.
   * 注意,导入`sqlContext.implicits._`的替代方法在这里是不可能的
   * This is because we create the [[SQLContext]] immediately before the first test is run,
   * 这是因为我们在运行第一个测试之前立即创建[[SQLContext]]
   * but the implicits import is needed in the constructor.
   * 但在构造函数中需要输入implicits
   */
  protected object testImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = self._sqlContext
  }

  /**
   * Materialize the test data immediately after the [[SQLContext]] is set up.
   * 实现测试数据后立即设置[SQLContext]
   * This is necessary if the data is accessed by name but not through direct reference.
   * 如果通过名称而不是通过直接引用访问数据,则这是必需的
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
   * 设置在“pair”中指定的所有SQL配置,调用“f”,然后还原所有SQL配置
   * 
   * @todo Probably this method should be moved to a more general place
   * 可能这个方法应该移动到更一般的地方
   */
  protected def withSQLConf(pairs: (String, String)*)(f: => Unit): Unit = {
    //unzip把对偶列表拆分还原为两个列表,其中一个列表由每对对偶的第一个元素组成,另一个由第二个元素组成
    val (keys, values) = pairs.unzip
    //Try[A]则表示一种计算:这种计算在成功的情况下,返回类型为 A 的值,在出错的情况下,返回 Throwable 
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
   * 生成一个临时路径,而不创建实际的文件/目录,然后把它传给F,如果一个文件/目录是由F创建,它将被F删除后返回
   *
   * @todo Probably this method should be moved to a more general place
   * 也许这个方法应该转移到一个更普遍的地方
   */
  protected def withTempPath(f: File => Unit): Unit = {
    //删除一个临时目录
    val path = Utils.createTempDir()
    //删除文件
    path.delete()
    try{
      f(path)
    }
    finally{
      //递归删除
    Utils.deleteRecursively(path)
    }
    
  }

  /**
   * Creates a temporary directory, which is then passed to `f` and will be deleted after `f`
   * returns.
   * 创建临时目录,然后传递给f,将被删除后f返回
   * @todo Probably this method should be moved to a more general place
   * 也许这个方法应该转移到一个更普遍的地方
   */
  protected def withTempDir(f: File => Unit): Unit = {
    val dir = Utils.createTempDir().getCanonicalFile
    try f(dir) finally Utils.deleteRecursively(dir)
  }

  /**
   * Drops temporary table `tableName` after calling `f`.
   * 调用后'f',删除临时表'tableName'
   * 注意:f是定义的方法
   */
  protected def withTempTable(tableNames: String*)(f: => Unit): Unit = {
    try f finally tableNames.foreach(_sqlContext.dropTempTable)
  }

  /**
   * Drops table `tableName` after calling `f`.
   * 调用f后删除表`tableName`,定义一个柯里化高阶函数
   */
  protected def withTable(tableNames: String*)(f: => Unit): Unit = {
    try f finally {
      //迭代tableNames
      tableNames.foreach { name =>
        val droptable=s"DROP TABLE IF EXISTS $name"
        _sqlContext.sql(droptable)
      }
    }
  }

  /**
   * Creates a temporary database and switches current database to it before executing `f`.  This
   * database is dropped after `f` returns.
   * 创建一个临时数据库,并将当前数据库转换为它之前执行的f,这个数据库被删除f后返回。
   */
  protected def withTempDatabase(f: String => Unit): Unit = {
    //获得随机数据库名
    val dbName = s"db_${UUID.randomUUID().toString.replace('-', '_')}"

    try {
      //创建数据库
      _sqlContext.sql(s"CREATE DATABASE $dbName")
    } catch { case cause: Throwable =>//失败抛出异常
      fail("Failed to create temporary database", cause)
    }
    //执行f函数,最后级联(CASCADE)删除数据库dbName
    try f(dbName) finally _sqlContext.sql(s"DROP DATABASE $dbName CASCADE")
  }

  /**
   * Activates database `db` before executing `f`, then switches back to `default` database after
   * `f` returns.
   * 激活数据库db之前执行f,然后切换回默认的数据库后f返回
   */
  protected def activateDatabase(db: String)(f: => Unit): Unit = {
    //切换数据db
    _sqlContext.sql(s"USE $db")
    //切换默认的数据库
    try f finally _sqlContext.sql(s"USE default")
  }

  /**
   * Turn a logical plan into a [[DataFrame]]. This should be removed once we have an easier
   * way to construct [[DataFrame]] directly out of local data without relying on implicits.
   * 把一个逻辑计划为DataFrame,这应该被删除一次,我们有一个更容易,
   * 如何构建DataFrame直接从本地数据不依赖于隐式转换
   */
  protected implicit def logicalPlanToSparkQuery(plan: LogicalPlan): DataFrame = {
    DataFrame(_sqlContext, plan)
  }
}
