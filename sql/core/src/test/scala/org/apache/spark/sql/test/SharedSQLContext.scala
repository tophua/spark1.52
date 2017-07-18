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

import org.apache.spark.sql.{ColumnName, SQLContext}


/**
 * Helper trait for SQL test suites where all tests share a single [[TestSQLContext]].
 * SQL的测试套件,测试共享一个单一的testsqlcontext辅助接口
 */
trait SharedSQLContext extends SQLTestUtils {

  /**
   * The [[TestSQLContext]] to use for all tests in this suite.
   * testsqlcontext 使用的所有测试的套件
   * By default, the underlying [[org.apache.spark.SparkContext]] will be run in local
   * mode with the default test configurations.
   * 将在本地模式下运行,用默认的测试配置
   */
  private var _ctx: TestSQLContext = null

  /**
   * The [[TestSQLContext]] to use for all tests in this suite.
   * testsqlcontext的使用的所有测试套件
   */
  protected def ctx: TestSQLContext = _ctx
  protected def sqlContext: TestSQLContext = _ctx
  protected override def _sqlContext: SQLContext = _ctx

  /**
   * Initialize the [[TestSQLContext]].
   * 初始化TestSQLContext
   */
  protected override def beforeAll(): Unit = {
    if (_ctx == null) {
      _ctx = new TestSQLContext
    }
    // Ensure we have initialized the context before calling parent code
    //确保我们在调用父代码之前已经初始化了上下文
    super.beforeAll()
  }

  /**
   * Stop the underlying [[org.apache.spark.SparkContext]], if any.
   * 暂停下列SparkContext,如果任何
   */
  protected override def afterAll(): Unit = {
    try {
      if (_ctx != null) {
        _ctx.sparkContext.stop()
        _ctx = null
      }
    } finally {
      super.afterAll()
    }
  }

  /**
   * Converts $"col name" into an Column.
   * 转换$"col name"到Column
   * @since 1.3.0
   */
  // This must be duplicated here to preserve binary compatibility with Spark < 1.5.
  //必须在这里复制以保持与Spark小于1.5相容兼性
  implicit class StringToColumn(val sc: StringContext) {
    def $(args: Any*): ColumnName = {
      new ColumnName(sc.s(args: _*))
    }
  }
}
