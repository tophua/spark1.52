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

package org.apache.spark

// scalastyle:off
import org.scalatest.{FunSuite, Outcome}

/**
 * Base abstract class for all unit tests in Spark for handling common functionality.
  * Spark中用于处理常用功能的所有单元测试的基本抽象类
 */
private[spark] abstract class SparkFunSuite extends FunSuite with Logging {
// scalastyle:on

  /**
   * Log the suite name and the test name before and after each test.
    * 在每个测试之前和之后记录套件名称和测试名称
   *
   * Subclasses should never override this method. If they wish to run
   * custom code before and after each test, they should mix in the
   * {{org.scalatest.BeforeAndAfter}} trait instead
    * 子类不应该覆盖此方法,如果他们希望在每个测试之前和之后运行自定义代码,
    * 那么它们应该混合在{{org.scalatest.BeforeAndAfter}} trait中
   */
  final protected override def withFixture(test: NoArgTest): Outcome = {
    val testName = test.text
    val suiteName = this.getClass.getName
    val shortSuiteName = suiteName.replaceAll("org.apache.spark", "o.a.s")
    try {
      logInfo(s"\n\n===== TEST OUTPUT FOR $shortSuiteName: '$testName' =====\n")
      test()//没有参数方法
    } finally {
      logInfo(s"\n\n===== FINISHED $shortSuiteName: '$testName' =====\n")
    }
  }

}
