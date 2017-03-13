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

package org.apache.spark.sql

import java.util.{Locale, TimeZone}

import scala.collection.JavaConversions._

import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.columnar.InMemoryRelation

class QueryTest extends PlanTest {

  // Timezone is fixed to America/Los_Angeles for those timezone sensitive tests (timestamp_*)
  //美国/洛杉矶,对于时区敏感重新设置时区
  TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"))
  // Add Locale setting
  //添加本地设置
  Locale.setDefault(Locale.US)

  /**
   * Runs the plan and makes sure the answer contains all of the keywords, or the
   * none of keywords are listed in the answer
   * 运行计划,并确保答案包含所有关键字,或没有一个关键字列在答案中
   * @param df the [[DataFrame]] to be executed
   * @param exists true for make sure the keywords are listed in the output, otherwise
   * 										 to make sure none of the keyword are not listed in the output
   * 				     存在true,以确保关键字列在输出中,否则确保关键字没有列在输出中
   * @param keywords keyword in string array 字符串数组中的关键字
   */
  def checkExistence(df: DataFrame, exists: Boolean, keywords: String*) {
    val outputs = df.collect().map(_.mkString).mkString
    for (key <- keywords) {
      if (exists) {//是否包含关键字
        //doesn't exist in result 在结果中不存在
        assert(outputs.contains(key), s"Failed for $df ($key doesn't exist in result)")
      } else {
        assert(!outputs.contains(key), s"Failed for $df ($key existed in the result)")
      }
    }
  }

  /**
   * Runs the plan and makes sure the answer matches the expected result.
   * 运行计划并确保答案与预期结果匹配
   * @param df the [[DataFrame]] to be executed 被执行DataFrame
   * @param expectedAnswer the expected result in a [[Seq]] of [[Row]]s.
   * 											在[[Row]]的[[Seq]]中的预期结果
   */
  protected def checkAnswer(df: DataFrame, expectedAnswer: Seq[Row]): Unit = {
    QueryTest.checkAnswer(df, expectedAnswer) match {
      case Some(errorMessage) => fail(errorMessage)
      case None =>
    }
  }

  protected def checkAnswer(df: DataFrame, expectedAnswer: Row): Unit = {
    checkAnswer(df, Seq(expectedAnswer))
  }

  protected def checkAnswer(df: DataFrame, expectedAnswer: DataFrame): Unit = {
    checkAnswer(df, expectedAnswer.collect())
  }

  /**
   * Asserts that a given [[DataFrame]] will be executed using the given number of cached results.
   * 断言一个给定的DataFrame, 将使用给定数量的缓存结果执行
   * 
   */
  def assertCached(query: DataFrame, numCachedTables: Int = 1): Unit = {
    //缓存结果数据
    val planWithCaching = query.queryExecution.withCachedData
    val cachedData = planWithCaching collect {
      //匹配内存关系
      case cached: InMemoryRelation => cached
    }

    assert(
      cachedData.size == numCachedTables,
      s"Expected query to contain $numCachedTables, but it actually had ${cachedData.size}\n" +
        planWithCaching)
  }
}

object QueryTest {
  /**
   * Runs the plan and makes sure the answer matches the expected result.
   * 运行计划并确保答案与预期结果匹配
   * If there was exception during the execution or the contents of the DataFrame does not  
   * match the expected result, an error message will be returned. Otherwise, a [[None]] will
   * be returned.
   * 如果在执行期间有异常或者DataFrame的内容与预期结果不匹配,将返回一条错误消息,否则,将返回[[无]]
   * @param df the [[DataFrame]] to be executed
   * @param expectedAnswer the expected result in a [[Seq]] of [[Row]]s.
   * 				在[[Row]]的[[Seq]]中的预期结果
   */
  def checkAnswer(df: DataFrame, expectedAnswer: Seq[Row]): Option[String] = {
    //nonEmpty测试容器是否包含元素
    val isSorted = df.logicalPlan.collect { case s: logical.Sort => s }.nonEmpty
    // We need to call prepareRow recursively to handle schemas with struct types.
    //我们需要递归调用prepareRow来处理带有struct类型的模式
    def prepareRow(row: Row): Row = {
      Row.fromSeq(row.toSeq.map {
        case null => null
        case d: java.math.BigDecimal => BigDecimal(d)
        // Convert array to Seq for easy equality check.
        //转换数组到序列,简单的等式检查
        case b: Array[_] => b.toSeq
        case r: Row => prepareRow(r)
        case o => o
      })
    }
    //准备回答
    def prepareAnswer(answer: Seq[Row]): Seq[Row] = {
      //Converts data to types that we can do equality comparison using Scala collections.
      //将数据转换为类型,我们可以使用Scala集合进行等式比较。
      // For BigDecimal type, the Scala type has a better definition of equality test (similar to
      // Java's java.math.BigDecimal.compareTo).
      //Scala类型具有平等性测试更好的定义
      // For binary arrays, we convert it to Seq to avoid of calling java.util.Arrays.equals for
      // equality test.
      val converted: Seq[Row] = answer.map(prepareRow)
      //
      if (!isSorted) converted.sortBy(_.toString()) else converted
    }
    //答案
    val sparkAnswer = try df.collect().toSeq catch {
      //如果异常返回
      case e: Exception =>
        val errorMessage =
          s"""
            |Exception thrown while executing query:
            |${df.queryExecution}
            |== Exception ==
            |$e
            |${org.apache.spark.sql.catalyst.util.stackTraceToString(e)}
          """.stripMargin
        return Some(errorMessage)
    }
    
    if (prepareAnswer(expectedAnswer) != prepareAnswer(sparkAnswer)) {
      val errorMessage =
        s"""
        |Results do not match for query:
        |${df.queryExecution}
        |== Results ==
        |${sideBySide(
          s"== Correct Answer - ${expectedAnswer.size} ==" +:
            prepareAnswer(expectedAnswer).map(_.toString()),
          s"== Spark Answer - ${sparkAnswer.size} ==" +:
            prepareAnswer(sparkAnswer).map(_.toString())).mkString("\n")}
      """.stripMargin
      return Some(errorMessage)
    }

    return None
  }

  def checkAnswer(df: DataFrame, expectedAnswer: java.util.List[Row]): String = {
    checkAnswer(df, expectedAnswer.toSeq) match {
      case Some(errorMessage) => errorMessage
      case None => null
    }
  }
}
