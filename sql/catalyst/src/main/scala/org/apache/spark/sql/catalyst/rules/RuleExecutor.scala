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

package org.apache.spark.sql.catalyst.rules

import scala.collection.JavaConverters._

import com.google.common.util.concurrent.AtomicLongMap

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.catalyst.util.sideBySide

object RuleExecutor {
  protected val timeMap = AtomicLongMap.create[String]()

  /** Resets statistics about time spent running specific rules
    * 重置有关运行特定规则所花费时间的统计信息 */
  def resetTime(): Unit = timeMap.clear()

  /** Dump statistics about time spent running specific rules.
    * 转储有关运行特定规则所花费时间的统计信息 */
  def dumpTimeSpent(): String = {
    val map = timeMap.asMap().asScala
    val maxSize = map.keys.map(_.toString.length).max
    map.toSeq.sortBy(_._2).reverseMap { case (k, v) =>
      s"${k.padTo(maxSize, " ").mkString} $v"
    }.mkString("\n")
  }
}

abstract class RuleExecutor[TreeType <: TreeNode[_]] extends Logging {

  /**
   * An execution strategy for rules that indicates the maximum number of executions. If the
   * execution reaches fix point (i.e. converge) before maxIterations, it will stop.
    * 规则的执行策略,指示最大执行次数,如果执行在maxIterations之前到达定点(即收敛),它将停止。
   */
  abstract class Strategy { def maxIterations: Int }

  /** A strategy that only runs once. 只运行一次的策略*/
  case object Once extends Strategy { val maxIterations = 1 }

  /** A strategy that runs until fix point or maxIterations times, whichever comes first.
    * 一种运行到修正点或maxIterations时间的策略,以先到者为准*/
  case class FixedPoint(maxIterations: Int) extends Strategy

  /** A batch of rules. 一批规则*/
  protected case class Batch(name: String, strategy: Strategy, rules: Rule[TreeType]*)

  /** Defines a sequence of rule batches, to be overridden by the implementation.
    * 定义一系列规则批处理,由实现覆盖*/
  protected val batches: Seq[Batch]


  /**
   * Executes the batches of rules defined by the subclass. The batches are executed serially
   * using the defined execution strategy. Within each batch, rules are also executed serially.
    * 执行子类定义的批量规则,批处理使用定义的执行策略串行执行,在每个批次中,规则也是连续执行的
   */
  def execute(plan: TreeType): TreeType = {
    var curPlan = plan

    batches.foreach { batch =>
      val batchStartPlan = curPlan
      var iteration = 1
      var lastPlan = curPlan
      var continue = true

      // Run until fix point (or the max number of iterations as specified in the strategy.
      //运行直到修复点(或策略中指定的最大迭代次数)
      while (continue) {
        curPlan = batch.rules.foldLeft(curPlan) {
          case (plan, rule) =>
            val startTime = System.nanoTime()
            val result = rule(plan)
            val runTime = System.nanoTime() - startTime
            RuleExecutor.timeMap.addAndGet(rule.ruleName, runTime)

            if (!result.fastEquals(plan)) {
              logTrace(
                s"""
                  |=== Applying Rule ${rule.ruleName} ===
                  |${sideBySide(plan.treeString, result.treeString).mkString("\n")}
                """.stripMargin)
            }

            result
        }
        iteration += 1
        if (iteration > batch.strategy.maxIterations) {
          // Only log if this is a rule that is supposed to run more than once.
          //如果这是一个应该运行多次的规则,则仅记录
          if (iteration != 2) {
            logInfo(s"Max iterations (${iteration - 1}) reached for batch ${batch.name}")
          }
          continue = false
        }

        if (curPlan.fastEquals(lastPlan)) {
          logTrace(
            s"Fixed point reached for batch ${batch.name} after ${iteration - 1} iterations.")
          continue = false
        }
        lastPlan = curPlan
      }

      if (!batchStartPlan.fastEquals(curPlan)) {
        logDebug(
          s"""
          |=== Result of Batch ${batch.name} ===
          |${sideBySide(plan.treeString, curPlan.treeString).mkString("\n")}
        """.stripMargin)
      } else {
        logTrace(s"Batch ${batch.name} has no effect.")
      }
    }

    curPlan
  }
}
