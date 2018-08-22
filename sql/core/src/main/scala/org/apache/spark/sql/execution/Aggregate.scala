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

package org.apache.spark.sql.execution

import java.util.HashMap

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.metric.SQLMetrics

/**
 * :: DeveloperApi ::
 * Groups input data by `groupingExpressions` and computes the `aggregateExpressions` for each
 * group.
  * 通过`groupingExpressions`对组输入数据,并为每个组计算`aggregateExpressions`
 *
 * @param partial if true then aggregation is done partially on local data without shuffling to
 *                ensure all values where `groupingExpressions` are equal are present.
  *                如果为true,则聚合部分地在本地数据上完成而不进行混洗以确保存在“groupingExpressions”相等的所有值
 * @param groupingExpressions expressions that are evaluated to determine grouping.
 * @param aggregateExpressions expressions that are computed for each group.
 * @param child the input data source.
 */
@DeveloperApi
case class Aggregate(
    partial: Boolean,
    groupingExpressions: Seq[Expression],
    aggregateExpressions: Seq[NamedExpression],
    child: SparkPlan)
  extends UnaryNode {

  override private[sql] lazy val metrics = Map(
    "numInputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of input rows"),
    "numOutputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of output rows"))

  override def requiredChildDistribution: List[Distribution] = {
    if (partial) {
      UnspecifiedDistribution :: Nil//列表结尾为Nil
    } else {//列表结尾为Nil
      if (groupingExpressions == Nil) {
        AllTuples :: Nil//列表结尾为Nil
      } else {//列表结尾为Nil
        ClusteredDistribution(groupingExpressions) :: Nil//列表结尾为Nil
      }
    }
  }

  override def output: Seq[Attribute] = aggregateExpressions.map(_.toAttribute)

  /**
   * An aggregate that needs to be computed for each row in a group.
    * 需要为组中的每一行计算的聚合
   *
   * @param unbound Unbound version of this aggregate, used for result substitution.
    *                此聚合的未绑定版本,用于结果替换
   * @param aggregate A bound copy of this aggregate used to create a new aggregation buffer.
    *                  用于创建新聚合缓冲区的此聚合的绑定副本
   * @param resultAttribute An attribute used to refer to the result of this aggregate in the final
   *                        output.
    *                        用于在最终输出中引用此聚合的结果的属性
   */
  case class ComputedAggregate(
      unbound: AggregateExpression1,
      aggregate: AggregateExpression1,
      resultAttribute: AttributeReference)

  /** A list of aggregates that need to be computed for each group.
    * 需要为每个组计算的聚合列表*/
  private[this] val computedAggregates = aggregateExpressions.flatMap { agg =>
    agg.collect {
      case a: AggregateExpression1 =>
        ComputedAggregate(
          a,
          BindReferences.bindReference(a, child.output),
          AttributeReference(s"aggResult:$a", a.dataType, a.nullable)())
    }
  }.toArray

  /** The schema of the result of all aggregate evaluations
    * 所有聚合评估结果的模式*/
  private[this] val computedSchema = computedAggregates.map(_.resultAttribute)

  /** Creates a new aggregate buffer for a group.
    * 为组创建新的聚合缓冲区 */
  private[this] def newAggregateBuffer(): Array[AggregateFunction1] = {
    val buffer = new Array[AggregateFunction1](computedAggregates.length)
    var i = 0
    while (i < computedAggregates.length) {
      buffer(i) = computedAggregates(i).aggregate.newInstance()
      i += 1
    }
    buffer
  }

  /** Named attributes used to substitute grouping attributes into the final result.
    * 用于将分组属性替换为最终结果的命名属性*/
  private[this] val namedGroups = groupingExpressions.map {
    case ne: NamedExpression => ne -> ne.toAttribute
    case e => e -> Alias(e, s"groupingExpr:$e")().toAttribute
  }

  /**
   * A map of substitutions that are used to insert the aggregate expressions and grouping
   * expression into the final result expression.
    * 用于插入聚合表达式并将表达式分组到最终结果表达式中的替换映射
   */
  private[this] val resultMap =
    (computedAggregates.map { agg => agg.unbound -> agg.resultAttribute } ++ namedGroups).toMap

  /**
   * Substituted version of aggregateExpressions expressions which are used to compute final
   * output rows given a group and the result of all aggregate computations.
    * aggregateExpressions表达式的替换版本,用于计算给定组的最终输出行和所有聚合计算的结果
   */
  private[this] val resultExpressions = aggregateExpressions.map { agg =>
    agg.transform {
      case e: Expression if resultMap.contains(e) => resultMap(e)
    }
  }

  protected override def doExecute(): RDD[InternalRow] = attachTree(this, "execute") {
    val numInputRows = longMetric("numInputRows")
    val numOutputRows = longMetric("numOutputRows")
    if (groupingExpressions.isEmpty) {
      child.execute().mapPartitions { iter =>
        val buffer = newAggregateBuffer()
        var currentRow: InternalRow = null
        while (iter.hasNext) {
          currentRow = iter.next()
          numInputRows += 1
          var i = 0
          while (i < buffer.length) {
            buffer(i).update(currentRow)
            i += 1
          }
        }
        val resultProjection = new InterpretedProjection(resultExpressions, computedSchema)
        val aggregateResults = new GenericMutableRow(computedAggregates.length)

        var i = 0
        while (i < buffer.length) {
          aggregateResults(i) = buffer(i).eval(EmptyRow)
          i += 1
        }

        numOutputRows += 1
        Iterator(resultProjection(aggregateResults))
      }
    } else {
      child.execute().mapPartitions { iter =>
        val hashTable = new HashMap[InternalRow, Array[AggregateFunction1]]
        val groupingProjection = new InterpretedMutableProjection(groupingExpressions, child.output)

        var currentRow: InternalRow = null
        while (iter.hasNext) {
          currentRow = iter.next()
          numInputRows += 1
          val currentGroup = groupingProjection(currentRow)
          var currentBuffer = hashTable.get(currentGroup)
          if (currentBuffer == null) {
            currentBuffer = newAggregateBuffer()
            hashTable.put(currentGroup.copy(), currentBuffer)
          }

          var i = 0
          while (i < currentBuffer.length) {
            currentBuffer(i).update(currentRow)
            i += 1
          }
        }

        new Iterator[InternalRow] {
          private[this] val hashTableIter = hashTable.entrySet().iterator()
          private[this] val aggregateResults = new GenericMutableRow(computedAggregates.length)
          private[this] val resultProjection =
            new InterpretedMutableProjection(
              resultExpressions, computedSchema ++ namedGroups.map(_._2))
          private[this] val joinedRow = new JoinedRow

          override final def hasNext: Boolean = hashTableIter.hasNext

          override final def next(): InternalRow = {
            val currentEntry = hashTableIter.next()
            val currentGroup = currentEntry.getKey
            val currentBuffer = currentEntry.getValue
            numOutputRows += 1

            var i = 0
            while (i < currentBuffer.length) {
              // Evaluating an aggregate buffer returns the result.  No row is required since we
              // already added all rows in the group using update.
              aggregateResults(i) = currentBuffer(i).eval(EmptyRow)
              i += 1
            }
            resultProjection(joinedRow(aggregateResults, currentGroup))
          }
        }
      }
    }
  }
}
