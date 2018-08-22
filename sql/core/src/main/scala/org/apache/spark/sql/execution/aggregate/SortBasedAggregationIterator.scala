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

package org.apache.spark.sql.execution.aggregate

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression2, AggregateFunction2}
import org.apache.spark.sql.execution.metric.LongSQLMetric
import org.apache.spark.unsafe.KVIterator

/**
 * An iterator used to evaluate [[AggregateFunction2]]. It assumes the input rows have been
 * sorted by values of [[groupingKeyAttributes]].
  * 用于评估[[AggregateFunction2]]的迭代器,它假定输入行已按[[groupingKeyAttributes]]的值排序
 */
class SortBasedAggregationIterator(
    groupingKeyAttributes: Seq[Attribute],
    valueAttributes: Seq[Attribute],
    inputKVIterator: KVIterator[InternalRow, InternalRow],
    nonCompleteAggregateExpressions: Seq[AggregateExpression2],
    nonCompleteAggregateAttributes: Seq[Attribute],
    completeAggregateExpressions: Seq[AggregateExpression2],
    completeAggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => (() => MutableProjection),
    outputsUnsafeRows: Boolean,
    numInputRows: LongSQLMetric,
    numOutputRows: LongSQLMetric)
  extends AggregationIterator(
    groupingKeyAttributes,
    valueAttributes,
    nonCompleteAggregateExpressions,
    nonCompleteAggregateAttributes,
    completeAggregateExpressions,
    completeAggregateAttributes,
    initialInputBufferOffset,
    resultExpressions,
    newMutableProjection,
    outputsUnsafeRows) {

  override protected def newBuffer: MutableRow = {
    val bufferSchema = allAggregateFunctions.flatMap(_.bufferAttributes)
    val bufferRowSize: Int = bufferSchema.length

    val genericMutableBuffer = new GenericMutableRow(bufferRowSize)
    val useUnsafeBuffer = bufferSchema.map(_.dataType).forall(UnsafeRow.isMutable)

    val buffer = if (useUnsafeBuffer) {
      val unsafeProjection =
        UnsafeProjection.create(bufferSchema.map(_.dataType))
      unsafeProjection.apply(genericMutableBuffer)
    } else {
      genericMutableBuffer
    }
    initializeBuffer(buffer)
    buffer
  }

  ///////////////////////////////////////////////////////////////////////////
  // Mutable states for sort based aggregation.基于排序的聚合的可变状态
  ///////////////////////////////////////////////////////////////////////////

  // The partition key of the current partition.
  //当前分区的分区键
  private[this] var currentGroupingKey: InternalRow = _

  // The partition key of next partition.
  //下一个分区的分区键
  private[this] var nextGroupingKey: InternalRow = _

  // The first row of next partition.
  //下一个分区的第一行
  private[this] var firstRowInNextGroup: InternalRow = _

  // Indicates if we has new group of rows from the sorted input iterator
  //指示我们是否从排序的输入迭代器中新建了一组行
  private[this] var sortedInputHasNewGroup: Boolean = false

  // The aggregation buffer used by the sort-based aggregation.
  //基于排序的聚合使用的聚合缓冲区
  private[this] val sortBasedAggregationBuffer: MutableRow = newBuffer

  /** Processes rows in the current group. It will stop when it find a new group.
    * 处理当前组中的行,它会在找到新组时停止*/
  protected def processCurrentSortedGroup(): Unit = {
    currentGroupingKey = nextGroupingKey
    // Now, we will start to find all rows belonging to this group.
    //现在,我们将开始查找属于该组的所有行
    // We create a variable to track if we see the next group.
    //我们创建一个变量来跟踪我们是否看到下一组
    var findNextPartition = false
    // firstRowInNextGroup is the first row of this group. We first process it.
    //firstRowInNextGroup是该组的第一行,我们先处理它
    processRow(sortBasedAggregationBuffer, firstRowInNextGroup)

    // The search will stop when we see the next group or there is no
    // input row left in the iter.
    //当我们看到下一组或者iter中没有剩下输入行时,搜索将停止
    var hasNext = inputKVIterator.next()
    while (!findNextPartition && hasNext) {
      // Get the grouping key.
      val groupingKey = inputKVIterator.getKey
      val currentRow = inputKVIterator.getValue
      numInputRows += 1

      // Check if the current row belongs the current input row.
      //检查当前行是否属于当前输入行
      if (currentGroupingKey == groupingKey) {
        processRow(sortBasedAggregationBuffer, currentRow)

        hasNext = inputKVIterator.next()
      } else {
        // We find a new group.
        findNextPartition = true
        nextGroupingKey = groupingKey.copy()
        firstRowInNextGroup = currentRow.copy()
      }
    }
    // We have not seen a new group. It means that there is no new row in the input
    // iter. The current group is the last group of the iter.
    if (!findNextPartition) {
      sortedInputHasNewGroup = false
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Iterator's public methods迭代器的公共方法
  ///////////////////////////////////////////////////////////////////////////

  override final def hasNext: Boolean = sortedInputHasNewGroup

  override final def next(): InternalRow = {
    if (hasNext) {
      // Process the current group.
      //处理当前组
      processCurrentSortedGroup()
      // Generate output row for the current group.
      //生成当前组的输出行
      val outputRow = generateOutput(currentGroupingKey, sortBasedAggregationBuffer)
      // Initialize buffer values for the next group.
      //初始化下一组的缓冲区值
      initializeBuffer(sortBasedAggregationBuffer)
      numOutputRows += 1
      outputRow
    } else {
      // no more result
      throw new NoSuchElementException
    }
  }

  protected def initialize(): Unit = {
    if (inputKVIterator.next()) {
      initializeBuffer(sortBasedAggregationBuffer)

      nextGroupingKey = inputKVIterator.getKey().copy()
      firstRowInNextGroup = inputKVIterator.getValue().copy()
      numInputRows += 1
      sortedInputHasNewGroup = true
    } else {
      // This inputIter is empty.
      sortedInputHasNewGroup = false
    }
  }

  initialize()

  def outputForEmptyGroupingKeyWithoutInput(): InternalRow = {
    initializeBuffer(sortBasedAggregationBuffer)
    generateOutput(new GenericInternalRow(0), sortBasedAggregationBuffer)
  }
}

object SortBasedAggregationIterator {
  // scalastyle:off
  def createFromInputIterator(
      groupingExprs: Seq[NamedExpression],
      nonCompleteAggregateExpressions: Seq[AggregateExpression2],
      nonCompleteAggregateAttributes: Seq[Attribute],
      completeAggregateExpressions: Seq[AggregateExpression2],
      completeAggregateAttributes: Seq[Attribute],
      initialInputBufferOffset: Int,
      resultExpressions: Seq[NamedExpression],
      newMutableProjection: (Seq[Expression], Seq[Attribute]) => (() => MutableProjection),
      newProjection: (Seq[Expression], Seq[Attribute]) => Projection,
      inputAttributes: Seq[Attribute],
      inputIter: Iterator[InternalRow],
      outputsUnsafeRows: Boolean,
      numInputRows: LongSQLMetric,
      numOutputRows: LongSQLMetric): SortBasedAggregationIterator = {
    val kvIterator = if (UnsafeProjection.canSupport(groupingExprs)) {
      AggregationIterator.unsafeKVIterator(
        groupingExprs,
        inputAttributes,
        inputIter).asInstanceOf[KVIterator[InternalRow, InternalRow]]
    } else {
      AggregationIterator.kvIterator(groupingExprs, newProjection, inputAttributes, inputIter)
    }

    new SortBasedAggregationIterator(
      groupingExprs.map(_.toAttribute),
      inputAttributes,
      kvIterator,
      nonCompleteAggregateExpressions,
      nonCompleteAggregateAttributes,
      completeAggregateExpressions,
      completeAggregateAttributes,
      initialInputBufferOffset,
      resultExpressions,
      newMutableProjection,
      outputsUnsafeRows,
      numInputRows,
      numOutputRows)
  }
  // scalastyle:on
}
