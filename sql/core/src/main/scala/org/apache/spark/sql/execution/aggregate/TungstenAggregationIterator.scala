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

import org.apache.spark.unsafe.KVIterator
import org.apache.spark.{InternalAccumulator, Logging, SparkEnv, TaskContext}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.{UnsafeKVExternalSorter, UnsafeFixedWidthAggregationMap}
import org.apache.spark.sql.execution.metric.LongSQLMetric
import org.apache.spark.sql.types.StructType

/**
 * An iterator used to evaluate aggregate functions. It operates on [[UnsafeRow]]s.
  * 用于评估聚合函数的迭代器,它在[[UnsafeRow]]上运行
 *
 * This iterator first uses hash-based aggregation to process input rows. It uses
 * a hash map to store groups and their corresponding aggregation buffers. If we
 * this map cannot allocate memory from [[org.apache.spark.shuffle.ShuffleMemoryManager]],
  * 此迭代器首先使用基于散列的聚合来处理输入行,它使用哈希映射来存储组及其相应的聚合缓冲区,
  * 如果我们这个Map不能从[[org.apache.spark.shuffle.ShuffleMemoryManager]]中分配内存,
 * it switches to sort-based aggregation. The process of the switch has the following step:
  * 它切换到基于排序的聚合,切换过程包括以下步骤：
 *  - Step 1: Sort all entries of the hash map based on values of grouping expressions and
 *            spill them to disk.
  *            根据分组表达式的值对哈希映射的所有条目进行排序,并将它们溢出到磁盘
 *  - Step 2: Create a external sorter based on the spilled sorted map entries.
  *           根据溢出的有序映射条目创建外部分拣机
 *  - Step 3: Redirect all input rows to the external sorter.将所有输入行重定向到外部分拣机
 *  - Step 4: Get a sorted [[KVIterator]] from the external sorter.从外部分拣机获取已排序的[[KVIterator]]。
 *  - Step 5: Initialize sort-based aggregation.初始化基于排序的聚合
 * Then, this iterator works in the way of sort-based aggregation.
 *
 * The code of this class is organized as follows:
  * 该类的代码组织如下：
 *  - Part 1: Initializing aggregate functions.初始化聚合函数
 *  - Part 2: Methods and fields used by setting aggregation buffer values,
 *            processing input rows from inputIter, and generating output
 *            rows.
  *            通过设置聚合缓冲区值,处理inputIter中的输入行以及生成输出行而使用的方法和字段
 *  - Part 3: Methods and fields used by hash-based aggregation.
  *           基于散列的聚合使用的方法和字段
 *  - Part 4: Methods and fields used when we switch to sort-based aggregation.
  *           切换到基于排序的聚合时使用的方法和字段
 *  - Part 5: Methods and fields used by sort-based aggregation.
  *           基于排序的聚合使用的方法和字段
 *  - Part 6: Loads input and process input rows.
  *           加载输入和处理输入行
 *  - Part 7: Public methods of this iterator.
  *           这个迭代器的公共方法
 *  - Part 8: A utility function used to generate a result when there is no
 *            input and there is no grouping expression.
  *            一个效用函数,用于在没有输入且没有分组表达式时生成结果
 *
 * @param groupingExpressions
 *   expressions for grouping keys 用于分组键的表达式
 * @param nonCompleteAggregateExpressions
 *   [[AggregateExpression2]] containing [[AggregateFunction2]]s with mode [[Partial]],
 *   [[PartialMerge]], or [[Final]].
 * @param completeAggregateExpressions
 *   [[AggregateExpression2]] containing [[AggregateFunction2]]s with mode [[Complete]].
 * @param initialInputBufferOffset
 *   If this iterator is used to handle functions with mode [[PartialMerge]] or [[Final]].
 *   The input rows have the format of `grouping keys + aggregation buffer`.
 *   This offset indicates the starting position of aggregation buffer in a input row.
 * @param resultExpressions
 *   expressions for generating output rows.用于生成输出行的表达式
 * @param newMutableProjection
 *   the function used to create mutable projections.用于创建可变投影的函数
 * @param originalInputAttributes
 *   attributes of representing input rows from `inputIter`.
  *   表示来自`inputIter`的输入行的属性
 */
class TungstenAggregationIterator(
    groupingExpressions: Seq[NamedExpression],
    nonCompleteAggregateExpressions: Seq[AggregateExpression2],
    completeAggregateExpressions: Seq[AggregateExpression2],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => (() => MutableProjection),
    originalInputAttributes: Seq[Attribute],
    testFallbackStartsAt: Option[Int],
    numInputRows: LongSQLMetric,
    numOutputRows: LongSQLMetric)
  extends Iterator[UnsafeRow] with Logging {

  // The parent partition iterator, to be initialized later in `start`
  //父分区迭代器,稍后在`start`中初始化
  private[this] var inputIter: Iterator[InternalRow] = null

  ///////////////////////////////////////////////////////////////////////////
  // Part 1: Initializing aggregate functions.初始化聚合函数
  ///////////////////////////////////////////////////////////////////////////

  // A Seq containing all AggregateExpressions.
  //包含所有AggregateExpressions的Seq
  // It is important that all AggregateExpressions with the mode Partial, PartialMerge or Final
  // are at the beginning of the allAggregateExpressions.
  //重要的是所有具有Partial,PartialMerge或Final模式的AggregateExpress都位于allAggregateExpressions的开头。
  private[this] val allAggregateExpressions: Seq[AggregateExpression2] =
    nonCompleteAggregateExpressions ++ completeAggregateExpressions

  // Check to make sure we do not have more than three modes in our AggregateExpressions.
  //检查以确保我们的AggregateExpressions中没有三种以上的模式
  // If we have, users are hitting a bug and we throw an IllegalStateException.
  //如果我们有,用户正在遇到错误,我们抛出IllegalStateException
  if (allAggregateExpressions.map(_.mode).distinct.length > 2) {
    throw new IllegalStateException(
      s"$allAggregateExpressions should have no more than 2 kinds of modes.")
  }

  //
  // The modes of AggregateExpressions. Right now, we can handle the following mode:
  //  AggregateExpressions的模式, 现在,我们可以处理以下模式：
  //  - Partial-only:
  //      All AggregateExpressions have the mode of Partial.
  //      所有聚合表达式都具有“部分”模式。
  //      For this case, aggregationMode is (Some(Partial), None).
  //  - PartialMerge-only:
  //      All AggregateExpressions have the mode of PartialMerge).
  //      For this case, aggregationMode is (Some(PartialMerge), None).
  //  - Final-only:
  //      All AggregateExpressions have the mode of Final.
  //      For this case, aggregationMode is (Some(Final), None).
  //  - Final-Complete:
  //      Some AggregateExpressions have the mode of Final and
  //      others have the mode of Complete. For this case,
  //      aggregationMode is (Some(Final), Some(Complete)).
  //  - Complete-only:
  //      nonCompleteAggregateExpressions is empty and we have AggregateExpressions
  //      with mode Complete in completeAggregateExpressions. For this case,
  //      aggregationMode is (None, Some(Complete)).
  //  - Grouping-only:
  //      There is no AggregateExpression. For this case, AggregationMode is (None,None).
  //
  private[this] var aggregationMode: (Option[AggregateMode], Option[AggregateMode]) = {
    nonCompleteAggregateExpressions.map(_.mode).distinct.headOption ->
      completeAggregateExpressions.map(_.mode).distinct.headOption
  }

  // All aggregate functions. TungstenAggregationIterator only handles AlgebraicAggregates.
  // If there is any functions that is not an AlgebraicAggregate, we throw an
  // IllegalStateException.
  //所有聚合函数,TungstenAggregationIterator只处理AlgebraicAggregates,
  //如果有任何函数不是AlgebraicAggregate,我们抛出IllegalStateException
  private[this] val allAggregateFunctions: Array[AlgebraicAggregate] = {
    if (!allAggregateExpressions.forall(_.aggregateFunction.isInstanceOf[AlgebraicAggregate])) {
      throw new IllegalStateException(
        "Only AlgebraicAggregates should be passed in TungstenAggregationIterator.")
    }

    allAggregateExpressions
      .map(_.aggregateFunction.asInstanceOf[AlgebraicAggregate])
      .toArray
  }

  ///////////////////////////////////////////////////////////////////////////
  // Part 2: Methods and fields used by setting aggregation buffer values,
  //         processing input rows from inputIter, and generating output
  //         rows.
  //通过设置聚合缓冲区值,处理inputIter中的输入行以及生成输出行而使用的方法和字段
  ///////////////////////////////////////////////////////////////////////////

  // The projection used to initialize buffer values.
  private[this] val algebraicInitialProjection: MutableProjection = {
    val initExpressions = allAggregateFunctions.flatMap(_.initialValues)
    newMutableProjection(initExpressions, Nil)()
  }

  // Creates a new aggregation buffer and initializes buffer values.
  //创建新的聚合缓冲区并初始化缓冲区值
  // This functions should be only called at most three times (when we create the hash map,
  // when we switch to sort-based aggregation, and when we create the re-used buffer for
  // sort-based aggregation).
  //这个函数最多只能被调用三次(当我们创建哈希映射时,当我们切换到基于排序的聚合时,以及当我们为基于排序的聚合创建重用缓冲区时)
  private def createNewAggregationBuffer(): UnsafeRow = {
    val bufferSchema = allAggregateFunctions.flatMap(_.bufferAttributes)
    val bufferRowSize: Int = bufferSchema.length

    val genericMutableBuffer = new GenericMutableRow(bufferRowSize)
    val unsafeProjection =
      UnsafeProjection.create(bufferSchema.map(_.dataType))
    val buffer = unsafeProjection.apply(genericMutableBuffer)
    algebraicInitialProjection.target(buffer)(EmptyRow)
    buffer
  }

  // Creates a function used to process a row based on the given inputAttributes.
  //创建一个用于根据给定的inputAttributes处理行的函数
  private def generateProcessRow(
      inputAttributes: Seq[Attribute]): (UnsafeRow, InternalRow) => Unit = {

    val aggregationBufferAttributes = allAggregateFunctions.flatMap(_.bufferAttributes)
    val joinedRow = new JoinedRow()

    aggregationMode match {
      // Partial-only
      case (Some(Partial), None) =>
        val updateExpressions = allAggregateFunctions.flatMap(_.updateExpressions)
        val algebraicUpdateProjection =
          newMutableProjection(updateExpressions, aggregationBufferAttributes ++ inputAttributes)()

        (currentBuffer: UnsafeRow, row: InternalRow) => {
          algebraicUpdateProjection.target(currentBuffer)
          algebraicUpdateProjection(joinedRow(currentBuffer, row))
        }

      // PartialMerge-only or Final-only
      case (Some(PartialMerge), None) | (Some(Final), None) =>
        val mergeExpressions = allAggregateFunctions.flatMap(_.mergeExpressions)
        // This projection is used to merge buffer values for all AlgebraicAggregates.
        //此投影用于合并所有AlgebraicAggregates的缓冲区值
        val algebraicMergeProjection =
          newMutableProjection(
            mergeExpressions,
            aggregationBufferAttributes ++ inputAttributes)()

        (currentBuffer: UnsafeRow, row: InternalRow) => {
          // Process all algebraic aggregate functions.
          //处理所有代数聚合函数
          algebraicMergeProjection.target(currentBuffer)
          algebraicMergeProjection(joinedRow(currentBuffer, row))
        }

      // Final-Complete
      case (Some(Final), Some(Complete)) =>
        val nonCompleteAggregateFunctions: Array[AlgebraicAggregate] =
          allAggregateFunctions.take(nonCompleteAggregateExpressions.length)
        val completeAggregateFunctions: Array[AlgebraicAggregate] =
          allAggregateFunctions.takeRight(completeAggregateExpressions.length)

        val completeOffsetExpressions =
          Seq.fill(completeAggregateFunctions.map(_.bufferAttributes.length).sum)(NoOp)
        val mergeExpressions =
          nonCompleteAggregateFunctions.flatMap(_.mergeExpressions) ++ completeOffsetExpressions
        val finalAlgebraicMergeProjection =
          newMutableProjection(
            mergeExpressions,
            aggregationBufferAttributes ++ inputAttributes)()

        // We do not touch buffer values of aggregate functions with the Final mode.
        //我们不会使用Final模式触摸聚合函数的缓冲区值
        val finalOffsetExpressions =
          Seq.fill(nonCompleteAggregateFunctions.map(_.bufferAttributes.length).sum)(NoOp)
        val updateExpressions =
          finalOffsetExpressions ++ completeAggregateFunctions.flatMap(_.updateExpressions)
        val completeAlgebraicUpdateProjection =
          newMutableProjection(updateExpressions, aggregationBufferAttributes ++ inputAttributes)()

        (currentBuffer: UnsafeRow, row: InternalRow) => {
          val input = joinedRow(currentBuffer, row)
          // For all aggregate functions with mode Complete, update the given currentBuffer.
          //对于模式完成的所有聚合函数,更新给定的currentBuffer
          completeAlgebraicUpdateProjection.target(currentBuffer)(input)

          // For all aggregate functions with mode Final, merge buffer values in row to
          // currentBuffer.
          //对于具有模式Final的所有聚合函数,将行中的缓冲区值合并到currentBuffer
          finalAlgebraicMergeProjection.target(currentBuffer)(input)
        }

      // Complete-only
      case (None, Some(Complete)) =>
        val completeAggregateFunctions: Array[AlgebraicAggregate] =
          allAggregateFunctions.takeRight(completeAggregateExpressions.length)

        val updateExpressions =
          completeAggregateFunctions.flatMap(_.updateExpressions)
        val completeAlgebraicUpdateProjection =
          newMutableProjection(updateExpressions, aggregationBufferAttributes ++ inputAttributes)()

        (currentBuffer: UnsafeRow, row: InternalRow) => {
          completeAlgebraicUpdateProjection.target(currentBuffer)
          // For all aggregate functions with mode Complete, update the given currentBuffer.
          //对于模式完成的所有聚合函数,更新给定的currentBuffer
          completeAlgebraicUpdateProjection(joinedRow(currentBuffer, row))
        }

      // Grouping only.
      case (None, None) => (currentBuffer: UnsafeRow, row: InternalRow) => {}

      case other =>
        throw new IllegalStateException(
          s"${aggregationMode} should not be passed into TungstenAggregationIterator.")
    }
  }

  // Creates a function used to generate output rows.
  //创建用于生成输出行的函数
  private def generateResultProjection(): (UnsafeRow, UnsafeRow) => UnsafeRow = {

    val groupingAttributes = groupingExpressions.map(_.toAttribute)
    val bufferAttributes = allAggregateFunctions.flatMap(_.bufferAttributes)

    aggregationMode match {
      // Partial-only or PartialMerge-only: every output row is basically the values of
      // the grouping expressions and the corresponding aggregation buffer.
      case (Some(Partial), None) | (Some(PartialMerge), None) =>
        val groupingKeySchema = StructType.fromAttributes(groupingAttributes)
        val bufferSchema = StructType.fromAttributes(bufferAttributes)
        val unsafeRowJoiner = GenerateUnsafeRowJoiner.create(groupingKeySchema, bufferSchema)

        (currentGroupingKey: UnsafeRow, currentBuffer: UnsafeRow) => {
          unsafeRowJoiner.join(currentGroupingKey, currentBuffer)
        }

      // Final-only, Complete-only and Final-Complete: a output row is generated based on
      // resultExpressions.
      case (Some(Final), None) | (Some(Final) | None, Some(Complete)) =>
        val joinedRow = new JoinedRow()
        val resultProjection =
          UnsafeProjection.create(resultExpressions, groupingAttributes ++ bufferAttributes)

        (currentGroupingKey: UnsafeRow, currentBuffer: UnsafeRow) => {
          resultProjection(joinedRow(currentGroupingKey, currentBuffer))
        }

      // Grouping-only: a output row is generated from values of grouping expressions.
      case (None, None) =>
        val resultProjection =
          UnsafeProjection.create(resultExpressions, groupingAttributes)

        (currentGroupingKey: UnsafeRow, currentBuffer: UnsafeRow) => {
          resultProjection(currentGroupingKey)
        }

      case other =>
        throw new IllegalStateException(
          s"${aggregationMode} should not be passed into TungstenAggregationIterator.")
    }
  }

  // An UnsafeProjection used to extract grouping keys from the input rows.
  //用于从输入行提取分组键的UnsafeProjection
  private[this] val groupProjection =
    UnsafeProjection.create(groupingExpressions, originalInputAttributes)

  // A function used to process a input row. Its first argument is the aggregation buffer
  // and the second argument is the input row.
  //用于处理输入行的函数,它的第一个参数是聚合缓冲区,第二个参数是输入行
  private[this] var processRow: (UnsafeRow, InternalRow) => Unit =
    generateProcessRow(originalInputAttributes)

  // A function used to generate output rows based on the grouping keys (first argument)
  // and the corresponding aggregation buffer (second argument).
  //用于根据分组键(第一个参数)和相应的聚合缓冲区(第二个参数)生成输出行的函数
  private[this] var generateOutput: (UnsafeRow, UnsafeRow) => UnsafeRow =
    generateResultProjection()

  // An aggregation buffer containing initial buffer values. It is used to
  // initialize other aggregation buffers.
  //包含初始缓冲区值的聚合缓冲区,它用于初始化其他聚合缓冲区
  private[this] val initialAggregationBuffer: UnsafeRow = createNewAggregationBuffer()

  ///////////////////////////////////////////////////////////////////////////
  // Part 3: Methods and fields used by hash-based aggregation.
  //基于散列的聚合使用的方法和字段
  ///////////////////////////////////////////////////////////////////////////

  // This is the hash map used for hash-based aggregation. It is backed by an
  // UnsafeFixedWidthAggregationMap and it is used to store
  // all groups and their corresponding aggregation buffers for hash-based aggregation.
  //这是用于基于散列的聚合的哈希映射,它由UnsafeFixedWidthAggregationMap支持,
  //用于存储所有组及其相应的聚合缓冲区,以进行基于散列的聚合
  private[this] val hashMap = new UnsafeFixedWidthAggregationMap(
    initialAggregationBuffer,
    StructType.fromAttributes(allAggregateFunctions.flatMap(_.bufferAttributes)),
    StructType.fromAttributes(groupingExpressions.map(_.toAttribute)),
    TaskContext.get.taskMemoryManager(),
    SparkEnv.get.shuffleMemoryManager,
    1024 * 16, // initial capacity
    SparkEnv.get.shuffleMemoryManager.pageSizeBytes,
    false // disable tracking of performance metrics
  )

  // Exposed for testing
  private[aggregate] def getHashMap: UnsafeFixedWidthAggregationMap = hashMap

  // The function used to read and process input rows. When processing input rows,
  //用于读取和处理输入行的函数,处理输入行时
  // it first uses hash-based aggregation by putting groups and their buffers in
  // hashMap. If we could not allocate more memory for the map, we switch to
  // sort-based aggregation (by calling switchToSortBasedAggregation).
  //它首先通过将组及其缓冲区放在hashMap中来使用基于散列的聚合,
  //如果我们无法为地图分配更多内存,我们会切换到基于排序的聚合(通过调用switchToSortBasedAggregation)
  private def processInputs(): Unit = {
    assert(inputIter != null, "attempted to process input when iterator was null")
    if (groupingExpressions.isEmpty) {
      // If there is no grouping expressions, we can just reuse the same buffer over and over again.
      //如果没有分组表达式,我们可以一遍又一遍地重用相同的缓冲区
      // Note that it would be better to eliminate the hash map entirely in the future.
      //请注意,最好在将来完全消除哈希映射
      val groupingKey = groupProjection.apply(null)
      val buffer: UnsafeRow = hashMap.getAggregationBufferFromUnsafeRow(groupingKey)
      while (inputIter.hasNext) {
        val newInput = inputIter.next()
        numInputRows += 1
        processRow(buffer, newInput)
      }
    } else {
      while (!sortBased && inputIter.hasNext) {
        val newInput = inputIter.next()
        numInputRows += 1
        val groupingKey = groupProjection.apply(newInput)
        val buffer: UnsafeRow = hashMap.getAggregationBufferFromUnsafeRow(groupingKey)
        if (buffer == null) {
          // buffer == null means that we could not allocate more memory.
          // Now, we need to spill the map and switch to sort-based aggregation.
          switchToSortBasedAggregation(groupingKey, newInput)
        } else {
          processRow(buffer, newInput)
        }
      }
    }
  }

  // This function is only used for testing. It basically the same as processInputs except
  // that it switch to sort-based aggregation after `fallbackStartsAt` input rows have
  // been processed.
  //此功能仅用于测试,它与processInputs基本相同,只是在处理了'fallbackStartsAt`输入行后切换到基于排序的聚合。
  private def processInputsWithControlledFallback(fallbackStartsAt: Int): Unit = {
    assert(inputIter != null, "attempted to process input when iterator was null")
    var i = 0
    while (!sortBased && inputIter.hasNext) {
      val newInput = inputIter.next()
      numInputRows += 1
      val groupingKey = groupProjection.apply(newInput)
      val buffer: UnsafeRow = if (i < fallbackStartsAt) {
        hashMap.getAggregationBufferFromUnsafeRow(groupingKey)
      } else {
        null
      }
      if (buffer == null) {
        // buffer == null means that we could not allocate more memory.
        // Now, we need to spill the map and switch to sort-based aggregation.
        switchToSortBasedAggregation(groupingKey, newInput)
      } else {
        processRow(buffer, newInput)
      }
      i += 1
    }
  }

  // The iterator created from hashMap. It is used to generate output rows when we
  // are using hash-based aggregation.
  //迭代器是从hashMap创建的。 当我们使用基于散列的聚合时，它用于生成输出行
  private[this] var aggregationBufferMapIterator: KVIterator[UnsafeRow, UnsafeRow] = null

  // Indicates if aggregationBufferMapIterator still has key-value pairs.
  //指示aggregationBufferMapIterator是否仍具有键值对
  private[this] var mapIteratorHasNext: Boolean = false

  ///////////////////////////////////////////////////////////////////////////
  // Part 4: Methods and fields used when we switch to sort-based aggregation.
  //切换到基于排序的聚合时使用的方法和字段
  ///////////////////////////////////////////////////////////////////////////

  // This sorter is used for sort-based aggregation. It is initialized as soon as
  // we switch from hash-based to sort-based aggregation. Otherwise, it is not used.
  //此分拣机用于基于排序的聚合,一旦我们从基于散列的聚合切换到基于排序的聚合,
  //它就会被初始化,否则,它不会被使用。
  private[this] var externalSorter: UnsafeKVExternalSorter = null

  /**
   * Switch to sort-based aggregation when the hash-based approach is unable to acquire memory.
    * 当基于散列的方法无法获取内存时.切换到基于排序的聚合
   */
  private def switchToSortBasedAggregation(firstKey: UnsafeRow, firstInput: InternalRow): Unit = {
    assert(inputIter != null, "attempted to process input when iterator was null")
    logInfo("falling back to sort based aggregation.")
    // Step 1: Get the ExternalSorter containing sorted entries of the map.
    //获取包含地图的已排序条目的ExternalSorter
    externalSorter = hashMap.destructAndCreateExternalSorter()

    // Step 2: Free the memory used by the map. 释放Map使用的内存
    hashMap.free()

    // Step 3: If we have aggregate function with mode Partial or Complete,
    // we need to process input rows to get aggregation buffer.
    //如果我们使用Partial或Complete模式的聚合函数,我们需要处理输入行以获得聚合缓冲区。
    // So, later in the sort-based aggregation iterator, we can do merge.
    // If aggregate functions are with mode Final and PartialMerge,
    // we just need to project the aggregation buffer from an input row.
    val needsProcess = aggregationMode match {
      case (Some(Partial), None) => true
      case (None, Some(Complete)) => true
      case (Some(Final), Some(Complete)) => true
      case _ => false
    }

    // Note: Since we spill the sorter's contents immediately after creating it, we must insert
    // something into the sorter here to ensure that we acquire at least a page of memory.
    //由于我们在创建分拣机的内容后立即将其分散,我们必须在分拣机中插入一些东西以确保我们至少获得一页内存
    // This is done through `externalSorter.insertKV`, which will trigger the page allocation.
    // Otherwise, children operators may steal the window of opportunity and starve our sorter.

    if (needsProcess) {
      // First, we create a buffer.
      //首先，我们创建一个缓冲区
      val buffer = createNewAggregationBuffer()

      // Process firstKey and firstInput.
      // Initialize buffer.
      //处理firstKey和firstInput,初始化缓冲区
      buffer.copyFrom(initialAggregationBuffer)
      processRow(buffer, firstInput)
      externalSorter.insertKV(firstKey, buffer)

      // Process the rest of input rows.
      //处理剩余的输入行
      while (inputIter.hasNext) {
        val newInput = inputIter.next()
        numInputRows += 1
        val groupingKey = groupProjection.apply(newInput)
        buffer.copyFrom(initialAggregationBuffer)
        processRow(buffer, newInput)
        externalSorter.insertKV(groupingKey, buffer)
      }
    } else {
      // When needsProcess is false, the format of input rows is groupingKey + aggregation buffer.
      // We need to project the aggregation buffer part from an input row.
      //当needsProcess为false时,输入行的格式为groupingKey + aggregation buffer。
      //我们需要从输入行投影聚合缓冲区部分。
      val buffer = createNewAggregationBuffer()
      // The originalInputAttributes are using cloneBufferAttributes. So, we need to use
      // allAggregateFunctions.flatMap(_.cloneBufferAttributes).
      val bufferExtractor = newMutableProjection(
        allAggregateFunctions.flatMap(_.cloneBufferAttributes),
        originalInputAttributes)()
      bufferExtractor.target(buffer)

      // Insert firstKey and its buffer.
      //插入firstKey及其缓冲区
      bufferExtractor(firstInput)
      externalSorter.insertKV(firstKey, buffer)

      // Insert the rest of input rows.
      //插入其余的输入行
      while (inputIter.hasNext) {
        val newInput = inputIter.next()
        numInputRows += 1
        val groupingKey = groupProjection.apply(newInput)
        bufferExtractor(newInput)
        externalSorter.insertKV(groupingKey, buffer)
      }
    }

    // Set aggregationMode, processRow, and generateOutput for sort-based aggregation.
    //为基于排序的聚合设置aggregationMode，processRow和generateOutput
    val newAggregationMode = aggregationMode match {
      case (Some(Partial), None) => (Some(PartialMerge), None)
      case (None, Some(Complete)) => (Some(Final), None)
      case (Some(Final), Some(Complete)) => (Some(Final), None)
      case other => other
    }
    aggregationMode = newAggregationMode

    // Basically the value of the KVIterator returned by externalSorter
    // will just aggregation buffer. At here, we use cloneBufferAttributes.
    //基本上,externalSorter返回的KVIterator的值只是聚合缓冲区,在这里,我们使用cloneBufferAttributes。
    val newInputAttributes: Seq[Attribute] =
      allAggregateFunctions.flatMap(_.cloneBufferAttributes)

    // Set up new processRow and generateOutput.
    //设置新的processRow和generateOutput
    processRow = generateProcessRow(newInputAttributes)
    generateOutput = generateResultProjection()

    // Step 5: Get the sorted iterator from the externalSorter.
    //从externalSorter获取已排序的迭代器
    sortedKVIterator = externalSorter.sortedIterator()

    // Step 6: Pre-load the first key-value pair from the sorted iterator to make
    // hasNext idempotent.
    //从已排序的迭代器预加载第一个键值对以使hasNext成为幂等
    sortedInputHasNewGroup = sortedKVIterator.next()

    // Copy the first key and value (aggregation buffer).
    //复制第一个键和值(聚合缓冲区)
    if (sortedInputHasNewGroup) {
      val key = sortedKVIterator.getKey
      val value = sortedKVIterator.getValue
      nextGroupingKey = key.copy()
      currentGroupingKey = key.copy()
      firstRowInNextGroup = value.copy()
    }

    // Step 7: set sortBased to true.
    sortBased = true
  }

  ///////////////////////////////////////////////////////////////////////////
  // Part 5: Methods and fields used by sort-based aggregation.
  //基于排序的聚合使用的方法和字段
  ///////////////////////////////////////////////////////////////////////////

  // Indicates if we are using sort-based aggregation. Because we first try to use
  // hash-based aggregation, its initial value is false.
  //指示我们是否使用基于排序的聚合,因为我们首先尝试使用基于散列的聚合,所以它的初始值为false。
  private[this] var sortBased: Boolean = false

  // The KVIterator containing input rows for the sort-based aggregation. It will be
  // set in switchToSortBasedAggregation when we switch to sort-based aggregation.
  //KVIterator包含基于排序的聚合的输入行,当我们切换到基于排序的聚合时,它将在switchToSortBasedAggregation中设置
  private[this] var sortedKVIterator: UnsafeKVExternalSorter#KVSorterIterator = null

  // The grouping key of the current group.
  //当前组的分组键
  private[this] var currentGroupingKey: UnsafeRow = null

  // The grouping key of next group.
  //下一组的分组键
  private[this] var nextGroupingKey: UnsafeRow = null

  // The first row of next group.
  //下一组的第一行
  private[this] var firstRowInNextGroup: UnsafeRow = null

  // Indicates if we has new group of rows from the sorted input iterator.
  //指示我们是否从排序的输入迭代器中新建了一组行
  private[this] var sortedInputHasNewGroup: Boolean = false

  // The aggregation buffer used by the sort-based aggregation.
  //基于排序的聚合使用的聚合缓冲区
  private[this] val sortBasedAggregationBuffer: UnsafeRow = createNewAggregationBuffer()

  // Processes rows in the current group. It will stop when it find a new group.
  //处理当前组中的行,它会在找到新组时停止
  private def processCurrentSortedGroup(): Unit = {
    // First, we need to copy nextGroupingKey to currentGroupingKey.
    //首先,我们需要将nextGroupingKey复制到currentGroupingKey
    currentGroupingKey.copyFrom(nextGroupingKey)
    // Now, we will start to find all rows belonging to this group.
    // We create a variable to track if we see the next group.
    //现在,我们将开始查找属于该组的所有行,我们创建一个变量来跟踪我们是否看到下一组。
    var findNextPartition = false
    // firstRowInNextGroup is the first row of this group. We first process it.
    //firstRowInNextGroup是该组的第一行,我们先处理它
    processRow(sortBasedAggregationBuffer, firstRowInNextGroup)

    // The search will stop when we see the next group or there is no
    // input row left in the iter.
    //当我们看到下一组或者iter中没有剩下输入行时,搜索将停止
    // Pre-load the first key-value pair to make the condition of the while loop
    // has no action (we do not trigger loading a new key-value pair
    // when we evaluate the condition).
    var hasNext = sortedKVIterator.next()
    while (!findNextPartition && hasNext) {
      // Get the grouping key and value (aggregation buffer).
      //获取分组键和值(聚合缓冲区)
      val groupingKey = sortedKVIterator.getKey
      val inputAggregationBuffer = sortedKVIterator.getValue

      // Check if the current row belongs the current input row.
      //检查当前行是否属于当前输入行
      if (currentGroupingKey.equals(groupingKey)) {
        processRow(sortBasedAggregationBuffer, inputAggregationBuffer)

        hasNext = sortedKVIterator.next()
      } else {
        // We find a new group.
        findNextPartition = true
        // copyFrom will fail when
        nextGroupingKey.copyFrom(groupingKey) // = groupingKey.copy()
        firstRowInNextGroup.copyFrom(inputAggregationBuffer) // = inputAggregationBuffer.copy()

      }
    }
    // We have not seen a new group. It means that there is no new row in the input
    // iter. The current group is the last group of the sortedKVIterator.
    //我们还没有看到一个新的团体,这意味着输入iter中没有新行,当前组是sortedKVIterator的最后一组
    if (!findNextPartition) {
      sortedInputHasNewGroup = false
      sortedKVIterator.close()
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Part 6: Loads input rows and setup aggregationBufferMapIterator if we
  //         have not switched to sort-based aggregation.
  //如果我们尚未切换到基于排序的聚合,则加载输入行并设置aggregationBufferMapIterator
  ///////////////////////////////////////////////////////////////////////////

  /**
   * Start processing input rows.开始处理输入行
   * Only after this method is called will this iterator be non-empty.
    * 只有在调用此方法之后,此迭代器才会为非空
   */
  def start(parentIter: Iterator[InternalRow]): Unit = {
    inputIter = parentIter
    testFallbackStartsAt match {
      case None =>
        processInputs()
      case Some(fallbackStartsAt) =>
        // This is the testing path. processInputsWithControlledFallback is same as processInputs
        // except that it switches to sort-based aggregation after `fallbackStartsAt` input rows
        // have been processed.
        //这是测试路径,processInputsWithControlledFallback与processInputs相同,
        // 只是它在处理了'fallbackStartsAt`输入行后切换到基于排序的聚合
        processInputsWithControlledFallback(fallbackStartsAt)
    }

    // If we did not switch to sort-based aggregation in processInputs,
    // we pre-load the first key-value pair from the map (to make hasNext idempotent).
    //如果我们没有在processInputs中切换到基于排序的聚合,我们会预先加载地图中的第一个键值对(使hasNext成为幂等)。
    if (!sortBased) {
      // First, set aggregationBufferMapIterator.
      //首先,设置aggregationBufferMapIterator
      aggregationBufferMapIterator = hashMap.iterator()
      // Pre-load the first key-value pair from the aggregationBufferMapIterator.
      //从aggregationBufferMapIterator预加载第一个键值对
      mapIteratorHasNext = aggregationBufferMapIterator.next()
      // If the map is empty, we just free it.
      if (!mapIteratorHasNext) {
        hashMap.free()
      }
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Part 7: Iterator's public methods.迭代器的公共方法
  ///////////////////////////////////////////////////////////////////////////

  override final def hasNext: Boolean = {
    (sortBased && sortedInputHasNewGroup) || (!sortBased && mapIteratorHasNext)
  }

  override final def next(): UnsafeRow = {
    if (hasNext) {
      val res = if (sortBased) {
        // Process the current group.
        //处理当前组
        processCurrentSortedGroup()
        // Generate output row for the current group.
        //生成当前组的输出行
        val outputRow = generateOutput(currentGroupingKey, sortBasedAggregationBuffer)
        // Initialize buffer values for the next group.
        //初始化下一组的缓冲区值
        sortBasedAggregationBuffer.copyFrom(initialAggregationBuffer)

        outputRow
      } else {
        // We did not fall back to sort-based aggregation.
        //我们没有回归基于排序的聚合
        val result =
          generateOutput(
            aggregationBufferMapIterator.getKey,
            aggregationBufferMapIterator.getValue)

        // Pre-load next key-value pair form aggregationBufferMapIterator to make hasNext
        // idempotent.
        //预加载下一个键值对形成aggregationBufferMapIterator,使hasNext成为幂等
        mapIteratorHasNext = aggregationBufferMapIterator.next()

        if (!mapIteratorHasNext) {
          // If there is no input from aggregationBufferMapIterator, we copy current result.
          //如果来自aggregationBufferMapIterator没有输入,我们复制当前结果
          val resultCopy = result.copy()
          // Then, we free the map.
          hashMap.free()

          resultCopy
        } else {
          result
        }
      }

      // If this is the last record, update the task's peak memory usage. Since we destroy
      // the map to create the sorter, their memory usages should not overlap, so it is safe
      // to just use the max of the two.
      //如果这是最后一条记录,请更新任务的峰值内存使用情况,由于我们销毁地图以创建分拣机,
      //因此它们的记忆用法不应重叠,因此只使用两者中的最大值是安全的。
      if (!hasNext) {
        val mapMemory = hashMap.getPeakMemoryUsedBytes
        val sorterMemory = Option(externalSorter).map(_.getPeakMemoryUsedBytes).getOrElse(0L)
        val peakMemory = Math.max(mapMemory, sorterMemory)
        TaskContext.get().internalMetricsToAccumulators(
          InternalAccumulator.PEAK_EXECUTION_MEMORY).add(peakMemory)
      }
      numOutputRows += 1
      res
    } else {
      // no more result
      throw new NoSuchElementException
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Part 8: Utility functions
  ///////////////////////////////////////////////////////////////////////////

  /**
   * Generate a output row when there is no input and there is no grouping expression.
    * 没有输入且没有分组表达式时生成输出行
   */
  def outputForEmptyGroupingKeyWithoutInput(): UnsafeRow = {
    assert(groupingExpressions.isEmpty)
    assert(inputIter == null)
    generateOutput(UnsafeRow.createFromByteArray(0, 0), initialAggregationBuffer)
  }

  /** Free memory used in the underlying map.
    * 底层Map中使用的可用内存*/
  def free(): Unit = {
    hashMap.free()
  }
}
