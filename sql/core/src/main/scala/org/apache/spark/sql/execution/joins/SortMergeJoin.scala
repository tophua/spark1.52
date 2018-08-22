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

package org.apache.spark.sql.execution.joins

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{BinaryNode, RowIterator, SparkPlan}
import org.apache.spark.sql.execution.metric.{LongSQLMetric, SQLMetrics}

/**
 * :: DeveloperApi ::
 * Performs an sort merge join of two child relations.
  * 执行两个子关系的排序合并连接
 */
@DeveloperApi
case class SortMergeJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    left: SparkPlan,
    right: SparkPlan) extends BinaryNode {

  override private[sql] lazy val metrics = Map(
    "numLeftRows" -> SQLMetrics.createLongMetric(sparkContext, "number of left rows"),
    "numRightRows" -> SQLMetrics.createLongMetric(sparkContext, "number of right rows"),
    "numOutputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of output rows"))

  override def output: Seq[Attribute] = left.output ++ right.output

  override def outputPartitioning: Partitioning =
    PartitioningCollection(Seq(left.outputPartitioning, right.outputPartitioning))

  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil

  override def outputOrdering: Seq[SortOrder] = requiredOrders(leftKeys)

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    requiredOrders(leftKeys) :: requiredOrders(rightKeys) :: Nil

  protected[this] def isUnsafeMode: Boolean = {
    (codegenEnabled && unsafeEnabled
      && UnsafeProjection.canSupport(leftKeys)
      && UnsafeProjection.canSupport(rightKeys)
      && UnsafeProjection.canSupport(schema))
  }

  override def outputsUnsafeRows: Boolean = isUnsafeMode
  override def canProcessUnsafeRows: Boolean = isUnsafeMode
  override def canProcessSafeRows: Boolean = !isUnsafeMode

  private def requiredOrders(keys: Seq[Expression]): Seq[SortOrder] = {
    // This must be ascending in order to agree with the `keyOrdering` defined in `doExecute()`.
    //这必须是升序才能与`doExecute（）`中定义的`keyOrdering`一致
    keys.map(SortOrder(_, Ascending))
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val numLeftRows = longMetric("numLeftRows")
    val numRightRows = longMetric("numRightRows")
    val numOutputRows = longMetric("numOutputRows")

    left.execute().zipPartitions(right.execute()) { (leftIter, rightIter) =>
      new RowIterator {
        // The projection used to extract keys from input rows of the left child.
        //用于从左子输入行中提取键的投影
        private[this] val leftKeyGenerator = {
          if (isUnsafeMode) {
            // It is very important to use UnsafeProjection if input rows are UnsafeRows.
            // Otherwise, GenerateProjection will cause wrong results.
            //如果输入行是UnsafeRows,则使用UnsafeProjection非常重要, 否则,GenerateProjection将导致错误的结果。
            UnsafeProjection.create(leftKeys, left.output)
          } else {
            newProjection(leftKeys, left.output)
          }
        }

        // The projection used to extract keys from input rows of the right child.
        //用于从右孩子的输入行中提取键的投影
        private[this] val rightKeyGenerator = {
          if (isUnsafeMode) {
            // It is very important to use UnsafeProjection if input rows are UnsafeRows.
            // Otherwise, GenerateProjection will cause wrong results.
            //如果输入行是UnsafeRows,则使用UnsafeProjection非常重要,否则,GenerateProjection将导致错误的结果。
            UnsafeProjection.create(rightKeys, right.output)
          } else {
            newProjection(rightKeys, right.output)
          }
        }

        // An ordering that can be used to compare keys from both sides.
        //可用于比较双方密钥的排序
        private[this] val keyOrdering = newNaturalAscendingOrdering(leftKeys.map(_.dataType))
        private[this] var currentLeftRow: InternalRow = _
        private[this] var currentRightMatches: ArrayBuffer[InternalRow] = _
        private[this] var currentMatchIdx: Int = -1
        private[this] val smjScanner = new SortMergeJoinScanner(
          leftKeyGenerator,
          rightKeyGenerator,
          keyOrdering,
          RowIterator.fromScala(leftIter),
          numLeftRows,
          RowIterator.fromScala(rightIter),
          numRightRows
        )
        private[this] val joinRow = new JoinedRow
        private[this] val resultProjection: (InternalRow) => InternalRow = {
          if (isUnsafeMode) {
            UnsafeProjection.create(schema)
          } else {
            identity[InternalRow]
          }
        }

        override def advanceNext(): Boolean = {
          if (currentMatchIdx == -1 || currentMatchIdx == currentRightMatches.length) {
            if (smjScanner.findNextInnerJoinRows()) {
              currentRightMatches = smjScanner.getBufferedMatches
              currentLeftRow = smjScanner.getStreamedRow
              currentMatchIdx = 0
            } else {
              currentRightMatches = null
              currentLeftRow = null
              currentMatchIdx = -1
            }
          }
          if (currentLeftRow != null) {
            joinRow(currentLeftRow, currentRightMatches(currentMatchIdx))
            currentMatchIdx += 1
            numOutputRows += 1
            true
          } else {
            false
          }
        }

        override def getRow: InternalRow = resultProjection(joinRow)
      }.toScala
    }
  }
}

/**
 * Helper class that is used to implement [[SortMergeJoin]] and [[SortMergeOuterJoin]].
 *
 * To perform an inner (outer) join, users of this class call [[findNextInnerJoinRows()]]
 * ([[findNextOuterJoinRows()]]), which returns `true` if a result has been produced and `false`
 * otherwise. If a result has been produced, then the caller may call [[getStreamedRow]] to return
 * the matching row from the streamed input and may call [[getBufferedMatches]] to return the
 * sequence of matching rows from the buffered input (in the case of an outer join, this will return
 * an empty sequence if there are no matches from the buffered input). For efficiency, both of these
 * methods return mutable objects which are re-used across calls to the `findNext*JoinRows()`
 * methods.
 *
 * @param streamedKeyGenerator a projection that produces join keys from the streamed input.
 * @param bufferedKeyGenerator a projection that produces join keys from the buffered input.
 * @param keyOrdering an ordering which can be used to compare join keys.
 * @param streamedIter an input whose rows will be streamed.
 * @param bufferedIter an input whose rows will be buffered to construct sequences of rows that
 *                     have the same join key.
 */
private[joins] class SortMergeJoinScanner(
    streamedKeyGenerator: Projection,
    bufferedKeyGenerator: Projection,
    keyOrdering: Ordering[InternalRow],
    streamedIter: RowIterator,
    numStreamedRows: LongSQLMetric,
    bufferedIter: RowIterator,
    numBufferedRows: LongSQLMetric) {
  private[this] var streamedRow: InternalRow = _
  private[this] var streamedRowKey: InternalRow = _
  private[this] var bufferedRow: InternalRow = _
  // Note: this is guaranteed to never have any null columns:
  //注意：保证永远不会有任何空列
  private[this] var bufferedRowKey: InternalRow = _
  /**
   * The join key for the rows buffered in `bufferedMatches`, or null if `bufferedMatches` is empty
    * 在`bufferedMatches`中缓冲的行的连接键,如果`bufferedMatches`为空，则为null
   */
  private[this] var matchJoinKey: InternalRow = _
  /** Buffered rows from the buffered side of the join. This is empty if there are no matches.
    * 来自连接的缓冲侧的缓冲行,如果没有匹配,则为空*/
  private[this] val bufferedMatches: ArrayBuffer[InternalRow] = new ArrayBuffer[InternalRow]

  // Initialization (note: do _not_ want to advance streamed here).
  //初始化（注意：请_not_想要在这里推进流式传输）
  advancedBufferedToRowWithNullFreeJoinKey()

  // --- Public methods ---------------------------------------------------------------------------

  def getStreamedRow: InternalRow = streamedRow

  def getBufferedMatches: ArrayBuffer[InternalRow] = bufferedMatches

  /**
   * Advances both input iterators, stopping when we have found rows with matching join keys.
    * 推进两个输入迭代器,当我们找到具有匹配连接键的行时停止
   * @return true if matching rows have been found and false otherwise. If this returns true, then
   *         [[getStreamedRow]] and [[getBufferedMatches]] can be called to construct the join
   *         results.
   */
  final def findNextInnerJoinRows(): Boolean = {
    while (advancedStreamed() && streamedRowKey.anyNull) {
      // Advance the streamed side of the join until we find the next row whose join key contains
      // no nulls or we hit the end of the streamed iterator.
      //推进连接的流端,直到找到其连接键不包含空值的下一行,或者我们到达流式迭代器的末尾。
    }
    if (streamedRow == null) {
      // We have consumed the entire streamed iterator, so there can be no more matches.
      //我们已经使用了整个流式迭代器,所以不能再有匹配了
      matchJoinKey = null
      bufferedMatches.clear()
      false
    } else if (matchJoinKey != null && keyOrdering.compare(streamedRowKey, matchJoinKey) == 0) {
      // The new streamed row has the same join key as the previous row, so return the same matches.
      //新的流式行与前一行具有相同的连接键,因此返回相同的匹配项
      true
    } else if (bufferedRow == null) {
      // The streamed row's join key does not match the current batch of buffered rows and there are
      // no more rows to read from the buffered iterator, so there can be no more matches.
      //流式行的连接键与当前批量的缓冲行不匹配,并且没有更多行可以从缓冲的迭代器中读取,因此不能再有匹配
      matchJoinKey = null
      bufferedMatches.clear()
      false
    } else {
      // Advance both the streamed and buffered iterators to find the next pair of matching rows.
      //推进流式和缓冲式迭代器以查找下一对匹配行
      var comp = keyOrdering.compare(streamedRowKey, bufferedRowKey)
      do {
        if (streamedRowKey.anyNull) {
          advancedStreamed()
        } else {
          assert(!bufferedRowKey.anyNull)
          comp = keyOrdering.compare(streamedRowKey, bufferedRowKey)
          if (comp > 0) advancedBufferedToRowWithNullFreeJoinKey()
          else if (comp < 0) advancedStreamed()
        }
      } while (streamedRow != null && bufferedRow != null && comp != 0)
      if (streamedRow == null || bufferedRow == null) {
        // We have either hit the end of one of the iterators, so there can be no more matches.
        //我们要么触及其中一个迭代器的末尾,所以不能再有匹配了
        matchJoinKey = null
        bufferedMatches.clear()
        false
      } else {
        // The streamed row's join key matches the current buffered row's join, so walk through the
        // buffered iterator to buffer the rest of the matching rows.
        //流式行的连接键与当前缓冲行的连接匹配,因此遍历缓冲的迭代器以缓冲其余匹配行
        assert(comp == 0)
        bufferMatchingRows()
        true
      }
    }
  }

  /**
   * Advances the streamed input iterator and buffers all rows from the buffered input that
   * have matching keys.
    * 推进流式输入迭代器并缓冲具有匹配键的缓冲输入中的所有行
   * @return true if the streamed iterator returned a row, false otherwise. If this returns true,
   *         then [getStreamedRow and [[getBufferedMatches]] can be called to produce the outer
   *         join results.
   */
  final def findNextOuterJoinRows(): Boolean = {
    if (!advancedStreamed()) {
      // We have consumed the entire streamed iterator, so there can be no more matches.
      //我们已经使用了整个流式迭代器,所以不能再有匹配了
      matchJoinKey = null
      bufferedMatches.clear()
      false
    } else {
      if (matchJoinKey != null && keyOrdering.compare(streamedRowKey, matchJoinKey) == 0) {
        // Matches the current group, so do nothing.
        //匹配当前组,所以什么都不做
      } else {
        // The streamed row does not match the current group.
        //流传输的行与当前组不匹配
        matchJoinKey = null
        bufferedMatches.clear()
        if (bufferedRow != null && !streamedRowKey.anyNull) {
          // The buffered iterator could still contain matching rows, so we'll need to walk through
          // it until we either find matches or pass where they would be found.
          //缓冲的迭代器仍然可以包含匹配的行,因此我们需要遍历它,直到我们找到匹配或传递它们将被找到的位置
          var comp = 1
          do {
            comp = keyOrdering.compare(streamedRowKey, bufferedRowKey)
          } while (comp > 0 && advancedBufferedToRowWithNullFreeJoinKey())
          if (comp == 0) {
            // We have found matches, so buffer them (this updates matchJoinKey)
            //我们找到了匹配项,所以缓冲它们(这会更新matchJoinKey)
            bufferMatchingRows()
          } else {
            // We have overshot the position where the row would be found, hence no matches.
            //我们已经超出了找到行的位置,因此没有匹配
          }
        }
      }
      // If there is a streamed input then we always return true
      //如果有流输入,那么我们总是返回true
      true
    }
  }

  // --- Private methods --------------------------------------------------------------------------

  /**
   * Advance the streamed iterator and compute the new row's join key.
    * 推进流式迭代器并计算新行的连接键
   * @return true if the streamed iterator returned a row and false otherwise.
   */
  private def advancedStreamed(): Boolean = {
    if (streamedIter.advanceNext()) {
      streamedRow = streamedIter.getRow
      streamedRowKey = streamedKeyGenerator(streamedRow)
      numStreamedRows += 1
      true
    } else {
      streamedRow = null
      streamedRowKey = null
      false
    }
  }

  /**
   * Advance the buffered iterator until we find a row with join key that does not contain nulls.
    * 推进缓冲的迭代器,直到我们找到一个不包含空值的连接键的行
   * @return true if the buffered iterator returned a row and false otherwise.
   */
  private def advancedBufferedToRowWithNullFreeJoinKey(): Boolean = {
    var foundRow: Boolean = false
    while (!foundRow && bufferedIter.advanceNext()) {
      bufferedRow = bufferedIter.getRow
      bufferedRowKey = bufferedKeyGenerator(bufferedRow)
      numBufferedRows += 1
      foundRow = !bufferedRowKey.anyNull
    }
    if (!foundRow) {
      bufferedRow = null
      bufferedRowKey = null
      false
    } else {
      true
    }
  }

  /**
   * Called when the streamed and buffered join keys match in order to buffer the matching rows.
    * 在流和缓冲的连接键匹配时调用,以缓冲匹配的行
   */
  private def bufferMatchingRows(): Unit = {
    assert(streamedRowKey != null)
    assert(!streamedRowKey.anyNull)
    assert(bufferedRowKey != null)
    assert(!bufferedRowKey.anyNull)
    assert(keyOrdering.compare(streamedRowKey, bufferedRowKey) == 0)
    // This join key may have been produced by a mutable projection, so we need to make a copy:
    //这个连接键可能是由一个可变投影产生的,所以我们需要制作一个副本：
    matchJoinKey = streamedRowKey.copy()
    bufferedMatches.clear()
    do {
      //需要在缓冲它们之前复制可变行
      bufferedMatches += bufferedRow.copy() // need to copy mutable rows before buffering them
      advancedBufferedToRowWithNullFreeJoinKey()
    } while (bufferedRow != null && keyOrdering.compare(streamedRowKey, bufferedRowKey) == 0)
  }
}
