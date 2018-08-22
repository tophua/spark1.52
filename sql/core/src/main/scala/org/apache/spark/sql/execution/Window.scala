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

import java.util

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.rdd.RDD
import org.apache.spark.util.collection.CompactBuffer
import scala.collection.mutable

/**
 * :: DeveloperApi ::
 * This class calculates and outputs (windowed) aggregates over the rows in a single (sorted)
 * partition. The aggregates are calculated for each row in the group. Special processing
 * instructions, frames, are used to calculate these aggregates. Frames are processed in the order
 * specified in the window specification (the ORDER BY ... clause). There are four different frame
 * types:
  * 此类计算并输出(窗口化)单个(已排序)分区中的行的聚合,将为组中的每一行计算聚合。
  * 特殊处理指令,帧用于计算这些聚合,按照窗口规范（ORDER BY ...子句)中指定的顺序处理帧,有四种不同的帧类型：
 * - Entire partition: The frame is the entire partition, i.e.
 *   UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING. For this case, window function will take all
 *   rows as inputs and be evaluated once.
 * - Growing frame: We only add new rows into the frame, i.e. UNBOUNDED PRECEDING AND ....
 *   Every time we move to a new row to process, we add some rows to the frame. We do not remove
 *   rows from this frame.
 * - Shrinking frame: We only remove rows from the frame, i.e. ... AND UNBOUNDED FOLLOWING.
 *   Every time we move to a new row to process, we remove some rows from the frame. We do not add
 *   rows to this frame.
 * - Moving frame: Every time we move to a new row to process, we remove some rows from the frame
 *   and we add some rows to the frame. Examples are:
 *     1 PRECEDING AND CURRENT ROW and 1 FOLLOWING AND 2 FOLLOWING.
 *
 * Different frame boundaries can be used in Growing, Shrinking and Moving frames. A frame
 * boundary can be either Row or Range based:
 * - Row Based: A row based boundary is based on the position of the row within the partition.
 *   An offset indicates the number of rows above or below the current row, the frame for the
 *   current row starts or ends. For instance, given a row based sliding frame with a lower bound
 *   offset of -1 and a upper bound offset of +2. The frame for row with index 5 would range from
 *   index 4 to index 6.
 * - Range based: A range based boundary is based on the actual value of the ORDER BY
 *   expression(s). An offset is used to alter the value of the ORDER BY expression, for
 *   instance if the current order by expression has a value of 10 and the lower bound offset
 *   is -3, the resulting lower bound for the current row will be 10 - 3 = 7. This however puts a
 *   number of constraints on the ORDER BY expressions: there can be only one expression and this
 *   expression must have a numerical data type. An exception can be made when the offset is 0,
 *   because no value modification is needed, in this case multiple and non-numeric ORDER BY
 *   expression are allowed.
 *
 * This is quite an expensive operator because every row for a single group must be in the same
 * partition and partitions must be sorted according to the grouping and sort order. The operator
 * requires the planner to take care of the partitioning and sorting.
 *
 * The operator is semi-blocking. The window functions and aggregates are calculated one group at
 * a time, the result will only be made available after the processing for the entire group has
 * finished. The operator is able to process different frame configurations at the same time. This
 * is done by delegating the actual frame processing (i.e. calculation of the window functions) to
 * specialized classes, see [[WindowFunctionFrame]], which take care of their own frame type:
 * Entire Partition, Sliding, Growing & Shrinking. Boundary evaluation is also delegated to a pair
 * of specialized classes: [[RowBoundOrdering]] & [[RangeBoundOrdering]].
 */
@DeveloperApi
case class Window(
    projectList: Seq[Attribute],
    windowExpression: Seq[NamedExpression],
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    child: SparkPlan)
  extends UnaryNode {

  override def output: Seq[Attribute] = projectList ++ windowExpression.map(_.toAttribute)

  override def requiredChildDistribution: Seq[Distribution] = {
    if (partitionSpec.isEmpty) {
      // Only show warning when the number of bytes is larger than 100 MB?
      //仅在字节数大于100 MB时显示警告？
      logWarning("No Partition Defined for Window operation! Moving all data to a single "
        + "partition, this can cause serious performance degradation.")
      AllTuples :: Nil
    } else ClusteredDistribution(partitionSpec) :: Nil
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(partitionSpec.map(SortOrder(_, Ascending)) ++ orderSpec)

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def canProcessUnsafeRows: Boolean = true

  /**
   * Create a bound ordering object for a given frame type and offset. A bound ordering object is
   * used to determine which input row lies within the frame boundaries of an output row.
    *
    * 为给定的帧类型和偏移量创建绑定的排序对象,绑定排序对象用于确定哪个输入行位于输出行的帧边界内
   *
   * This method uses Code Generation. It can only be used on the executor side.
    * 此方法使用代码生成,它只能在执行程序端使用
   *
   * @param frameType to evaluate. This can either be Row or Range based.
   * @param offset with respect to the row.
   * @return a bound ordering object.
   */
  private[this] def createBoundOrdering(frameType: FrameType, offset: Int): BoundOrdering = {
    frameType match {
      case RangeFrame =>
        val (exprs, current, bound) = if (offset == 0) {
          // Use the entire order expression when the offset is 0.
          //当偏移量为0时,使用整个订单表达式
          val exprs = orderSpec.map(_.child)
          val projection = newMutableProjection(exprs, child.output)
          (orderSpec, projection(), projection())
        } else if (orderSpec.size == 1) {
          // Use only the first order expression when the offset is non-null.
          //当偏移量为非null时,仅使用第一个顺序表达式
          val sortExpr = orderSpec.head
          val expr = sortExpr.child
          // Create the projection which returns the current 'value'.
          //创建返回当前“值”的投影
          val current = newMutableProjection(expr :: Nil, child.output)()
          // Flip the sign of the offset when processing the order is descending
          //在处理订单下降时翻转偏移量的符号
          val boundOffset =
            if (sortExpr.direction == Descending) {
              -offset
            } else {
              offset
            }
          // Create the projection which returns the current 'value' modified by adding the offset.
          //创建投影,返回通过添加偏移量修改的当前“值”
          val boundExpr = Add(expr, Cast(Literal.create(boundOffset, IntegerType), expr.dataType))
          val bound = newMutableProjection(boundExpr :: Nil, child.output)()
          (sortExpr :: Nil, current, bound)
        } else {
          sys.error("Non-Zero range offsets are not supported for windows " +
            "with multiple order expressions.")
        }
        // Construct the ordering. This is used to compare the result of current value projection
        // to the result of bound value projection. This is done manually because we want to use
        // Code Generation (if it is enabled).
        //构建订单,这用于将当前值投影的结果与绑定值投影的结果进行比较,这是手动完成的,因为我们想要使用代码生成(如果已启用)
        val sortExprs = exprs.zipWithIndex.map { case (e, i) =>
          SortOrder(BoundReference(i, e.dataType, e.nullable), e.direction)
        }
        val ordering = newOrdering(sortExprs, Nil)
        RangeBoundOrdering(ordering, current, bound)
      case RowFrame => RowBoundOrdering(offset)
    }
  }

  /**
   * Create a frame processor.
    * 创建一个帧处理器
   *
   * This method uses Code Generation. It can only be used on the executor side.
    * 此方法使用代码生成,它只能在执行程序端使用
   *
   * @param frame boundaries.
   * @param functions to process in the frame.
   * @param ordinal at which the processor starts writing to the output.处理器开始写入输出
   * @return a frame processor.
   */
  private[this] def createFrameProcessor(
      frame: WindowFrame,
      functions: Array[WindowFunction],
      ordinal: Int): WindowFunctionFrame = frame match {
    // Growing Frame.
    case SpecifiedWindowFrame(frameType, UnboundedPreceding, FrameBoundaryExtractor(high)) =>
      val uBoundOrdering = createBoundOrdering(frameType, high)
      new UnboundedPrecedingWindowFunctionFrame(ordinal, functions, uBoundOrdering)

    // Shrinking Frame.
    case SpecifiedWindowFrame(frameType, FrameBoundaryExtractor(low), UnboundedFollowing) =>
      val lBoundOrdering = createBoundOrdering(frameType, low)
      new UnboundedFollowingWindowFunctionFrame(ordinal, functions, lBoundOrdering)

    // Moving Frame.
    case SpecifiedWindowFrame(frameType,
        FrameBoundaryExtractor(low), FrameBoundaryExtractor(high)) =>
      val lBoundOrdering = createBoundOrdering(frameType, low)
      val uBoundOrdering = createBoundOrdering(frameType, high)
      new SlidingWindowFunctionFrame(ordinal, functions, lBoundOrdering, uBoundOrdering)

    // Entire Partition Frame. 整个分区框架
    case SpecifiedWindowFrame(_, UnboundedPreceding, UnboundedFollowing) =>
      new UnboundedWindowFunctionFrame(ordinal, functions)

    // Error
    case fr =>
      sys.error(s"Unsupported Frame $fr for functions: $functions")
  }

  /**
   * Create the resulting projection.创建结果投影
   *
   * This method uses Code Generation. It can only be used on the executor side.
    * 此方法使用代码生成,它只能在执行程序端使用
   *
   * @param expressions unbound ordered function expressions.
   * @return the final resulting projection.
   */
  private[this] def createResultProjection(
      expressions: Seq[Expression]): MutableProjection = {
    val references = expressions.zipWithIndex.map{ case (e, i) =>
      // Results of window expressions will be on the right side of child's output
      //窗口表达式的结果将位于子输出的右侧
      BoundReference(child.output.size + i, e.dataType, e.nullable)
    }
    val unboundToRefMap = expressions.zip(references).toMap
    val patchedWindowExpression = windowExpression.map(_.transform(unboundToRefMap))
    newMutableProjection(
      projectList ++ patchedWindowExpression,
      child.output)()
  }

  protected override def doExecute(): RDD[InternalRow] = {
    // Prepare processing.
    // Group the window expression by their processing frame.
    //按照处理框架对窗口表达式进行分组
    val windowExprs = windowExpression.flatMap {
      _.collect {
        case e: WindowExpression => e
      }
    }

    // Create Frame processor factories and order the unbound window expressions by the frame they
    // are processed in; this is the order in which their results will be written to window
    // function result buffer.
    //创建Frame处理器工厂,并按处理它们的帧对未绑定的窗口表达式进行排序; 这是将结果写入窗口函数结果缓冲区的顺序。
    val framedWindowExprs = windowExprs.groupBy(_.windowSpec.frameSpecification)
    val factories = Array.ofDim[() => WindowFunctionFrame](framedWindowExprs.size)
    val unboundExpressions = mutable.Buffer.empty[Expression]
    framedWindowExprs.zipWithIndex.foreach {
      case ((frame, unboundFrameExpressions), index) =>
        // Track the ordinal.追踪序数
        val ordinal = unboundExpressions.size

        // Track the unbound expressions 跟踪未绑定的表达式
        unboundExpressions ++= unboundFrameExpressions

        // Bind the expressions.绑定表达式
        val functions = unboundFrameExpressions.map { e =>
          BindReferences.bindReference(e.windowFunction, child.output)
        }.toArray

        // Create the frame processor factory.创建帧处理器工厂
        factories(index) = () => createFrameProcessor(frame, functions, ordinal)
    }

      // Start processing.开始处理
    child.execute().mapPartitions { stream =>
      new Iterator[InternalRow] {

        // Get all relevant projections.获取所有相关预测
        val result = createResultProjection(unboundExpressions)
        val grouping = if (child.outputsUnsafeRows) {
          UnsafeProjection.create(partitionSpec, child.output)
        } else {
          newProjection(partitionSpec, child.output)
        }

        // Manage the stream and the grouping.
        //管理流和分组
        var nextRow: InternalRow = EmptyRow
        var nextGroup: InternalRow = EmptyRow
        var nextRowAvailable: Boolean = false
        private[this] def fetchNextRow() {
          nextRowAvailable = stream.hasNext
          if (nextRowAvailable) {
            nextRow = stream.next()
            nextGroup = grouping(nextRow)
          } else {
            nextRow = EmptyRow
            nextGroup = EmptyRow
          }
        }
        fetchNextRow()

        // Manage the current partition.
        //管理当前分区
        var rows: CompactBuffer[InternalRow] = _
        val frames: Array[WindowFunctionFrame] = factories.map(_())
        val numFrames = frames.length
        private[this] def fetchNextPartition() {
          // Collect all the rows in the current partition.
          // Before we start to fetch new input rows, make a copy of nextGroup.
          //收集当前分区中的所有行,在我们开始获取新输入行之前,请复制nextGroup
          val currentGroup = nextGroup.copy()
          rows = new CompactBuffer
          while (nextRowAvailable && nextGroup == currentGroup) {
            rows += nextRow.copy()
            fetchNextRow()
          }

          // Setup the frames.
          //设置框架
          var i = 0
          while (i < numFrames) {
            frames(i).prepare(rows)
            i += 1
          }

          // Setup iteration 设置迭代
          rowIndex = 0
          rowsSize = rows.size
        }

        // Iteration
        var rowIndex = 0
        var rowsSize = 0
        override final def hasNext: Boolean = rowIndex < rowsSize || nextRowAvailable

        val join = new JoinedRow
        val windowFunctionResult = new GenericMutableRow(unboundExpressions.size)
        override final def next(): InternalRow = {
          // Load the next partition if we need to.
          //如果需要,加载下一个分区
          if (rowIndex >= rowsSize && nextRowAvailable) {
            fetchNextPartition()
          }

          if (rowIndex < rowsSize) {
            // Get the results for the window frames.
            //获取窗框的结果
            var i = 0
            while (i < numFrames) {
              frames(i).write(windowFunctionResult)
              i += 1
            }

            // 'Merge' the input row with the window function result
            //'合并'输入行和窗口函数结果
            join(rows(rowIndex), windowFunctionResult)
            rowIndex += 1

            // Return the projection.
            result(join)
          } else throw new NoSuchElementException
        }
      }
    }
  }
}

/**
 * Function for comparing boundary values.
  * 用于比较边界值的函数
 */
private[execution] abstract class BoundOrdering {
  def compare(input: Seq[InternalRow], inputIndex: Int, outputIndex: Int): Int
}

/**
 * Compare the input index to the bound of the output index.
  * 将输入索引与输出索引的边界进行比较
 */
private[execution] final case class RowBoundOrdering(offset: Int) extends BoundOrdering {
  override def compare(input: Seq[InternalRow], inputIndex: Int, outputIndex: Int): Int =
    inputIndex - (outputIndex + offset)
}

/**
 * Compare the value of the input index to the value bound of the output index.
  * 将输入索引的值与输出索引的值边界进行比较
 */
private[execution] final case class RangeBoundOrdering(
    ordering: Ordering[InternalRow],
    current: Projection,
    bound: Projection) extends BoundOrdering {
  override def compare(input: Seq[InternalRow], inputIndex: Int, outputIndex: Int): Int =
    ordering.compare(current(input(inputIndex)), bound(input(outputIndex)))
}

/**
 * A window function calculates the results of a number of window functions for a window frame.
 * Before use a frame must be prepared by passing it all the rows in the current partition. After
 * preparation the update method can be called to fill the output rows.
  *
  * 窗口函数计算窗口框架的许多窗口函数的结果,在使用之前,必须通过将其传递给当前分区中的所有行来准备帧,
  * 准备好之后,可以调用update方法来填充输出行。
 *
 * TODO How to improve performance? A few thoughts:
 * - Window functions are expensive due to its distribution and ordering requirements.
 * Unfortunately it is up to the Spark engine to solve this. Improvements in the form of project
 * Tungsten are on the way.
 * - The window frame processing bit can be improved though. But before we start doing that we
 * need to see how much of the time and resources are spent on partitioning and ordering, and
 * how much time and resources are spent processing the partitions. There are a couple ways to
 * improve on the current situation:
 * - Reduce memory footprint by performing streaming calculations. This can only be done when
 * there are no Unbound/Unbounded Following calculations present.
 * - Use Tungsten style memory usage.
 * - Use code generation in general, and use the approach to aggregation taken in the
 *   GeneratedAggregate class in specific.
 *
 * @param ordinal of the first column written by this frame.
 * @param functions to calculate the row values with.
 */
private[execution] abstract class WindowFunctionFrame(
    ordinal: Int,
    functions: Array[WindowFunction]) {

  // Make sure functions are initialized.
  //确保初始化功能
  functions.foreach(_.init())

  /** Number of columns the window function frame is managing
    * 窗口功能框架管理的列数*/
  val numColumns = functions.length

  /**
   * Create a fresh thread safe copy of the frame.
    * 创建框架的新线程安全副本
   *
   * @return the copied frame.
   */
  def copy: WindowFunctionFrame

  /**
   * Create new instances of the functions.
    * 创建函数的新实例
   *
   * @return an array containing copies of the current window functions.
   */
  protected final def copyFunctions: Array[WindowFunction] = functions.map(_.newInstance())

  /**
   * Prepare the frame for calculating the results for a partition.
    * 准备框架以计算分区的结果
   *
   * @param rows to calculate the frame results for.
   */
  def prepare(rows: CompactBuffer[InternalRow]): Unit

  /**
   * Write the result for the current row to the given target row.
    * 将当前行的结果写入给定的目标行
   *
   * @param target row to write the result for the current row to.
   */
  def write(target: GenericMutableRow): Unit

  /** Reset the current window functions.
    * 重置当前窗口功能 */
  protected final def reset(): Unit = {
    var i = 0
    while (i < numColumns) {
      functions(i).reset()
      i += 1
    }
  }

  /** Prepare an input row for processing.
    * 准备输入行以进行处理*/
  protected final def prepare(input: InternalRow): Array[AnyRef] = {
    val prepared = new Array[AnyRef](numColumns)
    var i = 0
    while (i < numColumns) {
      prepared(i) = functions(i).prepareInputParameters(input)
      i += 1
    }
    prepared
  }

  /** Evaluate a prepared buffer (iterator).评估准备好的缓冲区(迭代器) */
  protected final def evaluatePrepared(iterator: java.util.Iterator[Array[AnyRef]]): Unit = {
    reset()
    while (iterator.hasNext) {
      val prepared = iterator.next()
      var i = 0
      while (i < numColumns) {
        functions(i).update(prepared(i))
        i += 1
      }
    }
    evaluate()
  }

  /** Evaluate a prepared buffer (array). 评估准备好的缓冲区(数组)*/
  protected final def evaluatePrepared(prepared: Array[Array[AnyRef]],
      fromIndex: Int, toIndex: Int): Unit = {
    var i = 0
    while (i < numColumns) {
      val function = functions(i)
      function.reset()
      var j = fromIndex
      while (j < toIndex) {
        function.update(prepared(j)(i))
        j += 1
      }
      function.evaluate()
      i += 1
    }
  }

  /** Update an array of window functions. 更新窗口函数数组*/
  protected final def update(input: InternalRow): Unit = {
    var i = 0
    while (i < numColumns) {
      val aggregate = functions(i)
      val preparedInput = aggregate.prepareInputParameters(input)
      aggregate.update(preparedInput)
      i += 1
    }
  }

  /** Evaluate the window functions. 评估窗口函数*/
  protected final def evaluate(): Unit = {
    var i = 0
    while (i < numColumns) {
      functions(i).evaluate()
      i += 1
    }
  }

  /** Fill a target row with the current window function results. 使用当前窗口函数结果填充目标行*/
  protected final def fill(target: GenericMutableRow, rowIndex: Int): Unit = {
    var i = 0
    while (i < numColumns) {
      target.update(ordinal + i, functions(i).get(rowIndex))
      i += 1
    }
  }
}

/**
 * The sliding window frame calculates frames with the following SQL form:
  * 滑动窗口框架使用以下SQL格式计算框架：
 * ... BETWEEN 1 PRECEDING AND 1 FOLLOWING
 *
 * @param ordinal of the first column written by this frame.
 * @param functions to calculate the row values with.
 * @param lbound comparator used to identify the lower bound of an output row.
 * @param ubound comparator used to identify the upper bound of an output row.
 */
private[execution] final class SlidingWindowFunctionFrame(
    ordinal: Int,
    functions: Array[WindowFunction],
    lbound: BoundOrdering,
    ubound: BoundOrdering) extends WindowFunctionFrame(ordinal, functions) {

  /** Rows of the partition currently being processed.
    * 正在处理的分区的行*/
  private[this] var input: CompactBuffer[InternalRow] = null

  /** Index of the first input row with a value greater than the upper bound of the current
    * output row.
    * 第一个输入行的索引,其值大于当前输出行的上限*/
  private[this] var inputHighIndex = 0

  /** Index of the first input row with a value equal to or greater than the lower bound of the
    * current output row.
    * 第一个输入行的索引,其值等于或大于当前输出行的下限*/
  private[this] var inputLowIndex = 0

  /** Buffer used for storing prepared input for the window functions.
    * 缓冲区用于存储窗口函数的准备输入*/
  private[this] val buffer = new util.ArrayDeque[Array[AnyRef]]

  /** Index of the row we are currently writing.
    * 我们目前正在编写的行的索引*/
  private[this] var outputIndex = 0

  /** Prepare the frame for calculating a new partition. Reset all variables.
    * 准备用于计算新分区的框架,重置所有变量*/
  override def prepare(rows: CompactBuffer[InternalRow]): Unit = {
    input = rows
    inputHighIndex = 0
    inputLowIndex = 0
    outputIndex = 0
    buffer.clear()
  }

  /** Write the frame columns for the current row to the given target row.
    * 将当前行的帧列写入给定目标行*/
  override def write(target: GenericMutableRow): Unit = {
    var bufferUpdated = outputIndex == 0

    // Add all rows to the buffer for which the input row value is equal to or less than
    // the output row upper bound.
    //将所有行添加到缓冲区,其输入行值等于或小于输出行上限
    while (inputHighIndex < input.size &&
        ubound.compare(input, inputHighIndex, outputIndex) <= 0) {
      buffer.offer(prepare(input(inputHighIndex)))
      inputHighIndex += 1
      bufferUpdated = true
    }

    // Drop all rows from the buffer for which the input row value is smaller than
    // the output row lower bound.
    //从缓冲区中删除输入行值小于输出行下限的所有行
    while (inputLowIndex < inputHighIndex &&
        lbound.compare(input, inputLowIndex, outputIndex) < 0) {
      buffer.pop()
      inputLowIndex += 1
      bufferUpdated = true
    }

    // Only recalculate and update when the buffer changes.
    //仅在缓冲区更改时重新计算和更新
    if (bufferUpdated) {
      evaluatePrepared(buffer.iterator())
      fill(target, outputIndex)
    }

    // Move to the next row.
    outputIndex += 1
  }

  /** Copy the frame. */
  override def copy: SlidingWindowFunctionFrame =
    new SlidingWindowFunctionFrame(ordinal, copyFunctions, lbound, ubound)
}

/**
 * The unbounded window frame calculates frames with the following SQL forms:
  * 无界窗口框架使用以下SQL形式计算框架
 * ... (No Frame Definition)
 * ... BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
 *
 * Its results are  the same for each and every row in the partition. This class can be seen as a
 * special case of a sliding window, but is optimized for the unbound case.
 *
 * @param ordinal of the first column written by this frame.
 * @param functions to calculate the row values with.
 */
private[execution] final class UnboundedWindowFunctionFrame(
    ordinal: Int,
    functions: Array[WindowFunction]) extends WindowFunctionFrame(ordinal, functions) {

  /** Index of the row we are currently writing.
    * 我们目前正在编写的行的索引 */
  private[this] var outputIndex = 0

  /** Prepare the frame for calculating a new partition. Process all rows eagerly.
    * 准备用于计算新分区的框架,急切地处理所有行*/
  override def prepare(rows: CompactBuffer[InternalRow]): Unit = {
    reset()
    outputIndex = 0
    val iterator = rows.iterator
    while (iterator.hasNext) {
      update(iterator.next())
    }
    evaluate()
  }

  /** Write the frame columns for the current row to the given target row.
    * 将当前行的帧列写入给定目标行 */
  override def write(target: GenericMutableRow): Unit = {
    fill(target, outputIndex)
    outputIndex += 1
  }

  /** Copy the frame. */
  override def copy: UnboundedWindowFunctionFrame =
    new UnboundedWindowFunctionFrame(ordinal, copyFunctions)
}

/**
 * The UnboundPreceding window frame calculates frames with the following SQL form:
  * UnboundPreceding窗口框架使用以下SQL表单计算框架：
 * ... BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
 *
 * There is only an upper bound. Very common use cases are for instance running sums or counts
 * (row_number). Technically this is a special case of a sliding window. However a sliding window
 * has to maintain a buffer, and it must do a full evaluation everytime the buffer changes. This
 * is not the case when there is no lower bound, given the additive nature of most aggregates
 * streaming updates and partial evaluation suffice and no buffering is needed.
 *
 * @param ordinal of the first column written by this frame.
 * @param functions to calculate the row values with.
 * @param ubound comparator used to identify the upper bound of an output row.
 */
private[execution] final class UnboundedPrecedingWindowFunctionFrame(
    ordinal: Int,
    functions: Array[WindowFunction],
    ubound: BoundOrdering) extends WindowFunctionFrame(ordinal, functions) {

  /** Rows of the partition currently being processed.正在处理的分区的行 */
  private[this] var input: CompactBuffer[InternalRow] = null

  /** Index of the first input row with a value greater than the upper bound of the current
    * output row. 第一个输入行的索引,其值大于当前输出行的上限*/
  private[this] var inputIndex = 0

  /** Index of the row we are currently writing.
    * 我们目前正在编写的行的索引*/
  private[this] var outputIndex = 0

  /** Prepare the frame for calculating a new partition. 准备用于计算新分区的框架*/
  override def prepare(rows: CompactBuffer[InternalRow]): Unit = {
    reset()
    input = rows
    inputIndex = 0
    outputIndex = 0
  }

  /** Write the frame columns for the current row to the given target row.
    * 将当前行的帧列写入给定目标行*/
  override def write(target: GenericMutableRow): Unit = {
    var bufferUpdated = outputIndex == 0

    // Add all rows to the aggregates for which the input row value is equal to or less than
    // the output row upper bound.
    //将所有行添加到输入行值等于或小于输出行上限的聚合
    while (inputIndex < input.size && ubound.compare(input, inputIndex, outputIndex) <= 0) {
      update(input(inputIndex))
      inputIndex += 1
      bufferUpdated = true
    }

    // Only recalculate and update when the buffer changes.
    //仅在缓冲区更改时重新计算和更新
    if (bufferUpdated) {
      evaluate()
      fill(target, outputIndex)
    }

    // Move to the next row.
    outputIndex += 1
  }

  /** Copy the frame. */
  override def copy: UnboundedPrecedingWindowFunctionFrame =
    new UnboundedPrecedingWindowFunctionFrame(ordinal, copyFunctions, ubound)
}

/**
 * The UnboundFollowing window frame calculates frames with the following SQL form:
  * UnboundFollowing窗口框架使用以下SQL表单计算框架：
 * ... BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
 *
 * There is only an upper bound. This is a slightly modified version of the sliding window. The
 * sliding window operator has to check if both upper and the lower bound change when a new row
 * gets processed, where as the unbounded following only has to check the lower bound.
  * 只有一个上限,这是滑动窗口的略微修改版本,滑动窗口操作符必须检查在处理新行时上限和下限是否都会发生变化,
  * 而无界后续操作只需要检查下限
 *
 * This is a very expensive operator to use, O(n * (n - 1) /2), because we need to maintain a
 * buffer and must do full recalculation after each row. Reverse iteration would be possible, if
 * the communitativity of the used window functions can be guaranteed.
 *
 * @param ordinal of the first column written by this frame.
 * @param functions to calculate the row values with.
 * @param lbound comparator used to identify the lower bound of an output row.
 */
private[execution] final class UnboundedFollowingWindowFunctionFrame(
    ordinal: Int,
    functions: Array[WindowFunction],
    lbound: BoundOrdering) extends WindowFunctionFrame(ordinal, functions) {

  /** Buffer used for storing prepared input for the window functions.
    * 缓冲区用于存储窗口函数的准备输入*/
  private[this] var buffer: Array[Array[AnyRef]] = _

  /** Rows of the partition currently being processed.
    * 正在处理的分区的行*/
  private[this] var input: CompactBuffer[InternalRow] = null

  /** Index of the first input row with a value equal to or greater than the lower bound of the
    * current output row.
    * 第一个输入行的索引,其值等于或大于当前输出行的下限*/
  private[this] var inputIndex = 0

  /** Index of the row we are currently writing.
    * 我们目前正在编写的行的索引*/
  private[this] var outputIndex = 0

  /** Prepare the frame for calculating a new partition.
    * 准备用于计算新分区的框架*/
  override def prepare(rows: CompactBuffer[InternalRow]): Unit = {
    input = rows
    inputIndex = 0
    outputIndex = 0
    val size = input.size
    buffer = Array.ofDim(size)
    var i = 0
    while (i < size) {
      buffer(i) = prepare(input(i))
      i += 1
    }
    evaluatePrepared(buffer, 0, buffer.length)
  }

  /** Write the frame columns for the current row to the given target row.
    * 将当前行的帧列写入给定目标行*/
  override def write(target: GenericMutableRow): Unit = {
    var bufferUpdated = outputIndex == 0

    // Drop all rows from the buffer for which the input row value is smaller than
    // the output row lower bound.
    //从缓冲区中删除输入行值小于输出行下限的所有行
    while (inputIndex < input.size && lbound.compare(input, inputIndex, outputIndex) < 0) {
      inputIndex += 1
      bufferUpdated = true
    }

    // Only recalculate and update when the buffer changes.
    //仅在缓冲区更改时重新计算和更新
    if (bufferUpdated) {
      evaluatePrepared(buffer, inputIndex, buffer.length)
      fill(target, outputIndex)
    }

    // Move to the next row.
    outputIndex += 1
  }

  /** Copy the frame. */
  override def copy: UnboundedFollowingWindowFunctionFrame =
    new UnboundedFollowingWindowFunctionFrame(ordinal, copyFunctions, lbound)
}
