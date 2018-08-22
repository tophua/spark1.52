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

package org.apache.spark.sql.expressions

import org.apache.spark.sql.catalyst.expressions.aggregate.{Complete, AggregateExpression2}
import org.apache.spark.sql.execution.aggregate.ScalaUDAF
import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.types._
import org.apache.spark.annotation.Experimental

/**
 * :: Experimental ::
 * The base class for implementing user-defined aggregate functions (UDAF).
  * 实现用户定义的聚合函数(UDAF)的基类
 */
@Experimental
abstract class UserDefinedAggregateFunction extends Serializable {

  /**
   * A [[StructType]] represents data types of input arguments of this aggregate function.
   * For example, if a [[UserDefinedAggregateFunction]] expects two input arguments
   * with type of [[DoubleType]] and [[LongType]], the returned [[StructType]] will look like
    *
    * [[StructType]]表示此聚合函数的输入参数的数据类型,例如,如果[[UserDefinedAggregateFunction]]
    * 需要两个类型为[[DoubleType]]和[[LongType]]的输入参数,则返回的[[StructType]]将看起来像
   *
   * ```
   *   new StructType()
   *    .add("doubleInput", DoubleType)
   *    .add("longInput", LongType)
   * ```
   *
   * The name of a field of this [[StructType]] is only used to identify the corresponding
   * input argument. Users can choose names to identify the input arguments.
    * 此[[StructType]]的字段名称仅用于标识相应的输入参数,用户可以选择名称来标识输入参数
   */
  def inputSchema: StructType

  /**
   * A [[StructType]] represents data types of values in the aggregation buffer.
   * For example, if a [[UserDefinedAggregateFunction]]'s buffer has two values
   * (i.e. two intermediate values) with type of [[DoubleType]] and [[LongType]],
   * the returned [[StructType]] will look like
    *
    * [[StructType]]表示聚合缓冲区中值的数据类型,
    * 例如，如果[[UserDefinedAggregateFunction]]的缓冲区有两个值（即两个中间值）,
    * 类型为[[DoubleType]]和[[LongType]],（即两个中间值）类型为[[DoubleType] ]和[[LongType]]，
   *
   * ```
   *   new StructType()
   *    .add("doubleInput", DoubleType)
   *    .add("longInput", LongType)
   * ```
   *
   * The name of a field of this [[StructType]] is only used to identify the corresponding
   * buffer value. Users can choose names to identify the input arguments.
    * 此[[StructType]]的字段名称仅用于标识相应的缓冲区值,用户可以选择名称来标识输入参数。
   */
  def bufferSchema: StructType

  /**
   * The [[DataType]] of the returned value of this [[UserDefinedAggregateFunction]].
    * 此[[UserDefinedAggregateFunction]]的返回值的[[DataType]]
   */
  def dataType: DataType

  /**
   * Returns true iff this function is deterministic, i.e. given the same input,
   * always return the same output.
    * 如果此函数是确定性的,则返回true,即如果给定相同的输入,则始终返回相同的输出
   */
  def deterministic: Boolean

  /**
   * Initializes the given aggregation buffer, i.e. the zero value of the aggregation buffer.
    * 初始化给定的聚合缓冲区,即聚合缓冲区的零值
   *
   * The contract should be that applying the merge function on two initial buffers should just
   * return the initial buffer itself, i.e.
   * `merge(initialBuffer, initialBuffer)` should equal `initialBuffer`.
   */
  def initialize(buffer: MutableAggregationBuffer): Unit

  /**
   * Updates the given aggregation buffer `buffer` with new input data from `input`.
    * 使用来自`input`的新输入数据更新给定的聚合缓冲区`buffer`
   *
   * This is called once per input row.
   */
  def update(buffer: MutableAggregationBuffer, input: Row): Unit

  /**
   * Merges two aggregation buffers and stores the updated buffer values back to `buffer1`.
    * 合并两个聚合缓冲区并将更新的缓冲区值存储回“buffer1”
   *
   * This is called when we merge two partially aggregated data together.
   */
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit

  /**
   * Calculates the final result of this [[UserDefinedAggregateFunction]] based on the given
   * aggregation buffer.
    * 根据给定的聚合缓冲区计算此[[UserDefinedAggregateFunction]]的最终结果
   */
  def evaluate(buffer: Row): Any

  /**
   * Creates a [[Column]] for this UDAF using given [[Column]]s as input arguments.
    * 使用给定[[Column]] s作为输入参数为此UDAF创建[[Column]]
   */
  @scala.annotation.varargs
  def apply(exprs: Column*): Column = {
    val aggregateExpression =
      AggregateExpression2(
        ScalaUDAF(exprs.map(_.expr), this),
        Complete,
        isDistinct = false)
    Column(aggregateExpression)
  }

  /**
   * Creates a [[Column]] for this UDAF using the distinct values of the given
   * [[Column]]s as input arguments.
    * 使用给定[[Column]]的不同值作为输入参数,为此UDAF创建[[Column]]
   */
  @scala.annotation.varargs
  def distinct(exprs: Column*): Column = {
    val aggregateExpression =
      AggregateExpression2(
        ScalaUDAF(exprs.map(_.expr), this),
        Complete,
        isDistinct = true)
    Column(aggregateExpression)
  }
}

/**
 * :: Experimental ::
 * A [[Row]] representing an mutable aggregation buffer.
 *
 * This is not meant to be extended outside of Spark.
 */
@Experimental
abstract class MutableAggregationBuffer extends Row {

  /** Update the ith value of this buffer. 更新此缓冲区的第i个值*/
  def update(i: Int, value: Any): Unit
}
