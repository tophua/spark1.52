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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._

/**
 * An expression that produces zero or more rows given a single input row.
  * 在给定单个输入行的情况下生成零行或多行的表达式
 *
 * Generators produce multiple output rows instead of a single value like other expressions,
 * and thus they must have a schema to associate with the rows that are output.
  * 生成器像其他表达式一样生成多个输出行而不是单个值,因此它们必须具有与输出的行关联的模式。
 *
 * However, unlike row producing relational operators, which are either leaves or determine their
 * output schema functionally from their input, generators can contain other expressions that
 * might result in their modification by rules.  This structure means that they might be copied
 * multiple times after first determining their output schema. If a new output schema is created for
 * each copy references up the tree might be rendered invalid. As a result generators must
 * instead define a function `makeOutput` which is called only once when the schema is first
 * requested.  The attributes produced by this function will be automatically copied anytime rules
 * result in changes to the Generator or its children.
  * 但是,与生成关系运算符的行不同,生成器关系运算符要么离开,要么从输入中确定其输出模式,
  * 生成器可以包含其他可能导致其按规则修改的表达式
 */
trait Generator extends Expression {

  // TODO ideally we should return the type of ArrayType(StructType),
  // however, we don't keep the output field names in the Generator.
  override def dataType: DataType = throw new UnsupportedOperationException

  override def foldable: Boolean = false

  override def nullable: Boolean = false

  /**
   * The output element data types in structure of Seq[(DataType, Nullable)]
    * Seq [（DataType，Nullable）]结构中的输出元素数据类型
   * TODO we probably need to add more information like metadata etc.
   */
  def elementTypes: Seq[(DataType, Boolean)]

  /** Should be implemented by child classes to perform specific Generators.
    * 应由子类实现以执行特定的生成器 */
  override def eval(input: InternalRow): TraversableOnce[InternalRow]

  /**
   * Notifies that there are no more rows to process, clean up code, and additional
   * rows can be made here.
    * 通知没有更多行要处理,清理代码,并且可以在此处创建其他行
   */
  def terminate(): TraversableOnce[InternalRow] = Nil
}

/**
 * A generator that produces its output using the provided lambda function.
  * 使用提供的lambda函数生成其输出的生成器。
 */
case class UserDefinedGenerator(
    elementTypes: Seq[(DataType, Boolean)],
    function: Row => TraversableOnce[InternalRow],
    children: Seq[Expression])
  extends Generator with CodegenFallback {

  @transient private[this] var inputRow: InterpretedProjection = _
  @transient private[this] var convertToScala: (InternalRow) => Row = _

  private def initializeConverters(): Unit = {
    inputRow = new InterpretedProjection(children)
    convertToScala = {
      val inputSchema = StructType(children.map(e => StructField(e.simpleString, e.dataType, true)))
      CatalystTypeConverters.createToScalaConverter(inputSchema)
    }.asInstanceOf[InternalRow => Row]
  }

  override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
    if (inputRow == null) {
      initializeConverters()
    }
    // Convert the objects into Scala Type before calling function, we need schema to support UDT
     //在调用函数之前将对象转换为Scala Type，我们需要schema来支持UDT
    function(convertToScala(inputRow(input)))
  }

  override def toString: String = s"UserDefinedGenerator(${children.mkString(",")})"
}

/**
 * Given an input array produces a sequence of rows for each value in the array.
  * 给定输入数组为数组中的每个值生成一系列行
 */
case class Explode(child: Expression) extends UnaryExpression with Generator with CodegenFallback {
  //Nil是一个空的List,::向队列的头部追加数据,创造新的列表
  override def children: Seq[Expression] = child :: Nil

  override def checkInputDataTypes(): TypeCheckResult = {
    if (child.dataType.isInstanceOf[ArrayType] || child.dataType.isInstanceOf[MapType]) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      TypeCheckResult.TypeCheckFailure(
        s"input to function explode should be array or map type, not ${child.dataType}")
    }
  }

  override def elementTypes: Seq[(DataType, Boolean)] = child.dataType match {
    //Nil是一个空的List,::向队列的头部追加数据,创造新的列表
    case ArrayType(et, containsNull) => (et, containsNull) :: Nil
    //Nil是一个空的List,::向队列的头部追加数据,创造新的列表
    case MapType(kt, vt, valueContainsNull) => (kt, false) :: (vt, valueContainsNull) :: Nil
  }

  override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
    child.dataType match {
      case ArrayType(et, _) =>
        val inputArray = child.eval(input).asInstanceOf[ArrayData]
        if (inputArray == null) {
          Nil
        } else {
          val rows = new Array[InternalRow](inputArray.numElements())
          inputArray.foreach(et, (i, e) => {
            rows(i) = InternalRow(e)
          })
          rows
        }
      case MapType(kt, vt, _) =>
        val inputMap = child.eval(input).asInstanceOf[MapData]
        if (inputMap == null) {
          Nil
        } else {
          val rows = new Array[InternalRow](inputMap.numElements())
          var i = 0
          inputMap.foreach(kt, vt, (k, v) => {
            rows(i) = InternalRow(k, v)
            i += 1
          })
          rows
        }
    }
  }
}
