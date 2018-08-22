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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{GenerateSafeProjection, GenerateUnsafeProjection}
import org.apache.spark.sql.types.{DataType, Decimal, StructType, _}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

/**
 * A [[Projection]] that is calculated by calling the `eval` of each of the specified expressions.
  * 一个[[Projection]]，它是通过调用每个指定表达式的“eval”来计算的
 * @param expressions a sequence of expressions that determine the value of each column of the
 *                    output row.
 */
class InterpretedProjection(expressions: Seq[Expression]) extends Projection {
  def this(expressions: Seq[Expression], inputSchema: Seq[Attribute]) =
    this(expressions.map(BindReferences.bindReference(_, inputSchema)))

  expressions.foreach(_.foreach {
    case n: Nondeterministic => n.setInitialValues()
    case _ =>
  })

  // null check is required for when Kryo invokes the no-arg constructor.
  //当Kryo调用no-arg构造函数时,需要进行null检查
  protected val exprArray = if (expressions != null) expressions.toArray else null

  def apply(input: InternalRow): InternalRow = {
    val outputArray = new Array[Any](exprArray.length)
    var i = 0
    while (i < exprArray.length) {
      outputArray(i) = exprArray(i).eval(input)
      i += 1
    }
    new GenericInternalRow(outputArray)
  }

  override def toString(): String = s"Row => [${exprArray.mkString(",")}]"
}

/**
 * A [[MutableProjection]] that is calculated by calling `eval` on each of the specified
 * expressions.
  * 一个[[MutableProjection]],通过在每个指定的上调用“eval”来计算表达式
 * @param expressions a sequence of expressions that determine the value of each column of the
 *                    output row.
 */
case class InterpretedMutableProjection(expressions: Seq[Expression]) extends MutableProjection {
  def this(expressions: Seq[Expression], inputSchema: Seq[Attribute]) =
    this(expressions.map(BindReferences.bindReference(_, inputSchema)))

  expressions.foreach(_.foreach {
    case n: Nondeterministic => n.setInitialValues()
    case _ =>
  })

  private[this] val exprArray = expressions.toArray
  private[this] var mutableRow: MutableRow = new GenericMutableRow(exprArray.length)
  def currentValue: InternalRow = mutableRow

  override def target(row: MutableRow): MutableProjection = {
    mutableRow = row
    this
  }

  override def apply(input: InternalRow): InternalRow = {
    var i = 0
    while (i < exprArray.length) {
      mutableRow(i) = exprArray(i).eval(input)
      i += 1
    }
    mutableRow
  }
}

/**
 * A projection that returns UnsafeRow.
  * 返回UnsafeRow的投影
 */
abstract class UnsafeProjection extends Projection {
  override def apply(row: InternalRow): UnsafeRow
}

object UnsafeProjection {

  /*
   * Returns whether UnsafeProjection can support given StructType, Array[DataType] or
   * Seq[Expression].
   * 返回UnsafeProjection是否可以支持给定的StructType,Array [DataType]或Seq [Expression]
   */
  def canSupport(schema: StructType): Boolean = canSupport(schema.fields.map(_.dataType))
  def canSupport(exprs: Seq[Expression]): Boolean = canSupport(exprs.map(_.dataType).toArray)
  private def canSupport(types: Array[DataType]): Boolean = {
    types.forall(GenerateUnsafeProjection.canSupport)
  }

  /**
   * Returns an UnsafeProjection for given StructType.
    * 返回给定StructType的UnsafeProjection
   */
  def create(schema: StructType): UnsafeProjection = create(schema.fields.map(_.dataType))

  /**
   * Returns an UnsafeProjection for given Array of DataTypes.
    * 返回给定数据类型数组的不安全投影
   */
  def create(fields: Array[DataType]): UnsafeProjection = {
    create(fields.zipWithIndex.map(x => new BoundReference(x._2, x._1, true)))
  }

  /**
   * Returns an UnsafeProjection for given sequence of Expressions (bounded).
    * 返回给定的表达式序列(有界)的UnsafeProjection
   */
  def create(exprs: Seq[Expression]): UnsafeProjection = {
    GenerateUnsafeProjection.generate(exprs)
  }

  def create(expr: Expression): UnsafeProjection = create(Seq(expr))

  /**
   * Returns an UnsafeProjection for given sequence of Expressions, which will be bound to
   * `inputSchema`.
    * 返回给定的表达式序列的UnsafeProjection,它将绑定到`inputSchema`
   */
  def create(exprs: Seq[Expression], inputSchema: Seq[Attribute]): UnsafeProjection = {
    create(exprs.map(BindReferences.bindReference(_, inputSchema)))
  }
}

/**
 * A projection that could turn UnsafeRow into GenericInternalRow
  * 可以将UnsafeRow转换为GenericInternalRow的投影
 */
object FromUnsafeProjection {

  /**
   * Returns an Projection for given StructType.
    * 返回给定StructType的Projection
   */
  def apply(schema: StructType): Projection = {
    apply(schema.fields.map(_.dataType))
  }

  /**
   * Returns an UnsafeProjection for given Array of DataTypes.
    * 返回给定数据类型数组的不安全投影
   */
  def apply(fields: Seq[DataType]): Projection = {
    create(fields.zipWithIndex.map(x => {
      new BoundReference(x._2, x._1, true)
    }))
  }

  /**
   * Returns an Projection for given sequence of Expressions (bounded).
    * 返回给定表达式序列(有界)的Projection
   */
  private def create(exprs: Seq[Expression]): Projection = {
    GenerateSafeProjection.generate(exprs)
  }
}
