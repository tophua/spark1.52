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

package org.apache.spark.sql.catalyst.plans

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression, VirtualColumn}
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.types.{DataType, StructType}

abstract class QueryPlan[PlanType <: TreeNode[PlanType]] extends TreeNode[PlanType] {
  self: PlanType =>

  def output: Seq[Attribute]

  /**
   * Returns the set of attributes that are output by this node.
    * 返回此节点输出的属性集
   */
  def outputSet: AttributeSet = AttributeSet(output)

  /**
   * All Attributes that appear in expressions from this operator.  Note that this set does not
   * include attributes that are implicitly referenced by being passed through to the output tuple.
    *
    * 此运算符的表达式中显示的所有属性,请注意,此集合不包含通过传递给输出元组而隐式引用的属性。
   */
  def references: AttributeSet = AttributeSet(expressions.flatMap(_.references))

  /**
   * The set of all attributes that are input to this operator by its children.
    * 由其子项输入到此运算符的所有属性的集合
   */
  def inputSet: AttributeSet =
    AttributeSet(children.flatMap(_.asInstanceOf[QueryPlan[PlanType]].output))

  /**
   * Attributes that are referenced by expressions but not provided by this nodes children.
   * Subclasses should override this method if they produce attributes internally as it is used by
   * assertions designed to prevent the construction of invalid plans.
    *
    * 由表达式引用但未由此节点子节点提供的属性,子类应该覆盖此方法,
    * 如果它们在内部生成属性,因为它被用于防止构造无效计划的断言使用。
   *
   * Note that virtual columns should be excluded. Currently, we only support the grouping ID
   * virtual column.
   */
  def missingInput: AttributeSet =
    (references -- inputSet).filter(_.name != VirtualColumn.groupingIdName)

  /**
   * Runs [[transform]] with `rule` on all expressions present in this query operator.
   * Users should not expect a specific directionality. If a specific directionality is needed,
   * transformExpressionsDown or transformExpressionsUp should be used.
    * 对此查询运算符中存在的所有表达式运行[[transform]]和`rule`。用户不应期望具体的方向性,
    * 如果需要特定的方向性,则应使用transformExpressionsDown或transformExpressionsUp。
   * @param rule the rule to be applied to every expression in this operator.
   */
  def transformExpressions(rule: PartialFunction[Expression, Expression]): this.type = {
    transformExpressionsDown(rule)
  }

  /**
   * Runs [[transformDown]] with `rule` on all expressions present in this query operator.
    * 对此查询运算符中存在的所有表达式运行[[transformDown]]和`rule`
   * @param rule the rule to be applied to every expression in this operator.
   */
  def transformExpressionsDown(rule: PartialFunction[Expression, Expression]): this.type = {
    var changed = false

    @inline def transformExpressionDown(e: Expression): Expression = {
      val newE = e.transformDown(rule)
      if (newE.fastEquals(e)) {
        e
      } else {
        changed = true
        newE
      }
    }

    def recursiveTransform(arg: Any): AnyRef = arg match {
      case e: Expression => transformExpressionDown(e)
      case Some(e: Expression) => Some(transformExpressionDown(e))
      case m: Map[_, _] => m
      case d: DataType => d // Avoid unpacking Structs
      case seq: Traversable[_] => seq.map(recursiveTransform)
      case other: AnyRef => other
    }

    val newArgs = productIterator.map(recursiveTransform).toArray

    if (changed) makeCopy(newArgs).asInstanceOf[this.type] else this
  }

  /**
   * Runs [[transformUp]] with `rule` on all expressions present in this query operator.
    * 对此查询运算符中存在的所有表达式运行[[transformUp]]和`rule`
   * @param rule the rule to be applied to every expression in this operator.
   * @return
   */
  def transformExpressionsUp(rule: PartialFunction[Expression, Expression]): this.type = {
    var changed = false

    @inline def transformExpressionUp(e: Expression): Expression = {
      val newE = e.transformUp(rule)
      if (newE.fastEquals(e)) {
        e
      } else {
        changed = true
        newE
      }
    }

    def recursiveTransform(arg: Any): AnyRef = arg match {
      case e: Expression => transformExpressionUp(e)
      case Some(e: Expression) => Some(transformExpressionUp(e))
      case m: Map[_, _] => m
      case d: DataType => d // Avoid unpacking Structs
      case seq: Traversable[_] => seq.map(recursiveTransform)
      case other: AnyRef => other
    }

    val newArgs = productIterator.map(recursiveTransform).toArray

    if (changed) makeCopy(newArgs).asInstanceOf[this.type] else this
  }

  /** Returns the result of running [[transformExpressions]] on this node
    * and all its children.
    * 返回在此节点及其所有子节点上运行[[transformExpressions]]的结果*/
  def transformAllExpressions(rule: PartialFunction[Expression, Expression]): this.type = {
    transform {
      case q: QueryPlan[_] => q.transformExpressions(rule).asInstanceOf[PlanType]
    }.asInstanceOf[this.type]
  }

  /** Returns all of the expressions present in this query plan operator.
    * 返回此查询计划运算符中存在的所有表达式*/
  def expressions: Seq[Expression] = {
    productIterator.flatMap {
      case e: Expression => e :: Nil
      case Some(e: Expression) => e :: Nil
      case seq: Traversable[_] => seq.flatMap {
        case e: Expression => e :: Nil
        case other => Nil
      }
      case other => Nil
    }.toSeq
  }

  lazy val schema: StructType = StructType.fromAttributes(output)

  /** Returns the output schema in the tree format.
    * 以树格式返回输出模式 */
  def schemaString: String = schema.treeString

  /** Prints out the schema in the tree format
    * 以树格式打印出架构*/
  // scalastyle:off println
  def printSchema(): Unit = println(schemaString)
  // scalastyle:on println

  /**
   * A prefix string used when printing the plan.
   * 打印计划时使用的前缀字符串
   * We use "!" to indicate an invalid plan, and "'" to indicate an unresolved plan.
   */
  protected def statePrefix = if (missingInput.nonEmpty && children.nonEmpty) "!" else ""

  override def simpleString: String = statePrefix + super.simpleString
}
