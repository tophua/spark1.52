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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, TreeNode}


abstract class LogicalPlan extends QueryPlan[LogicalPlan] with Logging {

  private var _analyzed: Boolean = false

  /**
   * Marks this plan as already analyzed.  This should only be called by CheckAnalysis.
    * 将此计划标记为已分析,这应该只由CheckAnalysis调用
   */
  private[catalyst] def setAnalyzed(): Unit = { _analyzed = true }

  /**
   * Returns true if this node and its children have already been gone through analysis and
   * verification.  Note that this is only an optimization used to avoid analyzing trees that
   * have already been analyzed, and can be reset by transformations.
    * 如果此节点及其子节点已经过分析和验证,则返回true,
    * 请注意,这只是用于避免分析已经分析的树的优化,并且可以通过转换重置
   */
  def analyzed: Boolean = _analyzed

  /**
   * Returns a copy of this node where `rule` has been recursively applied first to all of its
   * children and then itself (post-order). When `rule` does not apply to a given node, it is left
   * unchanged.  This function is similar to `transformUp`, but skips sub-trees that have already
   * been marked as analyzed.
    * 返回此节点的副本,其中`rule`首先递归地应用于其所有子节点,然后自身(post-order),
    * 当`rule`不适用于给定节点时,它保持不变,此函数类似于`transformUp`,但跳过已标记为已分析的子树
   *
   * @param rule the function use to transform this nodes children
   */
  def resolveOperators(rule: PartialFunction[LogicalPlan, LogicalPlan]): LogicalPlan = {
    if (!analyzed) {
      val afterRuleOnChildren = transformChildren(rule, (t, r) => t.resolveOperators(r))
      if (this fastEquals afterRuleOnChildren) {
        CurrentOrigin.withOrigin(origin) {
          rule.applyOrElse(this, identity[LogicalPlan])
        }
      } else {
        CurrentOrigin.withOrigin(origin) {
          rule.applyOrElse(afterRuleOnChildren, identity[LogicalPlan])
        }
      }
    } else {
      this
    }
  }

  /**
   * Recursively transforms the expressions of a tree, skipping nodes that have already
   * been analyzed.
    * 递归转换树的表达式,跳过已经分析过的节点
   */
  def resolveExpressions(r: PartialFunction[Expression, Expression]): LogicalPlan = {
    this resolveOperators  {
      case p => p.transformExpressions(r)
    }
  }

  /**
   * Computes [[Statistics]] for this plan. The default implementation assumes the output
   * cardinality is the product of of all child plan's cardinality, i.e. applies in the case
   * of cartesian joins.
   *
   * [[LeafNode]]s must override this.
   */
  def statistics: Statistics = {
    if (children.size == 0) {
      throw new UnsupportedOperationException(s"LeafNode $nodeName must implement statistics.")
    }
    Statistics(sizeInBytes = children.map(_.statistics.sizeInBytes).product)
  }

  /**
   * Returns true if this expression and all its children have been resolved to a specific schema
   * and false if it still contains any unresolved placeholders. Implementations of LogicalPlan
   * can override this (e.g.
   * [[org.apache.spark.sql.catalyst.analysis.UnresolvedRelation UnresolvedRelation]]
   * should return `false`).
    * 如果此表达式及其所有子项已解析为特定模式,
    * 则返回true;如果该表达式仍包含任何未解析的占位符,则返回false,LogicalPlan的实现可以覆盖它
    *
   */
  lazy val resolved: Boolean = expressions.forall(_.resolved) && childrenResolved

  override protected def statePrefix = if (!resolved) "'" else super.statePrefix

  /**
   * Returns true if all its children of this query plan have been resolved.
    * 如果已解析此查询计划的所有子项,则返回true
   */
  def childrenResolved: Boolean = children.forall(_.resolved)

  /**
   * Returns true when the given logical plan will return the same results as this logical plan.
    * 当给定的逻辑计划将返回与此逻辑计划相同的结果时,返回true
   *
   * Since its likely undecidable to generally determine if two given plans will produce the same
   * results, it is okay for this function to return false, even if the results are actually
   * the same.  Such behavior will not affect correctness, only the application of performance
   * enhancements like caching.  However, it is not acceptable to return true if the results could
   * possibly be different.
    *
    * 由于通常确定两个给定的计划是否会产生相同的结果可能是不可判定的,
    * 因此即使结果实际上相同,该函数也可以返回false,这种行为不会影响正确性,只会影响性能增强,如缓存,
    * 但是,如果结果可能不同,则返回true是不可接受的
   *
   * By default this function performs a modified version of equality that is tolerant of cosmetic
   * differences like attribute naming and or expression id differences.  Logical operators that
   * can do better should override this function.
   */
  def sameResult(plan: LogicalPlan): Boolean = {
    val cleanLeft = EliminateSubQueries(this)
    val cleanRight = EliminateSubQueries(plan)

    cleanLeft.getClass == cleanRight.getClass &&
      cleanLeft.children.size == cleanRight.children.size && {
      logDebug(
        s"[${cleanRight.cleanArgs.mkString(", ")}] == [${cleanLeft.cleanArgs.mkString(", ")}]")
      cleanRight.cleanArgs == cleanLeft.cleanArgs
    } &&
    (cleanLeft.children, cleanRight.children).zipped.forall(_ sameResult _)
  }

  /** Args that have cleaned such that differences in expression id should not affect equality
    * 清除了表达式id差异的Args不应影响相等性*/
  protected lazy val cleanArgs: Seq[Any] = {
    val input = children.flatMap(_.output)
    productIterator.map {
      // Children are checked using sameResult above.
      //使用上面的sameResult检查子
      case tn: TreeNode[_] if containsChild(tn) => null
      case e: Expression => BindReferences.bindReference(e, input, allowFailures = true)
      case s: Option[_] => s.map {
        case e: Expression => BindReferences.bindReference(e, input, allowFailures = true)
        case other => other
      }
      case s: Seq[_] => s.map {
        case e: Expression => BindReferences.bindReference(e, input, allowFailures = true)
        case other => other
      }
      case other => other
    }.toSeq
  }

  /**
   * Optionally resolves the given strings to a [[NamedExpression]] using the input from all child
   * nodes of this LogicalPlan. The attribute is expressed as
   * as string in the following form: `[scope].AttributeName.[nested].[fields]...`.
    *
    * (可选)使用此LogicalPlan的所有子节点的输入将给定字符串解析为[[NamedExpression]]。
    * 该属性以下列形式表示为字符串：`[scope] .AttributeName。[nested]。[fields] ...`。
   */
  def resolveChildren(
      nameParts: Seq[String],
      resolver: Resolver): Option[NamedExpression] =
    resolve(nameParts, children.flatMap(_.output), resolver)

  /**
   * Optionally resolves the given strings to a [[NamedExpression]] based on the output of this
   * LogicalPlan. The attribute is expressed as string in the following form:
   * `[scope].AttributeName.[nested].[fields]...`.
    * (可选)根据此LogicalPlan的输出将给定字符串解析为[[NamedExpression]],该属性以下列形式表示为字符串：
   */
  def resolve(
      nameParts: Seq[String],
      resolver: Resolver): Option[NamedExpression] =
    resolve(nameParts, output, resolver)

  /**
   * Given an attribute name, split it to name parts by dot, but
   * don't split the name parts quoted by backticks, for example,
    * 给定属性名称,将其拆分为逐点命名,但不要拆分反引号引用的名称部分,例如
   * `ab.cd`.`efg` should be split into two parts "ab.cd" and "efg".
   */
  def resolveQuoted(
      name: String,
      resolver: Resolver): Option[NamedExpression] = {
    resolve(UnresolvedAttribute.parseAttributeName(name), output, resolver)
  }

  /**
   * Resolve the given `name` string against the given attribute, returning either 0 or 1 match.
    * 根据给定属性解析给定的`name`字符串,返回0或1匹配
   *
   * This assumes `name` has multiple parts, where the 1st part is a qualifier
   * (i.e. table name, alias, or subquery alias).
   * See the comment above `candidates` variable in resolve() for semantics the returned data.
   */
  private def resolveAsTableColumn(
      nameParts: Seq[String],
      resolver: Resolver,
      attribute: Attribute): Option[(Attribute, List[String])] = {
    assert(nameParts.length > 1)
    if (attribute.qualifiers.exists(resolver(_, nameParts.head))) {
      // At least one qualifier matches. See if remaining parts match.
      val remainingParts = nameParts.tail
      resolveAsColumn(remainingParts, resolver, attribute)
    } else {
      None
    }
  }

  /**
   * Resolve the given `name` string against the given attribute, returning either 0 or 1 match.
    * 根据给定属性解析给定的`name`字符串,返回0或1匹配
   *
   * Different from resolveAsTableColumn, this assumes `name` does NOT start with a qualifier.
   * See the comment above `candidates` variable in resolve() for semantics the returned data.
   */
  private def resolveAsColumn(
      nameParts: Seq[String],
      resolver: Resolver,
      attribute: Attribute): Option[(Attribute, List[String])] = {
    if (resolver(attribute.name, nameParts.head)) {
      Option((attribute.withName(nameParts.head), nameParts.tail.toList))
    } else {
      None
    }
  }

  /** Performs attribute resolution given a name and a sequence of possible attributes.
    * 给定名称和可能属性序列的属性解析*/
  protected def resolve(
      nameParts: Seq[String],
      input: Seq[Attribute],
      resolver: Resolver): Option[NamedExpression] = {

    // A sequence of possible candidate matches.
    //一系列可能的候选匹配
    // Each candidate is a tuple. The first element is a resolved attribute, followed by a list
    // of parts that are to be resolved.
    //每个候选人都是一个元组,第一个元素是已解析的属性,后跟要解析的部分列表
    // For example, consider an example where "a" is the table name, "b" is the column name,
    // and "c" is the struct field name, i.e. "a.b.c". In this case, Attribute will be "a.b",
    // and the second element will be List("c").
    var candidates: Seq[(Attribute, List[String])] = {
      // If the name has 2 or more parts, try to resolve it as `table.column` first.
      if (nameParts.length > 1) {
        input.flatMap { option =>
          resolveAsTableColumn(nameParts, resolver, option)
        }
      } else {
        Seq.empty
      }
    }

    // If none of attributes match `table.column` pattern, we try to resolve it as a column.
    //如果没有属性与`table.column`模式匹配，我们会尝试将其解析为列
    if (candidates.isEmpty) {
      candidates = input.flatMap { candidate =>
        resolveAsColumn(nameParts, resolver, candidate)
      }
    }

    def name = UnresolvedAttribute(nameParts).name

    candidates.distinct match {
      // One match, no nested fields, use it.
        //一个匹配,没有嵌套字段,使用它
      case Seq((a, Nil)) => Some(a)

      // One match, but we also need to extract the requested nested field.
        //一个匹配,但我们还需要提取请求的嵌套字段
      case Seq((a, nestedFields)) =>
        // The foldLeft adds ExtractValues for every remaining parts of the identifier,
        // and aliased it with the last part of the name.
        // For example, consider "a.b.c", where "a" is resolved to an existing attribute.
        // Then this will add ExtractValue("c", ExtractValue("b", a)), and alias the final
        // expression as "c".
        val fieldExprs = nestedFields.foldLeft(a: Expression)((expr, fieldName) =>
          ExtractValue(expr, Literal(fieldName), resolver))
        Some(Alias(fieldExprs, nestedFields.last)())

      // No matches.
      case Seq() =>
        logTrace(s"Could not find $name in ${input.mkString(", ")}")
        None

      // More than one match.
      case ambiguousReferences =>
        val referenceNames = ambiguousReferences.map(_._1).mkString(", ")
        throw new AnalysisException(
          s"Reference '$name' is ambiguous, could be: $referenceNames.")
    }
  }
}

/**
 * A logical plan node with no children.
  * 没有子节点的逻辑计划节点
 */
abstract class LeafNode extends LogicalPlan {
  override def children: Seq[LogicalPlan] = Nil
}

/**
 * A logical plan node with single child.
  * 具有单个子节点的逻辑计划节点
 */
abstract class UnaryNode extends LogicalPlan {
  def child: LogicalPlan

  override def children: Seq[LogicalPlan] = child :: Nil
}

/**
 * A logical plan node with a left and right child.
  * 具有左右子项的逻辑计划节点
 */
abstract class BinaryNode extends LogicalPlan {
  def left: LogicalPlan
  def right: LogicalPlan

  override def children: Seq[LogicalPlan] = Seq(left, right)
}
