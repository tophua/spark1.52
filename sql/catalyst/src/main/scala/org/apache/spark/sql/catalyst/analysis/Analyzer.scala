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

package org.apache.spark.sql.catalyst.analysis

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.aggregate.{Complete, AggregateExpression2, AggregateFunction2}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.trees.TreeNodeRef
import org.apache.spark.sql.catalyst.{SimpleCatalystConf, CatalystConf}
import org.apache.spark.sql.types._

/**
 * A trivial [[Analyzer]] with an [[EmptyCatalog]] and [[EmptyFunctionRegistry]]. Used for testing
 * when all relations are already filled in and the analyzer needs only to resolve attribute
 * references.
 */
object SimpleAnalyzer
  extends Analyzer(EmptyCatalog, EmptyFunctionRegistry, new SimpleCatalystConf(true))

/**
 * Provides a logical query plan analyzer, which translates [[UnresolvedAttribute]]s and
 * [[UnresolvedRelation]]s into fully typed objects using information in a schema [[Catalog]] and
 * a [[FunctionRegistry]].
  * 提供逻辑查询计划分析器,使用模式[[Catalog]]和[[FunctionRegistry]]中的信息将[[UnresolvedAttribute]]
  * 和[[UnresolvedRelation]]转换为完全类型的对象。
 */
class Analyzer(
    catalog: Catalog,
    registry: FunctionRegistry,
    conf: CatalystConf,
    maxIterations: Int = 100)
  extends RuleExecutor[LogicalPlan] with CheckAnalysis {

  def resolver: Resolver = {
    if (conf.caseSensitiveAnalysis) {
      caseSensitiveResolution
    } else {
      caseInsensitiveResolution
    }
  }

  val fixedPoint = FixedPoint(maxIterations)

  /**
   * Override to provide additional rules for the "Resolution" batch.
    * 覆盖以提供“解决方案”批次的其他规则
   */
  val extendedResolutionRules: Seq[Rule[LogicalPlan]] = Nil

  lazy val batches: Seq[Batch] = Seq(
    Batch("Substitution", fixedPoint,
      CTESubstitution ::
      WindowsSubstitution ::
      Nil : _*),
    Batch("Resolution", fixedPoint,
      ResolveRelations ::
      ResolveReferences ::
      ResolveGroupingAnalytics ::
      ResolveSortReferences ::
      ResolveGenerate ::
      ResolveFunctions ::
      ResolveAliases ::
      ExtractWindowExpressions ::
      GlobalAggregates ::
      ResolveAggregateFunctions ::
      HiveTypeCoercion.typeCoercionRules ++
      extendedResolutionRules : _*),
    Batch("Nondeterministic", Once,
      PullOutNondeterministic),
    Batch("Cleanup", fixedPoint,
      CleanupAliases)
  )

  /**
   * Substitute child plan with cte definitions
    * 用cte定义替换子计划
   */
  object CTESubstitution extends Rule[LogicalPlan] {
    // TODO allow subquery to define CTE
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators  {
      case With(child, relations) => substituteCTE(child, relations)
      case other => other
    }

    def substituteCTE(plan: LogicalPlan, cteRelations: Map[String, LogicalPlan]): LogicalPlan = {
      plan transform {
        // In hive, if there is same table name in database and CTE definition,
        // hive will use the table in database, not the CTE one.
        // Taking into account the reasonableness and the implementation complexity,
        // here use the CTE definition first, check table name only and ignore database name
        // see https://github.com/apache/spark/pull/4929#discussion_r27186638 for more info
        case u : UnresolvedRelation =>
          val substituted = cteRelations.get(u.tableIdentifier.last).map { relation =>
            val withAlias = u.alias.map(Subquery(_, relation))
            withAlias.getOrElse(relation)
          }
          substituted.getOrElse(u)
      }
    }
  }

  /**
   * Substitute child plan with WindowSpecDefinitions.
    * 使用WindowSpecDefinitions替换子计划
   */
  object WindowsSubstitution extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      // Lookup WindowSpecDefinitions. This rule works with unresolved children.
      case WithWindowDefinition(windowDefinitions, child) =>
        child.transform {
          case plan => plan.transformExpressions {
            case UnresolvedWindowExpression(c, WindowSpecReference(windowName)) =>
              val errorMessage =
                s"Window specification $windowName is not defined in the WINDOW clause."
              val windowSpecDefinition =
                windowDefinitions
                  .get(windowName)
                  .getOrElse(failAnalysis(errorMessage))
              WindowExpression(c, windowSpecDefinition)
          }
        }
    }
  }

  /**
   * Replaces [[UnresolvedAlias]]s with concrete aliases.
    * 用具体别名替换[[UnresolvedAlias]] s
   */
  object ResolveAliases extends Rule[LogicalPlan] {
    private def assignAliases(exprs: Seq[NamedExpression]) = {
      // The `UnresolvedAlias`s will appear only at root of a expression tree, we don't need
      // to traverse the whole tree.
      exprs.zipWithIndex.map {
        case (u @ UnresolvedAlias(child), i) =>
          child match {
            case _: UnresolvedAttribute => u
            case ne: NamedExpression => ne
            case g: Generator if g.resolved && g.elementTypes.size > 1 => MultiAlias(g, Nil)
            case e if !e.resolved => u
            case other => Alias(other, s"_c$i")()
          }
        case (other, _) => other
      }
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case Aggregate(groups, aggs, child)
        if child.resolved && aggs.exists(_.isInstanceOf[UnresolvedAlias]) =>
        Aggregate(groups, assignAliases(aggs), child)

      case g: GroupingAnalytics
        if g.child.resolved && g.aggregations.exists(_.isInstanceOf[UnresolvedAlias]) =>
        g.withNewAggs(assignAliases(g.aggregations))

      case Project(projectList, child)
        if child.resolved && projectList.exists(_.isInstanceOf[UnresolvedAlias]) =>
        Project(assignAliases(projectList), child)
    }
  }

  object ResolveGroupingAnalytics extends Rule[LogicalPlan] {
    /*
     *  GROUP BY a, b, c WITH ROLLUP
     *  is equivalent to
     *  GROUP BY a, b, c GROUPING SETS ( (a, b, c), (a, b), (a), ( ) ).
     *  Group Count: N + 1 (N is the number of group expressions)
     *
     *  We need to get all of its subsets for the rule described above, the subset is
     *  represented as the bit masks.
     */
    def bitmasks(r: Rollup): Seq[Int] = {
      Seq.tabulate(r.groupByExprs.length + 1)(idx => {(1 << idx) - 1})
    }

    /*
     *  GROUP BY a, b, c WITH CUBE
     *  is equivalent to
     *  GROUP BY a, b, c GROUPING SETS ( (a, b, c), (a, b), (b, c), (a, c), (a), (b), (c), ( ) ).
     *  Group Count: 2 ^ N (N is the number of group expressions)
     *
     *  We need to get all of its subsets for a given GROUPBY expression, the subsets are
     *  represented as the bit masks.
     */
    def bitmasks(c: Cube): Seq[Int] = {
      Seq.tabulate(1 << c.groupByExprs.length)(i => i)
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case a if !a.childrenResolved => a // be sure all of the children are resolved.
      case a: Cube =>
        GroupingSets(bitmasks(a), a.groupByExprs, a.child, a.aggregations)
      case a: Rollup =>
        GroupingSets(bitmasks(a), a.groupByExprs, a.child, a.aggregations)
      case x: GroupingSets =>
        val gid = AttributeReference(VirtualColumn.groupingIdName, IntegerType, false)()
        // We will insert another Projection if the GROUP BY keys contains the
        // non-attribute expressions. And the top operators can references those
        // expressions by its alias.
        // e.g. SELECT key%5 as c1 FROM src GROUP BY key%5 ==>
        //      SELECT a as c1 FROM (SELECT key%5 AS a FROM src) GROUP BY a

        // find all of the non-attribute expressions in the GROUP BY keys
        val nonAttributeGroupByExpressions = new ArrayBuffer[Alias]()

        // The pair of (the original GROUP BY key, associated attribute)
        val groupByExprPairs = x.groupByExprs.map(_ match {
          case e: NamedExpression => (e, e.toAttribute)
          case other => {
            val alias = Alias(other, other.toString)()
            nonAttributeGroupByExpressions += alias // add the non-attributes expression alias
            (other, alias.toAttribute)
          }
        })

        // substitute the non-attribute expressions for aggregations.
        //将非属性表达式替换为聚合
        val aggregation = x.aggregations.map(expr => expr.transformDown {
          case e => groupByExprPairs.find(_._1.semanticEquals(e)).map(_._2).getOrElse(e)
        }.asInstanceOf[NamedExpression])

        // substitute the group by expressions.
        //用表达式替换组
        val newGroupByExprs = groupByExprPairs.map(_._2)

        val child = if (nonAttributeGroupByExpressions.length > 0) {
          // insert additional projection if contains the
          // non-attribute expressions in the GROUP BY keys
          Project(x.child.output ++ nonAttributeGroupByExpressions, x.child)
        } else {
          x.child
        }

        Aggregate(
          newGroupByExprs :+ VirtualColumn.groupingIdAttribute,
          aggregation,
          Expand(x.bitmasks, newGroupByExprs, gid, child))
    }
  }

  /**
   * Replaces [[UnresolvedRelation]]s with concrete relations from the catalog.
    * 用目录中的具体关系替换[[UnresolvedRelation]]
   */
  object ResolveRelations extends Rule[LogicalPlan] {
    def getTable(u: UnresolvedRelation): LogicalPlan = {
      try {
        catalog.lookupRelation(u.tableIdentifier, u.alias)
      } catch {
        case _: NoSuchTableException =>
          u.failAnalysis(s"no such table ${u.tableName}")
      }
    }

    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case i @ InsertIntoTable(u: UnresolvedRelation, _, _, _, _) =>
        i.copy(table = EliminateSubQueries(getTable(u)))
      case u: UnresolvedRelation =>
        getTable(u)
    }
  }

  /**
   * Replaces [[UnresolvedAttribute]]s with concrete [[AttributeReference]]s from
   * a logical plan node's children.
    * 用来自逻辑计划节点的子节点的具体[[AttributeReference]]替换[[UnresolvedAttribute]]
   */
  object ResolveReferences extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case p: LogicalPlan if !p.childrenResolved => p

      // If the projection list contains Stars, expand it.
        //如果投影列表包含星号,请展开它
      case p @ Project(projectList, child) if containsStar(projectList) =>
        Project(
          projectList.flatMap {
            case s: Star => s.expand(child.output, resolver)
            case UnresolvedAlias(f @ UnresolvedFunction(_, args, _)) if containsStar(args) =>
              val expandedArgs = args.flatMap {
                case s: Star => s.expand(child.output, resolver)
                case o => o :: Nil
              }
              UnresolvedAlias(child = f.copy(children = expandedArgs)) :: Nil
            case UnresolvedAlias(c @ CreateArray(args)) if containsStar(args) =>
              val expandedArgs = args.flatMap {
                case s: Star => s.expand(child.output, resolver)
                case o => o :: Nil
              }
              UnresolvedAlias(c.copy(children = expandedArgs)) :: Nil
            case UnresolvedAlias(c @ CreateStruct(args)) if containsStar(args) =>
              val expandedArgs = args.flatMap {
                case s: Star => s.expand(child.output, resolver)
                case o => o :: Nil
              }
              UnresolvedAlias(c.copy(children = expandedArgs)) :: Nil
            case o => o :: Nil
          },
          child)
      case t: ScriptTransformation if containsStar(t.input) =>
        t.copy(
          input = t.input.flatMap {
            case s: Star => s.expand(t.child.output, resolver)
            case o => o :: Nil
          }
        )

      // If the aggregate function argument contains Stars, expand it.
        //如果aggregate函数参数包含Stars,请展开它。
      case a: Aggregate if containsStar(a.aggregateExpressions) =>
        a.copy(
          aggregateExpressions = a.aggregateExpressions.flatMap {
            case s: Star => s.expand(a.child.output, resolver)
            case o => o :: Nil
          }
        )

      // Special handling for cases when self-join introduce duplicate expression ids.
        //自联接引入重复表达式ID的情况下的特殊处理
      case j @ Join(left, right, _, _) if !j.selfJoinResolved =>
        val conflictingAttributes = left.outputSet.intersect(right.outputSet)
        logDebug(s"Conflicting attributes ${conflictingAttributes.mkString(",")} in $j")

        right.collect {
          // Handle base relations that might appear more than once.
          //处理可能出现多次的基本关系
          case oldVersion: MultiInstanceRelation
              if oldVersion.outputSet.intersect(conflictingAttributes).nonEmpty =>
            val newVersion = oldVersion.newInstance()
            (oldVersion, newVersion)

          // Handle projects that create conflicting aliases.
            //处理创建冲突别名的项目
          case oldVersion @ Project(projectList, _)
              if findAliases(projectList).intersect(conflictingAttributes).nonEmpty =>
            (oldVersion, oldVersion.copy(projectList = newAliases(projectList)))

          case oldVersion @ Aggregate(_, aggregateExpressions, _)
              if findAliases(aggregateExpressions).intersect(conflictingAttributes).nonEmpty =>
            (oldVersion, oldVersion.copy(aggregateExpressions = newAliases(aggregateExpressions)))

          case oldVersion: Generate
              if oldVersion.generatedSet.intersect(conflictingAttributes).nonEmpty =>
            val newOutput = oldVersion.generatorOutput.map(_.newInstance())
            (oldVersion, oldVersion.copy(generatorOutput = newOutput))

          case oldVersion @ Window(_, windowExpressions, _, _, child)
              if AttributeSet(windowExpressions.map(_.toAttribute)).intersect(conflictingAttributes)
                .nonEmpty =>
            (oldVersion, oldVersion.copy(windowExpressions = newAliases(windowExpressions)))
        }
        // Only handle first case, others will be fixed on the next pass.
          //只处理第一种情况,其他将在下一次传递时修复
        .headOption match {
          case None =>
            /*
             * No result implies that there is a logical plan node that produces new references
             * that this rule cannot handle. When that is the case, there must be another rule
             * that resolves these conflicts. Otherwise, the analysis will fail.
             */
            j
          case Some((oldRelation, newRelation)) =>
            val attributeRewrites = AttributeMap(oldRelation.output.zip(newRelation.output))
            val newRight = right transformUp {
              case r if r == oldRelation => newRelation
            } transformUp {
              case other => other transformExpressions {
                case a: Attribute => attributeRewrites.get(a).getOrElse(a)
              }
            }
            j.copy(right = newRight)
        }

      // When resolve `SortOrder`s in Sort based on child, don't report errors as
      // we still have chance to resolve it based on grandchild
        //在基于子的Sort中解决`SortOrder'时,不要报告错误,因为我们仍然有机会根据孙子来解决它
      case s @ Sort(ordering, global, child) if child.resolved && !s.resolved =>
        val newOrdering = resolveSortOrders(ordering, child, throws = false)
        Sort(newOrdering, global, child)

      // A special case for Generate, because the output of Generate should not be resolved by
      // ResolveReferences. Attributes in the output will be resolved by ResolveGenerate.
      case g @ Generate(generator, join, outer, qualifier, output, child)
        if child.resolved && !generator.resolved =>
        val newG = generator transformUp {
          case u @ UnresolvedAttribute(nameParts) =>
            withPosition(u) { child.resolve(nameParts, resolver).getOrElse(u) }
          case UnresolvedExtractValue(child, fieldExpr) =>
            ExtractValue(child, fieldExpr, resolver)
        }
        if (newG.fastEquals(generator)) {
          g
        } else {
          Generate(newG.asInstanceOf[Generator], join, outer, qualifier, output, child)
        }

      case q: LogicalPlan =>
        logTrace(s"Attempting to resolve ${q.simpleString}")
        q transformExpressionsUp  {
          case u @ UnresolvedAttribute(nameParts) =>
            // Leave unchanged if resolution fails.  Hopefully will be resolved next round.
            //如果分辨率失败则保持不变 希望下一轮能够得到解决
            val result =
              withPosition(u) { q.resolveChildren(nameParts, resolver).getOrElse(u) }
            logDebug(s"Resolving $u to $result")
            result
          case UnresolvedExtractValue(child, fieldExpr) if child.resolved =>
            ExtractValue(child, fieldExpr, resolver)
        }
    }

    def newAliases(expressions: Seq[NamedExpression]): Seq[NamedExpression] = {
      expressions.map {
        case a: Alias => Alias(a.child, a.name)()
        case other => other
      }
    }

    def findAliases(projectList: Seq[NamedExpression]): AttributeSet = {
      AttributeSet(projectList.collect { case a: Alias => a.toAttribute })
    }

    /**
     * Returns true if `exprs` contains a [[Star]].
     */
    def containsStar(exprs: Seq[Expression]): Boolean =
      exprs.exists(_.collect { case _: Star => true }.nonEmpty)
  }

  private def resolveSortOrders(ordering: Seq[SortOrder], plan: LogicalPlan, throws: Boolean) = {
    ordering.map { order =>
      // Resolve SortOrder in one round.
      // If throws == false or the desired attribute doesn't exist
      // (like try to resolve `a.b` but `a` doesn't exist), fail and return the origin one.
      // Else, throw exception.
      try {
        val newOrder = order transformUp {
          case u @ UnresolvedAttribute(nameParts) =>
            plan.resolve(nameParts, resolver).getOrElse(u)
          case UnresolvedExtractValue(child, fieldName) if child.resolved =>
            ExtractValue(child, fieldName, resolver)
        }
        newOrder.asInstanceOf[SortOrder]
      } catch {
        case a: AnalysisException if !throws => order
      }
    }
  }

  /**
   * In many dialects of SQL it is valid to sort by attributes that are not present in the SELECT
   * clause.  This rule detects such queries and adds the required attributes to the original
   * projection, so that they will be available during sorting. Another projection is added to
   * remove these attributes after sorting.
    * 在SQL的许多方言中,按SELECT子句中不存在的属性进行排序是有效的,此规则检测此类查询并将所需属性添加到原始投影,
    * 以便在排序期间它们可用,添加了另一个投影以在排序后删除这些属性。
   */
  object ResolveSortReferences extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case s @ Sort(ordering, global, p @ Project(projectList, child))
          if !s.resolved && p.resolved =>
        val (newOrdering, missing) = resolveAndFindMissing(ordering, p, child)

        // If this rule was not a no-op, return the transformed plan, otherwise return the original.
        //如果此规则不是无操作,则返回已转换的计划,否则返回原始规则
        if (missing.nonEmpty) {
          // Add missing attributes and then project them away after the sort.
          //添加缺少的属性,然后在排序后将它们投射出去
          Project(p.output,
            Sort(newOrdering, global,
              Project(projectList ++ missing, child)))
        } else {
          logDebug(s"Failed to find $missing in ${p.output.mkString(", ")}")
          s // Nothing we can do here. Return original plan.
        }
    }

    /**
     * Given a child and a grandchild that are present beneath a sort operator, try to resolve
     * the sort ordering and returns it with a list of attributes that are missing from the
     * child but are present in the grandchild.
      * 给定排序运算符下面的子项和孙项,尝试解析排序顺序并返回一个属性列表,这些属性从子项中丢失但存在于孙子中
     */
    def resolveAndFindMissing(
        ordering: Seq[SortOrder],
        child: LogicalPlan,
        grandchild: LogicalPlan): (Seq[SortOrder], Seq[Attribute]) = {
      val newOrdering = resolveSortOrders(ordering, grandchild, throws = true)
      // Construct a set that contains all of the attributes that we need to evaluate the
      // ordering.
      //构造一个包含评估排序所需的所有属性的集合
      val requiredAttributes = AttributeSet(newOrdering).filter(_.resolved)
      // Figure out which ones are missing from the projection, so that we can add them and
      // remove them after the sort.
      //找出投影中缺少哪些,以便我们可以添加它们并在排序后删除它们
      val missingInProject = requiredAttributes -- child.output
      // It is important to return the new SortOrders here, instead of waiting for the standard
      // resolving process as adding attributes to the project below can actually introduce
      // ambiguity that was not present before.
      (newOrdering, missingInProject.toSeq)
    }
  }

  /**
   * Replaces [[UnresolvedFunction]]s with concrete [[Expression]]s.
   */
  object ResolveFunctions extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case q: LogicalPlan =>
        q transformExpressions {
          case u if !u.childrenResolved => u // Skip until children are resolved.
          case u @ UnresolvedFunction(name, children, isDistinct) =>
            withPosition(u) {
              registry.lookupFunction(name, children) match {
                // We get an aggregate function built based on AggregateFunction2 interface.
                // So, we wrap it in AggregateExpression2.
                  //我们得到一个基于AggregateFunction2接口构建的聚合函数,所以,我们将它包装在AggregateExpression2中。
                case agg2: AggregateFunction2 => AggregateExpression2(agg2, Complete, isDistinct)
                // Currently, our old aggregate function interface supports SUM(DISTINCT ...)
                // and COUTN(DISTINCT ...).
                case sumDistinct: SumDistinct => sumDistinct
                case countDistinct: CountDistinct => countDistinct
                // DISTINCT is not meaningful with Max and Min.
                case max: Max if isDistinct => max
                case min: Min if isDistinct => min
                // For other aggregate functions, DISTINCT keyword is not supported for now.
                // Once we converted to the new code path, we will allow using DISTINCT keyword.
                  //对于其他聚合函数,目前不支持DISTINCT关键字,一旦我们转换为新的代码路径,我们将允许使用DISTINCT关键字。
                case other: AggregateExpression1 if isDistinct =>
                  failAnalysis(s"$name does not support DISTINCT keyword.")
                // If it does not have DISTINCT keyword, we will return it as is.
                  //如果它没有DISTINCT关键字,我们将按原样返回
                case other => other
              }
            }
        }
    }
  }

  /**
   * Turns projections that contain aggregate expressions into aggregations.
    * 将包含聚合表达式的投影转换为聚合
   */
  object GlobalAggregates extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case Project(projectList, child) if containsAggregates(projectList) =>
        Aggregate(Nil, projectList, child)
    }

    def containsAggregates(exprs: Seq[Expression]): Boolean = {
      exprs.foreach(_.foreach {
        case agg: AggregateExpression => return true
        case _ =>
      })
      false
    }
  }

  /**
   * This rule finds aggregate expressions that are not in an aggregate operator.  For example,
   * those in a HAVING clause or ORDER BY clause.  These expressions are pushed down to the
   * underlying aggregate operator and then projected away after the original operator.
    *
    * 此规则查找不在聚合运算符中的聚合表达式, 例如,HAVING子句或ORDER BY子句中的那些,
    * 这些表达式被下推到底层聚合运算符,然后在原始运算符之后被投射出去
   */
  object ResolveAggregateFunctions extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case filter @ Filter(havingCondition,
             aggregate @ Aggregate(grouping, originalAggExprs, child))
          if aggregate.resolved =>

        // Try resolving the condition of the filter as though it is in the aggregate clause
        //尝试解析过滤器的条件,就好像它在aggregate子句中一样
        val aggregatedCondition =
          Aggregate(grouping, Alias(havingCondition, "havingCondition")() :: Nil, child)
        val resolvedOperator = execute(aggregatedCondition)
        def resolvedAggregateFilter =
          resolvedOperator
            .asInstanceOf[Aggregate]
            .aggregateExpressions.head

        // If resolution was successful and we see the filter has an aggregate in it, add it to
        // the original aggregate operator.
        //如果解决方案成功并且我们看到过滤器中包含聚合,请将其添加到原始聚合运算符。
        if (resolvedOperator.resolved && containsAggregate(resolvedAggregateFilter)) {
          val aggExprsWithHaving = resolvedAggregateFilter +: originalAggExprs

          Project(aggregate.output,
            Filter(resolvedAggregateFilter.toAttribute,
              aggregate.copy(aggregateExpressions = aggExprsWithHaving)))
        } else {
          filter
        }

      case sort @ Sort(sortOrder, global, aggregate: Aggregate)
        if aggregate.resolved && !sort.resolved =>

        // Try resolving the ordering as though it is in the aggregate clause.
        //尝试解决排序,就好像它在aggregate子句中一样
        try {
          val aliasedOrdering = sortOrder.map(o => Alias(o.child, "aggOrder")())
          val aggregatedOrdering = aggregate.copy(aggregateExpressions = aliasedOrdering)
          val resolvedAggregate: Aggregate = execute(aggregatedOrdering).asInstanceOf[Aggregate]
          val resolvedAliasedOrdering: Seq[Alias] =
            resolvedAggregate.aggregateExpressions.asInstanceOf[Seq[Alias]]

          // If we pass the analysis check, then the ordering expressions should only reference to
          // aggregate expressions or grouping expressions, and it's safe to push them down to
          // Aggregate.
          //如果我们通过分析检查,那么排序表达式应该只引用聚合表达式或分组表达式,并且可以将它们推送到Aggregate
          checkAnalysis(resolvedAggregate)

          val originalAggExprs = aggregate.aggregateExpressions.map(
            CleanupAliases.trimNonTopLevelAliases(_).asInstanceOf[NamedExpression])

          // If the ordering expression is same with original aggregate expression, we don't need
          // to push down this ordering expression and can reference the original aggregate
          // expression instead.
          //如果排序表达式与原始聚合表达式相同,则我们不需要按下此排序表达式,而是可以引用原始聚合表达式
          val needsPushDown = ArrayBuffer.empty[NamedExpression]
          val evaluatedOrderings = resolvedAliasedOrdering.zip(sortOrder).map {
            case (evaluated, order) =>
              val index = originalAggExprs.indexWhere {
                case Alias(child, _) => child semanticEquals evaluated.child
                case other => other semanticEquals evaluated.child
              }

              if (index == -1) {
                needsPushDown += evaluated
                order.copy(child = evaluated.toAttribute)
              } else {
                order.copy(child = originalAggExprs(index).toAttribute)
              }
          }

          Project(aggregate.output,
            Sort(evaluatedOrderings, global,
              aggregate.copy(aggregateExpressions = originalAggExprs ++ needsPushDown)))
        } catch {
          // Attempting to resolve in the aggregate can result in ambiguity.  When this happens,
          // just return the original plan.
          case ae: AnalysisException => sort
        }
    }

    protected def containsAggregate(condition: Expression): Boolean = {
      condition.find(_.isInstanceOf[AggregateExpression]).isDefined
    }
  }

  /**
   * Rewrites table generating expressions that either need one or more of the following in order
   * to be resolved:
    * 重写生成表达式的表,这些表达式需要以下一个或多个才能被解析：
   *  - concrete attribute references for their output.具体的输出属性引用
   *  - to be relocated from a SELECT clause (i.e. from  a [[Project]]) into a [[Generate]]).
   *
   * Names for the output [[Attribute]]s are extracted from [[Alias]] or [[MultiAlias]] expressions
   * that wrap the [[Generator]]. If more than one [[Generator]] is found in a Project, an
   * [[AnalysisException]] is throw.
   */
  object ResolveGenerate extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case g: Generate if ResolveReferences.containsStar(g.generator.children) =>
        failAnalysis("Cannot explode *, explode can only be applied on a specific column.")
      case p: Generate if !p.child.resolved || !p.generator.resolved => p
      case g: Generate if !g.resolved =>
        g.copy(generatorOutput = makeGeneratorOutput(g.generator, g.generatorOutput.map(_.name)))

      case p @ Project(projectList, child) =>
        // Holds the resolved generator, if one exists in the project list.
        //如果项目列表中存在已解析的生成器,则保留该生成器
        var resolvedGenerator: Generate = null

        val newProjectList = projectList.flatMap {
          case AliasedGenerator(generator, names) if generator.childrenResolved =>
            if (resolvedGenerator != null) {
              failAnalysis(
                s"Only one generator allowed per select but ${resolvedGenerator.nodeName} and " +
                s"and ${generator.nodeName} found.")
            }

            resolvedGenerator =
              Generate(
                generator,
                join = projectList.size > 1, // Only join if there are other expressions in SELECT.
                outer = false,
                qualifier = None,
                generatorOutput = makeGeneratorOutput(generator, names),
                child)

            resolvedGenerator.generatorOutput
          case other => other :: Nil
        }

        if (resolvedGenerator != null) {
          Project(newProjectList, resolvedGenerator)
        } else {
          p
        }
    }

    /** Extracts a [[Generator]] expression and any names assigned by aliases to their output.
      * 将[[Generator]]表达式和别名指定的任何名称提取到其输出中 */
    private object AliasedGenerator {
      def unapply(e: Expression): Option[(Generator, Seq[String])] = e match {
        case Alias(g: Generator, name) if g.resolved && g.elementTypes.size > 1 =>
          // If not given the default names, and the TGF with multiple output columns
          //如果没有给出默认名称,并且TGF具有多个输出列
          failAnalysis(
            s"""Expect multiple names given for ${g.getClass.getName},
               |but only single name '${name}' specified""".stripMargin)
        case Alias(g: Generator, name) if g.resolved => Some((g, name :: Nil))
        case MultiAlias(g: Generator, names) if g.resolved => Some(g, names)
        case _ => None
      }
    }

    /**
     * Construct the output attributes for a [[Generator]], given a list of names.  If the list of
     * names is empty names are assigned by ordinal (i.e., _c0, _c1, ...) to match Hive's defaults.
      *
      * 给定名称列表，构造[[Generator]]的输出属性,如果名称列表为空名称由序数(即_c0，_c1，...)分配,以匹配Hive的默认值
     */
    private def makeGeneratorOutput(
        generator: Generator,
        names: Seq[String]): Seq[Attribute] = {
      val elementTypes = generator.elementTypes

      if (names.length == elementTypes.length) {
        names.zip(elementTypes).map {
          case (name, (t, nullable)) =>
            AttributeReference(name, t, nullable)()
        }
      } else if (names.isEmpty) {
        elementTypes.zipWithIndex.map {
          // keep the default column names as Hive does _c0, _c1, _cN
          //保留默认列名称为Hive执行_c0，_c1，_cN
          case ((t, nullable), i) => AttributeReference(s"_c$i", t, nullable)()
        }
      } else {
        failAnalysis(
          "The number of aliases supplied in the AS clause does not match the number of columns " +
          s"output by the UDTF expected ${elementTypes.size} aliases but got " +
          s"${names.mkString(",")} ")
      }
    }
  }

  /**
   * Extracts [[WindowExpression]]s from the projectList of a [[Project]] operator and
   * aggregateExpressions of an [[Aggregate]] operator and creates individual [[Window]]
   * operators for every distinct [[WindowSpecDefinition]].
    *
    * 从[[Project]]运算符的projectList和[[Aggregate]]运算符的aggregateExpressions中提取[[WindowExpression]],
    * 并为每个不同的[[WindowSpecDefinition]]创建单独的[[Window]]运算符
   *
   * This rule handles three cases:
   *  - A [[Project]] having [[WindowExpression]]s in its projectList;
   *  - An [[Aggregate]] having [[WindowExpression]]s in its aggregateExpressions.
   *  - An [[Filter]]->[[Aggregate]] pattern representing GROUP BY with a HAVING
   *    clause and the [[Aggregate]] has [[WindowExpression]]s in its aggregateExpressions.
   * Note: If there is a GROUP BY clause in the query, aggregations and corresponding
   * filters (expressions in the HAVING clause) should be evaluated before any
   * [[WindowExpression]]. If a query has SELECT DISTINCT, the DISTINCT part should be
   * evaluated after all [[WindowExpression]]s.
   *
   * For every case, the transformation works as follows:
   * 1. For a list of [[Expression]]s (a projectList or an aggregateExpressions), partitions
   *    it two lists of [[Expression]]s, one for all [[WindowExpression]]s and another for
   *    all regular expressions.
   * 2. For all [[WindowExpression]]s, groups them based on their [[WindowSpecDefinition]]s.
   * 3. For every distinct [[WindowSpecDefinition]], creates a [[Window]] operator and inserts
   *    it into the plan tree.
   */
  object ExtractWindowExpressions extends Rule[LogicalPlan] {
    private def hasWindowFunction(projectList: Seq[NamedExpression]): Boolean =
      projectList.exists(hasWindowFunction)

    private def hasWindowFunction(expr: NamedExpression): Boolean = {
      expr.find {
        case window: WindowExpression => true
        case _ => false
      }.isDefined
    }

    /**
     * From a Seq of [[NamedExpression]]s, extract expressions containing window expressions and
     * other regular expressions that do not contain any window expression. For example, for
     * `col1, Sum(col2 + col3) OVER (PARTITION BY col4 ORDER BY col5)`, we will extract
     * `col1`, `col2 + col3`, `col4`, and `col5` out and replace their appearances in
     * the window expression as attribute references. So, the first returned value will be
     * `[Sum(_w0) OVER (PARTITION BY _w1 ORDER BY _w2)]` and the second returned value will be
     * [col1, col2 + col3 as _w0, col4 as _w1, col5 as _w2].
      *
      * 从[[NamedExpression]]的Seq中,提取包含窗口表达式的表达式和不包含任何窗口表达式的其他正则表达式,
      * 例如,对于不包含任何窗口表达式的其他正则表达式,例如，对于`col1`，`col2 + col3`，`col4`和`col5` out,
      * 将它们在窗口表达式中的外观替换为属性引用,
      * 因此，第一个返回值为`[Sum（_w0）OVER（PARTITION BY _w1 ORDER BY _w2）]`
      * 第二个返回值为[col1，col2 + col3为_w0，col4为_w1，col5为_w2]。
     *
     * @return (seq of expressions containing at lease one window expressions,
     *          seq of non-window expressions)
     */
    private def extract(
        expressions: Seq[NamedExpression]): (Seq[NamedExpression], Seq[NamedExpression]) = {
      // First, we partition the input expressions to two part. For the first part,
      //首先,我们将输入表达式分为两部分,第一部分，
      // every expression in it contain at least one WindowExpression.
      //其中的每个表达式都包含至少一个WindowExpression
      // Expressions in the second part do not have any WindowExpression.
      //第二部分中的表达式没有任何WindowExpression
      val (expressionsWithWindowFunctions, regularExpressions) =
        expressions.partition(hasWindowFunction)

      // Then, we need to extract those regular expressions used in the WindowExpression.
      //然后,我们需要提取WindowExpression中使用的那些正则表达式
      // For example, when we have col1 - Sum(col2 + col3) OVER (PARTITION BY col4 ORDER BY col5),
      //例如，当我们有col1 - Sum(col2 + col3)OVER（PARTITION BY col4 ORDER BY col5)时
      // we need to make sure that col1 to col5 are all projected from the child of the Window
      // operator.
      //我们需要确保col1到col5都是从Window运算符的子节点投射出来的
      val extractedExprBuffer = new ArrayBuffer[NamedExpression]()
      def extractExpr(expr: Expression): Expression = expr match {
        case ne: NamedExpression =>
          // If a named expression is not in regularExpressions, add it to
          // extractedExprBuffer and replace it with an AttributeReference.
          //如果命名表达式不在regularExpressions中,
          // 请将其添加到extractedExprBuffer并将其替换为AttributeReference。
          val missingExpr =
            AttributeSet(Seq(expr)) -- (regularExpressions ++ extractedExprBuffer)
          if (missingExpr.nonEmpty) {
            extractedExprBuffer += ne
          }
          ne.toAttribute
        case e: Expression if e.foldable =>
          e // No need to create an attribute reference if it will be evaluated as a Literal.
          //如果它将被评估为Literal，则无需创建属性引用
        case e: Expression =>
          // For other expressions, we extract it and replace it with an AttributeReference (with
          // an interal column name, e.g. "_w0").
          //对于其他表达式,我们提取它并用AttributeReference(带有内部列名,例如“_w0”)替换它。
          val withName = Alias(e, s"_w${extractedExprBuffer.length}")()
          extractedExprBuffer += withName
          withName.toAttribute
      }

      // Now, we extract regular expressions from expressionsWithWindowFunctions
      // by using extractExpr.
      val newExpressionsWithWindowFunctions = expressionsWithWindowFunctions.map {
        _.transform {
          // Extracts children expressions of a WindowFunction (input parameters of
          // a WindowFunction).
          case wf : WindowFunction =>
            val newChildren = wf.children.map(extractExpr(_))
            wf.withNewChildren(newChildren)

          // Extracts expressions from the partition spec and order spec.
          case wsc @ WindowSpecDefinition(partitionSpec, orderSpec, _) =>
            val newPartitionSpec = partitionSpec.map(extractExpr(_))
            val newOrderSpec = orderSpec.map { so =>
              val newChild = extractExpr(so.child)
              so.copy(child = newChild)
            }
            wsc.copy(partitionSpec = newPartitionSpec, orderSpec = newOrderSpec)

          // Extracts AggregateExpression. For example, for SUM(x) - Sum(y) OVER (...),
          // we need to extract SUM(x).
          case agg: AggregateExpression =>
            val withName = Alias(agg, s"_w${extractedExprBuffer.length}")()
            extractedExprBuffer += withName
            withName.toAttribute

          // Extracts other attributes
          case attr: Attribute => extractExpr(attr)

        }.asInstanceOf[NamedExpression]
      }

      (newExpressionsWithWindowFunctions, regularExpressions ++ extractedExprBuffer)
    } // end of extract

    /**
     * Adds operators for Window Expressions. Every Window operator handles a single Window Spec.
      * 为Window Expressions添加运算符,每个Window操作符都处理一个Window Spec
     */
    private def addWindow(
        expressionsWithWindowFunctions: Seq[NamedExpression],
        child: LogicalPlan): LogicalPlan = {
      // First, we need to extract all WindowExpressions from expressionsWithWindowFunctions
      // and put those extracted WindowExpressions to extractedWindowExprBuffer.
      // This step is needed because it is possible that an expression contains multiple
      // WindowExpressions with different Window Specs.
      // After extracting WindowExpressions, we need to construct a project list to generate
      // expressionsWithWindowFunctions based on extractedWindowExprBuffer.
      // For example, for "sum(a) over (...) / sum(b) over (...)", we will first extract
      // "sum(a) over (...)" and "sum(b) over (...)" out, and assign "_we0" as the alias to
      // "sum(a) over (...)" and "_we1" as the alias to "sum(b) over (...)".
      // Then, the projectList will be [_we0/_we1].
      val extractedWindowExprBuffer = new ArrayBuffer[NamedExpression]()
      val newExpressionsWithWindowFunctions = expressionsWithWindowFunctions.map {
        // We need to use transformDown because we want to trigger
        // "case alias @ Alias(window: WindowExpression, _)" first.
        _.transformDown {
          case alias @ Alias(window: WindowExpression, _) =>
            // If a WindowExpression has an assigned alias, just use it.
            extractedWindowExprBuffer += alias
            alias.toAttribute
          case window: WindowExpression =>
            // If there is no alias assigned to the WindowExpressions. We create an
            // internal column.
            val withName = Alias(window, s"_we${extractedWindowExprBuffer.length}")()
            extractedWindowExprBuffer += withName
            withName.toAttribute
        }.asInstanceOf[NamedExpression]
      }

      // Second, we group extractedWindowExprBuffer based on their Partition and Order Specs.
      //其次,我们根据分区和订单规范对提取的WindowExprBuffer进行分组
      val groupedWindowExpressions = extractedWindowExprBuffer.groupBy { expr =>
        val distinctWindowSpec = expr.collect {
          case window: WindowExpression => window.windowSpec
        }.distinct

        // We do a final check and see if we only have a single Window Spec defined in an
        // expressions.
        //我们做最后检查,看看我们是否只在表达式中定义了一个Window Spec
        if (distinctWindowSpec.length == 0 ) {
          failAnalysis(s"$expr does not have any WindowExpression.")
        } else if (distinctWindowSpec.length > 1) {
          // newExpressionsWithWindowFunctions only have expressions with a single
          // WindowExpression. If we reach here, we have a bug.
          failAnalysis(s"$expr has multiple Window Specifications ($distinctWindowSpec)." +
            s"Please file a bug report with this error message, stack trace, and the query.")
        } else {
          val spec = distinctWindowSpec.head
          (spec.partitionSpec, spec.orderSpec)
        }
      }.toSeq

      // Third, for every Window Spec, we add a Window operator and set currentChild as the
      // child of it.
      //第三,对于每个Window Spec,我们添加一个Window运算符并将currentChild设置为它的子节点
      var currentChild = child
      var i = 0
      while (i < groupedWindowExpressions.size) {
        val ((partitionSpec, orderSpec), windowExpressions) = groupedWindowExpressions(i)
        // Set currentChild to the newly created Window operator.
        currentChild =
          Window(
            currentChild.output,
            windowExpressions,
            partitionSpec,
            orderSpec,
            currentChild)

        // Move to next Window Spec.
        i += 1
      }

      // Finally, we create a Project to output currentChild's output
      // newExpressionsWithWindowFunctions.
      Project(currentChild.output ++ newExpressionsWithWindowFunctions, currentChild)
    } // end of addWindow

    // We have to use transformDown at here to make sure the rule of
    // "Aggregate with Having clause" will be triggered.
    //我们必须在这里使用transformDown来确保将触发带有“子句”的Aggregate规则
    def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {

      // Aggregate with Having clause. This rule works with an unresolved Aggregate because
      // a resolved Aggregate will not have Window Functions.
      //与Having子句聚合,此规则适用于未解析的聚合,因为已解析的聚合将不具有窗口函数
      case f @ Filter(condition, a @ Aggregate(groupingExprs, aggregateExprs, child))
        if child.resolved &&
           hasWindowFunction(aggregateExprs) &&
           a.expressions.forall(_.resolved) =>
        val (windowExpressions, aggregateExpressions) = extract(aggregateExprs)
        // Create an Aggregate operator to evaluate aggregation functions.
        //创建Aggregate运算符以评估聚合函数
        val withAggregate = Aggregate(groupingExprs, aggregateExpressions, child)
        // Add a Filter operator for conditions in the Having clause.
        //为Having子句中的条件添加Filter运算符
        val withFilter = Filter(condition, withAggregate)
        val withWindow = addWindow(windowExpressions, withFilter)

        // Finally, generate output columns according to the original projectList.
        //最后,根据原始projectList生成输出列
        val finalProjectList = aggregateExprs.map (_.toAttribute)
        Project(finalProjectList, withWindow)

      case p: LogicalPlan if !p.childrenResolved => p

      // Aggregate without Having clause.
        //没有Having子句的Aggregate
      case a @ Aggregate(groupingExprs, aggregateExprs, child)
        if hasWindowFunction(aggregateExprs) &&
           a.expressions.forall(_.resolved) =>
        val (windowExpressions, aggregateExpressions) = extract(aggregateExprs)
        // Create an Aggregate operator to evaluate aggregation functions.
        //创建Aggregate运算符以评估聚合函数
        val withAggregate = Aggregate(groupingExprs, aggregateExpressions, child)
        // Add Window operators. 添加Window运算符
        val withWindow = addWindow(windowExpressions, withAggregate)

        // Finally, generate output columns according to the original projectList.
        //最后,根据原始projectList生成输出列
        val finalProjectList = aggregateExprs.map (_.toAttribute)
        Project(finalProjectList, withWindow)

      // We only extract Window Expressions after all expressions of the Project
      // have been resolved.
        //我们只在项目的所有表达式都已解决后才提取窗口表达式
      case p @ Project(projectList, child)
        if hasWindowFunction(projectList) && !p.expressions.exists(!_.resolved) =>
        val (windowExpressions, regularExpressions) = extract(projectList)
        // We add a project to get all needed expressions for window expressions from the child
        // of the original Project operator.
        //我们添加一个项目来从原始Project运算符的子代中获取窗口表达式的所有需要表达式
        val withProject = Project(regularExpressions, child)
        // Add Window operators.添加Window运算符
        val withWindow = addWindow(windowExpressions, withProject)

        // Finally, generate output columns according to the original projectList.
        //最后,根据原始projectList生成输出列
        val finalProjectList = projectList.map (_.toAttribute)
        Project(finalProjectList, withWindow)
    }
  }

  /**
   * Pulls out nondeterministic expressions from LogicalPlan which is not Project or Filter,
   * put them into an inner Project and finally project them away at the outer Project.
    * 从LogicalPlan中取出非确定性表达式,它不是Project或Filter,将它们放入内部Project中,最后将它们放在外部Project中。
   */
  object PullOutNondeterministic extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case p if !p.resolved => p // Skip unresolved nodes.
      case p: Project => p
      case f: Filter => f

      // todo: It's hard to write a general rule to pull out nondeterministic expressions
      // from LogicalPlan, currently we only do it for UnaryNode which has same output
      // schema with its child.
        //从LogicalPlan,目前我们只为UnaryNode做它,它的子输出模式相同。
      case p: UnaryNode if p.output == p.child.output && p.expressions.exists(!_.deterministic) =>
        val nondeterministicExprs = p.expressions.filterNot(_.deterministic).flatMap { expr =>
          val leafNondeterministic = expr.collect {
            case n: Nondeterministic => n
          }
          leafNondeterministic.map { e =>
            val ne = e match {
              case n: NamedExpression => n
              case _ => Alias(e, "_nondeterministic")()
            }
            new TreeNodeRef(e) -> ne
          }
        }.toMap
        val newPlan = p.transformExpressions { case e =>
          nondeterministicExprs.get(new TreeNodeRef(e)).map(_.toAttribute).getOrElse(e)
        }
        val newChild = Project(p.child.output ++ nondeterministicExprs.values, p.child)
        Project(p.output, newPlan.withNewChildren(newChild :: Nil))
    }
  }
}

/**
 * Removes [[Subquery]] operators from the plan. Subqueries are only required to provide
 * scoping information for attributes and can be removed once analysis is complete.
  * 从计划中删除[[Subquery]]运算符,子查询仅需要提供属性的作用域信息,并且可在分析完成后删除
 */
object EliminateSubQueries extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Subquery(_, child) => child
  }
}

/**
 * Cleans up unnecessary Aliases inside the plan. Basically we only need Alias as a top level
 * expression in Project(project list) or Aggregate(aggregate expressions) or
 * Window(window expressions).
  * 清除计划中不必要的别名,基本上我们只需要Alias作为Project(项目列表)或Aggregate(聚合表达式)或Window(窗口表达式)中的顶级表达式。
 */
object CleanupAliases extends Rule[LogicalPlan] {
  private def trimAliases(e: Expression): Expression = {
    var stop = false
    e.transformDown {
      // CreateStruct is a special case, we need to retain its top level Aliases as they decide the
      // name of StructField. We also need to stop transform down this expression, or the Aliases
      // under CreateStruct will be mistakenly trimmed.
      //CreateStruct是一个特殊情况,我们需要保留其顶级别名,因为它们决定了StructField的名称,
      // 我们还需要停止转换此表达式,否则将错误地修剪CreateStruct下的别名。
      case c: CreateStruct if !stop =>
        stop = true
        c.copy(children = c.children.map(trimNonTopLevelAliases))
      case c: CreateStructUnsafe if !stop =>
        stop = true
        c.copy(children = c.children.map(trimNonTopLevelAliases))
      case Alias(child, _) if !stop => child
    }
  }

  def trimNonTopLevelAliases(e: Expression): Expression = e match {
    case a: Alias =>
      Alias(trimAliases(a.child), a.name)(a.exprId, a.qualifiers, a.explicitMetadata)
    case other => trimAliases(other)
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case Project(projectList, child) =>
      val cleanedProjectList =
        projectList.map(trimNonTopLevelAliases(_).asInstanceOf[NamedExpression])
      Project(cleanedProjectList, child)

    case Aggregate(grouping, aggs, child) =>
      val cleanedAggs = aggs.map(trimNonTopLevelAliases(_).asInstanceOf[NamedExpression])
      Aggregate(grouping.map(trimAliases), cleanedAggs, child)

    case w @ Window(projectList, windowExprs, partitionSpec, orderSpec, child) =>
      val cleanedWindowExprs =
        windowExprs.map(e => trimNonTopLevelAliases(e).asInstanceOf[NamedExpression])
      Window(projectList, cleanedWindowExprs, partitionSpec.map(trimAliases),
        orderSpec.map(trimAliases(_).asInstanceOf[SortOrder]), child)

    case other =>
      var stop = false
      other transformExpressionsDown {
        case c: CreateStruct if !stop =>
          stop = true
          c.copy(children = c.children.map(trimNonTopLevelAliases))
        case c: CreateStructUnsafe if !stop =>
          stop = true
          c.copy(children = c.children.map(trimNonTopLevelAliases))
        case Alias(child, _) if !stop => child
      }
  }
}
