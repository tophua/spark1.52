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
package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Statistics}
import org.apache.spark.sql.sources.BaseRelation

/**
 * Used to link a [[BaseRelation]] in to a logical query plan.
  * 用于将[[BaseRelation]]链接到逻辑查询计划
 *
 * Note that sometimes we need to use `LogicalRelation` to replace an existing leaf node without
 * changing the output attributes' IDs.  The `expectedOutputAttributes` parameter is used for
 * this purpose.  See https://issues.apache.org/jira/browse/SPARK-10741 for more details.
  * 请注意,有时我们需要使用`LogicalRelation`来替换现有的叶节点而不更改输出属性的ID,`expectedOutputAttributes`参数用于此目的。
 */
private[sql] case class LogicalRelation(
    relation: BaseRelation,
    expectedOutputAttributes: Option[Seq[Attribute]] = None)
  extends LeafNode with MultiInstanceRelation {

  override val output: Seq[AttributeReference] = {
    val attrs = relation.schema.toAttributes
    expectedOutputAttributes.map { expectedAttrs =>
      assert(expectedAttrs.length == attrs.length)
      attrs.zip(expectedAttrs).map {
        // We should respect the attribute names provided by base relation and only use the
        // exprId in `expectedOutputAttributes`.
        // The reason is that, some relations(like parquet) will reconcile attribute names to
        // workaround case insensitivity issue.
        case (attr, expected) => attr.withExprId(expected.exprId)
      }
    }.getOrElse(attrs)
  }

  // Logical Relations are distinct if they have different output for the sake of transformations.
  //如果逻辑关系为了转换而具有不同的输出,则它们是不同的
  override def equals(other: Any): Boolean = other match {
    case l @ LogicalRelation(otherRelation, _) => relation == otherRelation && output == l.output
    case _ => false
  }

  override def hashCode: Int = {
    com.google.common.base.Objects.hashCode(relation, output)
  }

  override def sameResult(otherPlan: LogicalPlan): Boolean = otherPlan match {
    case LogicalRelation(otherRelation, _) => relation == otherRelation
    case _ => false
  }

  // When comparing two LogicalRelations from within LogicalPlan.sameResult, we only need
  // LogicalRelation.cleanArgs to return Seq(relation), since expectedOutputAttribute's
  // expId can be different but the relation is still the same.
  //当比较LogicalPlan.sameResult中的两个LogicalRelations时,
  //我们只需要LogicalRelation.cleanArgs来返回Seq(关系),因为expectedOutputAttribute的expId可以不同但关系仍然相同
  override lazy val cleanArgs: Seq[Any] = Seq(relation)

  @transient override lazy val statistics: Statistics = Statistics(
    sizeInBytes = BigInt(relation.sizeInBytes)
  )

  /** Used to lookup original attribute capitalization 用于查找原始属性大小写*/
  val attributeMap: AttributeMap[AttributeReference] = AttributeMap(output.map(o => (o, o)))

  def newInstance(): this.type = LogicalRelation(relation).asInstanceOf[this.type]

  override def simpleString: String = s"Relation[${output.mkString(",")}] $relation"
}
