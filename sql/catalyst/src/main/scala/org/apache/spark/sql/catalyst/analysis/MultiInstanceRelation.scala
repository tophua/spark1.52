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

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * A trait that should be mixed into query operators where an single instance might appear multiple
 * times in a logical query plan.  It is invalid to have multiple copies of the same attribute
 * produced by distinct operators in a query tree as this breaks the guarantee that expression
 * ids, which are used to differentiate attributes, are unique.
  *
  * 应该混合到查询运算符中的特征,其中单个实例可能在逻辑查询计划中多次出现,
  * 在查询树中具有由不同运算符生成的相同属性的多个副本是无效的,因为这破坏了用于区分属性的表达式id是唯一的保证
 *
 * During analysis, operators that include this trait may be asked to produce a new version
 * of itself with globally unique expression ids.
  * 在分析期间,可能会要求包含此特征的运算符生成具有全局唯一表达式ID的新版本
 */
trait MultiInstanceRelation {
  def newInstance(): LogicalPlan
}
