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

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Attribute}
import org.apache.spark.sql.types.StringType

/**
 * A logical node that represents a non-query command to be executed by the system.  For example,
 * commands can be used by parsers to represent DDL operations.  Commands, unlike queries, are
 * eagerly executed.
  *
  * 一个逻辑节点,表示要由系统执行的非查询命令,例如,解析器可以使用命令来表示DDL操作,
  * 与查询不同,命令是急切执行的。
 */
trait Command

/**
 * Returned for the "DESCRIBE [EXTENDED] FUNCTION functionName" command.
  * 返回“DESCRIBE [EXTENDED] FUNCTION functionName”命令
 * @param functionName The function to be described.
 * @param isExtended True if "DESCRIBE EXTENDED" is used. Otherwise, false.
 */
private[sql] case class DescribeFunction(
    functionName: String,
    isExtended: Boolean) extends LogicalPlan with Command {

  override def children: Seq[LogicalPlan] = Seq.empty
  override val output: Seq[Attribute] = Seq(
    AttributeReference("function_desc", StringType, nullable = false)())
}

/**
 * Returned for the "SHOW FUNCTIONS" command, which will list all of the
 * registered function list.
  * 返回“SHOW FUNCTIONS”命令,它将列出所有已注册的功能列表
 */
private[sql] case class ShowFunctions(
    db: Option[String], pattern: Option[String]) extends LogicalPlan with Command {
  override def children: Seq[LogicalPlan] = Seq.empty
  override val output: Seq[Attribute] = Seq(
    AttributeReference("function", StringType, nullable = false)())
}
