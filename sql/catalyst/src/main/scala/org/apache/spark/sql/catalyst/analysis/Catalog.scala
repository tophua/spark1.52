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

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{TableIdentifier, CatalystConf, EmptyConf}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Subquery}

/**
 * Thrown by a catalog when a table cannot be found.  The analyzer will rethrow the exception
 * as an AnalysisException with the correct position information.
  * 当找不到表时,由目录抛出,分析器将使用正确的位置信息重新抛出异常作为AnalysisException
 */
class NoSuchTableException extends Exception

class NoSuchDatabaseException extends Exception

/**
 * An interface for looking up relations by name.  Used by an [[Analyzer]].
  * 用于按名称查找关系的界面,由[[Analyzer]]使用
 */
trait Catalog {

  val conf: CatalystConf

  def tableExists(tableIdentifier: Seq[String]): Boolean

  def lookupRelation(
      tableIdentifier: Seq[String],
      alias: Option[String] = None): LogicalPlan

  /**
   * Returns tuples of (tableName, isTemporary) for all tables in the given database.
   * isTemporary is a Boolean value indicates if a table is a temporary or not.
    * 返回给定数据库中所有表的(tableName，isTemporary)元组,isTemporary是一个布尔值,表示表是否是临时表。
   */
  def getTables(databaseName: Option[String]): Seq[(String, Boolean)]

  def refreshTable(tableIdent: TableIdentifier): Unit

  // TODO: Refactor it in the work of SPARK-10104
  def registerTable(tableIdentifier: Seq[String], plan: LogicalPlan): Unit

  // TODO: Refactor it in the work of SPARK-10104
  def unregisterTable(tableIdentifier: Seq[String]): Unit

  def unregisterAllTables(): Unit

  // TODO: Refactor it in the work of SPARK-10104
  protected def processTableIdentifier(tableIdentifier: Seq[String]): Seq[String] = {
    if (conf.caseSensitiveAnalysis) {
      tableIdentifier
    } else {
      tableIdentifier.map(_.toLowerCase)
    }
  }

  // TODO: Refactor it in the work of SPARK-10104
  protected def getDbTableName(tableIdent: Seq[String]): String = {
    val size = tableIdent.size
    if (size <= 2) {
      tableIdent.mkString(".")
    } else {
      tableIdent.slice(size - 2, size).mkString(".")
    }
  }

  // TODO: Refactor it in the work of SPARK-10104
  protected def getDBTable(tableIdent: Seq[String]) : (Option[String], String) = {
    (tableIdent.lift(tableIdent.size - 2), tableIdent.last)
  }

  /**
   * It is not allowed to specifiy database name for tables stored in [[SimpleCatalog]].
   * We use this method to check it.
    * 不允许为[[Simple Catalog]]中存储的表指定数据库名称,我们使用这种方法来检查它
   */
  protected def checkTableIdentifier(tableIdentifier: Seq[String]): Unit = {
    if (tableIdentifier.length > 1) {
      throw new AnalysisException("Specifying database name or other qualifiers are not allowed " +
        "for temporary tables. If the table name has dots (.) in it, please quote the " +
        "table name with backticks (`).")
    }
  }
}

class SimpleCatalog(val conf: CatalystConf) extends Catalog {
  val tables = new ConcurrentHashMap[String, LogicalPlan]

  override def registerTable(
      tableIdentifier: Seq[String],
      plan: LogicalPlan): Unit = {
    checkTableIdentifier(tableIdentifier)
    val tableIdent = processTableIdentifier(tableIdentifier)
    tables.put(getDbTableName(tableIdent), plan)
  }

  override def unregisterTable(tableIdentifier: Seq[String]): Unit = {
    checkTableIdentifier(tableIdentifier)
    val tableIdent = processTableIdentifier(tableIdentifier)
    tables.remove(getDbTableName(tableIdent))
  }

  override def unregisterAllTables(): Unit = {
    tables.clear()
  }

  override def tableExists(tableIdentifier: Seq[String]): Boolean = {
    checkTableIdentifier(tableIdentifier)
    val tableIdent = processTableIdentifier(tableIdentifier)
    tables.containsKey(getDbTableName(tableIdent))
  }

  override def lookupRelation(
      tableIdentifier: Seq[String],
      alias: Option[String] = None): LogicalPlan = {
    checkTableIdentifier(tableIdentifier)
    val tableIdent = processTableIdentifier(tableIdentifier)
    val tableFullName = getDbTableName(tableIdent)
    val table = tables.get(tableFullName)
    if (table == null) {
      sys.error(s"Table Not Found: $tableFullName")
    }
    val tableWithQualifiers = Subquery(tableIdent.last, table)

    // If an alias was specified by the lookup, wrap the plan in a subquery so that attributes are
    // properly qualified with this alias.
    //如果查找指定了别名,请将计划包装在子查询中,以便使用此别名正确限定属性
    alias.map(a => Subquery(a, tableWithQualifiers)).getOrElse(tableWithQualifiers)
  }

  override def getTables(databaseName: Option[String]): Seq[(String, Boolean)] = {
    val result = ArrayBuffer.empty[(String, Boolean)]
    for (name <- tables.keySet()) {
      result += ((name, true))
    }
    result
  }

  override def refreshTable(tableIdent: TableIdentifier): Unit = {
    throw new UnsupportedOperationException
  }
}

/**
 * A trait that can be mixed in with other Catalogs allowing specific tables to be overridden with
 * new logical plans.  This can be used to bind query result to virtual tables, or replace tables
 * with in-memory cached versions.  Note that the set of overrides is stored in memory and thus
 * lost when the JVM exits.
  *可以与其他目录混合使用的特征,允许使用新逻辑计划覆盖特定表,这可用于将查询结果绑定到虚拟表,
  * 或用内存中缓存版本替换表,请注意,覆盖集存储在内存中,因此在JVM退出时会丢失
 */
trait OverrideCatalog extends Catalog {

  // TODO: This doesn't work when the database changes...
  val overrides = new mutable.HashMap[(Option[String], String), LogicalPlan]()

  abstract override def tableExists(tableIdentifier: Seq[String]): Boolean = {
    val tableIdent = processTableIdentifier(tableIdentifier)
    // A temporary tables only has a single part in the tableIdentifier.
    //临时表在tableIdentifier中只有一个部分
    val overriddenTable = if (tableIdentifier.length > 1) {
      None: Option[LogicalPlan]
    } else {
      overrides.get(getDBTable(tableIdent))
    }
    overriddenTable match {
      case Some(_) => true
      case None => super.tableExists(tableIdentifier)
    }
  }

  abstract override def lookupRelation(
      tableIdentifier: Seq[String],
      alias: Option[String] = None): LogicalPlan = {
    val tableIdent = processTableIdentifier(tableIdentifier)
    // A temporary tables only has a single part in the tableIdentifier.
    //临时表在tableIdentifier中只有一个部分
    val overriddenTable = if (tableIdentifier.length > 1) {
      None: Option[LogicalPlan]
    } else {
      overrides.get(getDBTable(tableIdent))
    }
    val tableWithQualifers = overriddenTable.map(r => Subquery(tableIdent.last, r))

    // If an alias was specified by the lookup, wrap the plan in a subquery so that attributes are
    // properly qualified with this alias.
    //如果查找指定了别名,请将计划包装在子查询中,以便使用此别名正确限定属性
    val withAlias =
      tableWithQualifers.map(r => alias.map(a => Subquery(a, r)).getOrElse(r))

    withAlias.getOrElse(super.lookupRelation(tableIdentifier, alias))
  }

  abstract override def getTables(databaseName: Option[String]): Seq[(String, Boolean)] = {
    // We always return all temporary tables.
    val temporaryTables = overrides.map {
      case ((_, tableName), _) => (tableName, true)
    }.toSeq

    temporaryTables ++ super.getTables(databaseName)
  }

  override def registerTable(
      tableIdentifier: Seq[String],
      plan: LogicalPlan): Unit = {
    checkTableIdentifier(tableIdentifier)
    val tableIdent = processTableIdentifier(tableIdentifier)
    overrides.put(getDBTable(tableIdent), plan)
  }

  override def unregisterTable(tableIdentifier: Seq[String]): Unit = {
    // A temporary tables only has a single part in the tableIdentifier.
    // If tableIdentifier has more than one parts, it is not a temporary table
    // and we do not need to do anything at here.
    if (tableIdentifier.length == 1) {
      val tableIdent = processTableIdentifier(tableIdentifier)
      overrides.remove(getDBTable(tableIdent))
    }
  }

  override def unregisterAllTables(): Unit = {
    overrides.clear()
  }
}

/**
 * A trivial catalog that returns an error when a relation is requested.  Used for testing when all
 * relations are already filled in and the analyzer needs only to resolve attribute references.
  * 一个简单的目录,在请求关系时返回错误,用于在已填写所有关系并且分析器仅需要解析属性引用时进行测试
 */
object EmptyCatalog extends Catalog {

  override val conf: CatalystConf = EmptyConf

  override def tableExists(tableIdentifier: Seq[String]): Boolean = {
    throw new UnsupportedOperationException
  }

  override def lookupRelation(
      tableIdentifier: Seq[String],
      alias: Option[String] = None): LogicalPlan = {
    throw new UnsupportedOperationException
  }

  override def getTables(databaseName: Option[String]): Seq[(String, Boolean)] = {
    throw new UnsupportedOperationException
  }

  override def registerTable(tableIdentifier: Seq[String], plan: LogicalPlan): Unit = {
    throw new UnsupportedOperationException
  }

  override def unregisterTable(tableIdentifier: Seq[String]): Unit = {
    throw new UnsupportedOperationException
  }

  override def unregisterAllTables(): Unit = {}

  override def refreshTable(tableIdent: TableIdentifier): Unit = {
    throw new UnsupportedOperationException
  }
}
