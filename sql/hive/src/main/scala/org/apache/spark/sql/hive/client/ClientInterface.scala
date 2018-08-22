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

package org.apache.spark.sql.hive.client

import java.io.PrintStream
import java.util.{Map => JMap}

import org.apache.spark.sql.catalyst.analysis.{NoSuchDatabaseException, NoSuchTableException}
import org.apache.spark.sql.catalyst.expressions.Expression

private[hive] case class HiveDatabase(
    name: String,
    location: String)

private[hive] abstract class TableType { val name: String }
private[hive] case object ExternalTable extends TableType { override val name = "EXTERNAL_TABLE" }
private[hive] case object IndexTable extends TableType { override val name = "INDEX_TABLE" }
private[hive] case object ManagedTable extends TableType { override val name = "MANAGED_TABLE" }
private[hive] case object VirtualView extends TableType { override val name = "VIRTUAL_VIEW" }

// TODO: Use this for Tables and Partitions
private[hive] case class HiveStorageDescriptor(
    location: String,
    inputFormat: String,
    outputFormat: String,
    serde: String,
    serdeProperties: Map[String, String])

private[hive] case class HivePartition(
    values: Seq[String],
    storage: HiveStorageDescriptor)

private[hive] case class HiveColumn(name: String, hiveType: String, comment: String)
private[hive] case class HiveTable(
    specifiedDatabase: Option[String],
    name: String,
    schema: Seq[HiveColumn],
    partitionColumns: Seq[HiveColumn],
    properties: Map[String, String],
    serdeProperties: Map[String, String],
    tableType: TableType,
    location: Option[String] = None,
    inputFormat: Option[String] = None,
    outputFormat: Option[String] = None,
    serde: Option[String] = None,
    viewText: Option[String] = None) {

  @transient
  private[client] var client: ClientInterface = _

  private[client] def withClient(ci: ClientInterface): this.type = {
    client = ci
    this
  }

  def database: String = specifiedDatabase.getOrElse(sys.error("database not resolved"))

  def isPartitioned: Boolean = partitionColumns.nonEmpty

  def getAllPartitions: Seq[HivePartition] = client.getAllPartitions(this)

  def getPartitions(predicates: Seq[Expression]): Seq[HivePartition] =
    client.getPartitionsByFilter(this, predicates)

  // Hive does not support backticks when passing names to the client.
  def qualifiedName: String = s"$database.$name"
}

/**
 * An externally visible interface to the Hive client.  This interface is shared across both the
 * internal and external classloaders for a given version of Hive and thus must expose only
 * shared classes.
  * Hive客户端的外部可见界面,对于给定版本的Hive,此接口在内部和外部类加载器之间共享,因此必须仅公开共享类。
 */
private[hive] trait ClientInterface {

  /** Returns the Hive Version of this client.
    *返回此客户端的Hive版本*/
  def version: HiveVersion

  /** Returns the configuration for the given key in the current session.
    * 返回当前会话中给定键的配置*/
  def getConf(key: String, defaultValue: String): String

  /**
   * Runs a HiveQL command using Hive, returning the results as a list of strings.  Each row will
   * result in one string.
    * 使用Hive运行HiveQL命令,将结果作为字符串列表返回,每行将产生一个字符串
   */
  def runSqlHive(sql: String): Seq[String]

  def setOut(stream: PrintStream): Unit
  def setInfo(stream: PrintStream): Unit
  def setError(stream: PrintStream): Unit

  /** Returns the names of all tables in the given database.
    * 返回给定数据库中所有表的名称*/
  def listTables(dbName: String): Seq[String]

  /** Returns the name of the active database.
    * 返回活动数据库的名称*/
  def currentDatabase: String

  /** Returns the metadata for specified database, throwing an exception if it doesn't exist
    * 返回指定数据库的元数据，如果不存在则抛出异常*/
  def getDatabase(name: String): HiveDatabase = {
    getDatabaseOption(name).getOrElse(throw new NoSuchDatabaseException)
  }

  /** Returns the metadata for a given database, or None if it doesn't exist.
    * 返回给定数据库的元数据,如果不存在,则返回None*/
  def getDatabaseOption(name: String): Option[HiveDatabase]

  /** Returns the specified table, or throws [[NoSuchTableException]].
    * 返回指定的表，或抛出[[NoSuchTableException]]*/
  def getTable(dbName: String, tableName: String): HiveTable = {
    getTableOption(dbName, tableName).getOrElse(throw new NoSuchTableException)
  }

  /** Returns the metadata for the specified table or None if it doens't exist.
    * 返回指定表的元数据，如果不存在，则返回None */
  def getTableOption(dbName: String, tableName: String): Option[HiveTable]

  /** Creates a table with the given metadata. 创建具有给定元数据的表*/
  def createTable(table: HiveTable): Unit

  /** Updates the given table with new metadata.使用新元数据更新给定表 */
  def alterTable(table: HiveTable): Unit

  /** Creates a new database with the given name. 使用给定名称创建新数据库*/
  def createDatabase(database: HiveDatabase): Unit

  /** Returns the specified paritition or None if it does not exist. 返回指定的分区,如果不存在,则返回None。*/
  def getPartitionOption(
      hTable: HiveTable,
      partitionSpec: JMap[String, String]): Option[HivePartition]

  /** Returns all partitions for the given table. 返回给定表的所有分区*/
  def getAllPartitions(hTable: HiveTable): Seq[HivePartition]

  /** Returns partitions filtered by predicates for the given table. 返回由给定表的谓词过滤的分区*/
  def getPartitionsByFilter(hTable: HiveTable, predicates: Seq[Expression]): Seq[HivePartition]

  /** Loads a static partition into an existing table. 将静态分区加载到现有表中*/
  def loadPartition(
      loadPath: String,
      tableName: String,
      partSpec: java.util.LinkedHashMap[String, String], // Hive relies on LinkedHashMap ordering
      replace: Boolean,
      holdDDLTime: Boolean,
      inheritTableSpecs: Boolean,
      isSkewedStoreAsSubdir: Boolean): Unit

  /** Loads data into an existing table.将数据加载到现有表中 */
  def loadTable(
      loadPath: String, // TODO URI
      tableName: String,
      replace: Boolean,
      holdDDLTime: Boolean): Unit

  /** Loads new dynamic partitions into an existing table. 将新动态分区加载到现有表中*/
  def loadDynamicPartitions(
      loadPath: String,
      tableName: String,
      partSpec: java.util.LinkedHashMap[String, String], // Hive relies on LinkedHashMap ordering
      replace: Boolean,
      numDP: Int,
      holdDDLTime: Boolean,
      listBucketingEnabled: Boolean): Unit

  /** Used for testing only.  Removes all metadata from this instance of Hive.
    * 仅用于测试,从此Hive实例中删除所有元数据。*/
  def reset(): Unit
}
