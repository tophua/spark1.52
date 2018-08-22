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

package org.apache.spark.sql.hive.execution

import java.io.File

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.MetaStoreUtils
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.{TableIdentifier, SqlParser}
import org.apache.spark.sql.catalyst.analysis.EliminateSubQueries
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.execution.datasources.{ResolvedDataSource, LogicalRelation}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

/**
 * Analyzes the given table in the current database to generate statistics, which will be
 * used in query optimizations.
 * 分析当前数据库中的给定表以生成统计信息,该统计信息将用于查询优化。
 * Right now, it only supports Hive tables and it only updates the size of a Hive table
 * in the Hive metastore.
  * 现在,它只支持Hive表,它只更新Hive Metastore中Hive表的大小
 */
private[hive]
case class AnalyzeTable(tableName: String) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    sqlContext.asInstanceOf[HiveContext].analyze(tableName)
    Seq.empty[Row]
  }
}

/**
 * Drops a table from the metastore and removes it if it is cached.
  * 从Metastore中删除一个表,如果它被缓存则将其删除
 */
private[hive]
case class DropTable(
    tableName: String,
    ifExists: Boolean) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val hiveContext = sqlContext.asInstanceOf[HiveContext]
    val ifExistsClause = if (ifExists) "IF EXISTS " else ""
    try {
      hiveContext.cacheManager.tryUncacheQuery(hiveContext.table(tableName))
    } catch {
      // This table's metadata is not in Hive metastore (e.g. the table does not exist).
      //此表的元数据不在Hive Metastore中(例如,该表不存在)
      case _: org.apache.hadoop.hive.ql.metadata.InvalidTableException =>
      case _: org.apache.spark.sql.catalyst.analysis.NoSuchTableException =>
      // Other Throwables can be caused by users providing wrong parameters in OPTIONS
      // (e.g. invalid paths). We catch it and log a warning message.
      // Users should be able to drop such kinds of tables regardless if there is an error.
      case e: Throwable => log.warn(s"${e.getMessage}", e)
    }
    hiveContext.invalidateTable(tableName)
    hiveContext.runSqlHive(s"DROP TABLE $ifExistsClause$tableName")
    hiveContext.catalog.unregisterTable(Seq(tableName))
    Seq.empty[Row]
  }
}

private[hive]
case class AddJar(path: String) extends RunnableCommand {

  override val output: Seq[Attribute] = {
    val schema = StructType(
      StructField("result", IntegerType, false) :: Nil)
    schema.toAttributes
  }

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val hiveContext = sqlContext.asInstanceOf[HiveContext]
    val currentClassLoader = Utils.getContextOrSparkClassLoader

    // Add jar to current context
    //将jar添加到当前上下文
    val jarURL = {
      val uri = new Path(path).toUri
      if (uri.getScheme == null) {
        // `path` is a local file path without a URL scheme
        //`path`是没有URL方案的本地文件路径
        new File(path).toURI.toURL
      } else {
        // `path` is a URL with a scheme
        //`path`是带有方案的URL
        uri.toURL
      }
    }

    val newClassLoader = new java.net.URLClassLoader(Array(jarURL), currentClassLoader)
    //Thread.currentThread().getContextClassLoader,可以获取当前线程的引用,getContextClassLoader用来获取线程的上下文类加载器
    Thread.currentThread.setContextClassLoader(newClassLoader)
    // We need to explicitly set the class loader associated with the conf in executionHive's
    // state because this class loader will be used as the context class loader of the current
    // thread to execute any Hive command.
    //我们需要在executionHive的状态中显式设置与conf相关联的类加载器,
    //因为这个类加载器将用作当前线程的上下文类加载器来执行任何Hive命令
    // We cannot use `org.apache.hadoop.hive.ql.metadata.Hive.get().getConf()` because Hive.get()
    // returns the value of a thread local variable and its HiveConf may not be the HiveConf
    // associated with `executionHive.state` (for example, HiveContext is created in one thread
    // and then add jar is called from another thread).
    //我们不能使用`org.apache.hadoop.hive.ql.metadata.Hive.get（）,
    // getConf()`因为Hive.get()返回一个线程局部变量的值,而它的HiveConf可能不是HiveConf返回的线程局部变量的值
    // 及其HiveConf可能不是HiveConf,然后从另一个线程调用add jar
    hiveContext.executionHive.state.getConf.setClassLoader(newClassLoader)
    // Add jar to isolated hive (metadataHive) class loader.
    //将jar添加到隔离的hive（metadataHive）类加载器
    hiveContext.runSqlHive(s"ADD JAR $path")

    // Add jar to executors
    //将jar添加到执行程序
    hiveContext.sparkContext.addJar(path)

    Seq(Row(0))
  }
}

private[hive]
case class AddFile(path: String) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val hiveContext = sqlContext.asInstanceOf[HiveContext]
    hiveContext.runSqlHive(s"ADD FILE $path")
    hiveContext.sparkContext.addFile(path)
    Seq.empty[Row]
  }
}

// TODO: Use TableIdentifier instead of String for tableName (SPARK-10104).
private[hive]
case class CreateMetastoreDataSource(
    tableIdent: TableIdentifier,
    userSpecifiedSchema: Option[StructType],
    provider: String,
    options: Map[String, String],
    allowExisting: Boolean,
    managedIfNoPath: Boolean) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    // Since we are saving metadata to metastore, we need to check if metastore supports
    // the table name and database name we have for this query. MetaStoreUtils.validateName
    // is the method used by Hive to check if a table name or a database name is valid for
    // the metastore.
    //由于我们将元数据保存到Metastore,我们需要检查Metastore是否支持我们为此查询提供的表名和数据库名。
    // MetaStoreUtils.validateName是Hive用于检查表名称或数据库名称是否对Metastore有效的方法。
    if (!MetaStoreUtils.validateName(tableIdent.table)) {
      throw new AnalysisException(s"Table name ${tableIdent.table} is not a valid name for " +
        s"metastore. Metastore only accepts table name containing characters, numbers and _.")
    }
    if (tableIdent.database.isDefined && !MetaStoreUtils.validateName(tableIdent.database.get)) {
      throw new AnalysisException(s"Database name ${tableIdent.database.get} is not a valid name " +
        s"for metastore. Metastore only accepts database name containing " +
        s"characters, numbers and _.")
    }

    val tableName = tableIdent.unquotedString
    val hiveContext = sqlContext.asInstanceOf[HiveContext]

    if (hiveContext.catalog.tableExists(tableIdent.toSeq)) {
      if (allowExisting) {
        return Seq.empty[Row]
      } else {
        throw new AnalysisException(s"Table $tableName already exists.")
      }
    }

    var isExternal = true
    val optionsWithPath =
      if (!options.contains("path") && managedIfNoPath) {
        isExternal = false
        options + ("path" -> hiveContext.catalog.hiveDefaultTableFilePath(tableIdent))
      } else {
        options
      }

    hiveContext.catalog.createDataSourceTable(
      tableIdent,
      userSpecifiedSchema,
      Array.empty[String],
      provider,
      optionsWithPath,
      isExternal)

    Seq.empty[Row]
  }
}

// TODO: Use TableIdentifier instead of String for tableName (SPARK-10104).
private[hive]
case class CreateMetastoreDataSourceAsSelect(
    tableIdent: TableIdentifier,
    provider: String,
    partitionColumns: Array[String],
    mode: SaveMode,
    options: Map[String, String],
    query: LogicalPlan) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    // Since we are saving metadata to metastore, we need to check if metastore supports
    // the table name and database name we have for this query. MetaStoreUtils.validateName
    // is the method used by Hive to check if a table name or a database name is valid for
    // the metastore.
    //由于我们将元数据保存到Metastore,我们需要检查Metastore是否支持我们为此查询提供的表名和数据库名,
    // MetaStoreUtils.validateName是Hive用于检查表名称或数据库名称是否对Metastore有效的方法。
    if (!MetaStoreUtils.validateName(tableIdent.table)) {
      throw new AnalysisException(s"Table name ${tableIdent.table} is not a valid name for " +
        s"metastore. Metastore only accepts table name containing characters, numbers and _.")
    }
    if (tableIdent.database.isDefined && !MetaStoreUtils.validateName(tableIdent.database.get)) {
      throw new AnalysisException(s"Database name ${tableIdent.database.get} is not a valid name " +
        s"for metastore. Metastore only accepts database name containing " +
        s"characters, numbers and _.")
    }

    val tableName = tableIdent.unquotedString
    val hiveContext = sqlContext.asInstanceOf[HiveContext]
    var createMetastoreTable = false
    var isExternal = true
    val optionsWithPath =
      if (!options.contains("path")) {
        isExternal = false
        options + ("path" -> hiveContext.catalog.hiveDefaultTableFilePath(tableIdent))
      } else {
        options
      }

    var existingSchema = None: Option[StructType]
    if (sqlContext.catalog.tableExists(tableIdent.toSeq)) {
      // Check if we need to throw an exception or just return.
      //检查我们是否需要抛出异常或返回
      mode match {
        case SaveMode.ErrorIfExists =>
          throw new AnalysisException(s"Table $tableName already exists. " +
            s"If you are using saveAsTable, you can set SaveMode to SaveMode.Append to " +
            s"insert data into the table or set SaveMode to SaveMode.Overwrite to overwrite" +
            s"the existing data. " +
            s"Or, if you are using SQL CREATE TABLE, you need to drop $tableName first.")
        case SaveMode.Ignore =>
          // Since the table already exists and the save mode is Ignore, we will just return.
          //由于表已经存在且保存模式为Ignore,我们将返回
          return Seq.empty[Row]
        case SaveMode.Append =>
          // Check if the specified data source match the data source of the existing table.
          //检查指定的数据源是否与现有表的数据源匹配
          val resolved = ResolvedDataSource(
            sqlContext, Some(query.schema.asNullable), partitionColumns, provider, optionsWithPath)
          val createdRelation = LogicalRelation(resolved.relation)
          EliminateSubQueries(sqlContext.catalog.lookupRelation(tableIdent.toSeq)) match {
            case l @ LogicalRelation(_: InsertableRelation | _: HadoopFsRelation, _) =>
              if (l.relation != createdRelation.relation) {
                val errorDescription =
                  s"Cannot append to table $tableName because the resolved relation does not " +
                  s"match the existing relation of $tableName. " +
                  s"You can use insertInto($tableName, false) to append this DataFrame to the " +
                  s"table $tableName and using its data source and options."
                val errorMessage =
                  s"""
                     |$errorDescription
                     |== Relations ==
                     |${sideBySide(
                        s"== Expected Relation ==" :: l.toString :: Nil,
                        s"== Actual Relation ==" :: createdRelation.toString :: Nil
                      ).mkString("\n")}
                   """.stripMargin
                throw new AnalysisException(errorMessage)
              }
              existingSchema = Some(l.schema)
            case o =>
              throw new AnalysisException(s"Saving data in ${o.toString} is not supported.")
          }
        case SaveMode.Overwrite =>
          hiveContext.sql(s"DROP TABLE IF EXISTS $tableName")
          // Need to create the table again.
          createMetastoreTable = true
      }
    } else {
      // The table does not exist. We need to create it in metastore.
      //该表不存在,我们需要在Metastore中创建它
      createMetastoreTable = true
    }

    val data = DataFrame(hiveContext, query)
    val df = existingSchema match {
      // If we are inserting into an existing table, just use the existing schema.
        //如果我们要插入现有表,只需使用现有模式
      case Some(schema) => sqlContext.internalCreateDataFrame(data.queryExecution.toRdd, schema)
      case None => data
    }

    // Create the relation based on the data of df.
    //根据df的数据创建关系
    val resolved =
      ResolvedDataSource(sqlContext, provider, partitionColumns, mode, optionsWithPath, df)

    if (createMetastoreTable) {
      // We will use the schema of resolved.relation as the schema of the table (instead of
      // the schema of df). It is important since the nullability may be changed by the relation
      // provider (for example, see org.apache.spark.sql.parquet.DefaultSource).
      //我们将使用resolved.relation的模式作为表的模式(而不是df的模式),这很重要,
      // 因为关系提供程序可以更改可为空性(例如.请参阅org.apache.spark.sql.parquet.DefaultSource)
      hiveContext.catalog.createDataSourceTable(
        tableIdent,
        Some(resolved.relation.schema),
        partitionColumns,
        provider,
        optionsWithPath,
        isExternal)
    }

    // Refresh the cache of the table in the catalog.
    //刷新目录中表的缓存
    hiveContext.catalog.refreshTable(tableIdent)
    Seq.empty[Row]
  }
}
