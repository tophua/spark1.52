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

package org.apache.spark.sql

import java.util.Properties

import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.Experimental
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCPartition, JDBCPartitioningInfo, JDBCRelation}
import org.apache.spark.sql.execution.datasources.json.JSONRelation
import org.apache.spark.sql.execution.datasources.parquet.ParquetRelation
import org.apache.spark.sql.execution.datasources.{LogicalRelation, ResolvedDataSource}
import org.apache.spark.sql.types.StructType
import org.apache.spark.{Logging, Partition}

/**
 * :: Experimental ::
 * Interface used to load a [[DataFrame]] from external storage systems (e.g. file systems,
 * key-value stores, etc). Use [[SQLContext.read]] to access this.
  *
  * 用于从外部存储系统(例如文件系统,键值存储等)加载[[DataFrame]]的接口, 使用[[SQLContext.read]]访问它。
 *
 * @since 1.4.0
 */
@Experimental
class DataFrameReader private[sql](sqlContext: SQLContext) extends Logging {

  /**
   * Specifies the input data source format.
    * 指定输入数据源格式
   *
   * @since 1.4.0
   */
  def format(source: String): DataFrameReader = {
    this.source = source
    this
  }

  /**
   * Specifies the input schema. Some data sources (e.g. JSON) can infer the input schema
   * automatically from data. By specifying the schema here, the underlying data source can
   * skip the schema inference step, and thus speed up data loading.
    *
    * 指定输入架构,某些数据源(例如JSON)可以根据数据自动推断输入模式,
    * 通过在此处指定模式,基础数据源可以跳过模式推断步骤,从而加速数据加载
   *
   * @since 1.4.0
   */
  def schema(schema: StructType): DataFrameReader = {
    this.userSpecifiedSchema = Option(schema)
    this
  }

  /**
   * Adds an input option for the underlying data source.
    * 为基础数据源添加输入选项
   *
   * @since 1.4.0
   */
  def option(key: String, value: String): DataFrameReader = {
    this.extraOptions += (key -> value)
    this
  }

  /**
   * (Scala-specific) Adds input options for the underlying data source.
    * （特定于Scala）为基础数据源添加输入选项
   *
   * @since 1.4.0
   */
  def options(options: scala.collection.Map[String, String]): DataFrameReader = {
    this.extraOptions ++= options
    this
  }

  /**
   * Adds input options for the underlying data source.
    * 添加基础数据源的输入选项
   *
   * @since 1.4.0
   */
  def options(options: java.util.Map[String, String]): DataFrameReader = {
    this.options(scala.collection.JavaConversions.mapAsScalaMap(options))
    this
  }

  /**
   * Loads input in as a [[DataFrame]], for data sources that require a path (e.g. data backed by
   * a local or distributed file system).
    * 加载作为[[DataFrame]]的输入,用于需要路径的数据源(例如,由本地或分布式文件系统支持的数据)
   *
   * @since 1.4.0
   */
  def load(path: String): DataFrame = {
    option("path", path).load()
  }

  /**
   * Loads input in as a [[DataFrame]], for data sources that don't require a path (e.g. external
   * key-value stores).
    * 对于不需要路径的数据源(例如外部键值存储),将输入加载为[[DataFrame]]。
   *
   * @since 1.4.0
   */
  def load(): DataFrame = {
    val resolved = ResolvedDataSource(
      sqlContext,
      userSpecifiedSchema = userSpecifiedSchema,
      partitionColumns = Array.empty[String],
      provider = source,
      options = extraOptions.toMap)
    DataFrame(sqlContext, LogicalRelation(resolved.relation))
  }

  /**
   * Construct a [[DataFrame]] representing the database table accessible via JDBC URL
   * url named table and connection properties.
    * 构造一个[[DataFrame]],表示可通过名为table和connection属性的JDBC URL url访问的数据库表。
   *
   * @since 1.4.0
   */
  def jdbc(url: String, table: String, properties: Properties): DataFrame = {
    jdbc(url, table, JDBCRelation.columnPartition(null), properties)
  }

  /**
   * Construct a [[DataFrame]] representing the database table accessible via JDBC URL
   * url named table. Partitions of the table will be retrieved in parallel based on the parameters
   * passed to this function.
   *
    * 构造一个[[DataFrame]]，表示可通过名为table的JDBC URL url访问的数据库表,
    * 将根据传递给此函数的参数并行检索表的分区。
    *
   * Don't create too many partitions in parallel on a large cluster; otherwise Spark might crash
   * your external database systems.
   *
   * @param url JDBC database url of the form `jdbc:subprotocol:subname`
   * @param table Name of the table in the external database.
   * @param columnName the name of a column of integral type that will be used for partitioning.
   * @param lowerBound the minimum value of `columnName` used to decide partition stride
   * @param upperBound the maximum value of `columnName` used to decide partition stride
   * @param numPartitions the number of partitions.  the range `minValue`-`maxValue` will be split
   *                      evenly into this many partitions
   * @param connectionProperties JDBC database connection arguments, a list of arbitrary string
   *                             tag/value. Normally at least a "user" and "password" property
   *                             should be included.
   *
   * @since 1.4.0
   */
  def jdbc(
      url: String,
      table: String,
      columnName: String,
      lowerBound: Long,
      upperBound: Long,
      numPartitions: Int,
      connectionProperties: Properties): DataFrame = {
    val partitioning = JDBCPartitioningInfo(columnName, lowerBound, upperBound, numPartitions)
    val parts = JDBCRelation.columnPartition(partitioning)
    jdbc(url, table, parts, connectionProperties)
  }

  /**
   * Construct a [[DataFrame]] representing the database table accessible via JDBC URL
   * url named table using connection properties. The `predicates` parameter gives a list
   * expressions suitable for inclusion in WHERE clauses; each one defines one partition
   * of the [[DataFrame]].
    *
    * 构造一个[[DataFrame]],表示可以使用连接属性通过名为table的JDBC URL url访问的数据库表。
    * `predicates`参数给出了一个适合包含在WHERE子句中的列表表达式;
    * 每一个都定义了[[DataFrame]]的一个分区。
   *
   * Don't create too many partitions in parallel on a large cluster; otherwise Spark might crash
   * your external database systems.
   *
   * @param url JDBC database url of the form `jdbc:subprotocol:subname`
   * @param table Name of the table in the external database.
   * @param predicates Condition in the where clause for each partition.
   * @param connectionProperties JDBC database connection arguments, a list of arbitrary string
   *                             tag/value. Normally at least a "user" and "password" property
   *                             should be included.
   * @since 1.4.0
   */
  def jdbc(
      url: String,
      table: String,
      predicates: Array[String],
      connectionProperties: Properties): DataFrame = {
    val parts: Array[Partition] = predicates.zipWithIndex.map { case (part, i) =>
      JDBCPartition(part, i) : Partition
    }
    jdbc(url, table, parts, connectionProperties)
  }

  private def jdbc(
      url: String,
      table: String,
      parts: Array[Partition],
      connectionProperties: Properties): DataFrame = {
    val props = new Properties()
    extraOptions.foreach { case (key, value) =>
      props.put(key, value)
    }
    // connectionProperties should override settings in extraOptions
    props.putAll(connectionProperties)
    val relation = JDBCRelation(url, table, parts, props)(sqlContext)
    sqlContext.baseRelationToDataFrame(relation)
  }

  /**
   * Loads a JSON file (one object per line) and returns the result as a [[DataFrame]].
    * 加载JSON文件（每行一个对象）并将结果作为[[DataFrame]]返回
   *
   * This function goes through the input once to determine the input schema. If you know the
   * schema in advance, use the version that specifies the schema to avoid the extra scan.
    *
    * 此函数通过输入一次以确定输入模式,如果您事先知道架构,请使用指定架构的版本以避免额外扫描
   *
   * @param path input path
   * @since 1.4.0
   */
  def json(path: String): DataFrame = format("json").load(path)

  /**
   * Loads an `JavaRDD[String]` storing JSON objects (one object per record) and
   * returns the result as a [[DataFrame]].
    * 加载一个`JavaRDD [String]`存储JSON对象(每个记录一个对象)并将结果作为[[DataFrame]]返回
   *
   * Unless the schema is specified using [[schema]] function, this function goes through the
   * input once to determine the input schema.
    *
    * 除非使用[[schema]]函数指定模式，否则此函数将通过输入一次以确定输入模式
   *
   * @param jsonRDD input RDD with one JSON object per record
   * @since 1.4.0
   */
  def json(jsonRDD: JavaRDD[String]): DataFrame = json(jsonRDD.rdd)

  /**
   * Loads an `RDD[String]` storing JSON objects (one object per record) and
   * returns the result as a [[DataFrame]].
    *
    * 加载一个`RDD [String]`存储JSON对象(每个记录一个对象)并将结果作为[[DataFrame]]返回。
   *
   * Unless the schema is specified using [[schema]] function, this function goes through the
   * input once to determine the input schema.
   *
   * @param jsonRDD input RDD with one JSON object per record
   * @since 1.4.0
   */
  def json(jsonRDD: RDD[String]): DataFrame = {
    val samplingRatio = extraOptions.getOrElse("samplingRatio", "1.0").toDouble
    sqlContext.baseRelationToDataFrame(
      new JSONRelation(Some(jsonRDD), samplingRatio, userSpecifiedSchema, None, None)(sqlContext))
  }

  /**
   * Loads a Parquet file, returning the result as a [[DataFrame]]. This function returns an empty
   * [[DataFrame]] if no paths are passed in.
    *
    * 加载Parquet文件,将结果作为[[DataFrame]]返回,如果没有传入路径,则此函数返回空[[DataFrame]]
   *
   * @since 1.4.0
   */
  @scala.annotation.varargs
  def parquet(paths: String*): DataFrame = {
    if (paths.isEmpty) {
      sqlContext.emptyDataFrame
    } else {
      val globbedPaths = paths.flatMap { path =>
        val hdfsPath = new Path(path)
        val fs = hdfsPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
        val qualified = hdfsPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
        SparkHadoopUtil.get.globPathIfNecessary(qualified)
      }.toArray

      sqlContext.baseRelationToDataFrame(
        new ParquetRelation(
          globbedPaths.map(_.toString), userSpecifiedSchema, None, extraOptions.toMap)(sqlContext))
    }
  }

  /**
   * Loads an ORC file and returns the result as a [[DataFrame]].
    * 加载ORC文件并将结果作为[[DataFrame]]返回
   *
   * @param path input path
   * @since 1.5.0
   * @note Currently, this method can only be used together with `HiveContext`.
   */
  def orc(path: String): DataFrame = format("orc").load(path)

  /**
   * Returns the specified table as a [[DataFrame]].
   *
   * @since 1.4.0
   */
  def table(tableName: String): DataFrame = {
    DataFrame(sqlContext, sqlContext.catalog.lookupRelation(Seq(tableName)))
  }

  ///////////////////////////////////////////////////////////////////////////////////////
  // Builder pattern config options Builder模式配置选项
  ///////////////////////////////////////////////////////////////////////////////////////

  private var source: String = sqlContext.conf.defaultDataSourceName

  private var userSpecifiedSchema: Option[StructType] = None

  private var extraOptions = new scala.collection.mutable.HashMap[String, String]

}
