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

package org.apache.spark.sql.sources

import scala.collection.mutable
import scala.util.Try

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}

import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateMutableProjection
import org.apache.spark.sql.execution.{FileRelation, RDDConversions}
import org.apache.spark.sql.execution.datasources.{PartitioningUtils, PartitionSpec, Partition}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql._
import org.apache.spark.util.SerializableConfiguration

/**
 * ::DeveloperApi::
 * Data sources should implement this trait so that they can register an alias to their data source.
 * This allows users to give the data source alias as the format type over the fully qualified
 * class name.
  * 数据源应该实现此特征,以便它们可以向其数据源注册别名,这允许用户将数据源别名作为格式类型提供给完全限定的类名
 *
 * A new instance of this class will be instantiated each time a DDL call is made.
  * 每次进行DDL调用时,都会实例化此类的新实例
 *
 * @since 1.5.0
 */
@DeveloperApi
trait DataSourceRegister {

  /**
   * The string that represents the format that this data source provider uses. This is
   * overridden by children to provide a nice alias for the data source. For example:
   *  表示此数据源提供程序使用的格式的字符串,这被孩子们覆盖,为数据源提供了一个很好的别名: 例如：
   * {{{
   *   override def format(): String = "parquet"
   * }}}
   *
   * @since 1.5.0
   */
  def shortName(): String
}

/**
 * ::DeveloperApi::
 * Implemented by objects that produce relations for a specific kind of data source.  When
 * Spark SQL is given a DDL operation with a USING clause specified (to specify the implemented
 * RelationProvider), this interface is used to pass in the parameters specified by a user.
  *
  * 由为特定类型的数据源生成关系的对象实现,当Spark SQL被指定了一个带有USING子句的DDL操作
  * （用于指定实现的RelationProvider）时,该接口用于传入用户指定的参数
 *
 * Users may specify the fully qualified class name of a given data source.  When that class is
 * not found Spark SQL will append the class name `DefaultSource` to the path, allowing for
 * less verbose invocation.  For example, 'org.apache.spark.sql.json' would resolve to the
 * data source 'org.apache.spark.sql.json.DefaultSource'
  *
  * 用户可以指定给定数据源的完全限定类名,当找不到该类时,Spark SQL会将类名“DefaultSource”附加到路径中,
  * 从而允许更简洁的调用。 例如:'org.apache.spark.sql.json'将解析为数据源'org.apache.spark.sql.json.DefaultSource'
 *
 * A new instance of this class will be instantiated each time a DDL call is made.
  * 每次进行DDL调用时,都会实例化此类的新实例
 *
 * @since 1.3.0
 */
@DeveloperApi
trait RelationProvider {
  /**
   * Returns a new base relation with the given parameters.
    * 返回具有给定参数的新基本关系
   * Note: the parameters' keywords are case insensitive and this insensitivity is enforced
   * by the Map that is passed to the function.
   */
  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation
}

/**
 * ::DeveloperApi::
 * Implemented by objects that produce relations for a specific kind of data source
 * with a given schema.  When Spark SQL is given a DDL operation with a USING clause specified (
 * to specify the implemented SchemaRelationProvider) and a user defined schema, this interface
 * is used to pass in the parameters specified by a user.
  *
  * 由具有给定模式的特定类型数据源生成关系的对象实现,当Spark SQL被赋予DDL操作并指定了USING子句
  * （以指定实现的SchemaRelationProvider）和用户定义的模式时,此接口用于传入用户指定的参数
 *
 * Users may specify the fully qualified class name of a given data source.  When that class is
 * not found Spark SQL will append the class name `DefaultSource` to the path, allowing for
 * less verbose invocation.  For example, 'org.apache.spark.sql.json' would resolve to the
 * data source 'org.apache.spark.sql.json.DefaultSource'
  *
  * 用户可以指定给定数据源的完全限定类名,当找不到该类时，Spark SQL会将类名“DefaultSource”附加到路径中,
  * 从而允许更简洁的调用。 例如:'org.apache.spark.sql.json'将解析为数据源
  * 'org.apache.spark.sql.json.DefaultSource'
 *
 * A new instance of this class will be instantiated each time a DDL call is made.
 *
 * The difference between a [[RelationProvider]] and a [[SchemaRelationProvider]] is that
 * users need to provide a schema when using a [[SchemaRelationProvider]].
 * A relation provider can inherits both [[RelationProvider]] and [[SchemaRelationProvider]]
 * if it can support both schema inference and user-specified schemas.
 *
 * @since 1.3.0
 */
@DeveloperApi
trait SchemaRelationProvider {
  /**
   * Returns a new base relation with the given parameters and user defined schema.
    * 返回具有给定参数和用户定义架构的新基本关系
   * Note: the parameters' keywords are case insensitive and this insensitivity is enforced
   * by the Map that is passed to the function.
   */
  def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): BaseRelation
}

/**
 * ::Experimental::
 * Implemented by objects that produce relations for a specific kind of data source
 * with a given schema and partitioned columns.  When Spark SQL is given a DDL operation with a
 * USING clause specified (to specify the implemented [[HadoopFsRelationProvider]]), a user defined
 * schema, and an optional list of partition columns, this interface is used to pass in the
 * parameters specified by a user.
 *
  * 由具有给定模式和分区列的特定类型数据源生成关系的对象实现,当Spark SQL被赋予一个DDL操作,
  * 其中指定了USING子句（用于指定实现的[[HadoopFsRelationProvider]]）,
  * 用户定义的模式和可选的分区列表,该接口用于传入由a指定的参数
  *
 * Users may specify the fully qualified class name of a given data source.  When that class is
 * not found Spark SQL will append the class name `DefaultSource` to the path, allowing for
 * less verbose invocation.  For example, 'org.apache.spark.sql.json' would resolve to the
 * data source 'org.apache.spark.sql.json.DefaultSource'
  *
  * 用户可以指定给定数据源的完全限定类名,当找不到该类时,Spark SQL会将类名“DefaultSource”附加到路径中,
  * 从而允许更简洁的调用, 例如，'org.apache.spark.sql.json'将解析为数据源'org.apache.spark.sql.json.DefaultSource'
 *
 * A new instance of this class will be instantiated each time a DDL call is made.
 *
 * The difference between a [[RelationProvider]] and a [[HadoopFsRelationProvider]] is
 * that users need to provide a schema and a (possibly empty) list of partition columns when
 * using a [[HadoopFsRelationProvider]]. A relation provider can inherits both [[RelationProvider]],
 * and [[HadoopFsRelationProvider]] if it can support schema inference, user-specified
 * schemas, and accessing partitioned relations.
 *
 * @since 1.4.0
 */
@Experimental
trait HadoopFsRelationProvider {
  /**
   * Returns a new base relation with the given parameters, a user defined schema, and a list of
   * partition columns. Note: the parameters' keywords are case insensitive and this insensitivity
   * is enforced by the Map that is passed to the function.
    *
    *返回具有给定参数的新基本关系,用户定义的模式和分区列的列表,注意：参数的关键字不区分大小写,并且传递给函数的Map强制执行此不敏感性
   *
   * @param dataSchema Schema of data columns (i.e., columns that are not partition columns).
   */
  def createRelation(
      sqlContext: SQLContext,
      paths: Array[String],
      dataSchema: Option[StructType],
      partitionColumns: Option[StructType],
      parameters: Map[String, String]): HadoopFsRelation
}

/**
 * @since 1.3.0
 */
@DeveloperApi
trait CreatableRelationProvider {
  /**
    * Creates a relation with the given parameters based on the contents of the given
    * DataFrame. The mode specifies the expected behavior of createRelation when
    * data already exists.
    *根据给定DataFrame的内容创建与给定参数的关系,该模式指定数据已存在时createRelation的预期行为
    * Right now, there are three modes, Append, Overwrite, and ErrorIfExists.
    * Append mode means that when saving a DataFrame to a data source, if data already exists,
    * contents of the DataFrame are expected to be appended to existing data.
    *
    * 现在,有三种模式，Append，Overwrite和ErrorIfExists.Append模式意味着在将DataFrame保存到数据源时,
    * 如果数据已经存在，则DataFrame的内容应该附加到现有数据。
    *
    * Overwrite mode means that when saving a DataFrame to a data source, if data already exists,
    * existing data is expected to be overwritten by the contents of the DataFrame.
    * ErrorIfExists mode means that when saving a DataFrame to a data source,
    * if data already exists, an exception is expected to be thrown.
    *
    * 覆盖模式意味着在将DataFrame保存到数据源时,如果数据已存在,则预期现有数据将被DataFrame的内容覆盖。
    * ErrorIfExists模式意味着在将DataFrame保存到数据源时,如果数据已存在,预计会抛出异常
     *
     * @since 1.3.0
    */
  def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation
}

/**
 * ::DeveloperApi::
 * Represents a collection of tuples with a known schema. Classes that extend BaseRelation must
 * be able to produce the schema of their data in the form of a [[StructType]]. Concrete
 * implementation should inherit from one of the descendant `Scan` classes, which define various
 * abstract methods for execution.
  *
  * 表示具有已知模式的元组的集合,扩展BaseRelation的类必须能够以[[StructType]]的形式生成其数据的模式,
  * 具体实现应该继承一个后代的`Scan`类,它定义了各种抽象的执行方法。
 *
 * BaseRelations must also define an equality function that only returns true when the two
 * instances will return the same data. This equality function is used when determining when
 * it is safe to substitute cached results for a given relation.
  *
  * BaseRelations还必须定义一个相等函数,该函数仅在两个实例返回相同数据时才返回true,
  * 在确定何时替换给定关系的缓存结果是安全的时，使用此相等函数。
 *
 * @since 1.3.0
 */
@DeveloperApi
abstract class BaseRelation {
  def sqlContext: SQLContext
  def schema: StructType

  /**
   * Returns an estimated size of this relation in bytes. This information is used by the planner
   * to decide when it is safe to broadcast a relation and can be overridden by sources that
   * know the size ahead of time. By default, the system will assume that tables are too
   * large to broadcast. This method will be called multiple times during query planning
   * and thus should not perform expensive operations for each invocation.
    *
    * 返回此关系的估计大小（以字节为单位）,规划人员使用此信息来确定何时广播关系是安全的,
    * 并且可以提前知道大小的来源覆盖此信息,默认情况下,系统将假定表太大而无法广播,
    * 在查询规划期间将多次调用此方法，因此不应对每次调用执行昂贵的操作。
   *
   * Note that it is always better to overestimate size than underestimate, because underestimation
   * could lead to execution plans that are suboptimal (i.e. broadcasting a very large table).
   *
   * @since 1.3.0
   */
  def sizeInBytes: Long = sqlContext.conf.defaultSizeInBytes

  /**
   * Whether does it need to convert the objects in Row to internal representation, for example:
    * 是否需要将Row中的对象转换为内部表示，例如：
   *  java.lang.String -> UTF8String
   *  java.lang.Decimal -> Decimal
   *
   * If `needConversion` is `false`, buildScan() should return an [[RDD]] of [[InternalRow]]
   *
   * Note: The internal representation is not stable across releases and thus data sources outside
   * of Spark SQL should leave this as true.
   *
   * @since 1.4.0
   */
  def needConversion: Boolean = true
}

/**
 * ::DeveloperApi::
 * A BaseRelation that can produce all of its tuples as an RDD of Row objects.
  * 一个BaseRelation，它可以将所有元组作为Row对象的RDD生成
 *
 * @since 1.3.0
 */
@DeveloperApi
trait TableScan {
  def buildScan(): RDD[Row]
}

/**
 * ::DeveloperApi::
 * A BaseRelation that can eliminate unneeded columns before producing an RDD
 * containing all of its tuples as Row objects.
  * 一个BaseRelation它可以在生成包含所有元组作为Row对象的RDD之前消除不需要的列
 *
 * @since 1.3.0
 */
@DeveloperApi
trait PrunedScan {
  def buildScan(requiredColumns: Array[String]): RDD[Row]
}

/**
 * ::DeveloperApi::
 * A BaseRelation that can eliminate unneeded columns and filter using selected
 * predicates before producing an RDD containing all matching tuples as Row objects.
  *
  * BaseRelation可以在生成包含所有匹配元组作为Row对象的RDD之前消除不需要的列并使用所选谓词进行过滤。
 *
 * The actual filter should be the conjunction of all `filters`,
  * 实际的过滤器应该是所有“过滤器”的结合
 * i.e. they should be "and" together.
 *
 * The pushed down filters are currently purely an optimization as they will all be evaluated
 * again.  This means it is safe to use them with methods that produce false positives such
 * as filtering partitions based on a bloom filter.
  * 推下的过滤器目前纯粹是一种优化,因为它们将再次进行评估,这意味着将它们与产生误报的方法一起使用是安全的,
  * 例如基于布隆过滤器过滤分区。
 *
 * @since 1.3.0
 */
@DeveloperApi
trait PrunedFilteredScan {
  def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row]
}

/**
 * ::DeveloperApi::
 * A BaseRelation that can be used to insert data into it through the insert method.
 * If overwrite in insert method is true, the old data in the relation should be overwritten with
 * the new data. If overwrite in insert method is false, the new data should be appended.
  *
  * BaseRelation,可用于通过insert方法将数据插入其中。如果insert方法中的覆盖为true,
  * 则应使用新数据覆盖关系中的旧数据,如果insert方法中的overwrite为false，则应追加新数据。
 *
 * InsertableRelation has the following three assumptions.
  * InsertableRelation有以下三个假设
 * 1. It assumes that the data (Rows in the DataFrame) provided to the insert method
 * exactly matches the ordinal of fields in the schema of the BaseRelation.
  * 它假定提供给insert方法的数据(DataFrame中的Rows)与BaseRelation的模式中的字段序号完全匹配。
 * 2. It assumes that the schema of this relation will not be changed.
 * Even if the insert method updates the schema (e.g. a relation of JSON or Parquet data may have a
 * schema update after an insert operation), the new schema will not be used.
  * 它假定此关系的模式不会更改,即使insert方法更新了模式(例如，JSON或Parquet数据的关系可能在插入操作后具有模式更新),
  * 也不会使用新模式。
 * 3. It assumes that fields of the data provided in the insert method are nullable.
 * If a data source needs to check the actual nullability of a field, it needs to do it in the
 * insert method.
  * 它假设insert方法中提供的数据字段是可空的。如果数据源需要检查字段的实际可为空性，则需要执行insert方法。
 *
 * @since 1.3.0
 */
@DeveloperApi
trait InsertableRelation {
  def insert(data: DataFrame, overwrite: Boolean): Unit
}

/**
 * ::Experimental::
 * An interface for experimenting with a more direct connection to the query planner.  Compared to
 * [[PrunedFilteredScan]], this operator receives the raw expressions from the
 * [[org.apache.spark.sql.catalyst.plans.logical.LogicalPlan]].  Unlike the other APIs this
 * interface is NOT designed to be binary compatible across releases and thus should only be used
 * for experimentation.
  *
  * 用于试验与查询计划程序的更直接连接的界面,与[[PrunedFilteredScan]]相比,
  * 此运算符从[[org.apache.spark.sql.catalyst.plans.logical.LogicalPlan]]接收原始表达式,
  * 与其他API不同,此接口并非设计为跨版本二进制兼容,因此只应用于实验
 *
 * @since 1.3.0
 */
@Experimental
trait CatalystScan {
  def buildScan(requiredColumns: Seq[Attribute], filters: Seq[Expression]): RDD[Row]
}

/**
 * ::Experimental::
 * A factory that produces [[OutputWriter]]s.  A new [[OutputWriterFactory]] is created on driver
 * side for each write job issued when writing to a [[HadoopFsRelation]], and then gets serialized
 * to executor side to create actual [[OutputWriter]]s on the fly.
  *
  * 生成[[OutputWriter]] s的工厂,在驱动程序端为写入[[HadoopFsRelation]]时发出的每个写入作业创建一个新的[[OutputWriterFactory]],
  * 然后序列化到执行程序端以动态创建实际的[[OutputWriter]]。
 *
 * @since 1.4.0
 */
@Experimental
abstract class OutputWriterFactory extends Serializable {
  /**
   * When writing to a [[HadoopFsRelation]], this method gets called by each task on executor side
   * to instantiate new [[OutputWriter]]s.
    * 当写入[[HadoopFsRelation]]时,执行程序端的每个任务都会调用此方法来实例化新的[[OutputWriter]]
   *
   * @param path Path of the file to which this [[OutputWriter]] is supposed to write.  Note that
   *        this may not point to the final output file.  For example, `FileOutputFormat` writes to
   *        temporary directories and then merge written files back to the final destination.  In
   *        this case, `path` points to a temporary output file under the temporary directory.
   * @param dataSchema Schema of the rows to be written. Partition columns are not included in the
   *        schema if the relation being written is partitioned.
   * @param context The Hadoop MapReduce task context.
   *
   * @since 1.4.0
   */
  def newInstance(path: String, dataSchema: StructType, context: TaskAttemptContext): OutputWriter
}

/**
 * ::Experimental::
 * [[OutputWriter]] is used together with [[HadoopFsRelation]] for persisting rows to the
 * underlying file system.  Subclasses of [[OutputWriter]] must provide a zero-argument constructor.
 * An [[OutputWriter]] instance is created and initialized when a new output file is opened on
 * executor side.  This instance is used to persist rows to this single output file.
  * [OutputWriter]]与[[HadoopFsRelation]]一起用于将行保存到底层文件系统,
  * [[OutputWriter]]的子类必须提供零参数构造函数,在执行程序端打开新的输出文件时，
  * 将创建并初始化[[OutputWriter]]实例,此实例用于将行保存到此单个输出文件。
 *
 * @since 1.4.0
 */
@Experimental
abstract class OutputWriter {
  /**
   * Persists a single row.  Invoked on the executor side.  When writing to dynamically partitioned
   * tables, dynamic partition columns are not included in rows to be written.
   * 持久化一行,在执行者一侧调用,写入动态分区表时,动态分区列不包含在要写入的行中
   * @since 1.4.0
   */
  def write(row: Row): Unit

  /**
   * Closes the [[OutputWriter]]. Invoked on the executor side after all rows are persisted, before
   * the task output is committed.
    * 关闭[[OutputWriter]],在关闭所有行之后在执行程序端调用,然后关闭[[OutputWriter]],
    * 在保留所有行之后在执行程序端调用
   *
   * @since 1.4.0
   */
  def close(): Unit

  private var converter: InternalRow => Row = _

  protected[sql] def initConverter(dataSchema: StructType) = {
    converter =
      CatalystTypeConverters.createToScalaConverter(dataSchema).asInstanceOf[InternalRow => Row]
  }

  protected[sql] def writeInternal(row: InternalRow): Unit = {
    write(converter(row))
  }
}

/**
 * ::Experimental::
 * A [[BaseRelation]] that provides much of the common code required for relations that store their
 * data to an HDFS compatible filesystem.
  * 一个[[BaseRelation]],它提供了将数据存储到HDFS兼容文件系统的关系所需的大部分公共代码。
 *
 * For the read path, similar to [[PrunedFilteredScan]], it can eliminate unneeded columns and
 * filter using selected predicates before producing an RDD containing all matching tuples as
 * [[Row]] objects. In addition, when reading from Hive style partitioned tables stored in file
 * systems, it's able to discover partitioning information from the paths of input directories, and
 * perform partition pruning before start reading the data. Subclasses of [[HadoopFsRelation()]]
 * must override one of the three `buildScan` methods to implement the read path.
  *
  * 对于读取路径，类似于[[PrunedFilteredScan]],它可以在生成包含所有匹配元组作为[[Row]]对象的RDD之前,
  * 消除不需要的列并使用选定谓词进行过滤,此外,当从存储在文件系统中的Hive样式分区表中读取时,
  * 它能够从输入目录的路径中发现分区信息,并在开始读取数据之前执行分区修剪。
  * [[HadoopFsRelation（）]]的子类必须覆盖三个`buildScan`方法中的一个来实现读取路径。
 *
 * For the write path, it provides the ability to write to both non-partitioned and partitioned
 * tables.  Directory layout of the partitioned tables is compatible with Hive.
 *
 * @constructor This constructor is for internal uses only. The [[PartitionSpec]] argument is for
 *              implementing metastore table conversion.
 *
 * @param maybePartitionSpec An [[HadoopFsRelation]] can be created with an optional
 *        [[PartitionSpec]], so that partition discovery can be skipped.
 *
 * @since 1.4.0
 */
@Experimental
abstract class HadoopFsRelation private[sql](maybePartitionSpec: Option[PartitionSpec])
  extends BaseRelation with FileRelation with Logging {

  override def toString: String = getClass.getSimpleName + paths.mkString("[", ",", "]")

  def this() = this(None)

  private val hadoopConf = new Configuration(sqlContext.sparkContext.hadoopConfiguration)

  private val codegenEnabled = sqlContext.conf.codegenEnabled

  private var _partitionSpec: PartitionSpec = _

  private class FileStatusCache {
    var leafFiles = mutable.Map.empty[Path, FileStatus]

    var leafDirToChildrenFiles = mutable.Map.empty[Path, Array[FileStatus]]

    private def listLeafFiles(paths: Array[String]): Set[FileStatus] = {
      if (paths.length >= sqlContext.conf.parallelPartitionDiscoveryThreshold) {
        HadoopFsRelation.listLeafFilesInParallel(paths, hadoopConf, sqlContext.sparkContext)
      } else {
        val statuses = paths.flatMap { path =>
          val hdfsPath = new Path(path)
          val fs = hdfsPath.getFileSystem(hadoopConf)
          val qualified = hdfsPath.makeQualified(fs.getUri, fs.getWorkingDirectory)

          logInfo(s"Listing $qualified on driver")
          Try(fs.listStatus(qualified)).getOrElse(Array.empty)
        }.filterNot { status =>
          val name = status.getPath.getName
          name.toLowerCase == "_temporary" || name.startsWith(".")
        }

        val (dirs, files) = statuses.partition(_.isDir)

        if (dirs.isEmpty) {
          files.toSet
        } else {
          files.toSet ++ listLeafFiles(dirs.map(_.getPath.toString))
        }
      }
    }

    def refresh(): Unit = {
      val files = listLeafFiles(paths)

      leafFiles.clear()
      leafDirToChildrenFiles.clear()

      leafFiles ++= files.map(f => f.getPath -> f).toMap
      leafDirToChildrenFiles ++= files.toArray.groupBy(_.getPath.getParent)
    }
  }

  private lazy val fileStatusCache = {
    val cache = new FileStatusCache
    cache.refresh()
    cache
  }

  protected def cachedLeafStatuses(): Set[FileStatus] = {
    fileStatusCache.leafFiles.values.toSet
  }

  final private[sql] def partitionSpec: PartitionSpec = {
    if (_partitionSpec == null) {
      _partitionSpec = maybePartitionSpec
        .flatMap {
          case spec if spec.partitions.nonEmpty =>
            Some(spec.copy(partitionColumns = spec.partitionColumns.asNullable))
          case _ =>
            None
        }
        .orElse {
          // We only know the partition columns and their data types. We need to discover
          // partition values.
          userDefinedPartitionColumns.map { partitionSchema =>
            val spec = discoverPartitions()
            val partitionColumnTypes = spec.partitionColumns.map(_.dataType)
            val castedPartitions = spec.partitions.map { case p @ Partition(values, path) =>
              val literals = partitionColumnTypes.zipWithIndex.map { case (dt, i) =>
                Literal.create(values.get(i, dt), dt)
              }
              val castedValues = partitionSchema.zip(literals).map { case (field, literal) =>
                Cast(literal, field.dataType).eval()
              }
              p.copy(values = InternalRow.fromSeq(castedValues))
            }
            PartitionSpec(partitionSchema, castedPartitions)
          }
        }
        .getOrElse {
          if (sqlContext.conf.partitionDiscoveryEnabled()) {
            discoverPartitions()
          } else {
            PartitionSpec(StructType(Nil), Array.empty[Partition])
          }
        }
    }
    _partitionSpec
  }

  /**
   * Base paths of this relation.  For partitioned relations, it should be either root directories
   * of all partition directories.
   *
   * @since 1.4.0
   */
  def paths: Array[String]

  override def inputFiles: Array[String] = cachedLeafStatuses().map(_.getPath.toString).toArray

  override def sizeInBytes: Long = cachedLeafStatuses().map(_.getLen).sum

  /**
   * Partition columns.  Can be either defined by [[userDefinedPartitionColumns]] or automatically
   * discovered.  Note that they should always be nullable.
    * 分区列,可以由[[userDefinedPartitionColumns]]定义或自动发现,请注意，它们应始终可以为空
   *
   * @since 1.4.0
   */
  final def partitionColumns: StructType =
    userDefinedPartitionColumns.getOrElse(partitionSpec.partitionColumns)

  /**
   * Optional user defined partition columns.
    * 可选的用户定义分区列
   *
   * @since 1.4.0
   */
  def userDefinedPartitionColumns: Option[StructType] = None

  private[sql] def refresh(): Unit = {
    fileStatusCache.refresh()
    if (sqlContext.conf.partitionDiscoveryEnabled()) {
      _partitionSpec = discoverPartitions()
    }
  }

  private def discoverPartitions(): PartitionSpec = {
    val typeInference = sqlContext.conf.partitionColumnTypeInferenceEnabled()
    // We use leaf dirs containing data files to discover the schema.
    //我们使用包含数据文件的叶子目录来发现模式
    val leafDirs = fileStatusCache.leafDirToChildrenFiles.keys.toSeq
    PartitioningUtils.parsePartitions(leafDirs, PartitioningUtils.DEFAULT_PARTITION_NAME,
      typeInference)
  }

  /**
   * Schema of this relation.  It consists of columns appearing in [[dataSchema]] and all partition
   * columns not appearing in [[dataSchema]].
    * 这种关系的模式,它由[[dataSchema]]中出现的列和[[dataSchema]]中未出现的所有分区列组成。
   *
   * @since 1.4.0
   */
  override lazy val schema: StructType = {
    val dataSchemaColumnNames = dataSchema.map(_.name.toLowerCase).toSet
    StructType(dataSchema ++ partitionColumns.filterNot { column =>
      dataSchemaColumnNames.contains(column.name.toLowerCase)
    })
  }

  final private[sql] def buildScan(
      requiredColumns: Array[String],
      filters: Array[Filter],
      inputPaths: Array[String],
      broadcastedConf: Broadcast[SerializableConfiguration]): RDD[Row] = {
    val inputStatuses = inputPaths.flatMap { input =>
      val path = new Path(input)

      // First assumes `input` is a directory path, and tries to get all files contained in it.
      //首先假设`input`是一个目录路径,并尝试获取其中包含的所有文件
      fileStatusCache.leafDirToChildrenFiles.getOrElse(
        path,
        // Otherwise, `input` might be a file path
        fileStatusCache.leafFiles.get(path).toArray
      ).filter { status =>
        val name = status.getPath.getName
        !name.startsWith("_") && !name.startsWith(".")
      }
    }

    buildScan(requiredColumns, filters, inputStatuses, broadcastedConf)
  }

  /**
   * Specifies schema of actual data files.  For partitioned relations, if one or more partitioned
   * columns are contained in the data files, they should also appear in `dataSchema`.
    *
    * 指定实际数据文件的模式,对于分区关系,如果数据文件中包含一个或多个分区列,它们也应出现在`dataSchema`中
   *
   * @since 1.4.0
   */
  def dataSchema: StructType

  /**
   * For a non-partitioned relation, this method builds an `RDD[Row]` containing all rows within
   * this relation. For partitioned relations, this method is called for each selected partition,
   * and builds an `RDD[Row]` containing all rows within that single partition.
    * 对于非分区关系,此方法构建一个`RDD [Row]`,其中包含此关系中的所有行。
    * 对于分区关系,为每个选定的分区调用此方法,并构建一个包含该单个分区内所有行的`RDD [Row]`。
   *
   * @param inputFiles For a non-partitioned relation, it contains paths of all data files in the
   *        relation. For a partitioned relation, it contains paths of all data files in a single
   *        selected partition.
   *
   * @since 1.4.0
   */
  def buildScan(inputFiles: Array[FileStatus]): RDD[Row] = {
    throw new UnsupportedOperationException(
      "At least one buildScan() method should be overridden to read the relation.")
  }

  /**
   * For a non-partitioned relation, this method builds an `RDD[Row]` containing all rows within
   * this relation. For partitioned relations, this method is called for each selected partition,
   * and builds an `RDD[Row]` containing all rows within that single partition.
    * 对于非分区关系,此方法构建一个`RDD [Row]`,其中包含此关系中的所有行。
    * 对于分区关系,为每个选定的分区调用此方法,并构建一个包含该单个分区内所有行的`RDD [Row]`。
   *
   * @param requiredColumns Required columns.
   * @param inputFiles For a non-partitioned relation, it contains paths of all data files in the
   *        relation. For a partitioned relation, it contains paths of all data files in a single
   *        selected partition.
   *
   * @since 1.4.0
   */
  // TODO Tries to eliminate the extra Catalyst-to-Scala conversion when `needConversion` is true
  //
  // PR #7626 separated `Row` and `InternalRow` completely.  One of the consequences is that we can
  // no longer treat an `InternalRow` containing Catalyst values as a `Row`.  Thus we have to
  // introduce another row value conversion for data sources whose `needConversion` is true.
  def buildScan(requiredColumns: Array[String], inputFiles: Array[FileStatus]): RDD[Row] = {
    // Yeah, to workaround serialization...
    val dataSchema = this.dataSchema
    val codegenEnabled = this.codegenEnabled
    val needConversion = this.needConversion

    val requiredOutput = requiredColumns.map { col =>
      val field = dataSchema(col)
      BoundReference(dataSchema.fieldIndex(col), field.dataType, field.nullable)
    }.toSeq

    val rdd: RDD[Row] = buildScan(inputFiles)
    val converted: RDD[InternalRow] =
      if (needConversion) {
        RDDConversions.rowToRowRdd(rdd, dataSchema.fields.map(_.dataType))
      } else {
        rdd.asInstanceOf[RDD[InternalRow]]
      }

    converted.mapPartitions { rows =>
      val buildProjection = if (codegenEnabled) {
        GenerateMutableProjection.generate(requiredOutput, dataSchema.toAttributes)
      } else {
        () => new InterpretedMutableProjection(requiredOutput, dataSchema.toAttributes)
      }

      val projectedRows = {
        val mutableProjection = buildProjection()
        rows.map(r => mutableProjection(r))
      }

      if (needConversion) {
        val requiredSchema = StructType(requiredColumns.map(dataSchema(_)))
        val toScala = CatalystTypeConverters.createToScalaConverter(requiredSchema)
        projectedRows.map(toScala(_).asInstanceOf[Row])
      } else {
        projectedRows
      }
    }.asInstanceOf[RDD[Row]]
  }

  /**
   * For a non-partitioned relation, this method builds an `RDD[Row]` containing all rows within
   * this relation. For partitioned relations, this method is called for each selected partition,
   * and builds an `RDD[Row]` containing all rows within that single partition.
    *
    * 对于非分区关系,此方法构建一个`RDD [Row]`,其中包含此关系中的所有行,对于分区关系,
    * 为每个选定的分区调用此方法,并构建一个包含该单个分区内所有行的`RDD [Row]`。
   *
   * @param requiredColumns Required columns.
   * @param filters Candidate filters to be pushed down. The actual filter should be the conjunction
   *        of all `filters`.  The pushed down filters are currently purely an optimization as they
   *        will all be evaluated again. This means it is safe to use them with methods that produce
   *        false positives such as filtering partitions based on a bloom filter.
   * @param inputFiles For a non-partitioned relation, it contains paths of all data files in the
   *        relation. For a partitioned relation, it contains paths of all data files in a single
   *        selected partition.
   *
   * @since 1.4.0
   */
  def buildScan(
      requiredColumns: Array[String],
      filters: Array[Filter],
      inputFiles: Array[FileStatus]): RDD[Row] = {
    buildScan(requiredColumns, inputFiles)
  }

  /**
   * For a non-partitioned relation, this method builds an `RDD[Row]` containing all rows within
   * this relation. For partitioned relations, this method is called for each selected partition,
   * and builds an `RDD[Row]` containing all rows within that single partition.
    *
   *对于非分区关系,此方法构建一个`RDD [Row]`,其中包含此关系中的所有行。
    * 对于分区关系,为每个选定的分区调用此方法,并构建一个包含该单个分区内所有行的`RDD [Row]`
    *
   * Note: This interface is subject to change in future.
   *
   * @param requiredColumns Required columns.
   * @param filters Candidate filters to be pushed down. The actual filter should be the conjunction
   *        of all `filters`.  The pushed down filters are currently purely an optimization as they
   *        will all be evaluated again. This means it is safe to use them with methods that produce
   *        false positives such as filtering partitions based on a bloom filter.
   * @param inputFiles For a non-partitioned relation, it contains paths of all data files in the
   *        relation. For a partitioned relation, it contains paths of all data files in a single
   *        selected partition.
   * @param broadcastedConf A shared broadcast Hadoop Configuration, which can be used to reduce the
   *                        overhead of broadcasting the Configuration for every Hadoop RDD.
   *
   * @since 1.4.0
   */
  private[sql] def buildScan(
      requiredColumns: Array[String],
      filters: Array[Filter],
      inputFiles: Array[FileStatus],
      broadcastedConf: Broadcast[SerializableConfiguration]): RDD[Row] = {
    buildScan(requiredColumns, filters, inputFiles)
  }

  /**
   * Prepares a write job and returns an [[OutputWriterFactory]].  Client side job preparation can
   * be put here.  For example, user defined output committer can be configured here
   * by setting the output committer class in the conf of spark.sql.sources.outputCommitterClass.
    *
    * 准备写入作业并返回[[OutputWriterFactory]],客户端的工作准备可以放在这里。
    * 例如,可以通过在spark.sql.sources.outputCommitterClass的conf中设置输出提交者类来配置用户定义的输出提交者。
   *
   * Note that the only side effect expected here is mutating `job` via its setters.  Especially,
   * Spark SQL caches [[BaseRelation]] instances for performance, mutating relation internal states
   * may cause unexpected behaviors.
   *
   * @since 1.4.0
   */
  def prepareJobForWrite(job: Job): OutputWriterFactory
}

private[sql] object HadoopFsRelation extends Logging {
  // We don't filter files/directories whose name start with "_" except "_temporary" here, as
  // specific data sources may take advantages over them (e.g. Parquet _metadata and
  // _common_metadata files). "_temporary" directories are explicitly ignored since failed
  // tasks/jobs may leave partial/corrupted data files there.  Files and directories whose name
  // start with "." are also ignored.
  def listLeafFiles(fs: FileSystem, status: FileStatus): Array[FileStatus] = {
    logInfo(s"Listing ${status.getPath}")
    val name = status.getPath.getName.toLowerCase
    if (name == "_temporary" || name.startsWith(".")) {
      Array.empty
    } else {
      val (dirs, files) = fs.listStatus(status.getPath).partition(_.isDir)
      files ++ dirs.flatMap(dir => listLeafFiles(fs, dir))
    }
  }

  // `FileStatus` is Writable but not serializable.  What make it worse, somehow it doesn't play
  // well with `SerializableWritable`.  So there seems to be no way to serialize a `FileStatus`.
  // Here we use `FakeFileStatus` to extract key components of a `FileStatus` to serialize it from
  // executor side and reconstruct it on driver side.
  case class FakeFileStatus(
      path: String,
      length: Long,
      isDir: Boolean,
      blockReplication: Short,
      blockSize: Long,
      modificationTime: Long,
      accessTime: Long)

  def listLeafFilesInParallel(
      paths: Array[String],
      hadoopConf: Configuration,
      sparkContext: SparkContext): Set[FileStatus] = {
    logInfo(s"Listing leaf files and directories in parallel under: ${paths.mkString(", ")}")

    val serializableConfiguration = new SerializableConfiguration(hadoopConf)
    val fakeStatuses = sparkContext.parallelize(paths).flatMap { path =>
      val hdfsPath = new Path(path)
      val fs = hdfsPath.getFileSystem(serializableConfiguration.value)
      val qualified = hdfsPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
      Try(listLeafFiles(fs, fs.getFileStatus(qualified))).getOrElse(Array.empty)
    }.map { status =>
      FakeFileStatus(
        status.getPath.toString,
        status.getLen,
        status.isDir,
        status.getReplication,
        status.getBlockSize,
        status.getModificationTime,
        status.getAccessTime)
    }.collect()

    fakeStatuses.map { f =>
      new FileStatus(
        f.length, f.isDir, f.blockReplication, f.blockSize, f.modificationTime, new Path(f.path))
    }.toSet
  }
}
