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

package org.apache.spark.sql.execution.datasources.parquet

import java.net.URI
import java.util.logging.{Logger => JLogger}
import java.util.{List => JList}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.{Failure, Try}

import com.google.common.base.Objects
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.hadoop._
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.ContextUtil
import org.apache.parquet.schema.MessageType
import org.apache.parquet.{Log => ApacheParquetLog}
import org.slf4j.bridge.SLF4JBridgeHandler

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.{RDD, SqlNewHadoopPartition, SqlNewHadoopRDD}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionSpec
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.util.{SerializableConfiguration, Utils}
import org.apache.spark.{Logging, Partition => SparkPartition, SparkException}


private[sql] class DefaultSource extends HadoopFsRelationProvider with DataSourceRegister {

  override def shortName(): String = "parquet"

  override def createRelation(
      sqlContext: SQLContext,
      paths: Array[String],
      schema: Option[StructType],
      partitionColumns: Option[StructType],
      parameters: Map[String, String]): HadoopFsRelation = {
    new ParquetRelation(paths, schema, None, partitionColumns, parameters)(sqlContext)
  }
}

// NOTE: This class is instantiated and used on executor side only, no need to be serializable.
private[sql] class ParquetOutputWriter(path: String, context: TaskAttemptContext)
  extends OutputWriter {

  private val recordWriter: RecordWriter[Void, InternalRow] = {
    val outputFormat = {
      new ParquetOutputFormat[InternalRow]() {
        // Here we override `getDefaultWorkFile` for two reasons:
        // 这里我们覆盖`getDefaultWorkFile`有两个原因：
        //  1. To allow appending.  We need to generate unique output file names to avoid
        //     overwriting existing files (either exist before the write job, or are just written
        //     by other tasks within the same write job).
        //
        //  2. To allow dynamic partitioning.  Default `getDefaultWorkFile` uses
        //     `FileOutputCommitter.getWorkPath()`, which points to the base directory of all
        //     partitions in the case of dynamic partitioning.
        override def getDefaultWorkFile(context: TaskAttemptContext, extension: String): Path = {
          val configuration = SparkHadoopUtil.get.getConfigurationFromJobContext(context)
          val uniqueWriteJobId = configuration.get("spark.sql.sources.writeJobUUID")
          val taskAttemptId = SparkHadoopUtil.get.getTaskAttemptIDFromTaskAttemptContext(context)
          val split = taskAttemptId.getTaskID.getId
          new Path(path, f"part-r-$split%05d-$uniqueWriteJobId$extension")
        }
      }
    }

    outputFormat.getRecordWriter(context)
  }

  override def write(row: Row): Unit = throw new UnsupportedOperationException("call writeInternal")

  override protected[sql] def writeInternal(row: InternalRow): Unit = recordWriter.write(null, row)

  override def close(): Unit = recordWriter.close(context)
}

private[sql] class ParquetRelation(
    override val paths: Array[String],
    private val maybeDataSchema: Option[StructType],
    // This is for metastore conversion.
    private val maybePartitionSpec: Option[PartitionSpec],
    override val userDefinedPartitionColumns: Option[StructType],
    parameters: Map[String, String])(
    val sqlContext: SQLContext)
  extends HadoopFsRelation(maybePartitionSpec)
  with Logging {

  private[sql] def this(
      paths: Array[String],
      maybeDataSchema: Option[StructType],
      maybePartitionSpec: Option[PartitionSpec],
      parameters: Map[String, String])(
      sqlContext: SQLContext) = {
    this(
      paths,
      maybeDataSchema,
      maybePartitionSpec,
      maybePartitionSpec.map(_.partitionColumns),
      parameters)(sqlContext)
  }

  // Should we merge schemas from all Parquet part-files?
  //我们应该合并所有Parquet零件文件中的模式吗？
  private val shouldMergeSchemas =
    parameters
      .get(ParquetRelation.MERGE_SCHEMA)
      .map(_.toBoolean)
      .getOrElse(sqlContext.conf.getConf(SQLConf.PARQUET_SCHEMA_MERGING_ENABLED))

  private val mergeRespectSummaries =
    sqlContext.conf.getConf(SQLConf.PARQUET_SCHEMA_RESPECT_SUMMARIES)

  private val maybeMetastoreSchema = parameters
    .get(ParquetRelation.METASTORE_SCHEMA)
    .map(DataType.fromJson(_).asInstanceOf[StructType])

  private lazy val metadataCache: MetadataCache = {
    val meta = new MetadataCache
    meta.refresh()
    meta
  }

  override def equals(other: Any): Boolean = other match {
    case that: ParquetRelation =>
      val schemaEquality = if (shouldMergeSchemas) {
        this.shouldMergeSchemas == that.shouldMergeSchemas
      } else {
        this.dataSchema == that.dataSchema &&
          this.schema == that.schema
      }

      this.paths.toSet == that.paths.toSet &&
        schemaEquality &&
        this.maybeDataSchema == that.maybeDataSchema &&
        this.partitionColumns == that.partitionColumns

    case _ => false
  }

  override def hashCode(): Int = {
    if (shouldMergeSchemas) {
      Objects.hashCode(
        Boolean.box(shouldMergeSchemas),
        paths.toSet,
        maybeDataSchema,
        partitionColumns)
    } else {
      Objects.hashCode(
        Boolean.box(shouldMergeSchemas),
        paths.toSet,
        dataSchema,
        schema,
        maybeDataSchema,
        partitionColumns)
    }
  }

  /** Constraints on schema of dataframe to be stored.
    * 对要存储的dataframe架构的约束。*/
  private def checkConstraints(schema: StructType): Unit = {
    if (schema.fieldNames.length != schema.fieldNames.distinct.length) {
      val duplicateColumns = schema.fieldNames.groupBy(identity).collect {
        case (x, ys) if ys.length > 1 => "\"" + x + "\""
      }.mkString(", ")
      throw new AnalysisException(s"Duplicate column(s) : $duplicateColumns found, " +
        s"cannot save to parquet format")
    }
  }

  override def dataSchema: StructType = {
    val schema = maybeDataSchema.getOrElse(metadataCache.dataSchema)
    // check if schema satisfies the constraints
    // before moving forward
    //在前进之前检查模式是否满足约束
    checkConstraints(schema)
    schema
  }

  override private[sql] def refresh(): Unit = {
    super.refresh()
    metadataCache.refresh()
  }

  // Parquet data source always uses Catalyst internal representations.
  //Parquet数据源始终使用Catalyst内部表示
  override val needConversion: Boolean = false

  override def sizeInBytes: Long = metadataCache.dataStatuses.map(_.getLen).sum

  override def prepareJobForWrite(job: Job): OutputWriterFactory = {
    val conf = ContextUtil.getConfiguration(job)

    // SPARK-9849 DirectParquetOutputCommitter qualified name should be backward compatible
    val committerClassname = conf.get(SQLConf.PARQUET_OUTPUT_COMMITTER_CLASS.key)
    if (committerClassname == "org.apache.spark.sql.parquet.DirectParquetOutputCommitter") {
      conf.set(SQLConf.PARQUET_OUTPUT_COMMITTER_CLASS.key,
        classOf[DirectParquetOutputCommitter].getCanonicalName)
    }

    val committerClass =
      conf.getClass(
        SQLConf.PARQUET_OUTPUT_COMMITTER_CLASS.key,
        classOf[ParquetOutputCommitter],
        classOf[ParquetOutputCommitter])

    if (conf.get(SQLConf.PARQUET_OUTPUT_COMMITTER_CLASS.key) == null) {
      logInfo("Using default output committer for Parquet: " +
        classOf[ParquetOutputCommitter].getCanonicalName)
    } else {
      logInfo("Using user defined output committer for Parquet: " + committerClass.getCanonicalName)
    }

    conf.setClass(
      SQLConf.OUTPUT_COMMITTER_CLASS.key,
      committerClass,
      classOf[ParquetOutputCommitter])

    // We're not really using `ParquetOutputFormat[Row]` for writing data here, because we override
    // it in `ParquetOutputWriter` to support appending and dynamic partitioning.  The reason why
    // we set it here is to setup the output committer class to `ParquetOutputCommitter`, which is
    // bundled with `ParquetOutputFormat[Row]`.
    job.setOutputFormatClass(classOf[ParquetOutputFormat[Row]])

    // TODO There's no need to use two kinds of WriteSupport
    // We should unify them. `SpecificMutableRow` can process both atomic (primitive) types and
    // complex types.
    //我们应该统一它们,`SpecificMutableRow`可以处理原子(原始)类型和复杂类型。
    val writeSupportClass =
      if (dataSchema.map(_.dataType).forall(ParquetTypesConverter.isPrimitiveType)) {
        classOf[MutableRowWriteSupport]
      } else {
        classOf[RowWriteSupport]
      }

    ParquetOutputFormat.setWriteSupportClass(job, writeSupportClass)
    RowWriteSupport.setSchema(dataSchema.toAttributes, conf)

    // Sets compression scheme 设置压缩方案
    conf.set(
      ParquetOutputFormat.COMPRESSION,
      ParquetRelation
        .shortParquetCompressionCodecNames
        .getOrElse(
          sqlContext.conf.parquetCompressionCodec.toUpperCase,
          CompressionCodecName.UNCOMPRESSED).name())

    new OutputWriterFactory {
      override def newInstance(
          path: String, dataSchema: StructType, context: TaskAttemptContext): OutputWriter = {
        new ParquetOutputWriter(path, context)
      }
    }
  }

  override def buildScan(
      requiredColumns: Array[String],
      filters: Array[Filter],
      inputFiles: Array[FileStatus],
      broadcastedConf: Broadcast[SerializableConfiguration]): RDD[Row] = {
    val useMetadataCache = sqlContext.getConf(SQLConf.PARQUET_CACHE_METADATA)
    val parquetFilterPushDown = sqlContext.conf.parquetFilterPushDown
    val assumeBinaryIsString = sqlContext.conf.isParquetBinaryAsString
    val assumeInt96IsTimestamp = sqlContext.conf.isParquetINT96AsTimestamp
    val followParquetFormatSpec = sqlContext.conf.followParquetFormatSpec

    // When merging schemas is enabled and the column of the given filter does not exist,
    // Parquet emits an exception which is an issue of Parquet (PARQUET-389).
    //当启用合并模式并且不存在给定过滤器的列时,Parquet会发出一个异常,这是Parquet(PARQUET-389)的问题
    val safeParquetFilterPushDown = !shouldMergeSchemas && parquetFilterPushDown

    // Parquet row group size. We will use this value as the value for
    // mapreduce.input.fileinputformat.split.minsize and mapred.min.split.size if the value
    // of these flags are smaller than the parquet row group size.
    //Parquet行组大小,如果这些标志的值小于镶木地板行组大小,
    // 我们将使用此值作为mapreduce.input.fileinputformat.split.minsize和mapred.min.split.size的值
    val parquetBlockSize = ParquetOutputFormat.getLongBlockSize(broadcastedConf.value.value)

    // Create the function to set variable Parquet confs at both driver and executor side.
    //创建函数以在驱动程序和执行程序端设置变量Parquet confs
    val initLocalJobFuncOpt =
      ParquetRelation.initializeLocalJobFunc(
        requiredColumns,
        filters,
        dataSchema,
        parquetBlockSize,
        useMetadataCache,
        safeParquetFilterPushDown,
        assumeBinaryIsString,
        assumeInt96IsTimestamp,
        followParquetFormatSpec) _

    // Create the function to set input paths at the driver side.
    //创建函数以在驱动程序端设置输入路径
    val setInputPaths =
      ParquetRelation.initializeDriverSideJobFunc(inputFiles, parquetBlockSize) _

    Utils.withDummyCallSite(sqlContext.sparkContext) {
      new SqlNewHadoopRDD(
        sc = sqlContext.sparkContext,
        broadcastedConf = broadcastedConf,
        initDriverSideJobFuncOpt = Some(setInputPaths),
        initLocalJobFuncOpt = Some(initLocalJobFuncOpt),
        inputFormatClass = classOf[ParquetInputFormat[InternalRow]],
        valueClass = classOf[InternalRow]) {

        val cacheMetadata = useMetadataCache

        @transient val cachedStatuses = inputFiles.map { f =>
          // In order to encode the authority of a Path containing special characters such as '/'
          // (which does happen in some S3N credentials), we need to use the string returned by the
          // URI of the path to create a new Path.
          val pathWithEscapedAuthority = escapePathUserInfo(f.getPath)
          new FileStatus(
            f.getLen, f.isDir, f.getReplication, f.getBlockSize, f.getModificationTime,
            f.getAccessTime, f.getPermission, f.getOwner, f.getGroup, pathWithEscapedAuthority)
        }.toSeq

        private def escapePathUserInfo(path: Path): Path = {
          val uri = path.toUri
          new Path(new URI(
            uri.getScheme, uri.getRawUserInfo, uri.getHost, uri.getPort, uri.getPath,
            uri.getQuery, uri.getFragment))
        }

        // Overridden so we can inject our own cached files statuses.
        //重写,以便我们可以注入我们自己的缓存文件状态
        override def getPartitions: Array[SparkPartition] = {
          val inputFormat = new ParquetInputFormat[InternalRow] {
            override def listStatus(jobContext: JobContext): JList[FileStatus] = {
              if (cacheMetadata) cachedStatuses else super.listStatus(jobContext)
            }
          }

          val jobContext = newJobContext(getConf(isDriverSide = true), jobId)
          val rawSplits = inputFormat.getSplits(jobContext)

          Array.tabulate[SparkPartition](rawSplits.size) { i =>
            new SqlNewHadoopPartition(id, i, rawSplits(i).asInstanceOf[InputSplit with Writable])
          }
        }
        //类型擦除黑客将RDD [InternalRow]作为RDD [Row]传递
      }.asInstanceOf[RDD[Row]]  // type erasure hack to pass RDD[InternalRow] as RDD[Row]
    }
  }

  private class MetadataCache {
    // `FileStatus` objects of all "_metadata" files.
    //所有“_metadata”文件的`FileStatus`对象
    private var metadataStatuses: Array[FileStatus] = _

    // `FileStatus` objects of all "_common_metadata" files.
    //所有“_common_metadata”文件的`FileStatus`对象
    private var commonMetadataStatuses: Array[FileStatus] = _

    // `FileStatus` objects of all data files (Parquet part-files).
    //`FileStatus`对象的所有数据文件（Parquet部分文件）
    var dataStatuses: Array[FileStatus] = _

    // Schema of the actual Parquet files, without partition columns discovered from partition
    // directory paths.
    //实际Parquet文件的模式,没有从分区目录路径发现的分区列
    var dataSchema: StructType = null

    // Schema of the whole table, including partition columns.
    //整个表的模式,包括分区列
    var schema: StructType = _

    // Cached leaves 缓存的叶子
    var cachedLeaves: Set[FileStatus] = null

    /**
     * Refreshes `FileStatus`es, footers, partition spec, and table schema.
      * 刷新`FileStatus`es，页脚,分区规范和表模式
     */
    def refresh(): Unit = {
      val currentLeafStatuses = cachedLeafStatuses()

      // Check if cachedLeafStatuses is changed or not
      //检查cachedLeafStatuses是否已更改
      val leafStatusesChanged = (cachedLeaves == null) ||
        !cachedLeaves.equals(currentLeafStatuses)

      if (leafStatusesChanged) {
        cachedLeaves = currentLeafStatuses.toIterator.toSet

        // Lists `FileStatus`es of all leaf nodes (files) under all base directories.
        //列出所有基目录下所有叶节点（文件）的`FileStatus`。
        val leaves = currentLeafStatuses.filter { f =>
          isSummaryFile(f.getPath) ||
            !(f.getPath.getName.startsWith("_") || f.getPath.getName.startsWith("."))
        }.toArray

        dataStatuses = leaves.filterNot(f => isSummaryFile(f.getPath))
        metadataStatuses =
          leaves.filter(_.getPath.getName == ParquetFileWriter.PARQUET_METADATA_FILE)
        commonMetadataStatuses =
          leaves.filter(_.getPath.getName == ParquetFileWriter.PARQUET_COMMON_METADATA_FILE)

        dataSchema = {
          val dataSchema0 = maybeDataSchema
            .orElse(readSchema())
            .orElse(maybeMetastoreSchema)
            .getOrElse(throw new AnalysisException(
              s"Failed to discover schema of Parquet file(s) in the following location(s):\n" +
                paths.mkString("\n\t")))

          // If this Parquet relation is converted from a Hive Metastore table, must reconcile case
          // case insensitivity issue and possible schema mismatch (probably caused by schema
          // evolution).
          //如果从Hive Metastore表转换此Parquet关系,则必须协调大小写不敏感问题和可能的模式不匹配(可能由模式演变引起)
          maybeMetastoreSchema
            .map(ParquetRelation.mergeMetastoreParquetSchema(_, dataSchema0))
            .getOrElse(dataSchema0)
        }
      }
    }

    private def isSummaryFile(file: Path): Boolean = {
      file.getName == ParquetFileWriter.PARQUET_COMMON_METADATA_FILE ||
        file.getName == ParquetFileWriter.PARQUET_METADATA_FILE
    }

    private def readSchema(): Option[StructType] = {
      // Sees which file(s) we need to touch in order to figure out the schema.
      //查看我们需要触摸哪些文件以找出架构
      //
      // Always tries the summary files first if users don't require a merged schema.  In this case,
      // "_common_metadata" is more preferable than "_metadata" because it doesn't contain row
      // groups information, and could be much smaller for large Parquet files with lots of row
      // groups.  If no summary file is available, falls back to some random part-file.
      //如果用户不需要合并架构,请始终首先尝试摘要文件,在这种情况下,“_ common_metadata”比“_metadata”更优选,
      //因为它不包含行组信息,对于具有大量行组的大型Parquet文件可能要小得多,如果没有可用的摘要文件,则返回到某个随机零件文件
      //
      // NOTE: Metadata stored in the summary files are merged from all part-files.  However, for
      // user defined key-value metadata (in which we store Spark SQL schema), Parquet doesn't know
      // how to merge them correctly if some key is associated with different values in different
      // part-files.  When this happens, Parquet simply gives up generating the summary file.  This
      // implies that if a summary file presents, then:
      //
      //   1. Either all part-files have exactly the same Spark SQL schema, or
      //   2. Some part-files don't contain Spark SQL schema in the key-value metadata at all (thus
      //      their schemas may differ from each other).
      //
      // Here we tend to be pessimistic and take the second case into account.  Basically this means
      // we can't trust the summary files if users require a merged schema, and must touch all part-
      // files to do the merge.
      //在这里,我们倾向于悲观,并考虑第二种情况,基本上这意味着如果用户需要合并模式,
      //我们就不能信任摘要文件,并且必须触摸所有部分文件才能进行合并
      val filesToTouch =
        if (shouldMergeSchemas) {
          // Also includes summary files, 'cause there might be empty partition directories.
          //还包括摘要文件,因为可能存在空分区目录

          // If mergeRespectSummaries config is true, we assume that all part-files are the same for
          // their schema with summary files, so we ignore them when merging schema.
          //如果mergeRespectSummaries config为true,
          // 我们假设所有部分文件对于带有摘要文件的模式都是相同的,因此我们在合并模式时忽略它们
          // If the config is disabled, which is the default setting, we merge all part-files.
          // In this mode, we only need to merge schemas contained in all those summary files.
          // You should enable this configuration only if you are very sure that for the parquet
          // part-files to read there are corresponding summary files containing correct schema.
          //如果配置被禁用,这是默认设置,我们合并所有part-files,在这种模式下,我们只需要合并所有这些摘要文件中包含的模式。
          //你应该只在你非常确定的时候启用这个配置,要读取的拼花零件文件有相应的包含正确模式的摘要文件

          val needMerged: Seq[FileStatus] =
            if (mergeRespectSummaries) {
              Seq()
            } else {
              dataStatuses
            }
          (metadataStatuses ++ commonMetadataStatuses ++ needMerged).toSeq
        } else {
          // Tries any "_common_metadata" first. Parquet files written by old versions or Parquet
          // don't have this.
          //首先尝试任何“_common_metadata”,旧版本或Parquet编写的镶木地板文件没有这个
          commonMetadataStatuses.headOption
            // Falls back to "_metadata"
            .orElse(metadataStatuses.headOption)
            // Summary file(s) not found, the Parquet file is either corrupted, or different part-
            // files contain conflicting user defined metadata (two or more values are associated
            // with a same key in different files).  In either case, we fall back to any of the
            // first part-file, and just assume all schemas are consistent.
            //未找到摘要文件,Parquet文件已损坏,或者不同的部分文件包含冲突的用户定义元数据(两个或多个值与不同文件中的相同密钥相关联）
            //在任何一种情况下,我们都会回退到第一个部分文件中的任何一个,并假设所有模式都是一致的
            .orElse(dataStatuses.headOption)
            .toSeq
        }

      assert(
        filesToTouch.nonEmpty || maybeDataSchema.isDefined || maybeMetastoreSchema.isDefined,
        "No predefined schema found, " +
          s"and no Parquet data files or summary files found under ${paths.mkString(", ")}.")

      ParquetRelation.mergeSchemasInParallel(filesToTouch, sqlContext)
    }
  }
}

private[sql] object ParquetRelation extends Logging {
  // Whether we should merge schemas collected from all Parquet part-files.
  //我们是否应该合并从所有Parquet部件文件中收集的模式
  private[sql] val MERGE_SCHEMA = "mergeSchema"

  // Hive Metastore schema, used when converting Metastore Parquet tables.  This option is only used
  // internally.
  //转换Metastore Parquet表时使用的Hive Metastore架构,此选项仅在内部使用
  private[sql] val METASTORE_SCHEMA = "metastoreSchema"

  /**
   * If parquet's block size (row group size) setting is larger than the min split size,
   * we use parquet's block size setting as the min split size. Otherwise, we will create
   * tasks processing nothing (because a split does not cover the starting point of a
   * parquet block). See https://issues.apache.org/jira/browse/SPARK-10143 for more information.
   */
  private def overrideMinSplitSize(parquetBlockSize: Long, conf: Configuration): Unit = {
    val minSplitSize =
      math.max(
        conf.getLong("mapred.min.split.size", 0L),
        conf.getLong("mapreduce.input.fileinputformat.split.minsize", 0L))
    if (parquetBlockSize > minSplitSize) {
      val message =
        s"Parquet's block size (row group size) is larger than " +
          s"mapred.min.split.size/mapreduce.input.fileinputformat.split.minsize. Setting " +
          s"mapred.min.split.size and mapreduce.input.fileinputformat.split.minsize to " +
          s"$parquetBlockSize."
      logDebug(message)
      conf.set("mapred.min.split.size", parquetBlockSize.toString)
      conf.set("mapreduce.input.fileinputformat.split.minsize", parquetBlockSize.toString)
    }
  }

  /** This closure sets various Parquet configurations at both driver side and executor side.
    * 该闭合在driver侧和执行者侧设置各种Parquet配置*/
  private[parquet] def initializeLocalJobFunc(
      requiredColumns: Array[String],
      filters: Array[Filter],
      dataSchema: StructType,
      parquetBlockSize: Long,
      useMetadataCache: Boolean,
      parquetFilterPushDown: Boolean,
      assumeBinaryIsString: Boolean,
      assumeInt96IsTimestamp: Boolean,
      followParquetFormatSpec: Boolean)(job: Job): Unit = {
    val conf = job.getConfiguration
    conf.set(ParquetInputFormat.READ_SUPPORT_CLASS, classOf[CatalystReadSupport].getName)

    // Try to push down filters when filter push-down is enabled.
    //启用过滤器下推时,尝试按下过滤器
    if (parquetFilterPushDown) {
      filters
        // Collects all converted Parquet filter predicates. Notice that not all predicates can be
        // converted (`ParquetFilters.createFilter` returns an `Option`). That's why a `flatMap`
        // is used here.
        //收集所有已转换的Parquet过滤器谓词,请注意,并非所有谓词都可以转换（`ParquetFilters.createFilter`返回`Option`）。
        //这就是为什么在这里使用`flatMap`的原因。
        .flatMap(ParquetFilters.createFilter(dataSchema, _))
        .reduceOption(FilterApi.and)
        .foreach(ParquetInputFormat.setFilterPredicate(conf, _))
    }

    conf.set(CatalystReadSupport.SPARK_ROW_REQUESTED_SCHEMA, {
      val requestedSchema = StructType(requiredColumns.map(dataSchema(_)))
      CatalystSchemaConverter.checkFieldNames(requestedSchema).json
    })

    conf.set(
      RowWriteSupport.SPARK_ROW_SCHEMA,
      CatalystSchemaConverter.checkFieldNames(dataSchema).json)

    // Tell FilteringParquetRowInputFormat whether it's okay to cache Parquet and FS metadata
    //告诉FilteringParquetRowInputFormat是否可以缓存Parquet和FS元数据
    conf.setBoolean(SQLConf.PARQUET_CACHE_METADATA.key, useMetadataCache)

    // Sets flags for Parquet schema conversion
    //设置Parquet架构转换的标志
    conf.setBoolean(SQLConf.PARQUET_BINARY_AS_STRING.key, assumeBinaryIsString)
    conf.setBoolean(SQLConf.PARQUET_INT96_AS_TIMESTAMP.key, assumeInt96IsTimestamp)
    conf.setBoolean(SQLConf.PARQUET_FOLLOW_PARQUET_FORMAT_SPEC.key, followParquetFormatSpec)

    overrideMinSplitSize(parquetBlockSize, conf)
  }

  /** This closure sets input paths at the driver side.
    * 该闭包设置driver侧的输入路径。 */
  private[parquet] def initializeDriverSideJobFunc(
      inputFiles: Array[FileStatus],
      parquetBlockSize: Long)(job: Job): Unit = {
    // We side the input paths at the driver side.
    //我们支持driver侧的输入路径
    logInfo(s"Reading Parquet file(s) from ${inputFiles.map(_.getPath).mkString(", ")}")
    if (inputFiles.nonEmpty) {
      FileInputFormat.setInputPaths(job, inputFiles.map(_.getPath): _*)
    }

    overrideMinSplitSize(parquetBlockSize, job.getConfiguration)
  }

  private[parquet] def readSchema(
      footers: Seq[Footer], sqlContext: SQLContext): Option[StructType] = {

    def parseParquetSchema(schema: MessageType): StructType = {
      val converter = new CatalystSchemaConverter(
        sqlContext.conf.isParquetBinaryAsString,
        sqlContext.conf.isParquetBinaryAsString,
        sqlContext.conf.followParquetFormatSpec)

      converter.convert(schema)
    }

    val seen = mutable.HashSet[String]()
    val finalSchemas: Seq[StructType] = footers.flatMap { footer =>
      val metadata = footer.getParquetMetadata.getFileMetaData
      val serializedSchema = metadata
        .getKeyValueMetaData
        .toMap
        .get(CatalystReadSupport.SPARK_METADATA_KEY)
      if (serializedSchema.isEmpty) {
        // Falls back to Parquet schema if no Spark SQL schema found.
        //如果未找到Spark SQL架构,则回退到Parquet架构
        Some(parseParquetSchema(metadata.getSchema))
      } else if (!seen.contains(serializedSchema.get)) {
        seen += serializedSchema.get

        // Don't throw even if we failed to parse the serialized Spark schema. Just fallback to
        // whatever is available.
        //即使我们无法解析序列化的Spark模式,也不要抛出,只是回到可用的任何东西。
        Some(Try(DataType.fromJson(serializedSchema.get))
          .recover { case _: Throwable =>
            logInfo(
              s"Serialized Spark schema in Parquet key-value metadata is not in JSON format, " +
                "falling back to the deprecated DataType.fromCaseClassString parser.")
            DataType.fromCaseClassString(serializedSchema.get)
          }
          .recover { case cause: Throwable =>
            logWarning(
              s"""Failed to parse serialized Spark schema in Parquet key-value metadata:
                 |\t$serializedSchema
               """.stripMargin,
              cause)
          }
          .map(_.asInstanceOf[StructType])
          .getOrElse {
            // Falls back to Parquet schema if Spark SQL schema can't be parsed.
            //如果无法解析Spark SQL架构,则回退到Parquet架构
            parseParquetSchema(metadata.getSchema)
          })
      } else {
        None
      }
    }

    finalSchemas.reduceOption { (left, right) =>
      try left.merge(right) catch { case e: Throwable =>
        throw new SparkException(s"Failed to merge incompatible schemas $left and $right", e)
      }
    }
  }

  /**
   * Reconciles Hive Metastore case insensitivity issue and data type conflicts between Metastore
   * schema and Parquet schema.
    * 协调Hive Metastore不区分大小写问题以及Metastore架构和Parquet架构之间的数据类型冲突
   *
   * Hive doesn't retain case information, while Parquet is case sensitive. On the other hand, the
   * schema read from Parquet files may be incomplete (e.g. older versions of Parquet doesn't
   * distinguish binary and string).  This method generates a correct schema by merging Metastore
   * schema data types and Parquet schema field names.
    *
    * Hive不保留案例信息,而Parquet区分大小写,另一方面,从Parquet文件读取的模式可能不完整(例如,旧版本的Parquet不区分二进制和字符串)。
    * 此方法通过合并Metastore架构数据类型和Parquet架构字段名称来生成正确的架构。
   */
  private[parquet] def mergeMetastoreParquetSchema(
      metastoreSchema: StructType,
      parquetSchema: StructType): StructType = {
    def schemaConflictMessage: String =
      s"""Converting Hive Metastore Parquet, but detected conflicting schemas. Metastore schema:
         |${metastoreSchema.prettyJson}
         |
         |Parquet schema:
         |${parquetSchema.prettyJson}
       """.stripMargin

    val mergedParquetSchema = mergeMissingNullableFields(metastoreSchema, parquetSchema)

    assert(metastoreSchema.size <= mergedParquetSchema.size, schemaConflictMessage)

    val ordinalMap = metastoreSchema.zipWithIndex.map {
      case (field, index) => field.name.toLowerCase -> index
    }.toMap

    val reorderedParquetSchema = mergedParquetSchema.sortBy(f =>
      ordinalMap.getOrElse(f.name.toLowerCase, metastoreSchema.size + 1))

    StructType(metastoreSchema.zip(reorderedParquetSchema).map {
      // Uses Parquet field names but retains Metastore data types.
      //使用Parquet字段名称但保留Metastore数据类型
      case (mSchema, pSchema) if mSchema.name.toLowerCase == pSchema.name.toLowerCase =>
        mSchema.copy(name = pSchema.name)
      case _ =>
        throw new SparkException(schemaConflictMessage)
    })
  }

  /**
   * Returns the original schema from the Parquet file with any missing nullable fields from the
   * Hive Metastore schema merged in.
    * 返回Parquet文件中的原始模式,其中包含Hive Metastore模式中合并的任何缺少的可空字段
   *
   * When constructing a DataFrame from a collection of structured data, the resulting object has
   * a schema corresponding to the union of the fields present in each element of the collection.
   * Spark SQL simply assigns a null value to any field that isn't present for a particular row.
   * In some cases, it is possible that a given table partition stored as a Parquet file doesn't
   * contain a particular nullable field in its schema despite that field being present in the
   * table schema obtained from the Hive Metastore. This method returns a schema representing the
   * Parquet file schema along with any additional nullable fields from the Metastore schema
   * merged in.
   */
  private[parquet] def mergeMissingNullableFields(
      metastoreSchema: StructType,
      parquetSchema: StructType): StructType = {
    val fieldMap = metastoreSchema.map(f => f.name.toLowerCase -> f).toMap
    val missingFields = metastoreSchema
      .map(_.name.toLowerCase)
      .diff(parquetSchema.map(_.name.toLowerCase))
      .map(fieldMap(_))
      .filter(_.nullable)
    StructType(parquetSchema ++ missingFields)
  }

  /**
   * Figures out a merged Parquet schema with a distributed Spark job.
   * 找出合并的Parquet架构与分布式Spark作业
   * Note that locality is not taken into consideration here because:
   * 请注意，此处未考虑局部性，因为：
   *  1. For a single Parquet part-file, in most cases the footer only resides in the last block of
   *     that file.  Thus we only need to retrieve the location of the last block.  However, Hadoop
   *     `FileSystem` only provides API to retrieve locations of all blocks, which can be
   *     potentially expensive.
   *
   *  2. This optimization is mainly useful for S3, where file metadata operations can be pretty
   *     slow.  And basically locality is not available when using S3 (you can't run computation on
   *     S3 nodes).
   */
  def mergeSchemasInParallel(
      filesToTouch: Seq[FileStatus], sqlContext: SQLContext): Option[StructType] = {
    val assumeBinaryIsString = sqlContext.conf.isParquetBinaryAsString
    val assumeInt96IsTimestamp = sqlContext.conf.isParquetINT96AsTimestamp
    val followParquetFormatSpec = sqlContext.conf.followParquetFormatSpec
    val serializedConf = new SerializableConfiguration(sqlContext.sparkContext.hadoopConfiguration)

    // !! HACK ALERT !!
    //
    // Parquet requires `FileStatus`es to read footers.  Here we try to send cached `FileStatus`es
    // to executor side to avoid fetching them again.  However, `FileStatus` is not `Serializable`
    // but only `Writable`.  What makes it worth, for some reason, `FileStatus` doesn't play well
    // with `SerializableWritable[T]` and always causes a weird `IllegalStateException`.  These
    // facts virtually prevents us to serialize `FileStatus`es.
    //
    // Since Parquet only relies on path and length information of those `FileStatus`es to read
    // footers, here we just extract them (which can be easily serialized), send them to executor
    // side, and resemble fake `FileStatus`es there.
    val partialFileStatusInfo = filesToTouch.map(f => (f.getPath.toString, f.getLen))

    // Issues a Spark job to read Parquet schema in parallel.
    //发出Spark作业以并行读取Parquet架构
    val partiallyMergedSchemas =
      sqlContext
        .sparkContext
        .parallelize(partialFileStatusInfo)
        .mapPartitions { iterator =>
          // Resembles fake `FileStatus`es with serialized path and length information.
          //类似于假的`FileStatus`es，带有序列化的路径和长度信息
          val fakeFileStatuses = iterator.map { case (path, length) =>
            new FileStatus(length, false, 0, 0, 0, 0, null, null, null, new Path(path))
          }.toSeq

          // Skips row group information since we only need the schema
          //跳过行组信息,因为我们只需要模式
          val skipRowGroups = true

          // Reads footers in multi-threaded manner within each task
          //在每个任务中以多线程方式读取页脚
          val footers =
            ParquetFileReader.readAllFootersInParallel(
              serializedConf.value, fakeFileStatuses, skipRowGroups)

          // Converter used to convert Parquet `MessageType` to Spark SQL `StructType`
          //用于将Parquet`FessageType`转换为Spark SQL`StructType`的转换器
          val converter =
            new CatalystSchemaConverter(
              assumeBinaryIsString = assumeBinaryIsString,
              assumeInt96IsTimestamp = assumeInt96IsTimestamp,
              followParquetFormatSpec = followParquetFormatSpec)

          footers.map { footer =>
            ParquetRelation.readSchemaFromFooter(footer, converter)
          }.reduceOption(_ merge _).iterator
        }.collect()

    partiallyMergedSchemas.reduceOption(_ merge _)
  }

  /**
   * Reads Spark SQL schema from a Parquet footer.  If a valid serialized Spark SQL schema string
   * can be found in the file metadata, returns the deserialized [[StructType]], otherwise, returns
   * a [[StructType]] converted from the [[MessageType]] stored in this footer.
    * 从Parquet页脚读取Spark SQL架构,如果可以在文件元数据中找到有效的序列化Spark SQL架构字符串,则返回反序列化的[[StructType]],
    * 否则返回从此页脚中存储的[[MessageType]]转换的[[StructType]]。
   */
  def readSchemaFromFooter(
      footer: Footer, converter: CatalystSchemaConverter): StructType = {
    val fileMetaData = footer.getParquetMetadata.getFileMetaData
    fileMetaData
      .getKeyValueMetaData
      .toMap
      .get(CatalystReadSupport.SPARK_METADATA_KEY)
      .flatMap(deserializeSchemaString)
      .getOrElse(converter.convert(fileMetaData.getSchema))
  }

  private def deserializeSchemaString(schemaString: String): Option[StructType] = {
    // Tries to deserialize the schema string as JSON first, then falls back to the case class
    // string parser (data generated by older versions of Spark SQL uses this format).
    //尝试首先将模式字符串反序列化为JSON,然后返回到案例类字符串解析器(旧版本的Spark SQL生成的数据使用此格式)
    Try(DataType.fromJson(schemaString).asInstanceOf[StructType]).recover {
      case _: Throwable =>
        logInfo(
          s"Serialized Spark schema in Parquet key-value metadata is not in JSON format, " +
            "falling back to the deprecated DataType.fromCaseClassString parser.")
        DataType.fromCaseClassString(schemaString).asInstanceOf[StructType]
    }.recoverWith {
      case cause: Throwable =>
        logWarning(
          "Failed to parse and ignored serialized Spark schema in " +
            s"Parquet key-value metadata:\n\t$schemaString", cause)
        Failure(cause)
    }.toOption
  }

  // JUL loggers must be held by a strong reference, otherwise they may get destroyed by GC.
  //JUL记录器必须由强引用保留,否则它们可能会被GC破坏
  // However, the root JUL logger used by Parquet isn't properly referenced.  Here we keep
  // references to loggers in both parquet-mr <= 1.6 and >= 1.7
  //但是,Parquet使用的根JUL记录器未正确引用,这里我们在timber-mr <= 1.6和> = 1.7中保留对记录器的引用
  val apacheParquetLogger: JLogger = JLogger.getLogger(classOf[ApacheParquetLog].getPackage.getName)
  val parquetLogger: JLogger = JLogger.getLogger("parquet")

  // Parquet initializes its own JUL logger in a static block which always prints to stdout.  Here
  // we redirect the JUL logger via SLF4J JUL bridge handler.
  //Parquet在静态块中初始化自己的JUL记录器,该块始终打印到stdout,在这里,我们通过SLF4J JUL桥接处理程序重定向JUL记录器
  val redirectParquetLogsViaSLF4J: Unit = {
    def redirect(logger: JLogger): Unit = {
      logger.getHandlers.foreach(logger.removeHandler)
      logger.setUseParentHandlers(false)
      logger.addHandler(new SLF4JBridgeHandler)
    }

    // For parquet-mr 1.7.0 and above versions, which are under `org.apache.parquet` namespace.
    // scalastyle:off classforname
    Class.forName(classOf[ApacheParquetLog].getName)
    // scalastyle:on classforname
    redirect(JLogger.getLogger(classOf[ApacheParquetLog].getPackage.getName))

    // For parquet-mr 1.6.0 and lower versions bundled with Hive, which are under `parquet`
    // namespace.
    try {
      // scalastyle:off classforname
      Class.forName("parquet.Log")
      // scalastyle:on classforname
      redirect(JLogger.getLogger("parquet"))
    } catch { case _: Throwable =>
      // SPARK-9974: com.twitter:parquet-hadoop-bundle:1.6.0 is not packaged into the assembly jar
      // when Spark is built with SBT. So `parquet.Log` may not be found.  This try/catch block
      // should be removed after this issue is fixed.
    }
  }

  // The parquet compression short names
  val shortParquetCompressionCodecNames = Map(
    "NONE"         -> CompressionCodecName.UNCOMPRESSED,
    "UNCOMPRESSED" -> CompressionCodecName.UNCOMPRESSED,
    "SNAPPY"       -> CompressionCodecName.SNAPPY,
    "GZIP"         -> CompressionCodecName.GZIP,
    "LZO"          -> CompressionCodecName.LZO)
}
