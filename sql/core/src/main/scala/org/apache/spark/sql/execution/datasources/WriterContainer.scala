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

import java.util.{Date, UUID}

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.{FileOutputCommitter => MapReduceFileOutputCommitter}
import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.mapred.SparkHadoopMapRedUtil
import org.apache.spark.mapreduce.SparkHadoopMapReduceUtil
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.UnsafeKVExternalSorter
import org.apache.spark.sql.sources.{HadoopFsRelation, OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.types.{StructType, StringType}
import org.apache.spark.util.SerializableConfiguration


private[sql] abstract class BaseWriterContainer(
    @transient val relation: HadoopFsRelation,
    @transient job: Job,
    isAppend: Boolean)
  extends SparkHadoopMapReduceUtil
  with Logging
  with Serializable {

  protected val dataSchema = relation.dataSchema

  protected val serializableConf = new SerializableConfiguration(job.getConfiguration)

  // This UUID is used to avoid output file name collision between different appending write jobs.
  //此UUID用于避免不同的附加写入作业之间的输出文件名冲突
  // These jobs may belong to different SparkContext instances. Concrete data source implementations
  // may use this UUID to generate unique file names (e.g., `part-r-<task-id>-<job-uuid>.parquet`).
  //这些作业可能属于不同的SparkContext实例,具体的数据源实现可以使用该UUID来生成唯一的文件名
  //(例如，`part-r- <task-id> - <job-uuid> .parquet`)
  //  The reason why this ID is used to identify a job rather than a single task output file is
  // that, speculative tasks must generate the same output file name as the original task.
  //此ID用于标识作业而不是单个任务输出文件的原因是,推测任务必须生成与原始任务相同的输出文件名
  private val uniqueWriteJobId = UUID.randomUUID()

  // This is only used on driver side.
  //这仅用于driver
  @transient private val jobContext: JobContext = job

  private val speculationEnabled: Boolean =
    relation.sqlContext.sparkContext.conf.getBoolean("spark.speculation", defaultValue = false)

  // The following fields are initialized and used on both driver and executor side.
  //初始化以下字段并在驱动程序和执行程序端使用
  @transient protected var outputCommitter: OutputCommitter = _
  @transient private var jobId: JobID = _
  @transient private var taskId: TaskID = _
  @transient private var taskAttemptId: TaskAttemptID = _
  @transient protected var taskAttemptContext: TaskAttemptContext = _

  protected val outputPath: String = {
    assert(
      relation.paths.length == 1,
      s"Cannot write to multiple destinations: ${relation.paths.mkString(",")}")
    relation.paths.head
  }

  protected var outputWriterFactory: OutputWriterFactory = _

  private var outputFormatClass: Class[_ <: OutputFormat[_, _]] = _

  def writeRows(taskContext: TaskContext, iterator: Iterator[InternalRow]): Unit

  def driverSideSetup(): Unit = {
    setupIDs(0, 0, 0)
    setupConf()

    // This UUID is sent to executor side together with the serialized `Configuration` object within
    // the `Job` instance.  `OutputWriters` on the executor side should use this UUID to generate
    // unique task output files.
    //此UUID与`Job`实例中的序列化`Configuration`对象一起发送到执行程序端,
    //执行程序端的`OutputWriters`应使用此UUID生成唯一的任务输出文件。
    job.getConfiguration.set("spark.sql.sources.writeJobUUID", uniqueWriteJobId.toString)

    // Order of the following two lines is important.  For Hadoop 1, TaskAttemptContext constructor
    // clones the Configuration object passed in.  If we initialize the TaskAttemptContext first,
    // configurations made in prepareJobForWrite(job) are not populated into the TaskAttemptContext.
    //以下两行的顺序很重要,对于Hadoop 1,TaskAttemptContext构造函数克隆传入的Configuration对象。
    //如果我们首先初始化TaskAttemptContext,则prepareAobForWrite(job)中的配置不会填充到TaskAttemptContext中
    //
    // Also, the `prepareJobForWrite` call must happen before initializing output format and output
    // committer, since their initialization involve the job configuration, which can be potentially
    // decorated in `prepareJobForWrite`.
    //此外,`prepareJobForWrite`调用必须在初始化输出格式和输出提交者之前进行,
    //因为它们的初始化涉及作业配置,可以在`prepareJobForWrite`中进行装饰。
    outputWriterFactory = relation.prepareJobForWrite(job)
    taskAttemptContext = newTaskAttemptContext(serializableConf.value, taskAttemptId)

    outputFormatClass = job.getOutputFormatClass
    outputCommitter = newOutputCommitter(taskAttemptContext)
    outputCommitter.setupJob(jobContext)
  }

  def executorSideSetup(taskContext: TaskContext): Unit = {
    setupIDs(taskContext.stageId(), taskContext.partitionId(), taskContext.attemptNumber())
    setupConf()
    taskAttemptContext = newTaskAttemptContext(serializableConf.value, taskAttemptId)
    outputCommitter = newOutputCommitter(taskAttemptContext)
    outputCommitter.setupTask(taskAttemptContext)
  }

  protected def getWorkPath: String = {
    outputCommitter match {
      // FileOutputCommitter writes to a temporary location returned by `getWorkPath`.
        //FileOutputCommitter写入`getWorkPath`返回的临时位置
      case f: MapReduceFileOutputCommitter => f.getWorkPath.toString
      case _ => outputPath
    }
  }

  protected def newOutputWriter(path: String): OutputWriter = {
    try {
      outputWriterFactory.newInstance(path, dataSchema, taskAttemptContext)
    } catch {
      case e: org.apache.hadoop.fs.FileAlreadyExistsException =>
        if (outputCommitter.isInstanceOf[parquet.DirectParquetOutputCommitter]) {
          // Spark-11382: DirectParquetOutputCommitter is not idempotent, meaning on retry
          // attempts, the task will fail because the output file is created from a prior attempt.
          // This often means the most visible error to the user is misleading. Augment the error
          // to tell the user to look for the actual error.
          throw new SparkException("The output file already exists but this could be due to a " +
            "failure from an earlier attempt. Look through the earlier logs or stage page for " +
            "the first error.\n  File exists error: " + e)
        }
        throw e
    }
  }

  private def newOutputCommitter(context: TaskAttemptContext): OutputCommitter = {
    val defaultOutputCommitter = outputFormatClass.newInstance().getOutputCommitter(context)

    if (isAppend) {
      // If we are appending data to an existing dir, we will only use the output committer
      // associated with the file output format since it is not safe to use a custom
      // committer for appending. For example, in S3, direct parquet output committer may
      // leave partial data in the destination dir when the the appending job fails.
      //如果我们将数据附加到现有目录,我们将仅使用与文件输出格式关联的输出提交者,
      //因为使用自定义提交者进行追加是不安全的,
      // 例如,在S3中,当附加作业失败时，直接镶木地板输出提交者可能会在目标目录中留下部分数据。
      //
      // See SPARK-8578 for more details
      logInfo(
        s"Using default output committer ${defaultOutputCommitter.getClass.getCanonicalName} " +
          "for appending.")
      defaultOutputCommitter
    } else if (speculationEnabled) {
      // When speculation is enabled, it's not safe to use customized output committer classes,
      // especially direct output committers (e.g. `DirectParquetOutputCommitter`).
      //当启用推测时,使用自定义输出提交者类是不安全的,尤其是直接输出提交者（例如`DirectParquetOutputCommitter`）。
      // See SPARK-9899 for more details.
      logInfo(
        s"Using default output committer ${defaultOutputCommitter.getClass.getCanonicalName} " +
          "because spark.speculation is configured to be true.")
      defaultOutputCommitter
    } else {
      val configuration = SparkHadoopUtil.get.getConfigurationFromJobContext(context)
      val committerClass = configuration.getClass(
        SQLConf.OUTPUT_COMMITTER_CLASS.key, null, classOf[OutputCommitter])

      Option(committerClass).map { clazz =>
        logInfo(s"Using user defined output committer class ${clazz.getCanonicalName}")

        // Every output format based on org.apache.hadoop.mapreduce.lib.output.OutputFormat
        // has an associated output committer. To override this output committer,
        // we will first try to use the output committer set in SQLConf.OUTPUT_COMMITTER_CLASS.
        // If a data source needs to override the output committer, it needs to set the
        // output committer in prepareForWrite method.
        if (classOf[MapReduceFileOutputCommitter].isAssignableFrom(clazz)) {
          // The specified output committer is a FileOutputCommitter.
          // So, we will use the FileOutputCommitter-specified constructor.
          val ctor = clazz.getDeclaredConstructor(classOf[Path], classOf[TaskAttemptContext])
          ctor.newInstance(new Path(outputPath), context)
        } else {
          // The specified output committer is just a OutputCommitter.
          // So, we will use the no-argument constructor.
          val ctor = clazz.getDeclaredConstructor()
          ctor.newInstance()
        }
      }.getOrElse {
        // If output committer class is not set, we will use the one associated with the
        // file output format.
        //如果未设置输出提交者类,我们将使用与文件输出格式关联的那个
        logInfo(
          s"Using output committer class ${defaultOutputCommitter.getClass.getCanonicalName}")
        defaultOutputCommitter
      }
    }
  }

  private def setupIDs(jobId: Int, splitId: Int, attemptId: Int): Unit = {
    this.jobId = SparkHadoopWriter.createJobID(new Date, jobId)
    this.taskId = new TaskID(this.jobId, true, splitId)
    this.taskAttemptId = new TaskAttemptID(taskId, attemptId)
  }

  private def setupConf(): Unit = {
    serializableConf.value.set("mapred.job.id", jobId.toString)
    serializableConf.value.set("mapred.tip.id", taskAttemptId.getTaskID.toString)
    serializableConf.value.set("mapred.task.id", taskAttemptId.toString)
    serializableConf.value.setBoolean("mapred.task.is.map", true)
    serializableConf.value.setInt("mapred.task.partition", 0)
  }

  def commitTask(): Unit = {
    SparkHadoopMapRedUtil.commitTask(outputCommitter, taskAttemptContext, jobId.getId, taskId.getId)
  }

  def abortTask(): Unit = {
    if (outputCommitter != null) {
      outputCommitter.abortTask(taskAttemptContext)
    }
    logError(s"Task attempt $taskAttemptId aborted.")
  }

  def commitJob(): Unit = {
    outputCommitter.commitJob(jobContext)
    logInfo(s"Job $jobId committed.")
  }

  def abortJob(): Unit = {
    if (outputCommitter != null) {
      outputCommitter.abortJob(jobContext, JobStatus.State.FAILED)
    }
    logError(s"Job $jobId aborted.")
  }
}

/**
 * A writer that writes all of the rows in a partition to a single file.
  * 将分区中的所有行写入单个文件的编写器
 */
private[sql] class DefaultWriterContainer(
    @transient relation: HadoopFsRelation,
    @transient job: Job,
    isAppend: Boolean)
  extends BaseWriterContainer(relation, job, isAppend) {

  def writeRows(taskContext: TaskContext, iterator: Iterator[InternalRow]): Unit = {
    executorSideSetup(taskContext)
    val configuration = SparkHadoopUtil.get.getConfigurationFromJobContext(taskAttemptContext)
    configuration.set("spark.sql.sources.output.path", outputPath)
    val writer = newOutputWriter(getWorkPath)
    writer.initConverter(dataSchema)

    var writerClosed = false

    // If anything below fails, we should abort the task.
    //如果下面的任何内容失败,我们应该中止任务
    try {
      while (iterator.hasNext) {
        val internalRow = iterator.next()
        writer.writeInternal(internalRow)
      }

      commitTask()
    } catch {
      case cause: Throwable =>
        logError("Aborting task.", cause)
        abortTask()
        throw new SparkException("Task failed while writing rows.", cause)
    }

    def commitTask(): Unit = {
      try {
        assert(writer != null, "OutputWriter instance should have been initialized")
        if (!writerClosed) {
          writer.close()
          writerClosed = true
        }
        super.commitTask()
      } catch {
        case cause: Throwable =>
          // This exception will be handled in `InsertIntoHadoopFsRelation.insert$writeRows`, and
          // will cause `abortTask()` to be invoked.
          //此异常将在`InsertIntoHadoopFsRelation.insert $ writeRows`中处理,并将导致调用`abortTask()`。
          throw new RuntimeException("Failed to commit task", cause)
      }
    }

    def abortTask(): Unit = {
      try {
        if (!writerClosed) {
          writer.close()
          writerClosed = true
        }
      } finally {
        super.abortTask()
      }
    }
  }
}

/**
 * A writer that dynamically opens files based on the given partition columns.  Internally this is
 * done by maintaining a HashMap of open files until `maxFiles` is reached.  If this occurs, the
 * writer externally sorts the remaining rows and then writes out them out one file at a time.
  *
  * 一个基于给定分区列动态打开文件的编写器,在内部,这是通过维护打开文件的HashMap直到达到“maxFiles”来完成的。
  * 如果发生这种情况,编写器会对剩余的行进行外部排序,然后一次将它们写出一个文件。
 */
private[sql] class DynamicPartitionWriterContainer(
    @transient relation: HadoopFsRelation,
    @transient job: Job,
    partitionColumns: Seq[Attribute],
    dataColumns: Seq[Attribute],
    inputSchema: Seq[Attribute],
    defaultPartitionName: String,
    maxOpenFiles: Int,
    isAppend: Boolean)
  extends BaseWriterContainer(relation, job, isAppend) {

  def writeRows(taskContext: TaskContext, iterator: Iterator[InternalRow]): Unit = {
    val outputWriters = new java.util.HashMap[InternalRow, OutputWriter]
    executorSideSetup(taskContext)

    var outputWritersCleared = false

    // Returns the partition key given an input row
    //返回给定输入行的分区键
    val getPartitionKey = UnsafeProjection.create(partitionColumns, inputSchema)
    // Returns the data columns to be written given an input row
    //给定输入行返回要写入的数据列
    val getOutputRow = UnsafeProjection.create(dataColumns, inputSchema)

    // Expressions that given a partition key build a string like: col1=val/col2=val/...
    val partitionStringExpression = partitionColumns.zipWithIndex.flatMap { case (c, i) =>
      val escaped =
        ScalaUDF(
          PartitioningUtils.escapePathName _, StringType, Seq(Cast(c, StringType)), Seq(StringType))
      val str = If(IsNull(c), Literal(defaultPartitionName), escaped)
      val partitionName = Literal(c.name + "=") :: str :: Nil
      if (i == 0) partitionName else Literal(Path.SEPARATOR_CHAR.toString) :: partitionName
    }

    // Returns the partition path given a partition key.
    //返回给定分区键的分区路径
    val getPartitionString =
      UnsafeProjection.create(Concat(partitionStringExpression) :: Nil, partitionColumns)

    // If anything below fails, we should abort the task.
    //如果下面的任何内容失败,我们应该中止任务
    try {
      // This will be filled in if we have to fall back on sorting.
      //如果我们不得不依靠排序,这将被填写
      var sorter: UnsafeKVExternalSorter = null
      while (iterator.hasNext && sorter == null) {
        val inputRow = iterator.next()
        val currentKey = getPartitionKey(inputRow)
        var currentWriter = outputWriters.get(currentKey)

        if (currentWriter == null) {
          if (outputWriters.size < maxOpenFiles) {
            currentWriter = newOutputWriter(currentKey)
            outputWriters.put(currentKey.copy(), currentWriter)
            currentWriter.writeInternal(getOutputRow(inputRow))
          } else {
            logInfo(s"Maximum partitions reached, falling back on sorting.")
            sorter = new UnsafeKVExternalSorter(
              StructType.fromAttributes(partitionColumns),
              StructType.fromAttributes(dataColumns),
              SparkEnv.get.blockManager,
              SparkEnv.get.shuffleMemoryManager,
              SparkEnv.get.shuffleMemoryManager.pageSizeBytes)
            sorter.insertKV(currentKey, getOutputRow(inputRow))
          }
        } else {
          currentWriter.writeInternal(getOutputRow(inputRow))
        }
      }

      // If the sorter is not null that means that we reached the maxFiles above and need to finish
      // using external sort.
      //如果分拣机不为空,则意味着我们达到了上面的maxFiles并且需要完成使用外部排序
      if (sorter != null) {
        while (iterator.hasNext) {
          val currentRow = iterator.next()
          sorter.insertKV(getPartitionKey(currentRow), getOutputRow(currentRow))
        }

        logInfo(s"Sorting complete. Writing out partition files one at a time.")

        val sortedIterator = sorter.sortedIterator()
        var currentKey: InternalRow = null
        var currentWriter: OutputWriter = null
        try {
          while (sortedIterator.next()) {
            if (currentKey != sortedIterator.getKey) {
              if (currentWriter != null) {
                currentWriter.close()
              }
              currentKey = sortedIterator.getKey.copy()
              logDebug(s"Writing partition: $currentKey")

              // Either use an existing file from before, or open a new one.
              //使用之前的现有文件,或打开一个新文件
              currentWriter = outputWriters.remove(currentKey)
              if (currentWriter == null) {
                currentWriter = newOutputWriter(currentKey)
              }
            }

            currentWriter.writeInternal(sortedIterator.getValue)
          }
        } finally {
          if (currentWriter != null) { currentWriter.close() }
        }
      }

      commitTask()
    } catch {
      case cause: Throwable =>
        logError("Aborting task.", cause)
        abortTask()
        throw new SparkException("Task failed while writing rows.", cause)
    }

    /** Open and returns a new OutputWriter given a partition key.
      * 给定分区键,打开并返回一个新的OutputWrite*/
    def newOutputWriter(key: InternalRow): OutputWriter = {
      val partitionPath = getPartitionString(key).getString(0)
      val path = new Path(getWorkPath, partitionPath)
      val configuration = SparkHadoopUtil.get.getConfigurationFromJobContext(taskAttemptContext)
      configuration.set(
        "spark.sql.sources.output.path", new Path(outputPath, partitionPath).toString)
      val newWriter = super.newOutputWriter(path.toString)
      newWriter.initConverter(dataSchema)
      newWriter
    }

    def clearOutputWriters(): Unit = {
      if (!outputWritersCleared) {
        outputWriters.asScala.values.foreach(_.close())
        outputWriters.clear()
        outputWritersCleared = true
      }
    }

    def commitTask(): Unit = {
      try {
        clearOutputWriters()
        super.commitTask()
      } catch {
        case cause: Throwable =>
          throw new RuntimeException("Failed to commit task", cause)
      }
    }

    def abortTask(): Unit = {
      try {
        clearOutputWriters()
      } finally {
        super.abortTask()
      }
    }
  }
}
