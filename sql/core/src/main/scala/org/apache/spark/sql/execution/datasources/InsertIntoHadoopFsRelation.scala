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

import java.io.IOException

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.{RunnableCommand, SQLExecution}
import org.apache.spark.sql.sources._
import org.apache.spark.util.Utils


/**
 * A command for writing data to a [[HadoopFsRelation]].  Supports both overwriting and appending.
 * Writing to dynamic partitions is also supported.  Each [[InsertIntoHadoopFsRelation]] issues a
 * single write job, and owns a UUID that identifies this job.  Each concrete implementation of
 * [[HadoopFsRelation]] should use this UUID together with task id to generate unique file path for
 * each task output file.  This UUID is passed to executor side via a property named
 * `spark.sql.sources.writeJobUUID`.
  *
  * 用于将数据写入[[HadoopFsRelation]]的命令,支持覆盖和追加, 还支持写入动态分区,
  * 每个[[InsertIntoHadoopFsRelation]]发出一个写作业,并拥有一个标识此作业的UUID,
  * [[HadoopFsRelation]]的每个具体实现都应该将此UUID与任务ID一起使用,以为每个任务输出文件生成唯一的文件路径,
  * 此UUID通过名为`spark.sql.sources.writeJobUUID`的属性传递给执行程序端
 *
 * Different writer containers, [[DefaultWriterContainer]] and [[DynamicPartitionWriterContainer]]
 * are used to write to normal tables and tables with dynamic partitions.
  * 使用不同的编写器容器[[DefaultWriterContainer]]和[[DynamicPartitionWriterContainer]]来写入具有动态分区的普通表和表
 *
 * Basic work flow of this command is:
  * 该命令的基本工作流程是:
 *
 *   1. Driver side setup, including output committer initialization and data source specific
 *      preparation work for the write job to be issued.
  *      驱动程序端设置,包括输出提交程序初始化和要发布的写入作业的数据源特定准备工作
 *   2. Issues a write job consists of one or more executor side tasks, each of which writes all
 *      rows within an RDD partition.
  *      发出写入作业的问题包括一个或多个执行程序端任务,每个任务都写入RDD分区中的所有行
 *   3. If no exception is thrown in a task, commits that task, otherwise aborts that task;  If any
 *      exception is thrown during task commitment, also aborts that task.
  *      如果任务中没有抛出异常,则提交该任务,否则中止该任务; 如果在任务承诺期间抛出任何异常,也会中止该任务。
 *   4. If all tasks are committed, commit the job, otherwise aborts the job;  If any exception is
 *      thrown during job commitment, also aborts the job.
  *      如果所有任务都已提交,则提交作业,否则中止作业;如果在工作承诺期间抛出任何异常,也会中止该工作。
 */
private[sql] case class InsertIntoHadoopFsRelation(
    @transient relation: HadoopFsRelation,
    @transient query: LogicalPlan,
    mode: SaveMode)
  extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    require(
      relation.paths.length == 1,
      s"Cannot write to multiple destinations: ${relation.paths.mkString(",")}")

    val hadoopConf = sqlContext.sparkContext.hadoopConfiguration
    val outputPath = new Path(relation.paths.head)
    val fs = outputPath.getFileSystem(hadoopConf)
    val qualifiedOutputPath = outputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)

    val pathExists = fs.exists(qualifiedOutputPath)
    val doInsertion = (mode, pathExists) match {
      case (SaveMode.ErrorIfExists, true) =>
        throw new AnalysisException(s"path $qualifiedOutputPath already exists.")
      case (SaveMode.Overwrite, true) =>
        Utils.tryOrIOException {
          if (!fs.delete(qualifiedOutputPath, true /* recursively */)) {
            throw new IOException(s"Unable to clear output " +
              s"directory $qualifiedOutputPath prior to writing to it")
          }
        }
        true
      case (SaveMode.Append, _) | (SaveMode.Overwrite, _) | (SaveMode.ErrorIfExists, false) =>
        true
      case (SaveMode.Ignore, exists) =>
        !exists
      case (s, exists) =>
        throw new IllegalStateException(s"unsupported save mode $s ($exists)")
    }
    // If we are appending data to an existing dir.
    //如果我们要将数据附加到现有目录
    val isAppend = pathExists && (mode == SaveMode.Append)

    if (doInsertion) {
      val job = new Job(hadoopConf)
      job.setOutputKeyClass(classOf[Void])
      job.setOutputValueClass(classOf[InternalRow])
      FileOutputFormat.setOutputPath(job, qualifiedOutputPath)

      // A partitioned relation schema's can be different from the input logicalPlan, since
      // partition columns are all moved after data column. We Project to adjust the ordering.
      //分区关系模式可以与输入logicalPlan不同,因为分区列都在数据列之后移动,我们计划调整排序
      // TODO: this belongs in the analyzer.
      val project = Project(
        relation.schema.map(field => UnresolvedAttribute.quoted(field.name)), query)
      val queryExecution = DataFrame(sqlContext, project).queryExecution

      SQLExecution.withNewExecutionId(sqlContext, queryExecution) {
        val df = sqlContext.internalCreateDataFrame(queryExecution.toRdd, relation.schema)
        val partitionColumns = relation.partitionColumns.fieldNames

        // Some pre-flight checks.
        require(
          df.schema == relation.schema,
          s"""DataFrame must have the same schema as the relation to which is inserted.
             |DataFrame schema: ${df.schema}
             |Relation schema: ${relation.schema}
          """.stripMargin)
        val partitionColumnsInSpec = relation.partitionColumns.fieldNames
        require(
          partitionColumnsInSpec.sameElements(partitionColumns),
          s"""Partition columns mismatch.
             |Expected: ${partitionColumnsInSpec.mkString(", ")}
             |Actual: ${partitionColumns.mkString(", ")}
          """.stripMargin)

        val writerContainer = if (partitionColumns.isEmpty) {
          new DefaultWriterContainer(relation, job, isAppend)
        } else {
          val output = df.queryExecution.executedPlan.output
          val (partitionOutput, dataOutput) =
            output.partition(a => partitionColumns.contains(a.name))

          new DynamicPartitionWriterContainer(
            relation,
            job,
            partitionOutput,
            dataOutput,
            output,
            PartitioningUtils.DEFAULT_PARTITION_NAME,
            sqlContext.conf.getConf(SQLConf.PARTITION_MAX_FILES),
            isAppend)
        }

        // This call shouldn't be put into the `try` block below because it only initializes and
        // prepares the job, any exception thrown from here shouldn't cause abortJob() to be called.
        //这个调用不应该放在下面的`try`块中,因为它只是初始化和准备作业,
        //从这里抛出的任何异常都不应该导致调用abortJob（）
        writerContainer.driverSideSetup()

        try {
          sqlContext.sparkContext.runJob(df.queryExecution.toRdd, writerContainer.writeRows _)
          writerContainer.commitJob()
          relation.refresh()
        } catch { case cause: Throwable =>
          logError("Aborting job.", cause)
          writerContainer.abortJob()
          throw new SparkException("Job aborted.", cause)
        }
      }
    } else {
      logInfo("Skipping insertion into a relation that already exists.")
    }

    Seq.empty[Row]
  }
}
