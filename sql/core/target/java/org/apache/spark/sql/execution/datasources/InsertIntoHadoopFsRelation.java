package org.apache.spark.sql.execution.datasources;
/**
 * A command for writing data to a {@link HadoopFsRelation}.  Supports both overwriting and appending.
 * Writing to dynamic partitions is also supported.  Each {@link InsertIntoHadoopFsRelation} issues a
 * single write job, and owns a UUID that identifies this job.  Each concrete implementation of
 * {@link HadoopFsRelation} should use this UUID together with task id to generate unique file path for
 * each task output file.  This UUID is passed to executor side via a property named
 * <code>spark.sql.sources.writeJobUUID</code>.
 * <p>
 * Different writer containers, {@link DefaultWriterContainer} and {@link DynamicPartitionWriterContainer}
 * are used to write to normal tables and tables with dynamic partitions.
 * <p>
 * Basic work flow of this command is:
 * <p>
 *   1. Driver side setup, including output committer initialization and data source specific
 *      preparation work for the write job to be issued.
 *   2. Issues a write job consists of one or more executor side tasks, each of which writes all
 *      rows within an RDD partition.
 *   3. If no exception is thrown in a task, commits that task, otherwise aborts that task;  If any
 *      exception is thrown during task commitment, also aborts that task.
 *   4. If all tasks are committed, commit the job, otherwise aborts the job;  If any exception is
 *      thrown during job commitment, also aborts the job.
 */
  class InsertIntoHadoopFsRelation extends org.apache.spark.sql.catalyst.plans.logical.LogicalPlan implements org.apache.spark.sql.execution.RunnableCommand, scala.Product, scala.Serializable {
  public  org.apache.spark.sql.sources.HadoopFsRelation relation () { throw new RuntimeException(); }
  public  org.apache.spark.sql.catalyst.plans.logical.LogicalPlan query () { throw new RuntimeException(); }
  public  org.apache.spark.sql.SaveMode mode () { throw new RuntimeException(); }
  // not preceding
  public   InsertIntoHadoopFsRelation (org.apache.spark.sql.sources.HadoopFsRelation relation, org.apache.spark.sql.catalyst.plans.logical.LogicalPlan query, org.apache.spark.sql.SaveMode mode) { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.Row> run (org.apache.spark.sql.SQLContext sqlContext) { throw new RuntimeException(); }
}
