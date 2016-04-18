package org.apache.spark.sql.execution.datasources.parquet;
/**
 * An output committer for writing Parquet files.  In stead of writing to the <code>_temporary</code> folder
 * like what {@link ParquetOutputCommitter} does, this output committer writes data directly to the
 * destination folder.  This can be useful for data stored in S3, where directory operations are
 * relatively expensive.
 * <p>
 * To enable this output committer, users may set the "spark.sql.parquet.output.committer.class"
 * property via Hadoop {@link Configuration}.  Not that this property overrides
 * "spark.sql.sources.outputCommitterClass".
 * <p>
 * *NOTE*
 * <p>
 *   NEVER use {@link DirectParquetOutputCommitter} when appending data, because currently there's
 *   no safe way undo a failed appending job (that's why both <code>abortTask()</code> and <code>abortJob()</code> are
 *   left * empty).
 */
  class DirectParquetOutputCommitter extends org.apache.parquet.hadoop.ParquetOutputCommitter {
  public   DirectParquetOutputCommitter (org.apache.hadoop.fs.Path outputPath, org.apache.hadoop.mapreduce.TaskAttemptContext context) { throw new RuntimeException(); }
  public  org.apache.parquet.Log LOG () { throw new RuntimeException(); }
  public  org.apache.hadoop.fs.Path getWorkPath () { throw new RuntimeException(); }
  public  void abortTask (org.apache.hadoop.mapreduce.TaskAttemptContext taskContext) { throw new RuntimeException(); }
  public  void commitTask (org.apache.hadoop.mapreduce.TaskAttemptContext taskContext) { throw new RuntimeException(); }
  public  boolean needsTaskCommit (org.apache.hadoop.mapreduce.TaskAttemptContext taskContext) { throw new RuntimeException(); }
  public  void setupJob (org.apache.hadoop.mapreduce.JobContext jobContext) { throw new RuntimeException(); }
  public  void setupTask (org.apache.hadoop.mapreduce.TaskAttemptContext taskContext) { throw new RuntimeException(); }
  public  void commitJob (org.apache.hadoop.mapreduce.JobContext jobContext) { throw new RuntimeException(); }
}
