package org.apache.spark.sql.execution.datasources.parquet;
public  class TaskCommitFailureParquetOutputCommitter extends org.apache.parquet.hadoop.ParquetOutputCommitter {
  public   TaskCommitFailureParquetOutputCommitter (org.apache.hadoop.fs.Path outputPath, org.apache.hadoop.mapreduce.TaskAttemptContext context) { throw new RuntimeException(); }
  public  void commitTask (org.apache.hadoop.mapreduce.TaskAttemptContext context) { throw new RuntimeException(); }
}
