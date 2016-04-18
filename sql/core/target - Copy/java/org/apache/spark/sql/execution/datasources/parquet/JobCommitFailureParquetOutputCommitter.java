package org.apache.spark.sql.execution.datasources.parquet;
public  class JobCommitFailureParquetOutputCommitter extends org.apache.parquet.hadoop.ParquetOutputCommitter {
  public   JobCommitFailureParquetOutputCommitter (org.apache.hadoop.fs.Path outputPath, org.apache.hadoop.mapreduce.TaskAttemptContext context) { throw new RuntimeException(); }
  public  void commitJob (org.apache.hadoop.mapreduce.JobContext jobContext) { throw new RuntimeException(); }
}
