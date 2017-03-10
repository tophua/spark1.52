package org.apache.spark.sql.execution.datasources.parquet;
/**
 +---+---+
 | _1| _2|
 +---+---+
 |  1|  1|
 |  2|  2|
 |  3|  3|
 |  4|  4|
 |  5|  5|
 |  6|  6|
 |  7|  7|
 |  8|  8|
 |  9|  9|
 | 10| 10|
 +---+---+*/
public  class JobCommitFailureParquetOutputCommitter extends org.apache.parquet.hadoop.ParquetOutputCommitter {
  public   JobCommitFailureParquetOutputCommitter (org.apache.hadoop.fs.Path outputPath, org.apache.hadoop.mapreduce.TaskAttemptContext context) { throw new RuntimeException(); }
  public  void commitJob (org.apache.hadoop.mapreduce.JobContext jobContext) { throw new RuntimeException(); }
}
