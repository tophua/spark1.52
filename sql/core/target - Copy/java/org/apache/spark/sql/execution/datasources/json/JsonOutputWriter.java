package org.apache.spark.sql.execution.datasources.json;
  class JsonOutputWriter extends org.apache.spark.sql.sources.OutputWriter implements org.apache.spark.mapred.SparkHadoopMapRedUtil, org.apache.spark.Logging {
  public   JsonOutputWriter (java.lang.String path, org.apache.spark.sql.types.StructType dataSchema, org.apache.hadoop.mapreduce.TaskAttemptContext context) { throw new RuntimeException(); }
  public  java.io.CharArrayWriter writer () { throw new RuntimeException(); }
  public  com.fasterxml.jackson.core.JsonGenerator gen () { throw new RuntimeException(); }
  public  org.apache.hadoop.io.Text result () { throw new RuntimeException(); }
  private  org.apache.hadoop.mapreduce.RecordWriter<org.apache.hadoop.io.NullWritable, org.apache.hadoop.io.Text> recordWriter () { throw new RuntimeException(); }
  public  void write (org.apache.spark.sql.Row row) { throw new RuntimeException(); }
  protected  void writeInternal (org.apache.spark.sql.catalyst.InternalRow row) { throw new RuntimeException(); }
  public  void close () { throw new RuntimeException(); }
}
