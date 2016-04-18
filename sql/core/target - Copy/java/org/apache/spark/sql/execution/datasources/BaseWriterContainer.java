package org.apache.spark.sql.execution.datasources;
 abstract class BaseWriterContainer implements org.apache.spark.mapreduce.SparkHadoopMapReduceUtil, org.apache.spark.Logging, scala.Serializable {
  public  org.apache.spark.sql.sources.HadoopFsRelation relation () { throw new RuntimeException(); }
  // not preceding
  public   BaseWriterContainer (org.apache.spark.sql.sources.HadoopFsRelation relation, org.apache.hadoop.mapreduce.Job job, boolean isAppend) { throw new RuntimeException(); }
  protected  org.apache.spark.sql.types.StructType dataSchema () { throw new RuntimeException(); }
  protected  org.apache.spark.util.SerializableConfiguration serializableConf () { throw new RuntimeException(); }
  private  java.util.UUID uniqueWriteJobId () { throw new RuntimeException(); }
  private  org.apache.hadoop.mapreduce.JobContext jobContext () { throw new RuntimeException(); }
  private  boolean speculationEnabled () { throw new RuntimeException(); }
  protected  org.apache.hadoop.mapreduce.OutputCommitter outputCommitter () { throw new RuntimeException(); }
  private  org.apache.hadoop.mapreduce.JobID jobId () { throw new RuntimeException(); }
  private  org.apache.hadoop.mapreduce.TaskID taskId () { throw new RuntimeException(); }
  private  org.apache.hadoop.mapreduce.TaskAttemptID taskAttemptId () { throw new RuntimeException(); }
  protected  org.apache.hadoop.mapreduce.TaskAttemptContext taskAttemptContext () { throw new RuntimeException(); }
  protected  java.lang.String outputPath () { throw new RuntimeException(); }
  protected  org.apache.spark.sql.sources.OutputWriterFactory outputWriterFactory () { throw new RuntimeException(); }
  private  Object outputFormatClass () { throw new RuntimeException(); }
  public abstract  void writeRows (org.apache.spark.TaskContext taskContext, scala.collection.Iterator<org.apache.spark.sql.catalyst.InternalRow> iterator) ;
  public  void driverSideSetup () { throw new RuntimeException(); }
  public  void executorSideSetup (org.apache.spark.TaskContext taskContext) { throw new RuntimeException(); }
  protected  java.lang.String getWorkPath () { throw new RuntimeException(); }
  protected  org.apache.spark.sql.sources.OutputWriter newOutputWriter (java.lang.String path) { throw new RuntimeException(); }
  private  org.apache.hadoop.mapreduce.OutputCommitter newOutputCommitter (org.apache.hadoop.mapreduce.TaskAttemptContext context) { throw new RuntimeException(); }
  private  void setupIDs (int jobId, int splitId, int attemptId) { throw new RuntimeException(); }
  private  void setupConf () { throw new RuntimeException(); }
  public  void commitTask () { throw new RuntimeException(); }
  public  void abortTask () { throw new RuntimeException(); }
  public  void commitJob () { throw new RuntimeException(); }
  public  void abortJob () { throw new RuntimeException(); }
}
