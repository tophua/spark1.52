package org.apache.spark.sql.execution.datasources;
/**
 * A writer that writes all of the rows in a partition to a single file.
 */
  class DefaultWriterContainer extends org.apache.spark.sql.execution.datasources.BaseWriterContainer {
  public   DefaultWriterContainer (org.apache.spark.sql.sources.HadoopFsRelation relation, org.apache.hadoop.mapreduce.Job job, boolean isAppend) { throw new RuntimeException(); }
  public  void writeRows (org.apache.spark.TaskContext taskContext, scala.collection.Iterator<org.apache.spark.sql.catalyst.InternalRow> iterator) { throw new RuntimeException(); }
}
