package org.apache.spark.sql.execution.datasources;
/**
 * A writer that dynamically opens files based on the given partition columns.  Internally this is
 * done by maintaining a HashMap of open files until <code>maxFiles</code> is reached.  If this occurs, the
 * writer externally sorts the remaining rows and then writes out them out one file at a time.
 */
  class DynamicPartitionWriterContainer extends org.apache.spark.sql.execution.datasources.BaseWriterContainer {
  public   DynamicPartitionWriterContainer (org.apache.spark.sql.sources.HadoopFsRelation relation, org.apache.hadoop.mapreduce.Job job, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> partitionColumns, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> dataColumns, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> inputSchema, java.lang.String defaultPartitionName, int maxOpenFiles, boolean isAppend) { throw new RuntimeException(); }
  public  void writeRows (org.apache.spark.TaskContext taskContext, scala.collection.Iterator<org.apache.spark.sql.catalyst.InternalRow> iterator) { throw new RuntimeException(); }
}
