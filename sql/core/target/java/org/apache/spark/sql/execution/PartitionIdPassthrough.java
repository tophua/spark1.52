package org.apache.spark.sql.execution;
/**
 * A dummy partitioner for use with records whose partition ids have been pre-computed (i.e. for
 * use on RDDs of (Int, Row) pairs where the Int is a partition id in the expected range).
 */
public  class PartitionIdPassthrough extends org.apache.spark.Partitioner {
  public  int numPartitions () { throw new RuntimeException(); }
  // not preceding
  public   PartitionIdPassthrough (int numPartitions) { throw new RuntimeException(); }
  public  int getPartition (Object key) { throw new RuntimeException(); }
}
