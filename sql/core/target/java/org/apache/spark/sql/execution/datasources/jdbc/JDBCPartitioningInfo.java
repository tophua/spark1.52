package org.apache.spark.sql.execution.datasources.jdbc;
/**
 * Instructions on how to partition the table among workers.
 */
  class JDBCPartitioningInfo implements scala.Product, scala.Serializable {
  public  java.lang.String column () { throw new RuntimeException(); }
  public  long lowerBound () { throw new RuntimeException(); }
  public  long upperBound () { throw new RuntimeException(); }
  public  int numPartitions () { throw new RuntimeException(); }
  // not preceding
  public   JDBCPartitioningInfo (java.lang.String column, long lowerBound, long upperBound, int numPartitions) { throw new RuntimeException(); }
}
