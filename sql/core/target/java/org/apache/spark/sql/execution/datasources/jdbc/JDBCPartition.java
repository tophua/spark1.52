package org.apache.spark.sql.execution.datasources.jdbc;
/**
 * Data corresponding to one partition of a JDBCRDD.
 */
  class JDBCPartition implements org.apache.spark.Partition, scala.Product, scala.Serializable {
  public  java.lang.String whereClause () { throw new RuntimeException(); }
  public  int idx () { throw new RuntimeException(); }
  // not preceding
  public   JDBCPartition (java.lang.String whereClause, int idx) { throw new RuntimeException(); }
  public  int index () { throw new RuntimeException(); }
}
