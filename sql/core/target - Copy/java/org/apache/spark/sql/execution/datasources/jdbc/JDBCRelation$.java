package org.apache.spark.sql.execution.datasources.jdbc;
// no position
  class JDBCRelation$ implements scala.Serializable {
  /**
   * Static reference to the singleton instance of this Scala object.
   */
  public static final JDBCRelation$ MODULE$ = null;
  public   JDBCRelation$ () { throw new RuntimeException(); }
  /**
   * Given a partitioning schematic (a column of integral type, a number of
   * partitions, and upper and lower bounds on the column's value), generate
   * WHERE clauses for each partition so that each row in the table appears
   * exactly once.  The parameters minValue and maxValue are advisory in that
   * incorrect values may cause the partitioning to be poor, but no data
   * will fail to be represented.
   * @param partitioning (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.Partition[] columnPartition (org.apache.spark.sql.execution.datasources.jdbc.JDBCPartitioningInfo partitioning) { throw new RuntimeException(); }
}
