package org.apache.spark.sql.execution.datasources.jdbc;
  class JDBCRelation extends org.apache.spark.sql.sources.BaseRelation implements org.apache.spark.sql.sources.PrunedFilteredScan, org.apache.spark.sql.sources.InsertableRelation, scala.Product, scala.Serializable {
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
  static public  org.apache.spark.Partition[] columnPartition (org.apache.spark.sql.execution.datasources.jdbc.JDBCPartitioningInfo partitioning) { throw new RuntimeException(); }
  public  java.lang.String url () { throw new RuntimeException(); }
  public  java.lang.String table () { throw new RuntimeException(); }
  public  org.apache.spark.Partition[] parts () { throw new RuntimeException(); }
  public  java.util.Properties properties () { throw new RuntimeException(); }
  public  org.apache.spark.sql.SQLContext sqlContext () { throw new RuntimeException(); }
  // not preceding
  public   JDBCRelation (java.lang.String url, java.lang.String table, org.apache.spark.Partition[] parts, java.util.Properties properties, org.apache.spark.sql.SQLContext sqlContext) { throw new RuntimeException(); }
  public  boolean needConversion () { throw new RuntimeException(); }
  public  org.apache.spark.sql.types.StructType schema () { throw new RuntimeException(); }
  public  org.apache.spark.rdd.RDD<org.apache.spark.sql.Row> buildScan (java.lang.String[] requiredColumns, org.apache.spark.sql.sources.Filter[] filters) { throw new RuntimeException(); }
  public  void insert (org.apache.spark.sql.DataFrame data, boolean overwrite) { throw new RuntimeException(); }
}
