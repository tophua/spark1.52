package org.apache.spark.sql.execution.datasources;
  class PartitionSpec implements scala.Product, scala.Serializable {
  static public  org.apache.spark.sql.execution.datasources.PartitionSpec emptySpec () { throw new RuntimeException(); }
  public  org.apache.spark.sql.types.StructType partitionColumns () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.execution.datasources.Partition> partitions () { throw new RuntimeException(); }
  // not preceding
  public   PartitionSpec (org.apache.spark.sql.types.StructType partitionColumns, scala.collection.Seq<org.apache.spark.sql.execution.datasources.Partition> partitions) { throw new RuntimeException(); }
}
