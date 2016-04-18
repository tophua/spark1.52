package org.apache.spark.sql.sources;
public  class SimpleScan extends org.apache.spark.sql.sources.BaseRelation implements org.apache.spark.sql.sources.TableScan, scala.Product, scala.Serializable {
  public  int from () { throw new RuntimeException(); }
  public  int to () { throw new RuntimeException(); }
  public  org.apache.spark.sql.SQLContext sqlContext () { throw new RuntimeException(); }
  // not preceding
  public   SimpleScan (int from, int to, org.apache.spark.sql.SQLContext sqlContext) { throw new RuntimeException(); }
  public  org.apache.spark.sql.types.StructType schema () { throw new RuntimeException(); }
  public  org.apache.spark.rdd.RDD<org.apache.spark.sql.Row> buildScan () { throw new RuntimeException(); }
}
