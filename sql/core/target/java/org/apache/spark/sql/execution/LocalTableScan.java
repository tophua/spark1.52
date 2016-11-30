package org.apache.spark.sql.execution;
/**
 * Physical plan node for scanning data from a local collection.
 */
  class LocalTableScan extends org.apache.spark.sql.execution.SparkPlan implements org.apache.spark.sql.execution.LeafNode, scala.Product, scala.Serializable {
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> output () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.InternalRow> rows () { throw new RuntimeException(); }
  // not preceding
  public   LocalTableScan (scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> output, scala.collection.Seq<org.apache.spark.sql.catalyst.InternalRow> rows) { throw new RuntimeException(); }
  private  org.apache.spark.rdd.RDD<org.apache.spark.sql.catalyst.InternalRow> rdd () { throw new RuntimeException(); }
  protected  org.apache.spark.rdd.RDD<org.apache.spark.sql.catalyst.InternalRow> doExecute () { throw new RuntimeException(); }
  public  org.apache.spark.sql.Row[] executeCollect () { throw new RuntimeException(); }
  public  org.apache.spark.sql.Row[] executeTake (int limit) { throw new RuntimeException(); }
}
