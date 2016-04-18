package org.apache.spark.sql.execution;
/** Physical plan node for scanning data from an RDD. */
  class PhysicalRDD extends org.apache.spark.sql.execution.SparkPlan implements org.apache.spark.sql.execution.LeafNode, scala.Product, scala.Serializable {
  static public  org.apache.spark.sql.execution.PhysicalRDD createFromDataSource (scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> output, org.apache.spark.rdd.RDD<org.apache.spark.sql.catalyst.InternalRow> rdd, org.apache.spark.sql.sources.BaseRelation relation) { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> output () { throw new RuntimeException(); }
  public  org.apache.spark.rdd.RDD<org.apache.spark.sql.catalyst.InternalRow> rdd () { throw new RuntimeException(); }
  public  java.lang.String extraInformation () { throw new RuntimeException(); }
  // not preceding
  public   PhysicalRDD (scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> output, org.apache.spark.rdd.RDD<org.apache.spark.sql.catalyst.InternalRow> rdd, java.lang.String extraInformation) { throw new RuntimeException(); }
  protected  org.apache.spark.rdd.RDD<org.apache.spark.sql.catalyst.InternalRow> doExecute () { throw new RuntimeException(); }
  public  java.lang.String simpleString () { throw new RuntimeException(); }
}
