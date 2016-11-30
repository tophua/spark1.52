package org.apache.spark.sql.execution;
/** Logical plan node for scanning data from an RDD. */
  class LogicalRDD extends org.apache.spark.sql.catalyst.plans.logical.LogicalPlan implements org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation, scala.Product, scala.Serializable {
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> output () { throw new RuntimeException(); }
  public  org.apache.spark.rdd.RDD<org.apache.spark.sql.catalyst.InternalRow> rdd () { throw new RuntimeException(); }
  // not preceding
  public   LogicalRDD (scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> output, org.apache.spark.rdd.RDD<org.apache.spark.sql.catalyst.InternalRow> rdd, org.apache.spark.sql.SQLContext sqlContext) { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.plans.logical.LogicalPlan> children () { throw new RuntimeException(); }
  public  org.apache.spark.sql.execution.LogicalRDD newInstance () { throw new RuntimeException(); }
  public  boolean sameResult (org.apache.spark.sql.catalyst.plans.logical.LogicalPlan plan) { throw new RuntimeException(); }
  public  org.apache.spark.sql.catalyst.plans.logical.Statistics statistics () { throw new RuntimeException(); }
}
