package org.apache.spark.sql.execution;
/**
 * Performs a sort, spilling to disk as needed.
 * param:  global when true performs a global sort of all partitions by shuffling the data first
 *               if necessary.
 */
public  class ExternalSort extends org.apache.spark.sql.execution.SparkPlan implements org.apache.spark.sql.execution.UnaryNode, scala.Product, scala.Serializable {
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.SortOrder> sortOrder () { throw new RuntimeException(); }
  public  boolean global () { throw new RuntimeException(); }
  public  org.apache.spark.sql.execution.SparkPlan child () { throw new RuntimeException(); }
  // not preceding
  public   ExternalSort (scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.SortOrder> sortOrder, boolean global, org.apache.spark.sql.execution.SparkPlan child) { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.plans.physical.Distribution> requiredChildDistribution () { throw new RuntimeException(); }
  protected  org.apache.spark.rdd.RDD<org.apache.spark.sql.catalyst.InternalRow> doExecute () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> output () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.SortOrder> outputOrdering () { throw new RuntimeException(); }
}
