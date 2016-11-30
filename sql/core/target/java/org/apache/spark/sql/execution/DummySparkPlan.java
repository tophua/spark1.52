package org.apache.spark.sql.execution;
public  class DummySparkPlan extends org.apache.spark.sql.execution.SparkPlan implements scala.Product, scala.Serializable {
  public  scala.collection.Seq<org.apache.spark.sql.execution.SparkPlan> children () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.SortOrder> outputOrdering () { throw new RuntimeException(); }
  public  org.apache.spark.sql.catalyst.plans.physical.Partitioning outputPartitioning () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.plans.physical.Distribution> requiredChildDistribution () { throw new RuntimeException(); }
  public  scala.collection.Seq<scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.SortOrder>> requiredChildOrdering () { throw new RuntimeException(); }
  // not preceding
  public   DummySparkPlan (scala.collection.Seq<org.apache.spark.sql.execution.SparkPlan> children, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.SortOrder> outputOrdering, org.apache.spark.sql.catalyst.plans.physical.Partitioning outputPartitioning, scala.collection.Seq<org.apache.spark.sql.catalyst.plans.physical.Distribution> requiredChildDistribution, scala.collection.Seq<scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.SortOrder>> requiredChildOrdering) { throw new RuntimeException(); }
  protected  org.apache.spark.rdd.RDD<org.apache.spark.sql.catalyst.InternalRow> doExecute () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> output () { throw new RuntimeException(); }
}
