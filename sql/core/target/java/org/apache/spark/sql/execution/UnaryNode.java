package org.apache.spark.sql.execution;
  interface UnaryNode {
  public  org.apache.spark.sql.execution.SparkPlan child () ;
  public  scala.collection.Seq<org.apache.spark.sql.execution.SparkPlan> children () ;
  public  org.apache.spark.sql.catalyst.plans.physical.Partitioning outputPartitioning () ;
}
