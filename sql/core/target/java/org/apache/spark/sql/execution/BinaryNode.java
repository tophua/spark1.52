package org.apache.spark.sql.execution;
  interface BinaryNode {
  public  org.apache.spark.sql.execution.SparkPlan left () ;
  public  org.apache.spark.sql.execution.SparkPlan right () ;
  public  scala.collection.Seq<org.apache.spark.sql.execution.SparkPlan> children () ;
}
