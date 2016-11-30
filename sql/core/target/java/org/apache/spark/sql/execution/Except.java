package org.apache.spark.sql.execution;
/**
 * :: DeveloperApi ::
 * Returns a table with the elements from left that are not in right using
 * the built-in spark subtract function.
 */
public  class Except extends org.apache.spark.sql.execution.SparkPlan implements org.apache.spark.sql.execution.BinaryNode, scala.Product, scala.Serializable {
  public  org.apache.spark.sql.execution.SparkPlan left () { throw new RuntimeException(); }
  public  org.apache.spark.sql.execution.SparkPlan right () { throw new RuntimeException(); }
  // not preceding
  public   Except (org.apache.spark.sql.execution.SparkPlan left, org.apache.spark.sql.execution.SparkPlan right) { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> output () { throw new RuntimeException(); }
  protected  org.apache.spark.rdd.RDD<org.apache.spark.sql.catalyst.InternalRow> doExecute () { throw new RuntimeException(); }
}
