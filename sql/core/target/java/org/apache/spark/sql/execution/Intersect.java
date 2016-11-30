package org.apache.spark.sql.execution;
/**
 * :: DeveloperApi ::
 * Returns the rows in left that also appear in right using the built in spark
 * intersection function.
 */
public  class Intersect extends org.apache.spark.sql.execution.SparkPlan implements org.apache.spark.sql.execution.BinaryNode, scala.Product, scala.Serializable {
  public  org.apache.spark.sql.execution.SparkPlan left () { throw new RuntimeException(); }
  public  org.apache.spark.sql.execution.SparkPlan right () { throw new RuntimeException(); }
  // not preceding
  public   Intersect (org.apache.spark.sql.execution.SparkPlan left, org.apache.spark.sql.execution.SparkPlan right) { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> output () { throw new RuntimeException(); }
  protected  org.apache.spark.rdd.RDD<org.apache.spark.sql.catalyst.InternalRow> doExecute () { throw new RuntimeException(); }
}
