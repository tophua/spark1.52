package org.apache.spark.sql.execution;
/**
 * :: DeveloperApi ::
 */
public  class DescribeCommand extends org.apache.spark.sql.catalyst.plans.logical.LogicalPlan implements org.apache.spark.sql.execution.RunnableCommand, scala.Product, scala.Serializable {
  public  org.apache.spark.sql.execution.SparkPlan child () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> output () { throw new RuntimeException(); }
  public  boolean isExtended () { throw new RuntimeException(); }
  // not preceding
  public   DescribeCommand (org.apache.spark.sql.execution.SparkPlan child, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> output, boolean isExtended) { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.Row> run (org.apache.spark.sql.SQLContext sqlContext) { throw new RuntimeException(); }
}
