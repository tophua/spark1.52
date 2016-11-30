package org.apache.spark.sql.execution;
/**
 * :: DeveloperApi ::
 */
public  class CacheTableCommand extends org.apache.spark.sql.catalyst.plans.logical.LogicalPlan implements org.apache.spark.sql.execution.RunnableCommand, scala.Product, scala.Serializable {
  public  java.lang.String tableName () { throw new RuntimeException(); }
  public  scala.Option<org.apache.spark.sql.catalyst.plans.logical.LogicalPlan> plan () { throw new RuntimeException(); }
  public  boolean isLazy () { throw new RuntimeException(); }
  // not preceding
  public   CacheTableCommand (java.lang.String tableName, scala.Option<org.apache.spark.sql.catalyst.plans.logical.LogicalPlan> plan, boolean isLazy) { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.Row> run (org.apache.spark.sql.SQLContext sqlContext) { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> output () { throw new RuntimeException(); }
}
