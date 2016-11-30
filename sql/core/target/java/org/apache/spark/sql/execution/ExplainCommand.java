package org.apache.spark.sql.execution;
/**
 * An explain command for users to see how a command will be executed.
 * <p>
 * Note that this command takes in a logical plan, runs the optimizer on the logical plan
 * (but do NOT actually execute it).
 * <p>
 * :: DeveloperApi ::
 */
public  class ExplainCommand extends org.apache.spark.sql.catalyst.plans.logical.LogicalPlan implements org.apache.spark.sql.execution.RunnableCommand, scala.Product, scala.Serializable {
  public  org.apache.spark.sql.catalyst.plans.logical.LogicalPlan logicalPlan () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> output () { throw new RuntimeException(); }
  public  boolean extended () { throw new RuntimeException(); }
  // not preceding
  public   ExplainCommand (org.apache.spark.sql.catalyst.plans.logical.LogicalPlan logicalPlan, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> output, boolean extended) { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.Row> run (org.apache.spark.sql.SQLContext sqlContext) { throw new RuntimeException(); }
}
