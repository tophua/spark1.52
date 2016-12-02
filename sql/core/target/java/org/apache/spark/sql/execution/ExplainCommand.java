package org.apache.spark.sql.execution;
/**
 * An explain command for users to see how a command will be executed.
 * &#x89e3;&#x91ca;&#x547d;&#x4ee4;&#x7528;&#x6237;&#x67e5;&#x770b;&#x547d;&#x4ee4;&#x5c06;&#x5982;&#x4f55;&#x6267;&#x884c;&#x7684;&#x547d;&#x4ee4;
 * Note that this command takes in a logical plan, runs the optimizer on the logical plan
 * &#x8bf7;&#x6ce8;&#x610f;,&#x8fd9;&#x4e2a;&#x547d;&#x4ee4;&#x9700;&#x8981;&#x4e00;&#x4e2a;&#x903b;&#x8f91;&#x8ba1;&#x5212;,&#x8fd0;&#x884c;&#x903b;&#x8f91;&#x8ba1;&#x5212;&#x7684;&#x4f18;&#x5316;&#x7a0b;&#x5e8f;
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
