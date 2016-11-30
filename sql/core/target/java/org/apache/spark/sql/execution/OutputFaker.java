package org.apache.spark.sql.execution;
/**
 * :: DeveloperApi ::
 * A plan node that does nothing but lie about the output of its child.  Used to spice a
 * (hopefully structurally equivalent) tree from a different optimization sequence into an already
 * resolved tree.
 */
public  class OutputFaker extends org.apache.spark.sql.execution.SparkPlan implements scala.Product, scala.Serializable {
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> output () { throw new RuntimeException(); }
  public  org.apache.spark.sql.execution.SparkPlan child () { throw new RuntimeException(); }
  // not preceding
  public   OutputFaker (scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> output, org.apache.spark.sql.execution.SparkPlan child) { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.execution.SparkPlan> children () { throw new RuntimeException(); }
  protected  org.apache.spark.rdd.RDD<org.apache.spark.sql.catalyst.InternalRow> doExecute () { throw new RuntimeException(); }
}
