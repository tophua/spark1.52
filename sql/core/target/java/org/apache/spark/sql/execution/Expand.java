package org.apache.spark.sql.execution;
/**
 * Apply the all of the GroupExpressions to every input row, hence we will get
 * multiple output rows for a input row.
 * param:  projections The group of expressions, all of the group expressions should
 *                    output the same schema specified bye the parameter <code>output</code>
 * param:  output      The output Schema
 * param:  child       Child operator
 */
public  class Expand extends org.apache.spark.sql.execution.SparkPlan implements org.apache.spark.sql.execution.UnaryNode, scala.Product, scala.Serializable {
  public  scala.collection.Seq<scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression>> projections () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> output () { throw new RuntimeException(); }
  public  org.apache.spark.sql.execution.SparkPlan child () { throw new RuntimeException(); }
  // not preceding
  public   Expand (scala.collection.Seq<scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression>> projections, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> output, org.apache.spark.sql.execution.SparkPlan child) { throw new RuntimeException(); }
  public  org.apache.spark.sql.catalyst.plans.physical.Partitioning outputPartitioning () { throw new RuntimeException(); }
  protected  org.apache.spark.rdd.RDD<org.apache.spark.sql.catalyst.InternalRow> doExecute () { throw new RuntimeException(); }
}
