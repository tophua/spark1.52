package org.apache.spark.sql.execution;
/**
 * :: DeveloperApi ::
 * Applies a {@link Generator} to a stream of input rows, combining the
 * output of each into a new stream of rows.  This operation is similar to a <code>flatMap</code> in functional
 * programming with one important additional feature, which allows the input rows to be joined with
 * their output.
 * param:  generator the generator expression
 * param:  join  when true, each output row is implicitly joined with the input tuple that produced
 *              it.
 * param:  outer when true, each input row will be output at least once, even if the output of the
 *              given <code>generator</code> is empty. <code>outer</code> has no effect when <code>join</code> is false.
 * param:  output the output attributes of this node, which constructed in analysis phase,
 *               and we can not change it, as the parent node bound with it already.
 */
public  class Generate extends org.apache.spark.sql.execution.SparkPlan implements org.apache.spark.sql.execution.UnaryNode, scala.Product, scala.Serializable {
  public  org.apache.spark.sql.catalyst.expressions.Generator generator () { throw new RuntimeException(); }
  public  boolean join () { throw new RuntimeException(); }
  public  boolean outer () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> output () { throw new RuntimeException(); }
  public  org.apache.spark.sql.execution.SparkPlan child () { throw new RuntimeException(); }
  // not preceding
  public   Generate (org.apache.spark.sql.catalyst.expressions.Generator generator, boolean join, boolean outer, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> output, org.apache.spark.sql.execution.SparkPlan child) { throw new RuntimeException(); }
  public  org.apache.spark.sql.catalyst.expressions.Generator boundGenerator () { throw new RuntimeException(); }
  protected  org.apache.spark.rdd.RDD<org.apache.spark.sql.catalyst.InternalRow> doExecute () { throw new RuntimeException(); }
}
