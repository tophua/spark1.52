package org.apache.spark.sql.execution;
/**
 * :: DeveloperApi ::
 * Sample the dataset.
 * param:  lowerBound Lower-bound of the sampling probability (usually 0.0)
 * param:  upperBound Upper-bound of the sampling probability. The expected fraction sampled
 *                   will be ub - lb.
 * param:  withReplacement Whether to sample with replacement.
 * param:  seed the random seed
 * param:  child the QueryPlan
 */
public  class Sample extends org.apache.spark.sql.execution.SparkPlan implements org.apache.spark.sql.execution.UnaryNode, scala.Product, scala.Serializable {
  public  double lowerBound () { throw new RuntimeException(); }
  public  double upperBound () { throw new RuntimeException(); }
  public  boolean withReplacement () { throw new RuntimeException(); }
  public  long seed () { throw new RuntimeException(); }
  public  org.apache.spark.sql.execution.SparkPlan child () { throw new RuntimeException(); }
  // not preceding
  public   Sample (double lowerBound, double upperBound, boolean withReplacement, long seed, org.apache.spark.sql.execution.SparkPlan child) { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> output () { throw new RuntimeException(); }
  public  boolean outputsUnsafeRows () { throw new RuntimeException(); }
  public  boolean canProcessUnsafeRows () { throw new RuntimeException(); }
  public  boolean canProcessSafeRows () { throw new RuntimeException(); }
  protected  org.apache.spark.rdd.RDD<org.apache.spark.sql.catalyst.InternalRow> doExecute () { throw new RuntimeException(); }
}
