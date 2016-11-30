package org.apache.spark.sql.execution;
/**
 * :: DeveloperApi ::
 * Return a new RDD that has exactly <code>numPartitions</code> partitions.
 */
public  class Repartition extends org.apache.spark.sql.execution.SparkPlan implements org.apache.spark.sql.execution.UnaryNode, scala.Product, scala.Serializable {
  public  int numPartitions () { throw new RuntimeException(); }
  public  boolean shuffle () { throw new RuntimeException(); }
  public  org.apache.spark.sql.execution.SparkPlan child () { throw new RuntimeException(); }
  // not preceding
  public   Repartition (int numPartitions, boolean shuffle, org.apache.spark.sql.execution.SparkPlan child) { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> output () { throw new RuntimeException(); }
  public  org.apache.spark.sql.catalyst.plans.physical.Partitioning outputPartitioning () { throw new RuntimeException(); }
  protected  org.apache.spark.rdd.RDD<org.apache.spark.sql.catalyst.InternalRow> doExecute () { throw new RuntimeException(); }
}
