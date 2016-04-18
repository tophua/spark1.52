package org.apache.spark.sql.execution;
/**
 * :: DeveloperApi ::
 */
public  class Union extends org.apache.spark.sql.execution.SparkPlan implements scala.Product, scala.Serializable {
  public  scala.collection.Seq<org.apache.spark.sql.execution.SparkPlan> children () { throw new RuntimeException(); }
  // not preceding
  public   Union (scala.collection.Seq<org.apache.spark.sql.execution.SparkPlan> children) { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> output () { throw new RuntimeException(); }
  public  boolean outputsUnsafeRows () { throw new RuntimeException(); }
  public  boolean canProcessUnsafeRows () { throw new RuntimeException(); }
  public  boolean canProcessSafeRows () { throw new RuntimeException(); }
  protected  org.apache.spark.rdd.RDD<org.apache.spark.sql.catalyst.InternalRow> doExecute () { throw new RuntimeException(); }
}
