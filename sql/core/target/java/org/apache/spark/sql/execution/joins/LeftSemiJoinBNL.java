package org.apache.spark.sql.execution.joins;
/**
 * :: DeveloperApi ::
 * Using BroadcastNestedLoopJoin to calculate left semi join result when there's no join keys
 * for hash join.
 */
public  class LeftSemiJoinBNL extends org.apache.spark.sql.execution.SparkPlan implements org.apache.spark.sql.execution.BinaryNode, scala.Product, scala.Serializable {
  public  org.apache.spark.sql.execution.SparkPlan streamed () { throw new RuntimeException(); }
  public  org.apache.spark.sql.execution.SparkPlan broadcast () { throw new RuntimeException(); }
  public  scala.Option<org.apache.spark.sql.catalyst.expressions.Expression> condition () { throw new RuntimeException(); }
  // not preceding
  public   LeftSemiJoinBNL (org.apache.spark.sql.execution.SparkPlan streamed, org.apache.spark.sql.execution.SparkPlan broadcast, scala.Option<org.apache.spark.sql.catalyst.expressions.Expression> condition) { throw new RuntimeException(); }
  public  scala.collection.immutable.Map<java.lang.String, org.apache.spark.sql.execution.metric.LongSQLMetric> metrics () { throw new RuntimeException(); }
  public  org.apache.spark.sql.catalyst.plans.physical.Partitioning outputPartitioning () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> output () { throw new RuntimeException(); }
  public  boolean outputsUnsafeRows () { throw new RuntimeException(); }
  public  boolean canProcessUnsafeRows () { throw new RuntimeException(); }
  /** The Streamed Relation */
  public  org.apache.spark.sql.execution.SparkPlan left () { throw new RuntimeException(); }
  /** The Broadcast relation */
  public  org.apache.spark.sql.execution.SparkPlan right () { throw new RuntimeException(); }
  private  scala.Function1<org.apache.spark.sql.catalyst.InternalRow, java.lang.Object> boundCondition () { throw new RuntimeException(); }
  protected  org.apache.spark.rdd.RDD<org.apache.spark.sql.catalyst.InternalRow> doExecute () { throw new RuntimeException(); }
}
