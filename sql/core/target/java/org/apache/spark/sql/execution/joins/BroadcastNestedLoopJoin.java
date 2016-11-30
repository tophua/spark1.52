package org.apache.spark.sql.execution.joins;
/**
 * :: DeveloperApi ::
 */
public  class BroadcastNestedLoopJoin extends org.apache.spark.sql.execution.SparkPlan implements org.apache.spark.sql.execution.BinaryNode, scala.Product, scala.Serializable {
  public  org.apache.spark.sql.execution.SparkPlan left () { throw new RuntimeException(); }
  public  org.apache.spark.sql.execution.SparkPlan right () { throw new RuntimeException(); }
  public  org.apache.spark.sql.execution.joins.BuildSide buildSide () { throw new RuntimeException(); }
  public  org.apache.spark.sql.catalyst.plans.JoinType joinType () { throw new RuntimeException(); }
  public  scala.Option<org.apache.spark.sql.catalyst.expressions.Expression> condition () { throw new RuntimeException(); }
  // not preceding
  public   BroadcastNestedLoopJoin (org.apache.spark.sql.execution.SparkPlan left, org.apache.spark.sql.execution.SparkPlan right, org.apache.spark.sql.execution.joins.BuildSide buildSide, org.apache.spark.sql.catalyst.plans.JoinType joinType, scala.Option<org.apache.spark.sql.catalyst.expressions.Expression> condition) { throw new RuntimeException(); }
  public  scala.collection.immutable.Map<java.lang.String, org.apache.spark.sql.execution.metric.LongSQLMetric> metrics () { throw new RuntimeException(); }
  /** BuildRight means the right relation <=&gt; the broadcast relation. */
  private  org.apache.spark.sql.execution.SparkPlan streamed () { throw new RuntimeException(); }
  private  org.apache.spark.sql.execution.SparkPlan broadcast () { throw new RuntimeException(); }
  public  boolean outputsUnsafeRows () { throw new RuntimeException(); }
  public  boolean canProcessUnsafeRows () { throw new RuntimeException(); }
  private  scala.Function1<org.apache.spark.sql.catalyst.InternalRow, org.apache.spark.sql.catalyst.InternalRow> genResultProjection () { throw new RuntimeException(); }
  public  org.apache.spark.sql.catalyst.plans.physical.Partitioning outputPartitioning () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> output () { throw new RuntimeException(); }
  private  scala.Function1<org.apache.spark.sql.catalyst.InternalRow, java.lang.Object> boundCondition () { throw new RuntimeException(); }
  protected  org.apache.spark.rdd.RDD<org.apache.spark.sql.catalyst.InternalRow> doExecute () { throw new RuntimeException(); }
}
