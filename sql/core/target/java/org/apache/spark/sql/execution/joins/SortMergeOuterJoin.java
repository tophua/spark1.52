package org.apache.spark.sql.execution.joins;
/**
 * :: DeveloperApi ::
 * Performs an sort merge outer join of two child relations.
 * <p>
 * Note: this does not support full outer join yet; see SPARK-9730 for progress on this.
 */
public  class SortMergeOuterJoin extends org.apache.spark.sql.execution.SparkPlan implements org.apache.spark.sql.execution.BinaryNode, scala.Product, scala.Serializable {
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression> leftKeys () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression> rightKeys () { throw new RuntimeException(); }
  public  org.apache.spark.sql.catalyst.plans.JoinType joinType () { throw new RuntimeException(); }
  public  scala.Option<org.apache.spark.sql.catalyst.expressions.Expression> condition () { throw new RuntimeException(); }
  public  org.apache.spark.sql.execution.SparkPlan left () { throw new RuntimeException(); }
  public  org.apache.spark.sql.execution.SparkPlan right () { throw new RuntimeException(); }
  // not preceding
  public   SortMergeOuterJoin (scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression> leftKeys, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression> rightKeys, org.apache.spark.sql.catalyst.plans.JoinType joinType, scala.Option<org.apache.spark.sql.catalyst.expressions.Expression> condition, org.apache.spark.sql.execution.SparkPlan left, org.apache.spark.sql.execution.SparkPlan right) { throw new RuntimeException(); }
  public  scala.collection.immutable.Map<java.lang.String, org.apache.spark.sql.execution.metric.LongSQLMetric> metrics () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> output () { throw new RuntimeException(); }
  public  org.apache.spark.sql.catalyst.plans.physical.Partitioning outputPartitioning () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.SortOrder> outputOrdering () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.plans.physical.Distribution> requiredChildDistribution () { throw new RuntimeException(); }
  public  scala.collection.Seq<scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.SortOrder>> requiredChildOrdering () { throw new RuntimeException(); }
  private  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.SortOrder> requiredOrders (scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression> keys) { throw new RuntimeException(); }
  private  boolean isUnsafeMode () { throw new RuntimeException(); }
  public  boolean outputsUnsafeRows () { throw new RuntimeException(); }
  public  boolean canProcessUnsafeRows () { throw new RuntimeException(); }
  public  boolean canProcessSafeRows () { throw new RuntimeException(); }
  private  org.apache.spark.sql.catalyst.expressions.Projection createLeftKeyGenerator () { throw new RuntimeException(); }
  private  org.apache.spark.sql.catalyst.expressions.Projection createRightKeyGenerator () { throw new RuntimeException(); }
  public  org.apache.spark.rdd.RDD<org.apache.spark.sql.catalyst.InternalRow> doExecute () { throw new RuntimeException(); }
}
