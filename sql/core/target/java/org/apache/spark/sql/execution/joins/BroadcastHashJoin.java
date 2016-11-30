package org.apache.spark.sql.execution.joins;
/**
 * :: DeveloperApi ::
 * Performs an inner hash join of two child relations.  When the output RDD of this operator is
 * being constructed, a Spark job is asynchronously started to calculate the values for the
 * broadcasted relation.  This data is then placed in a Spark broadcast variable.  The streamed
 * relation is not shuffled.
 */
public  class BroadcastHashJoin extends org.apache.spark.sql.execution.SparkPlan implements org.apache.spark.sql.execution.BinaryNode, org.apache.spark.sql.execution.joins.HashJoin, scala.Product, scala.Serializable {
  static public  scala.concurrent.ExecutionContextExecutorService broadcastHashJoinExecutionContext () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression> leftKeys () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression> rightKeys () { throw new RuntimeException(); }
  public  org.apache.spark.sql.execution.joins.BuildSide buildSide () { throw new RuntimeException(); }
  public  org.apache.spark.sql.execution.SparkPlan left () { throw new RuntimeException(); }
  public  org.apache.spark.sql.execution.SparkPlan right () { throw new RuntimeException(); }
  // not preceding
  public   BroadcastHashJoin (scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression> leftKeys, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression> rightKeys, org.apache.spark.sql.execution.joins.BuildSide buildSide, org.apache.spark.sql.execution.SparkPlan left, org.apache.spark.sql.execution.SparkPlan right) { throw new RuntimeException(); }
  public  scala.collection.immutable.Map<java.lang.String, org.apache.spark.sql.execution.metric.LongSQLMetric> metrics () { throw new RuntimeException(); }
  public  scala.concurrent.duration.Duration timeout () { throw new RuntimeException(); }
  public  org.apache.spark.sql.catalyst.plans.physical.Partitioning outputPartitioning () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.plans.physical.Distribution> requiredChildDistribution () { throw new RuntimeException(); }
  private  scala.concurrent.Future<org.apache.spark.broadcast.Broadcast<org.apache.spark.sql.execution.joins.HashedRelation>> broadcastFuture () { throw new RuntimeException(); }
  protected  void doPrepare () { throw new RuntimeException(); }
  protected  org.apache.spark.rdd.RDD<org.apache.spark.sql.catalyst.InternalRow> doExecute () { throw new RuntimeException(); }
}
