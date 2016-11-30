package org.apache.spark.sql.execution.joins;
/**
 * :: DeveloperApi ::
 * Build the right table's join keys into a HashSet, and iteratively go through the left
 * table, to find the if join keys are in the Hash set.
 */
public  class BroadcastLeftSemiJoinHash extends org.apache.spark.sql.execution.SparkPlan implements org.apache.spark.sql.execution.BinaryNode, org.apache.spark.sql.execution.joins.HashSemiJoin, scala.Product, scala.Serializable {
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression> leftKeys () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression> rightKeys () { throw new RuntimeException(); }
  public  org.apache.spark.sql.execution.SparkPlan left () { throw new RuntimeException(); }
  public  org.apache.spark.sql.execution.SparkPlan right () { throw new RuntimeException(); }
  public  scala.Option<org.apache.spark.sql.catalyst.expressions.Expression> condition () { throw new RuntimeException(); }
  // not preceding
  public   BroadcastLeftSemiJoinHash (scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression> leftKeys, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression> rightKeys, org.apache.spark.sql.execution.SparkPlan left, org.apache.spark.sql.execution.SparkPlan right, scala.Option<org.apache.spark.sql.catalyst.expressions.Expression> condition) { throw new RuntimeException(); }
  public  scala.collection.immutable.Map<java.lang.String, org.apache.spark.sql.execution.metric.LongSQLMetric> metrics () { throw new RuntimeException(); }
  protected  org.apache.spark.rdd.RDD<org.apache.spark.sql.catalyst.InternalRow> doExecute () { throw new RuntimeException(); }
}
