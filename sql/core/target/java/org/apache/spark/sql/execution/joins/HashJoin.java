package org.apache.spark.sql.execution.joins;
public  interface HashJoin {
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression> leftKeys () ;
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression> rightKeys () ;
  public  org.apache.spark.sql.execution.joins.BuildSide buildSide () ;
  public  org.apache.spark.sql.execution.SparkPlan left () ;
  public  org.apache.spark.sql.execution.SparkPlan right () ;
  // not preceding
  public  org.apache.spark.sql.execution.SparkPlan buildPlan () ;
  public  org.apache.spark.sql.execution.SparkPlan streamedPlan () ;
  // not preceding
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression> buildKeys () ;
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression> streamedKeys () ;
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> output () ;
  public  boolean isUnsafeMode () ;
  public  boolean outputsUnsafeRows () ;
  public  boolean canProcessUnsafeRows () ;
  public  boolean canProcessSafeRows () ;
  public  org.apache.spark.sql.catalyst.expressions.Projection buildSideKeyGenerator () ;
  public  org.apache.spark.sql.catalyst.expressions.Projection streamSideKeyGenerator () ;
  public  scala.collection.Iterator<org.apache.spark.sql.catalyst.InternalRow> hashJoin (scala.collection.Iterator<org.apache.spark.sql.catalyst.InternalRow> streamIter, org.apache.spark.sql.execution.metric.LongSQLMetric numStreamRows, org.apache.spark.sql.execution.joins.HashedRelation hashedRelation, org.apache.spark.sql.execution.metric.LongSQLMetric numOutputRows) ;
}
