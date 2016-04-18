package org.apache.spark.sql.execution.joins;
public  interface HashSemiJoin {
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression> leftKeys () ;
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression> rightKeys () ;
  public  org.apache.spark.sql.execution.SparkPlan left () ;
  public  org.apache.spark.sql.execution.SparkPlan right () ;
  public  scala.Option<org.apache.spark.sql.catalyst.expressions.Expression> condition () ;
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> output () ;
  public  boolean supportUnsafe () ;
  public  boolean outputsUnsafeRows () ;
  public  boolean canProcessUnsafeRows () ;
  public  boolean canProcessSafeRows () ;
  public  org.apache.spark.sql.catalyst.expressions.Projection leftKeyGenerator () ;
  public  org.apache.spark.sql.catalyst.expressions.Projection rightKeyGenerator () ;
  public  scala.Function1<org.apache.spark.sql.catalyst.InternalRow, java.lang.Object> boundCondition () ;
  public  java.util.Set<org.apache.spark.sql.catalyst.InternalRow> buildKeyHashSet (scala.collection.Iterator<org.apache.spark.sql.catalyst.InternalRow> buildIter, org.apache.spark.sql.execution.metric.LongSQLMetric numBuildRows) ;
  public  scala.collection.Iterator<org.apache.spark.sql.catalyst.InternalRow> hashSemiJoin (scala.collection.Iterator<org.apache.spark.sql.catalyst.InternalRow> streamIter, org.apache.spark.sql.execution.metric.LongSQLMetric numStreamRows, java.util.Set<org.apache.spark.sql.catalyst.InternalRow> hashSet, org.apache.spark.sql.execution.metric.LongSQLMetric numOutputRows) ;
  public  scala.collection.Iterator<org.apache.spark.sql.catalyst.InternalRow> hashSemiJoin (scala.collection.Iterator<org.apache.spark.sql.catalyst.InternalRow> streamIter, org.apache.spark.sql.execution.metric.LongSQLMetric numStreamRows, org.apache.spark.sql.execution.joins.HashedRelation hashedRelation, org.apache.spark.sql.execution.metric.LongSQLMetric numOutputRows) ;
}
