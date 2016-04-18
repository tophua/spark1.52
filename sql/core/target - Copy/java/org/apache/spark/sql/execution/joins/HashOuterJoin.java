package org.apache.spark.sql.execution.joins;
public  interface HashOuterJoin {
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression> leftKeys () ;
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression> rightKeys () ;
  public  org.apache.spark.sql.catalyst.plans.JoinType joinType () ;
  public  scala.Option<org.apache.spark.sql.catalyst.expressions.Expression> condition () ;
  public  org.apache.spark.sql.execution.SparkPlan left () ;
  public  org.apache.spark.sql.execution.SparkPlan right () ;
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> output () ;
  // not preceding
  public  org.apache.spark.sql.execution.SparkPlan buildPlan () ;
  public  org.apache.spark.sql.execution.SparkPlan streamedPlan () ;
  // not preceding
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression> buildKeys () ;
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression> streamedKeys () ;
  public  boolean isUnsafeMode () ;
  public  boolean outputsUnsafeRows () ;
  public  boolean canProcessUnsafeRows () ;
  public  boolean canProcessSafeRows () ;
  public  org.apache.spark.sql.catalyst.expressions.Projection buildKeyGenerator () ;
  public  org.apache.spark.sql.catalyst.expressions.Projection streamedKeyGenerator () ;
  public  scala.Function1<org.apache.spark.sql.catalyst.InternalRow, org.apache.spark.sql.catalyst.InternalRow> resultProjection () ;
  public  org.apache.spark.util.collection.CompactBuffer<org.apache.spark.sql.catalyst.InternalRow> DUMMY_LIST () ;
  public  org.apache.spark.util.collection.CompactBuffer<org.apache.spark.sql.catalyst.InternalRow> EMPTY_LIST () ;
  public  org.apache.spark.sql.catalyst.expressions.GenericInternalRow leftNullRow () ;
  public  org.apache.spark.sql.catalyst.expressions.GenericInternalRow rightNullRow () ;
  public  scala.Function1<org.apache.spark.sql.catalyst.InternalRow, java.lang.Object> boundCondition () ;
  public  scala.collection.Iterator<org.apache.spark.sql.catalyst.InternalRow> leftOuterIterator (org.apache.spark.sql.catalyst.InternalRow key, org.apache.spark.sql.catalyst.expressions.JoinedRow joinedRow, scala.collection.Iterable<org.apache.spark.sql.catalyst.InternalRow> rightIter, scala.Function1<org.apache.spark.sql.catalyst.InternalRow, org.apache.spark.sql.catalyst.InternalRow> resultProjection, org.apache.spark.sql.execution.metric.LongSQLMetric numOutputRows) ;
  public  scala.collection.Iterator<org.apache.spark.sql.catalyst.InternalRow> rightOuterIterator (org.apache.spark.sql.catalyst.InternalRow key, scala.collection.Iterable<org.apache.spark.sql.catalyst.InternalRow> leftIter, org.apache.spark.sql.catalyst.expressions.JoinedRow joinedRow, scala.Function1<org.apache.spark.sql.catalyst.InternalRow, org.apache.spark.sql.catalyst.InternalRow> resultProjection, org.apache.spark.sql.execution.metric.LongSQLMetric numOutputRows) ;
  public  scala.collection.Iterator<org.apache.spark.sql.catalyst.InternalRow> fullOuterIterator (org.apache.spark.sql.catalyst.InternalRow key, scala.collection.Iterable<org.apache.spark.sql.catalyst.InternalRow> leftIter, scala.collection.Iterable<org.apache.spark.sql.catalyst.InternalRow> rightIter, org.apache.spark.sql.catalyst.expressions.JoinedRow joinedRow, org.apache.spark.sql.execution.metric.LongSQLMetric numOutputRows) ;
  public  java.util.HashMap<org.apache.spark.sql.catalyst.InternalRow, org.apache.spark.util.collection.CompactBuffer<org.apache.spark.sql.catalyst.InternalRow>> buildHashTable (scala.collection.Iterator<org.apache.spark.sql.catalyst.InternalRow> iter, org.apache.spark.sql.execution.metric.LongSQLMetric numIterRows, org.apache.spark.sql.catalyst.expressions.Projection keyGenerator) ;
}
