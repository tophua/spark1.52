package org.apache.spark.sql.execution.aggregate;
// no position
public  class SortBasedAggregationIterator$ {
  /**
   * Static reference to the singleton instance of this Scala object.
   */
  public static final SortBasedAggregationIterator$ MODULE$ = null;
  public   SortBasedAggregationIterator$ () { throw new RuntimeException(); }
  public  org.apache.spark.sql.execution.aggregate.SortBasedAggregationIterator createFromInputIterator (scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.NamedExpression> groupingExprs, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression2> nonCompleteAggregateExpressions, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> nonCompleteAggregateAttributes, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression2> completeAggregateExpressions, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> completeAggregateAttributes, int initialInputBufferOffset, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.NamedExpression> resultExpressions, scala.Function2<scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression>, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute>, scala.Function0<org.apache.spark.sql.catalyst.expressions.MutableProjection>> newMutableProjection, scala.Function2<scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression>, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute>, org.apache.spark.sql.catalyst.expressions.Projection> newProjection, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> inputAttributes, scala.collection.Iterator<org.apache.spark.sql.catalyst.InternalRow> inputIter, boolean outputsUnsafeRows, org.apache.spark.sql.execution.metric.LongSQLMetric numInputRows, org.apache.spark.sql.execution.metric.LongSQLMetric numOutputRows) { throw new RuntimeException(); }
}
