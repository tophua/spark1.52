package org.apache.spark.sql.execution.aggregate;
// no position
public  class AggregationIterator$ {
  /**
   * Static reference to the singleton instance of this Scala object.
   */
  public static final AggregationIterator$ MODULE$ = null;
  public   AggregationIterator$ () { throw new RuntimeException(); }
  public  org.apache.spark.unsafe.KVIterator<org.apache.spark.sql.catalyst.InternalRow, org.apache.spark.sql.catalyst.InternalRow> kvIterator (scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.NamedExpression> groupingExpressions, scala.Function2<scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression>, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute>, org.apache.spark.sql.catalyst.expressions.Projection> newProjection, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> inputAttributes, scala.collection.Iterator<org.apache.spark.sql.catalyst.InternalRow> inputIter) { throw new RuntimeException(); }
  public  org.apache.spark.unsafe.KVIterator<org.apache.spark.sql.catalyst.expressions.UnsafeRow, org.apache.spark.sql.catalyst.InternalRow> unsafeKVIterator (scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.NamedExpression> groupingExpressions, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> inputAttributes, scala.collection.Iterator<org.apache.spark.sql.catalyst.InternalRow> inputIter) { throw new RuntimeException(); }
}
