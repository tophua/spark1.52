package org.apache.spark.sql.execution.aggregate;
/**
 * The base class of {@link SortBasedAggregationIterator} and {@link UnsafeHybridAggregationIterator}.
 * It mainly contains two parts:
 * 1. It initializes aggregate functions.
 * 2. It creates two functions, <code>processRow</code> and <code>generateOutput</code> based on {@link AggregateMode} of
 *    its aggregate functions. <code>processRow</code> is the function to handle an input. <code>generateOutput</code>
 *    is used to generate result.
 */
public abstract class AggregationIterator implements scala.collection.Iterator<org.apache.spark.sql.catalyst.InternalRow>, org.apache.spark.Logging {
  static public  org.apache.spark.unsafe.KVIterator<org.apache.spark.sql.catalyst.InternalRow, org.apache.spark.sql.catalyst.InternalRow> kvIterator (scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.NamedExpression> groupingExpressions, scala.Function2<scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression>, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute>, org.apache.spark.sql.catalyst.expressions.Projection> newProjection, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> inputAttributes, scala.collection.Iterator<org.apache.spark.sql.catalyst.InternalRow> inputIter) { throw new RuntimeException(); }
  static public  org.apache.spark.unsafe.KVIterator<org.apache.spark.sql.catalyst.expressions.UnsafeRow, org.apache.spark.sql.catalyst.InternalRow> unsafeKVIterator (scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.NamedExpression> groupingExpressions, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> inputAttributes, scala.collection.Iterator<org.apache.spark.sql.catalyst.InternalRow> inputIter) { throw new RuntimeException(); }
  public   AggregationIterator (scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> groupingKeyAttributes, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> valueAttributes, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression2> nonCompleteAggregateExpressions, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> nonCompleteAggregateAttributes, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression2> completeAggregateExpressions, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> completeAggregateAttributes, int initialInputBufferOffset, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.NamedExpression> resultExpressions, scala.Function2<scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression>, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute>, scala.Function0<org.apache.spark.sql.catalyst.expressions.MutableProjection>> newMutableProjection, boolean outputsUnsafeRows) { throw new RuntimeException(); }
  protected  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression2> allAggregateExpressions () { throw new RuntimeException(); }
  /**
   * The distinct modes of AggregateExpressions. Right now, we can handle the following mode:
   *  - Partial-only: all AggregateExpressions have the mode of Partial;
   *  - PartialMerge-only: all AggregateExpressions have the mode of PartialMerge);
   *  - Final-only: all AggregateExpressions have the mode of Final;
   *  - Final-Complete: some AggregateExpressions have the mode of Final and
   *    others have the mode of Complete;
   *  - Complete-only: nonCompleteAggregateExpressions is empty and we have AggregateExpressions
   *    with mode Complete in completeAggregateExpressions; and
   *  - Grouping-only: there is no AggregateExpression.
   * @return (undocumented)
   */
  protected  scala.Tuple2<scala.Option<org.apache.spark.sql.catalyst.expressions.aggregate.AggregateMode>, scala.Option<org.apache.spark.sql.catalyst.expressions.aggregate.AggregateMode>> aggregationMode () { throw new RuntimeException(); }
  protected  org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction2[] allAggregateFunctions () { throw new RuntimeException(); }
  protected  scala.Function2<org.apache.spark.sql.catalyst.expressions.MutableRow, org.apache.spark.sql.catalyst.InternalRow, scala.runtime.BoxedUnit> processRow () { throw new RuntimeException(); }
  protected  scala.Function2<org.apache.spark.sql.catalyst.InternalRow, org.apache.spark.sql.catalyst.expressions.MutableRow, org.apache.spark.sql.catalyst.InternalRow> generateOutput () { throw new RuntimeException(); }
  /** Initializes buffer values for all aggregate functions. */
  protected  void initializeBuffer (org.apache.spark.sql.catalyst.expressions.MutableRow buffer) { throw new RuntimeException(); }
  /**
   * Creates a new aggregation buffer and initializes buffer values
   * for all aggregate functions.
   * @return (undocumented)
   */
  protected abstract  org.apache.spark.sql.catalyst.expressions.MutableRow newBuffer () ;
}
