package org.apache.spark.sql.execution.aggregate;
/**
 * An iterator used to evaluate aggregate functions. It operates on {@link UnsafeRow}s.
 * <p>
 * This iterator first uses hash-based aggregation to process input rows. It uses
 * a hash map to store groups and their corresponding aggregation buffers. If we
 * this map cannot allocate memory from {@link org.apache.spark.shuffle.ShuffleMemoryManager},
 * it switches to sort-based aggregation. The process of the switch has the following step:
 *  - Step 1: Sort all entries of the hash map based on values of grouping expressions and
 *            spill them to disk.
 *  - Step 2: Create a external sorter based on the spilled sorted map entries.
 *  - Step 3: Redirect all input rows to the external sorter.
 *  - Step 4: Get a sorted {@link KVIterator} from the external sorter.
 *  - Step 5: Initialize sort-based aggregation.
 * Then, this iterator works in the way of sort-based aggregation.
 * <p>
 * The code of this class is organized as follows:
 *  - Part 1: Initializing aggregate functions.
 *  - Part 2: Methods and fields used by setting aggregation buffer values,
 *            processing input rows from inputIter, and generating output
 *            rows.
 *  - Part 3: Methods and fields used by hash-based aggregation.
 *  - Part 4: Methods and fields used when we switch to sort-based aggregation.
 *  - Part 5: Methods and fields used by sort-based aggregation.
 *  - Part 6: Loads input and process input rows.
 *  - Part 7: Public methods of this iterator.
 *  - Part 8: A utility function used to generate a result when there is no
 *            input and there is no grouping expression.
 * <p>
 * param:  groupingExpressions
 *   expressions for grouping keys
 * param:  nonCompleteAggregateExpressions
 *   {@link AggregateExpression2} containing {@link AggregateFunction2}s with mode {@link Partial},
 *   {@link PartialMerge}, or {@link Final}.
 * param:  completeAggregateExpressions
 *   {@link AggregateExpression2} containing {@link AggregateFunction2}s with mode {@link Complete}.
 * param:  initialInputBufferOffset
 *   If this iterator is used to handle functions with mode {@link PartialMerge} or {@link Final}.
 *   The input rows have the format of <code>grouping keys + aggregation buffer</code>.
 *   This offset indicates the starting position of aggregation buffer in a input row.
 * param:  resultExpressions
 *   expressions for generating output rows.
 * param:  newMutableProjection
 *   the function used to create mutable projections.
 * param:  originalInputAttributes
 *   attributes of representing input rows from <code>inputIter</code>.
 */
public  class TungstenAggregationIterator implements scala.collection.Iterator<org.apache.spark.sql.catalyst.expressions.UnsafeRow>, org.apache.spark.Logging {
  public   TungstenAggregationIterator (scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.NamedExpression> groupingExpressions, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression2> nonCompleteAggregateExpressions, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression2> completeAggregateExpressions, int initialInputBufferOffset, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.NamedExpression> resultExpressions, scala.Function2<scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression>, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute>, scala.Function0<org.apache.spark.sql.catalyst.expressions.MutableProjection>> newMutableProjection, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> originalInputAttributes, scala.Option<java.lang.Object> testFallbackStartsAt, org.apache.spark.sql.execution.metric.LongSQLMetric numInputRows, org.apache.spark.sql.execution.metric.LongSQLMetric numOutputRows) { throw new RuntimeException(); }
  private  org.apache.spark.sql.catalyst.expressions.UnsafeRow createNewAggregationBuffer () { throw new RuntimeException(); }
  private  scala.Function2<org.apache.spark.sql.catalyst.expressions.UnsafeRow, org.apache.spark.sql.catalyst.InternalRow, scala.runtime.BoxedUnit> generateProcessRow (scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> inputAttributes) { throw new RuntimeException(); }
  private  scala.Function2<org.apache.spark.sql.catalyst.expressions.UnsafeRow, org.apache.spark.sql.catalyst.expressions.UnsafeRow, org.apache.spark.sql.catalyst.expressions.UnsafeRow> generateResultProjection () { throw new RuntimeException(); }
    org.apache.spark.sql.execution.UnsafeFixedWidthAggregationMap getHashMap () { throw new RuntimeException(); }
  private  void processInputs () { throw new RuntimeException(); }
  private  void processInputsWithControlledFallback (int fallbackStartsAt) { throw new RuntimeException(); }
  /**
   * Switch to sort-based aggregation when the hash-based approach is unable to acquire memory.
   * @param firstKey (undocumented)
   * @param firstInput (undocumented)
   */
  private  void switchToSortBasedAggregation (org.apache.spark.sql.catalyst.expressions.UnsafeRow firstKey, org.apache.spark.sql.catalyst.InternalRow firstInput) { throw new RuntimeException(); }
  private  void processCurrentSortedGroup () { throw new RuntimeException(); }
  /**
   * Start processing input rows.
   * Only after this method is called will this iterator be non-empty.
   * @param parentIter (undocumented)
   */
  public  void start (scala.collection.Iterator<org.apache.spark.sql.catalyst.InternalRow> parentIter) { throw new RuntimeException(); }
  public final  boolean hasNext () { throw new RuntimeException(); }
  public final  org.apache.spark.sql.catalyst.expressions.UnsafeRow next () { throw new RuntimeException(); }
  /**
   * Generate a output row when there is no input and there is no grouping expression.
   * @return (undocumented)
   */
  public  org.apache.spark.sql.catalyst.expressions.UnsafeRow outputForEmptyGroupingKeyWithoutInput () { throw new RuntimeException(); }
  /** Free memory used in the underlying map. */
  public  void free () { throw new RuntimeException(); }
}
