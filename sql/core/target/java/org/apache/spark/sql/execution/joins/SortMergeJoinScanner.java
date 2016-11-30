package org.apache.spark.sql.execution.joins;
/**
 * Helper class that is used to implement {@link SortMergeJoin} and {@link SortMergeOuterJoin}.
 * <p>
 * To perform an inner (outer) join, users of this class call {@link findNextInnerJoinRows()}
 * ({@link findNextOuterJoinRows()}), which returns <code>true</code> if a result has been produced and <code>false</code>
 * otherwise. If a result has been produced, then the caller may call {@link getStreamedRow} to return
 * the matching row from the streamed input and may call {@link getBufferedMatches} to return the
 * sequence of matching rows from the buffered input (in the case of an outer join, this will return
 * an empty sequence if there are no matches from the buffered input). For efficiency, both of these
 * methods return mutable objects which are re-used across calls to the <code>findNext*JoinRows()</code>
 * methods.
 * <p>
 * param:  streamedKeyGenerator a projection that produces join keys from the streamed input.
 * param:  bufferedKeyGenerator a projection that produces join keys from the buffered input.
 * param:  keyOrdering an ordering which can be used to compare join keys.
 * param:  streamedIter an input whose rows will be streamed.
 * param:  bufferedIter an input whose rows will be buffered to construct sequences of rows that
 *                     have the same join key.
 */
  class SortMergeJoinScanner {
  public   SortMergeJoinScanner (org.apache.spark.sql.catalyst.expressions.Projection streamedKeyGenerator, org.apache.spark.sql.catalyst.expressions.Projection bufferedKeyGenerator, scala.math.Ordering<org.apache.spark.sql.catalyst.InternalRow> keyOrdering, org.apache.spark.sql.execution.RowIterator streamedIter, org.apache.spark.sql.execution.metric.LongSQLMetric numStreamedRows, org.apache.spark.sql.execution.RowIterator bufferedIter, org.apache.spark.sql.execution.metric.LongSQLMetric numBufferedRows) { throw new RuntimeException(); }
  public  org.apache.spark.sql.catalyst.InternalRow getStreamedRow () { throw new RuntimeException(); }
  public  scala.collection.mutable.ArrayBuffer<org.apache.spark.sql.catalyst.InternalRow> getBufferedMatches () { throw new RuntimeException(); }
  /**
   * Advances both input iterators, stopping when we have found rows with matching join keys.
   * @return true if matching rows have been found and false otherwise. If this returns true, then
   *         {@link getStreamedRow} and {@link getBufferedMatches} can be called to construct the join
   *         results.
   */
  public final  boolean findNextInnerJoinRows () { throw new RuntimeException(); }
  /**
   * Advances the streamed input iterator and buffers all rows from the buffered input that
   * have matching keys.
   * @return true if the streamed iterator returned a row, false otherwise. If this returns true,
   *         then [getStreamedRow and {@link getBufferedMatches} can be called to produce the outer
   *         join results.
   */
  public final  boolean findNextOuterJoinRows () { throw new RuntimeException(); }
  /**
   * Advance the streamed iterator and compute the new row's join key.
   * @return true if the streamed iterator returned a row and false otherwise.
   */
  private  boolean advancedStreamed () { throw new RuntimeException(); }
  /**
   * Advance the buffered iterator until we find a row with join key that does not contain nulls.
   * @return true if the buffered iterator returned a row and false otherwise.
   */
  private  boolean advancedBufferedToRowWithNullFreeJoinKey () { throw new RuntimeException(); }
  /**
   * Called when the streamed and buffered join keys match in order to buffer the matching rows.
   */
  private  void bufferMatchingRows () { throw new RuntimeException(); }
}
