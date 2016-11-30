package org.apache.spark.sql.execution;
/**
 * The UnboundPreceding window frame calculates frames with the following SQL form:
 * ... BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
 * <p>
 * There is only an upper bound. Very common use cases are for instance running sums or counts
 * (row_number). Technically this is a special case of a sliding window. However a sliding window
 * has to maintain a buffer, and it must do a full evaluation everytime the buffer changes. This
 * is not the case when there is no lower bound, given the additive nature of most aggregates
 * streaming updates and partial evaluation suffice and no buffering is needed.
 * <p>
 * param:  ordinal of the first column written by this frame.
 * param:  functions to calculate the row values with.
 * param:  ubound comparator used to identify the upper bound of an output row.
 */
 final class UnboundedPrecedingWindowFunctionFrame extends org.apache.spark.sql.execution.WindowFunctionFrame {
  public   UnboundedPrecedingWindowFunctionFrame (int ordinal, org.apache.spark.sql.catalyst.expressions.WindowFunction[] functions, org.apache.spark.sql.execution.BoundOrdering ubound) { throw new RuntimeException(); }
  /** Prepare the frame for calculating a new partition. */
  public  void prepare (org.apache.spark.util.collection.CompactBuffer<org.apache.spark.sql.catalyst.InternalRow> rows) { throw new RuntimeException(); }
  /** Write the frame columns for the current row to the given target row. */
  public  void write (org.apache.spark.sql.catalyst.expressions.GenericMutableRow target) { throw new RuntimeException(); }
  /** Copy the frame. */
  public  org.apache.spark.sql.execution.UnboundedPrecedingWindowFunctionFrame copy () { throw new RuntimeException(); }
}
