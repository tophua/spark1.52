package org.apache.spark.sql.execution;
/**
 * The sliding window frame calculates frames with the following SQL form:
 * ... BETWEEN 1 PRECEDING AND 1 FOLLOWING
 * <p>
 * param:  ordinal of the first column written by this frame.
 * param:  functions to calculate the row values with.
 * param:  lbound comparator used to identify the lower bound of an output row.
 * param:  ubound comparator used to identify the upper bound of an output row.
 */
 final class SlidingWindowFunctionFrame extends org.apache.spark.sql.execution.WindowFunctionFrame {
  public   SlidingWindowFunctionFrame (int ordinal, org.apache.spark.sql.catalyst.expressions.WindowFunction[] functions, org.apache.spark.sql.execution.BoundOrdering lbound, org.apache.spark.sql.execution.BoundOrdering ubound) { throw new RuntimeException(); }
  /** Prepare the frame for calculating a new partition. Reset all variables. */
  public  void prepare (org.apache.spark.util.collection.CompactBuffer<org.apache.spark.sql.catalyst.InternalRow> rows) { throw new RuntimeException(); }
  /** Write the frame columns for the current row to the given target row. */
  public  void write (org.apache.spark.sql.catalyst.expressions.GenericMutableRow target) { throw new RuntimeException(); }
  /** Copy the frame. */
  public  org.apache.spark.sql.execution.SlidingWindowFunctionFrame copy () { throw new RuntimeException(); }
}
