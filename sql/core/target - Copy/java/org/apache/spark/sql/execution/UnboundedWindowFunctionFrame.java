package org.apache.spark.sql.execution;
/**
 * The unbounded window frame calculates frames with the following SQL forms:
 * ... (No Frame Definition)
 * ... BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
 * <p>
 * Its results are  the same for each and every row in the partition. This class can be seen as a
 * special case of a sliding window, but is optimized for the unbound case.
 * <p>
 * param:  ordinal of the first column written by this frame.
 * param:  functions to calculate the row values with.
 */
 final class UnboundedWindowFunctionFrame extends org.apache.spark.sql.execution.WindowFunctionFrame {
  public   UnboundedWindowFunctionFrame (int ordinal, org.apache.spark.sql.catalyst.expressions.WindowFunction[] functions) { throw new RuntimeException(); }
  /** Prepare the frame for calculating a new partition. Process all rows eagerly. */
  public  void prepare (org.apache.spark.util.collection.CompactBuffer<org.apache.spark.sql.catalyst.InternalRow> rows) { throw new RuntimeException(); }
  /** Write the frame columns for the current row to the given target row. */
  public  void write (org.apache.spark.sql.catalyst.expressions.GenericMutableRow target) { throw new RuntimeException(); }
  /** Copy the frame. */
  public  org.apache.spark.sql.execution.UnboundedWindowFunctionFrame copy () { throw new RuntimeException(); }
}
