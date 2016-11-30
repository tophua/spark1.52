package org.apache.spark.sql.execution;
/**
 * The UnboundFollowing window frame calculates frames with the following SQL form:
 * ... BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
 * <p>
 * There is only an upper bound. This is a slightly modified version of the sliding window. The
 * sliding window operator has to check if both upper and the lower bound change when a new row
 * gets processed, where as the unbounded following only has to check the lower bound.
 * <p>
 * This is a very expensive operator to use, O(n * (n - 1) /2), because we need to maintain a
 * buffer and must do full recalculation after each row. Reverse iteration would be possible, if
 * the communitativity of the used window functions can be guaranteed.
 * <p>
 * param:  ordinal of the first column written by this frame.
 * param:  functions to calculate the row values with.
 * param:  lbound comparator used to identify the lower bound of an output row.
 */
 final class UnboundedFollowingWindowFunctionFrame extends org.apache.spark.sql.execution.WindowFunctionFrame {
  public   UnboundedFollowingWindowFunctionFrame (int ordinal, org.apache.spark.sql.catalyst.expressions.WindowFunction[] functions, org.apache.spark.sql.execution.BoundOrdering lbound) { throw new RuntimeException(); }
  /** Prepare the frame for calculating a new partition. */
  public  void prepare (org.apache.spark.util.collection.CompactBuffer<org.apache.spark.sql.catalyst.InternalRow> rows) { throw new RuntimeException(); }
  /** Write the frame columns for the current row to the given target row. */
  public  void write (org.apache.spark.sql.catalyst.expressions.GenericMutableRow target) { throw new RuntimeException(); }
  /** Copy the frame. */
  public  org.apache.spark.sql.execution.UnboundedFollowingWindowFunctionFrame copy () { throw new RuntimeException(); }
}
