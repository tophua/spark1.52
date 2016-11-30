package org.apache.spark.sql.execution;
/**
 * A window function calculates the results of a number of window functions for a window frame.
 * Before use a frame must be prepared by passing it all the rows in the current partition. After
 * preparation the update method can be called to fill the output rows.
 * <p>
 * TODO How to improve performance? A few thoughts:
 * - Window functions are expensive due to its distribution and ordering requirements.
 * Unfortunately it is up to the Spark engine to solve this. Improvements in the form of project
 * Tungsten are on the way.
 * - The window frame processing bit can be improved though. But before we start doing that we
 * need to see how much of the time and resources are spent on partitioning and ordering, and
 * how much time and resources are spent processing the partitions. There are a couple ways to
 * improve on the current situation:
 * - Reduce memory footprint by performing streaming calculations. This can only be done when
 * there are no Unbound/Unbounded Following calculations present.
 * - Use Tungsten style memory usage.
 * - Use code generation in general, and use the approach to aggregation taken in the
 *   GeneratedAggregate class in specific.
 * <p>
 * param:  ordinal of the first column written by this frame.
 * param:  functions to calculate the row values with.
 */
 abstract class WindowFunctionFrame {
  public   WindowFunctionFrame (int ordinal, org.apache.spark.sql.catalyst.expressions.WindowFunction[] functions) { throw new RuntimeException(); }
  /** Number of columns the window function frame is managing */
  public  int numColumns () { throw new RuntimeException(); }
  /**
   * Create a fresh thread safe copy of the frame.
   * <p>
   * @return the copied frame.
   */
  public abstract  org.apache.spark.sql.execution.WindowFunctionFrame copy () ;
  /**
   * Create new instances of the functions.
   * <p>
   * @return an array containing copies of the current window functions.
   */
  protected final  org.apache.spark.sql.catalyst.expressions.WindowFunction[] copyFunctions () { throw new RuntimeException(); }
  /**
   * Prepare the frame for calculating the results for a partition.
   * <p>
   * @param rows to calculate the frame results for.
   */
  public abstract  void prepare (org.apache.spark.util.collection.CompactBuffer<org.apache.spark.sql.catalyst.InternalRow> rows) ;
  /**
   * Write the result for the current row to the given target row.
   * <p>
   * @param target row to write the result for the current row to.
   */
  public abstract  void write (org.apache.spark.sql.catalyst.expressions.GenericMutableRow target) ;
  /** Reset the current window functions. */
  protected final  void reset () { throw new RuntimeException(); }
  /** Prepare an input row for processing. */
  protected final  java.lang.Object[] prepare (org.apache.spark.sql.catalyst.InternalRow input) { throw new RuntimeException(); }
  /** Evaluate a prepared buffer (iterator). */
  protected final  void evaluatePrepared (java.util.Iterator<java.lang.Object[]> iterator) { throw new RuntimeException(); }
  /** Evaluate a prepared buffer (array). */
  protected final  void evaluatePrepared (java.lang.Object[][] prepared, int fromIndex, int toIndex) { throw new RuntimeException(); }
  /** Update an array of window functions. */
  protected final  void update (org.apache.spark.sql.catalyst.InternalRow input) { throw new RuntimeException(); }
  /** Evaluate the window functions. */
  protected final  void evaluate () { throw new RuntimeException(); }
  /** Fill a target row with the current window function results. */
  protected final  void fill (org.apache.spark.sql.catalyst.expressions.GenericMutableRow target, int rowIndex) { throw new RuntimeException(); }
}
