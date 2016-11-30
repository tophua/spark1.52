package org.apache.spark.sql.expressions;
/**
 * :: Experimental ::
 * A {@link Row} representing an mutable aggregation buffer.
 * <p>
 * This is not meant to be extended outside of Spark.
 */
public abstract class MutableAggregationBuffer implements org.apache.spark.sql.Row {
  public   MutableAggregationBuffer () { throw new RuntimeException(); }
  /** Update the ith value of this buffer. */
  public abstract  void update (int i, Object value) ;
}
