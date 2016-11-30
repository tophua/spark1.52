package org.apache.spark.sql.execution.aggregate;
/**
 * A {@link Row} representing an immutable aggregation buffer.
 */
  class InputAggregationBuffer implements org.apache.spark.sql.Row, org.apache.spark.sql.execution.aggregate.BufferSetterGetterUtils {
  public  org.apache.spark.sql.catalyst.InternalRow underlyingInputBuffer () { throw new RuntimeException(); }
  // not preceding
     InputAggregationBuffer (org.apache.spark.sql.types.StructType schema, scala.Function1<java.lang.Object, java.lang.Object>[] toCatalystConverters, scala.Function1<java.lang.Object, java.lang.Object>[] toScalaConverters, int bufferOffset, org.apache.spark.sql.catalyst.InternalRow underlyingInputBuffer) { throw new RuntimeException(); }
  public  int getBufferOffset () { throw new RuntimeException(); }
  public  int length () { throw new RuntimeException(); }
  public  Object get (int i) { throw new RuntimeException(); }
  public  boolean isNullAt (int i) { throw new RuntimeException(); }
  public  org.apache.spark.sql.execution.aggregate.InputAggregationBuffer copy () { throw new RuntimeException(); }
}
