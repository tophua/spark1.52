package org.apache.spark.sql.execution.aggregate;
/**
 * A Mutable {@link Row} representing an mutable aggregation buffer.
 */
  class MutableAggregationBufferImpl extends org.apache.spark.sql.expressions.MutableAggregationBuffer implements org.apache.spark.sql.execution.aggregate.BufferSetterGetterUtils {
  public  org.apache.spark.sql.catalyst.expressions.MutableRow underlyingBuffer () { throw new RuntimeException(); }
  // not preceding
  public   MutableAggregationBufferImpl (org.apache.spark.sql.types.StructType schema, scala.Function1<java.lang.Object, java.lang.Object>[] toCatalystConverters, scala.Function1<java.lang.Object, java.lang.Object>[] toScalaConverters, int bufferOffset, org.apache.spark.sql.catalyst.expressions.MutableRow underlyingBuffer) { throw new RuntimeException(); }
  public  int length () { throw new RuntimeException(); }
  public  Object get (int i) { throw new RuntimeException(); }
  public  void update (int i, Object value) { throw new RuntimeException(); }
  public  boolean isNullAt (int i) { throw new RuntimeException(); }
  public  org.apache.spark.sql.execution.aggregate.MutableAggregationBufferImpl copy () { throw new RuntimeException(); }
}
