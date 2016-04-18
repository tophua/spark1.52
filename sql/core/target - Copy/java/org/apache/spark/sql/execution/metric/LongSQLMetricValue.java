package org.apache.spark.sql.execution.metric;
/**
 * A wrapper of Long to avoid boxing and unboxing when using Accumulator
 */
  class LongSQLMetricValue implements org.apache.spark.sql.execution.metric.SQLMetricValue<java.lang.Object> {
  private  long _value () { throw new RuntimeException(); }
  // not preceding
  public   LongSQLMetricValue (long _value) { throw new RuntimeException(); }
  public  org.apache.spark.sql.execution.metric.LongSQLMetricValue add (long incr) { throw new RuntimeException(); }
  public  long value () { throw new RuntimeException(); }
}
