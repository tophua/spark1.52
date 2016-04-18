package org.apache.spark.sql.execution.metric;
/**
 * A wrapper of Int to avoid boxing and unboxing when using Accumulator
 */
  class IntSQLMetricValue implements org.apache.spark.sql.execution.metric.SQLMetricValue<java.lang.Object> {
  private  int _value () { throw new RuntimeException(); }
  // not preceding
  public   IntSQLMetricValue (int _value) { throw new RuntimeException(); }
  public  org.apache.spark.sql.execution.metric.IntSQLMetricValue add (int term) { throw new RuntimeException(); }
  public  int value () { throw new RuntimeException(); }
}
