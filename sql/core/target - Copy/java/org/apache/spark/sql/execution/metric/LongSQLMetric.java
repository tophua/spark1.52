package org.apache.spark.sql.execution.metric;
/**
 * A specialized long Accumulable to avoid boxing and unboxing when using Accumulator's
 * <code>+=</code> and <code>add</code>.
 */
  class LongSQLMetric extends org.apache.spark.sql.execution.metric.SQLMetric<org.apache.spark.sql.execution.metric.LongSQLMetricValue, java.lang.Object> {
     LongSQLMetric (java.lang.String name) { throw new RuntimeException(); }
  public  void add (long term) { throw new RuntimeException(); }
}
