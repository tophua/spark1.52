package org.apache.spark.sql.execution.metric;
/**
 * Create a layer for specialized metric. We cannot add <code>@specialized</code> to
 * <code>Accumulable/AccumulableParam</code> because it will break Java source compatibility.
 * <p>
 * An implementation of SQLMetric should override <code>+=</code> and <code>add</code> to avoid boxing.
 */
 abstract class SQLMetric<R extends org.apache.spark.sql.execution.metric.SQLMetricValue<T>, T extends java.lang.Object> extends org.apache.spark.Accumulable<R, T> {
  public  org.apache.spark.sql.execution.metric.SQLMetricParam<R, T> param () { throw new RuntimeException(); }
  // not preceding
  public   SQLMetric (java.lang.String name, org.apache.spark.sql.execution.metric.SQLMetricParam<R, T> param) { throw new RuntimeException(); }
}
