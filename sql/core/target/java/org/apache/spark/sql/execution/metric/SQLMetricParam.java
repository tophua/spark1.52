package org.apache.spark.sql.execution.metric;
/**
 * Create a layer for specialized metric. We cannot add <code>@specialized</code> to
 * <code>Accumulable/AccumulableParam</code> because it will break Java source compatibility.
 */
  interface SQLMetricParam<R extends org.apache.spark.sql.execution.metric.SQLMetricValue<T>, T extends java.lang.Object> extends org.apache.spark.AccumulableParam<R, T> {
  public  R zero () ;
}
