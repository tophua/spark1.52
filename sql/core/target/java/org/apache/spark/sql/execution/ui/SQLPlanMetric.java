package org.apache.spark.sql.execution.ui;
/**
 * Represent a metric in a SQLPlan.
 * <p>
 * Because we cannot revert our changes for an "Accumulator", we need to maintain accumulator
 * updates for each task. So that if a task is retried, we can simply override the old updates with
 * the new updates of the new attempt task. Since we cannot add them to accumulator, we need to use
 * "AccumulatorParam" to get the aggregation value.
 */
  class SQLPlanMetric implements scala.Product, scala.Serializable {
  public  java.lang.String name () { throw new RuntimeException(); }
  public  long accumulatorId () { throw new RuntimeException(); }
  public  org.apache.spark.sql.execution.metric.SQLMetricParam<org.apache.spark.sql.execution.metric.SQLMetricValue<java.lang.Object>, java.lang.Object> metricParam () { throw new RuntimeException(); }
  // not preceding
  public   SQLPlanMetric (java.lang.String name, long accumulatorId, org.apache.spark.sql.execution.metric.SQLMetricParam<org.apache.spark.sql.execution.metric.SQLMetricValue<java.lang.Object>, java.lang.Object> metricParam) { throw new RuntimeException(); }
}
