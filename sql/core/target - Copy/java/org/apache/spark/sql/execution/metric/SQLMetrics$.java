package org.apache.spark.sql.execution.metric;
// no position
  class SQLMetrics$ {
  /**
   * Static reference to the singleton instance of this Scala object.
   */
  public static final SQLMetrics$ MODULE$ = null;
  public   SQLMetrics$ () { throw new RuntimeException(); }
  public  org.apache.spark.sql.execution.metric.LongSQLMetric createLongMetric (org.apache.spark.SparkContext sc, java.lang.String name) { throw new RuntimeException(); }
  /**
   * A metric that its value will be ignored. Use this one when we need a metric parameter but don't
   * care about the value.
   * @return (undocumented)
   */
  public  org.apache.spark.sql.execution.metric.LongSQLMetric nullLongMetric () { throw new RuntimeException(); }
}
