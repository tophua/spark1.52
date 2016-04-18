package org.apache.spark.sql.execution.metric;
/**
 * Create a layer for specialized metric. We cannot add <code>@specialized</code> to
 * <code>Accumulable/AccumulableParam</code> because it will break Java source compatibility.
 */
  interface SQLMetricValue<T extends java.lang.Object> extends scala.Serializable {
  public  T value () ;
  public  java.lang.String toString () ;
}
