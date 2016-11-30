package org.apache.spark.sql.execution.ui;
/**
 * Store all accumulatorUpdates for a Spark task.
 */
  class SQLTaskMetrics {
  public  long attemptId () { throw new RuntimeException(); }
  public  boolean finished () { throw new RuntimeException(); }
  public  scala.collection.immutable.Map<java.lang.Object, java.lang.Object> accumulatorUpdates () { throw new RuntimeException(); }
  // not preceding
  public   SQLTaskMetrics (long attemptId, boolean finished, scala.collection.immutable.Map<java.lang.Object, java.lang.Object> accumulatorUpdates) { throw new RuntimeException(); }
}
