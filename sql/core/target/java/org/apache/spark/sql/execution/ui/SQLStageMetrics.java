package org.apache.spark.sql.execution.ui;
/**
 * Store all accumulatorUpdates for all tasks in a Spark stage.
 */
  class SQLStageMetrics {
  public  long stageAttemptId () { throw new RuntimeException(); }
  public  scala.collection.mutable.HashMap<java.lang.Object, org.apache.spark.sql.execution.ui.SQLTaskMetrics> taskIdToMetricUpdates () { throw new RuntimeException(); }
  // not preceding
  public   SQLStageMetrics (long stageAttemptId, scala.collection.mutable.HashMap<java.lang.Object, org.apache.spark.sql.execution.ui.SQLTaskMetrics> taskIdToMetricUpdates) { throw new RuntimeException(); }
}
