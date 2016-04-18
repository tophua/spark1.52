package org.apache.spark.sql.execution.ui;
/**
 * Represent all necessary data for an execution that will be used in Web UI.
 */
  class SQLExecutionUIData {
  public  long executionId () { throw new RuntimeException(); }
  public  java.lang.String description () { throw new RuntimeException(); }
  public  java.lang.String details () { throw new RuntimeException(); }
  public  java.lang.String physicalPlanDescription () { throw new RuntimeException(); }
  public  org.apache.spark.sql.execution.ui.SparkPlanGraph physicalPlanGraph () { throw new RuntimeException(); }
  public  scala.collection.immutable.Map<java.lang.Object, org.apache.spark.sql.execution.ui.SQLPlanMetric> accumulatorMetrics () { throw new RuntimeException(); }
  public  long submissionTime () { throw new RuntimeException(); }
  public  scala.Option<java.lang.Object> completionTime () { throw new RuntimeException(); }
  public  scala.collection.mutable.HashMap<java.lang.Object, org.apache.spark.JobExecutionStatus> jobs () { throw new RuntimeException(); }
  public  scala.collection.mutable.ArrayBuffer<java.lang.Object> stages () { throw new RuntimeException(); }
  // not preceding
  public   SQLExecutionUIData (long executionId, java.lang.String description, java.lang.String details, java.lang.String physicalPlanDescription, org.apache.spark.sql.execution.ui.SparkPlanGraph physicalPlanGraph, scala.collection.immutable.Map<java.lang.Object, org.apache.spark.sql.execution.ui.SQLPlanMetric> accumulatorMetrics, long submissionTime, scala.Option<java.lang.Object> completionTime, scala.collection.mutable.HashMap<java.lang.Object, org.apache.spark.JobExecutionStatus> jobs, scala.collection.mutable.ArrayBuffer<java.lang.Object> stages) { throw new RuntimeException(); }
  /**
   * Return whether there are running jobs in this execution.
   * @return (undocumented)
   */
  public  boolean hasRunningJobs () { throw new RuntimeException(); }
  /**
   * Return whether there are any failed jobs in this execution.
   * @return (undocumented)
   */
  public  boolean isFailed () { throw new RuntimeException(); }
  public  scala.collection.Seq<java.lang.Object> runningJobs () { throw new RuntimeException(); }
  public  scala.collection.Seq<java.lang.Object> succeededJobs () { throw new RuntimeException(); }
  public  scala.collection.Seq<java.lang.Object> failedJobs () { throw new RuntimeException(); }
}
