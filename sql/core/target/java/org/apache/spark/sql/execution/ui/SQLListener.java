package org.apache.spark.sql.execution.ui;
  class SQLListener implements org.apache.spark.scheduler.SparkListener, org.apache.spark.Logging {
  public   SQLListener (org.apache.spark.sql.SQLContext sqlContext) { throw new RuntimeException(); }
  private  int retainedExecutions () { throw new RuntimeException(); }
  private  scala.collection.mutable.HashMap<java.lang.Object, org.apache.spark.sql.execution.ui.SQLExecutionUIData> activeExecutions () { throw new RuntimeException(); }
  private  scala.collection.mutable.HashMap<java.lang.Object, org.apache.spark.sql.execution.ui.SQLExecutionUIData> _executionIdToData () { throw new RuntimeException(); }
  /**
   * Maintain the relation between job id and execution id so that we can get the execution id in
   * the "onJobEnd" method.
   * @return (undocumented)
   */
  private  scala.collection.mutable.HashMap<java.lang.Object, java.lang.Object> _jobIdToExecutionId () { throw new RuntimeException(); }
  private  scala.collection.mutable.HashMap<java.lang.Object, org.apache.spark.sql.execution.ui.SQLStageMetrics> _stageIdToStageMetrics () { throw new RuntimeException(); }
  private  scala.collection.mutable.ListBuffer<org.apache.spark.sql.execution.ui.SQLExecutionUIData> failedExecutions () { throw new RuntimeException(); }
  private  scala.collection.mutable.ListBuffer<org.apache.spark.sql.execution.ui.SQLExecutionUIData> completedExecutions () { throw new RuntimeException(); }
  public  scala.collection.immutable.Map<java.lang.Object, org.apache.spark.sql.execution.ui.SQLExecutionUIData> executionIdToData () { throw new RuntimeException(); }
  public  scala.collection.immutable.Map<java.lang.Object, java.lang.Object> jobIdToExecutionId () { throw new RuntimeException(); }
  public  scala.collection.immutable.Map<java.lang.Object, org.apache.spark.sql.execution.ui.SQLStageMetrics> stageIdToStageMetrics () { throw new RuntimeException(); }
  private  void trimExecutionsIfNecessary (scala.collection.mutable.ListBuffer<org.apache.spark.sql.execution.ui.SQLExecutionUIData> executions) { throw new RuntimeException(); }
  public  void onJobStart (org.apache.spark.scheduler.SparkListenerJobStart jobStart) { throw new RuntimeException(); }
  public  void onJobEnd (org.apache.spark.scheduler.SparkListenerJobEnd jobEnd) { throw new RuntimeException(); }
  public  void onExecutorMetricsUpdate (org.apache.spark.scheduler.SparkListenerExecutorMetricsUpdate executorMetricsUpdate) { throw new RuntimeException(); }
  public  void onStageSubmitted (org.apache.spark.scheduler.SparkListenerStageSubmitted stageSubmitted) { throw new RuntimeException(); }
  public  void onTaskEnd (org.apache.spark.scheduler.SparkListenerTaskEnd taskEnd) { throw new RuntimeException(); }
  /**
   * Update the accumulator values of a task with the latest metrics for this task. This is called
   * every time we receive an executor heartbeat or when a task finishes.
   * @param taskId (undocumented)
   * @param stageId (undocumented)
   * @param stageAttemptID (undocumented)
   * @param metrics (undocumented)
   * @param finishTask (undocumented)
   */
  private  void updateTaskAccumulatorValues (long taskId, int stageId, int stageAttemptID, org.apache.spark.executor.TaskMetrics metrics, boolean finishTask) { throw new RuntimeException(); }
  public  void onExecutionStart (long executionId, java.lang.String description, java.lang.String details, java.lang.String physicalPlanDescription, org.apache.spark.sql.execution.ui.SparkPlanGraph physicalPlanGraph, long time) { throw new RuntimeException(); }
  public  void onExecutionEnd (long executionId, long time) { throw new RuntimeException(); }
  private  void markExecutionFinished (long executionId) { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.execution.ui.SQLExecutionUIData> getRunningExecutions () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.execution.ui.SQLExecutionUIData> getFailedExecutions () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.execution.ui.SQLExecutionUIData> getCompletedExecutions () { throw new RuntimeException(); }
  public  scala.Option<org.apache.spark.sql.execution.ui.SQLExecutionUIData> getExecution (long executionId) { throw new RuntimeException(); }
  /**
   * Get all accumulator updates from all tasks which belong to this execution and merge them.
   * @param executionId (undocumented)
   * @return (undocumented)
   */
  public  scala.collection.immutable.Map<java.lang.Object, java.lang.Object> getExecutionMetrics (long executionId) { throw new RuntimeException(); }
  private  scala.collection.immutable.Map<java.lang.Object, java.lang.Object> mergeAccumulatorUpdates (scala.collection.Seq<scala.Tuple2<java.lang.Object, java.lang.Object>> accumulatorUpdates, scala.Function1<java.lang.Object, org.apache.spark.sql.execution.metric.SQLMetricParam<org.apache.spark.sql.execution.metric.SQLMetricValue<java.lang.Object>, java.lang.Object>> paramFunc) { throw new RuntimeException(); }
}
