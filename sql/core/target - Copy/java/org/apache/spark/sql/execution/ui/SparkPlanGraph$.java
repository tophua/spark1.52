package org.apache.spark.sql.execution.ui;
// no position
  class SparkPlanGraph$ implements scala.Serializable {
  /**
   * Static reference to the singleton instance of this Scala object.
   */
  public static final SparkPlanGraph$ MODULE$ = null;
  public   SparkPlanGraph$ () { throw new RuntimeException(); }
  /**
   * Build a SparkPlanGraph from the root of a SparkPlan tree.
   * @param plan (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.execution.ui.SparkPlanGraph apply (org.apache.spark.sql.execution.SparkPlan plan) { throw new RuntimeException(); }
  private  org.apache.spark.sql.execution.ui.SparkPlanGraphNode buildSparkPlanGraphNode (org.apache.spark.sql.execution.SparkPlan plan, java.util.concurrent.atomic.AtomicLong nodeIdGenerator, scala.collection.mutable.ArrayBuffer<org.apache.spark.sql.execution.ui.SparkPlanGraphNode> nodes, scala.collection.mutable.ArrayBuffer<org.apache.spark.sql.execution.ui.SparkPlanGraphEdge> edges) { throw new RuntimeException(); }
}
