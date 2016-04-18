package org.apache.spark.sql.execution.ui;
/**
 * Represent an edge in the SparkPlan tree. <code>fromId</code> is the parent node id, and <code>toId</code> is the child
 * node id.
 */
  class SparkPlanGraphEdge implements scala.Product, scala.Serializable {
  public  long fromId () { throw new RuntimeException(); }
  public  long toId () { throw new RuntimeException(); }
  // not preceding
  public   SparkPlanGraphEdge (long fromId, long toId) { throw new RuntimeException(); }
  public  java.lang.String makeDotEdge () { throw new RuntimeException(); }
}
