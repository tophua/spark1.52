package org.apache.spark.sql.execution.ui;
  class ExecutionPage extends org.apache.spark.ui.WebUIPage implements org.apache.spark.Logging {
  public   ExecutionPage (org.apache.spark.sql.execution.ui.SQLTab parent) { throw new RuntimeException(); }
  private  org.apache.spark.sql.execution.ui.SQLListener listener () { throw new RuntimeException(); }
  public  scala.collection.Seq<scala.xml.Node> render (javax.servlet.http.HttpServletRequest request) { throw new RuntimeException(); }
  private  scala.collection.Seq<scala.xml.Node> planVisualizationResources () { throw new RuntimeException(); }
  private  scala.collection.Seq<scala.xml.Node> planVisualization (scala.collection.immutable.Map<java.lang.Object, java.lang.Object> metrics, org.apache.spark.sql.execution.ui.SparkPlanGraph graph) { throw new RuntimeException(); }
  private  java.lang.String jobURL (long jobId) { throw new RuntimeException(); }
  private  scala.collection.Seq<scala.xml.Node> physicalPlanDescription (java.lang.String physicalPlanDescription) { throw new RuntimeException(); }
}
