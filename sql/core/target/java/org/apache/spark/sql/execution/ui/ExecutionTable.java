package org.apache.spark.sql.execution.ui;
 abstract class ExecutionTable {
  public   ExecutionTable (org.apache.spark.sql.execution.ui.SQLTab parent, java.lang.String tableId, java.lang.String tableName, long currentTime, scala.collection.Seq<org.apache.spark.sql.execution.ui.SQLExecutionUIData> executionUIDatas, boolean showRunningJobs, boolean showSucceededJobs, boolean showFailedJobs) { throw new RuntimeException(); }
  protected  scala.collection.Seq<java.lang.String> baseHeader () { throw new RuntimeException(); }
  protected abstract  scala.collection.Seq<java.lang.String> header () ;
  protected  scala.collection.Seq<scala.xml.Node> row (long currentTime, org.apache.spark.sql.execution.ui.SQLExecutionUIData executionUIData) { throw new RuntimeException(); }
  private  scala.collection.Seq<scala.xml.Node> descriptionCell (org.apache.spark.sql.execution.ui.SQLExecutionUIData execution) { throw new RuntimeException(); }
  private  scala.collection.Seq<scala.xml.Node> detailCell (java.lang.String physicalPlan) { throw new RuntimeException(); }
  public  scala.collection.Seq<scala.xml.Node> toNodeSeq () { throw new RuntimeException(); }
  private  java.lang.String jobURL (long jobId) { throw new RuntimeException(); }
  private  java.lang.String executionURL (long executionID) { throw new RuntimeException(); }
}
