package org.apache.spark.sql.execution.datasources;
public  class RefreshTable extends org.apache.spark.sql.catalyst.plans.logical.LogicalPlan implements org.apache.spark.sql.execution.RunnableCommand, scala.Product, scala.Serializable {
  public  org.apache.spark.sql.catalyst.TableIdentifier tableIdent () { throw new RuntimeException(); }
  // not preceding
  public   RefreshTable (org.apache.spark.sql.catalyst.TableIdentifier tableIdent) { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.Row> run (org.apache.spark.sql.SQLContext sqlContext) { throw new RuntimeException(); }
}
