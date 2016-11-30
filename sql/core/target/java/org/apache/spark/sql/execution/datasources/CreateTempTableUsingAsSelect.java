package org.apache.spark.sql.execution.datasources;
public  class CreateTempTableUsingAsSelect extends org.apache.spark.sql.catalyst.plans.logical.LogicalPlan implements org.apache.spark.sql.execution.RunnableCommand, scala.Product, scala.Serializable {
  public  org.apache.spark.sql.catalyst.TableIdentifier tableIdent () { throw new RuntimeException(); }
  public  java.lang.String provider () { throw new RuntimeException(); }
  public  java.lang.String[] partitionColumns () { throw new RuntimeException(); }
  public  org.apache.spark.sql.SaveMode mode () { throw new RuntimeException(); }
  public  scala.collection.immutable.Map<java.lang.String, java.lang.String> options () { throw new RuntimeException(); }
  public  org.apache.spark.sql.catalyst.plans.logical.LogicalPlan query () { throw new RuntimeException(); }
  // not preceding
  public   CreateTempTableUsingAsSelect (org.apache.spark.sql.catalyst.TableIdentifier tableIdent, java.lang.String provider, java.lang.String[] partitionColumns, org.apache.spark.sql.SaveMode mode, scala.collection.immutable.Map<java.lang.String, java.lang.String> options, org.apache.spark.sql.catalyst.plans.logical.LogicalPlan query) { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.Row> run (org.apache.spark.sql.SQLContext sqlContext) { throw new RuntimeException(); }
}
