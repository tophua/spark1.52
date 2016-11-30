package org.apache.spark.sql.execution.datasources;
public  class CreateTempTableUsing extends org.apache.spark.sql.catalyst.plans.logical.LogicalPlan implements org.apache.spark.sql.execution.RunnableCommand, scala.Product, scala.Serializable {
  public  org.apache.spark.sql.catalyst.TableIdentifier tableIdent () { throw new RuntimeException(); }
  public  scala.Option<org.apache.spark.sql.types.StructType> userSpecifiedSchema () { throw new RuntimeException(); }
  public  java.lang.String provider () { throw new RuntimeException(); }
  public  scala.collection.immutable.Map<java.lang.String, java.lang.String> options () { throw new RuntimeException(); }
  // not preceding
  public   CreateTempTableUsing (org.apache.spark.sql.catalyst.TableIdentifier tableIdent, scala.Option<org.apache.spark.sql.types.StructType> userSpecifiedSchema, java.lang.String provider, scala.collection.immutable.Map<java.lang.String, java.lang.String> options) { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.Row> run (org.apache.spark.sql.SQLContext sqlContext) { throw new RuntimeException(); }
}
