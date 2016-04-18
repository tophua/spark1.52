package org.apache.spark.sql.execution.datasources;
/**
 * Inserts the results of <code>query</code> in to a relation that extends {@link InsertableRelation}.
 */
  class InsertIntoDataSource extends org.apache.spark.sql.catalyst.plans.logical.LogicalPlan implements org.apache.spark.sql.execution.RunnableCommand, scala.Product, scala.Serializable {
  public  org.apache.spark.sql.execution.datasources.LogicalRelation logicalRelation () { throw new RuntimeException(); }
  public  org.apache.spark.sql.catalyst.plans.logical.LogicalPlan query () { throw new RuntimeException(); }
  public  boolean overwrite () { throw new RuntimeException(); }
  // not preceding
  public   InsertIntoDataSource (org.apache.spark.sql.execution.datasources.LogicalRelation logicalRelation, org.apache.spark.sql.catalyst.plans.logical.LogicalPlan query, boolean overwrite) { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.Row> run (org.apache.spark.sql.SQLContext sqlContext) { throw new RuntimeException(); }
}
