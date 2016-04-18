package org.apache.spark.sql.execution;
/**
 * A command for users to get tables in the given database.
 * If a databaseName is not given, the current database will be used.
 * The syntax of using this command in SQL is:
 * <pre><code>
 *    SHOW TABLES [IN databaseName]
 * </code></pre>
 * :: DeveloperApi ::
 */
public  class ShowTablesCommand extends org.apache.spark.sql.catalyst.plans.logical.LogicalPlan implements org.apache.spark.sql.execution.RunnableCommand, scala.Product, scala.Serializable {
  public  scala.Option<java.lang.String> databaseName () { throw new RuntimeException(); }
  // not preceding
  public   ShowTablesCommand (scala.Option<java.lang.String> databaseName) { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> output () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.Row> run (org.apache.spark.sql.SQLContext sqlContext) { throw new RuntimeException(); }
}
