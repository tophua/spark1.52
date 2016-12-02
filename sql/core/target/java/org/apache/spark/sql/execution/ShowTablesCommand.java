package org.apache.spark.sql.execution;
/**
 * A command for users to get tables in the given database.
 * &#x4e00;&#x4e2a;&#x7528;&#x6237;&#x5728;&#x7ed9;&#x5b9a;&#x6570;&#x636e;&#x5e93;&#x4e2d;&#x83b7;&#x53d6;&#x8868;&#x7684;&#x547d;&#x4ee4;
 * If a databaseName is not given, the current database will be used.
 * &#x5982;&#x679c;&#x6ca1;&#x6709;&#x7ed9;&#x51fa;&#x4e00;&#x4e2a;&#x6570;&#x636e;&#x5e93;,&#x5c06;&#x4f7f;&#x7528;&#x5f53;&#x524d;&#x6570;&#x636e;&#x5e93;,
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
