package org.apache.spark.sql.execution;
/**
 * A command for users to get the usage of a registered function.
 * &#x547d;&#x4ee4;&#x7528;&#x6237;&#x83b7;&#x53d6;&#x6ce8;&#x518c;&#x51fd;&#x6570;&#x7684;&#x7528;&#x6cd5;
 * The syntax of using this command in SQL is
 * <pre><code>
 *   DESCRIBE FUNCTION [EXTENDED] upper;
 * </code></pre>
 */
public  class DescribeFunction extends org.apache.spark.sql.catalyst.plans.logical.LogicalPlan implements org.apache.spark.sql.execution.RunnableCommand, scala.Product, scala.Serializable {
  public  java.lang.String functionName () { throw new RuntimeException(); }
  public  boolean isExtended () { throw new RuntimeException(); }
  // not preceding
  public   DescribeFunction (java.lang.String functionName, boolean isExtended) { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> output () { throw new RuntimeException(); }
  private  java.lang.String replaceFunctionName (java.lang.String usage, java.lang.String functionName) { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.Row> run (org.apache.spark.sql.SQLContext sqlContext) { throw new RuntimeException(); }
}
