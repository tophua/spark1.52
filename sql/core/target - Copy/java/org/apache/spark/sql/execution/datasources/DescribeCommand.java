package org.apache.spark.sql.execution.datasources;
/**
 * Returned for the "DESCRIBE [EXTENDED] [dbName.]tableName" command.
 * param:  table The table to be described.
 * param:  isExtended True if "DESCRIBE EXTENDED" is used. Otherwise, false.
 *                   It is effective only when the table is a Hive table.
 */
public  class DescribeCommand extends org.apache.spark.sql.catalyst.plans.logical.LogicalPlan implements org.apache.spark.sql.catalyst.plans.logical.Command, scala.Product, scala.Serializable {
  public  org.apache.spark.sql.catalyst.plans.logical.LogicalPlan table () { throw new RuntimeException(); }
  public  boolean isExtended () { throw new RuntimeException(); }
  // not preceding
  public   DescribeCommand (org.apache.spark.sql.catalyst.plans.logical.LogicalPlan table, boolean isExtended) { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.plans.logical.LogicalPlan> children () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> output () { throw new RuntimeException(); }
}
