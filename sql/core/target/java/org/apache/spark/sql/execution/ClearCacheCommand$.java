package org.apache.spark.sql.execution;
// no position
/**
 * :: DeveloperApi ::
 * Clear all cached data from the in-memory cache.
 */
public  class ClearCacheCommand$ extends org.apache.spark.sql.catalyst.plans.logical.LogicalPlan implements org.apache.spark.sql.execution.RunnableCommand, scala.Product, scala.Serializable {
  /**
   * Static reference to the singleton instance of this Scala object.
   */
  public static final ClearCacheCommand$ MODULE$ = null;
  public   ClearCacheCommand$ () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.Row> run (org.apache.spark.sql.SQLContext sqlContext) { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> output () { throw new RuntimeException(); }
}
