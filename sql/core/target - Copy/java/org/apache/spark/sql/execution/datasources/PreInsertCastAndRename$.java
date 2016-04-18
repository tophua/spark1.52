package org.apache.spark.sql.execution.datasources;
// no position
/**
 * A rule to do pre-insert data type casting and field renaming. Before we insert into
 * an {@link InsertableRelation}, we will use this rule to make sure that
 * the columns to be inserted have the correct data type and fields have the correct names.
 */
  class PreInsertCastAndRename$ extends org.apache.spark.sql.catalyst.rules.Rule<org.apache.spark.sql.catalyst.plans.logical.LogicalPlan> {
  /**
   * Static reference to the singleton instance of this Scala object.
   */
  public static final PreInsertCastAndRename$ MODULE$ = null;
  public   PreInsertCastAndRename$ () { throw new RuntimeException(); }
  public  org.apache.spark.sql.catalyst.plans.logical.LogicalPlan apply (org.apache.spark.sql.catalyst.plans.logical.LogicalPlan plan) { throw new RuntimeException(); }
  /** If necessary, cast data types and rename fields to the expected types and names. */
  public  org.apache.spark.sql.catalyst.plans.logical.InsertIntoTable castAndRenameChildOutput (org.apache.spark.sql.catalyst.plans.logical.InsertIntoTable insertInto, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> expectedOutput, org.apache.spark.sql.catalyst.plans.logical.LogicalPlan child) { throw new RuntimeException(); }
}
