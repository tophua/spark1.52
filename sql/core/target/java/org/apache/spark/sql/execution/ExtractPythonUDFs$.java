package org.apache.spark.sql.execution;
// no position
/**
 * Extracts PythonUDFs from operators, rewriting the query plan so that the UDF can be evaluated
 * alone in a batch.
 * <p>
 * This has the limitation that the input to the Python UDF is not allowed include attributes from
 * multiple child operators.
 */
  class ExtractPythonUDFs$ extends org.apache.spark.sql.catalyst.rules.Rule<org.apache.spark.sql.catalyst.plans.logical.LogicalPlan> {
  /**
   * Static reference to the singleton instance of this Scala object.
   */
  public static final ExtractPythonUDFs$ MODULE$ = null;
  public   ExtractPythonUDFs$ () { throw new RuntimeException(); }
  public  org.apache.spark.sql.catalyst.plans.logical.LogicalPlan apply (org.apache.spark.sql.catalyst.plans.logical.LogicalPlan plan) { throw new RuntimeException(); }
}
