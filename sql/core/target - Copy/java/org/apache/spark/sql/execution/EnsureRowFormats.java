package org.apache.spark.sql.execution;
// no position
  class EnsureRowFormats extends org.apache.spark.sql.catalyst.rules.Rule<org.apache.spark.sql.execution.SparkPlan> {
  static private  boolean onlyHandlesSafeRows (org.apache.spark.sql.execution.SparkPlan operator) { throw new RuntimeException(); }
  static private  boolean onlyHandlesUnsafeRows (org.apache.spark.sql.execution.SparkPlan operator) { throw new RuntimeException(); }
  static private  boolean handlesBothSafeAndUnsafeRows (org.apache.spark.sql.execution.SparkPlan operator) { throw new RuntimeException(); }
  static public  org.apache.spark.sql.execution.SparkPlan apply (org.apache.spark.sql.execution.SparkPlan operator) { throw new RuntimeException(); }
}
