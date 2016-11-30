package org.apache.spark.sql.execution;
/**
 * Optimized version of {@link ExternalSort} that operates on binary data (implemented as part of
 * Project Tungsten).
 * <p>
 * param:  global when true performs a global sort of all partitions by shuffling the data first
 *               if necessary.
 * param:  testSpillFrequency Method for configuring periodic spilling in unit tests. If set, will
 *                           spill every <code>frequency</code> records.
 */
public  class TungstenSort extends org.apache.spark.sql.execution.SparkPlan implements org.apache.spark.sql.execution.UnaryNode, scala.Product, scala.Serializable {
  /**
   * Return true if UnsafeExternalSort can sort rows with the given schema, false otherwise.
   * @param schema (undocumented)
   * @return (undocumented)
   */
  static public  boolean supportsSchema (org.apache.spark.sql.types.StructType schema) { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.SortOrder> sortOrder () { throw new RuntimeException(); }
  public  boolean global () { throw new RuntimeException(); }
  public  org.apache.spark.sql.execution.SparkPlan child () { throw new RuntimeException(); }
  public  int testSpillFrequency () { throw new RuntimeException(); }
  // not preceding
  public   TungstenSort (scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.SortOrder> sortOrder, boolean global, org.apache.spark.sql.execution.SparkPlan child, int testSpillFrequency) { throw new RuntimeException(); }
  public  boolean outputsUnsafeRows () { throw new RuntimeException(); }
  public  boolean canProcessUnsafeRows () { throw new RuntimeException(); }
  public  boolean canProcessSafeRows () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> output () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.SortOrder> outputOrdering () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.plans.physical.Distribution> requiredChildDistribution () { throw new RuntimeException(); }
  protected  org.apache.spark.rdd.RDD<org.apache.spark.sql.catalyst.InternalRow> doExecute () { throw new RuntimeException(); }
}
