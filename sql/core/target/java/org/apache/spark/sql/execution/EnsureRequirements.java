package org.apache.spark.sql.execution;
/**
 * Ensures that the {@link org.apache.spark.sql.catalyst.plans.physical.Partitioning Partitioning}
 * of input data meets the
 * {@link org.apache.spark.sql.catalyst.plans.physical.Distribution Distribution} requirements for
 * each operator by inserting {@link Exchange} Operators where required.  Also ensure that the
 * input partition ordering requirements are met.
 */
  class EnsureRequirements extends org.apache.spark.sql.catalyst.rules.Rule<org.apache.spark.sql.execution.SparkPlan> implements scala.Product, scala.Serializable {
  public  org.apache.spark.sql.SQLContext sqlContext () { throw new RuntimeException(); }
  // not preceding
  public   EnsureRequirements (org.apache.spark.sql.SQLContext sqlContext) { throw new RuntimeException(); }
  private  int numPartitions () { throw new RuntimeException(); }
  /**
   * Given a required distribution, returns a partitioning that satisfies that distribution.
   * @param requiredDistribution (undocumented)
   * @return (undocumented)
   */
  private  org.apache.spark.sql.catalyst.plans.physical.Partitioning canonicalPartitioning (org.apache.spark.sql.catalyst.plans.physical.Distribution requiredDistribution) { throw new RuntimeException(); }
  private  org.apache.spark.sql.execution.SparkPlan ensureDistributionAndOrdering (org.apache.spark.sql.execution.SparkPlan operator) { throw new RuntimeException(); }
  public  org.apache.spark.sql.execution.SparkPlan apply (org.apache.spark.sql.execution.SparkPlan plan) { throw new RuntimeException(); }
}
