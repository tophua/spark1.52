package org.apache.spark.sql;
/**
 * :: Experimental ::
 * Holder for experimental methods for the bravest. We make NO guarantee about the stability
 * regarding binary compatibility and source compatibility of methods here.
 * <p>
 * <pre><code>
 *   sqlContext.experimental.extraStrategies += ...
 * </code></pre>
 * <p>
 * @since 1.3.0
 */
public  class ExperimentalMethods {
  protected   ExperimentalMethods (org.apache.spark.sql.SQLContext sqlContext) { throw new RuntimeException(); }
  /**
   * Allows extra strategies to be injected into the query planner at runtime.  Note this API
   * should be consider experimental and is not intended to be stable across releases.
   * <p>
   * @since 1.3.0
   * @return (undocumented)
   */
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.planning.GenericStrategy<org.apache.spark.sql.execution.SparkPlan>> extraStrategies () { throw new RuntimeException(); }
}
