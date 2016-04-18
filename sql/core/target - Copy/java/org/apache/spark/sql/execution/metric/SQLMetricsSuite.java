package org.apache.spark.sql.execution.metric;
public  class SQLMetricsSuite extends org.apache.spark.SparkFunSuite implements org.apache.spark.sql.test.SharedSQLContext {
  public   SQLMetricsSuite () { throw new RuntimeException(); }
  /**
   * Call <code>df.collect()</code> and verify if the collected metrics are same as "expectedMetrics".
   * <p>
   * @param df <code>DataFrame</code> to run
   * @param expectedNumOfJobs number of jobs that will run
   * @param expectedMetrics the expected metrics. The format is
   *                        <code>nodeId -> (operatorName, metric name -> metric value)</code>.
   */
  private  void testSparkPlanMetrics (org.apache.spark.sql.DataFrame df, int expectedNumOfJobs, scala.collection.immutable.Map<java.lang.Object, scala.Tuple2<java.lang.String, scala.collection.immutable.Map<java.lang.String, java.lang.Object>>> expectedMetrics) { throw new RuntimeException(); }
}
