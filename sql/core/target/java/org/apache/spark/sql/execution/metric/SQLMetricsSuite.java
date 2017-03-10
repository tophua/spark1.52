package org.apache.spark.sql.execution.metric;
public  class SQLMetricsSuite extends org.apache.spark.SparkFunSuite implements org.apache.spark.sql.test.SharedSQLContext {
  public   SQLMetricsSuite () { throw new RuntimeException(); }
  /**
   * Call <code>df.collect()</code> and verify if the collected metrics are same as "expectedMetrics".
   * &#x8c03;&#x7528; <code>df.collect()</code>&#x5e76;&#x9a8c;&#x8bc1;&#x6240;&#x6536;&#x96c6;&#x7684;&#x5ea6;&#x91cf;&#x662f;&#x5426;&#x4e0e;expectedMetrics
   * @param df <code>DataFrame</code> to run
   * @param expectedNumOfJobs number of jobs that will run &#x5c06;&#x8fd0;&#x884c;&#x7684;&#x4f5c;&#x4e1a;&#x6570;
   * @param expectedMetrics the expected metrics. The format is &#x9884;&#x671f;&#x6d4b;&#x91cf;
   *                        <code>nodeId -> (operatorName, metric name -> metric value)</code>.
   *                        &#x683c;&#x5f0f;&#x662f;<code> NodeID ->(operatorname,&#x5ea6;&#x91cf;&#x540d;&#x79f0;->&#x5ea6;&#x91cf;&#x503c;)</code>
   */
  private  void testSparkPlanMetrics (org.apache.spark.sql.DataFrame df, int expectedNumOfJobs, scala.collection.immutable.Map<java.lang.Object, scala.Tuple2<java.lang.String, scala.collection.immutable.Map<java.lang.String, java.lang.Object>>> expectedMetrics) { throw new RuntimeException(); }
}
