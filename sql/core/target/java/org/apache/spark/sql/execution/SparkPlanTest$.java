package org.apache.spark.sql.execution;
// no position
/**
 * Helper methods for writing tests of individual physical operators.
 * &#x7528;&#x4e8e;&#x7f16;&#x5199;&#x5355;&#x4e2a;&#x7269;&#x7406;&#x8fd0;&#x7b97;&#x7b26;&#x7684;&#x6d4b;&#x8bd5;&#x7684;&#x8f85;&#x52a9;&#x65b9;&#x6cd5;
 */
public  class SparkPlanTest$ implements scala.Serializable {
  /**
   * Static reference to the singleton instance of this Scala object.
   */
  public static final SparkPlanTest$ MODULE$ = null;
  public   SparkPlanTest$ () { throw new RuntimeException(); }
  /**
   * Runs the plan and makes sure the answer matches the result produced by a reference plan.
   * &#x8fd0;&#x884c;&#x8ba1;&#x5212;,&#x5e76;&#x786e;&#x4fdd;&#x7b54;&#x6848;&#x5339;&#x914d;&#x7684;&#x7ed3;&#x679c;&#x6240;&#x4ea7;&#x751f;&#x7684;&#x53c2;&#x8003;&#x8ba1;&#x5212;
   * @param input the input data to be used. &#x4f7f;&#x7528;&#x7684;&#x8f93;&#x5165;&#x6570;&#x636e;
   * @param planFunction a function which accepts the input SparkPlan and uses it to instantiate
   *                     the physical operator that's being tested.
   *                     &#x4e00;&#x4e2a;&#x51fd;&#x6570;&#x63a5;&#x53d7;&#x8f93;&#x5165;sparkplan&#x548c;&#x7528;&#x5b83;&#x6765;&#x5b9e;&#x4f8b;&#x5316;&#x7269;&#x7406;&#x8fd0;&#x7b97;&#x7b26;&#x88ab;&#x68c0;&#x6d4b;
   * @param expectedPlanFunction a function which accepts the input SparkPlan and uses it to
   *                             instantiate a reference implementation of the physical operator
   *                             that's being tested. The result of executing this plan will be
   *                             treated as the source-of-truth for the test.
   * @param sortAnswers (undocumented)
   * @param _sqlContext (undocumented)
   * @return (undocumented)
   */
  public  scala.Option<java.lang.String> checkAnswer (org.apache.spark.sql.DataFrame input, scala.Function1<org.apache.spark.sql.execution.SparkPlan, org.apache.spark.sql.execution.SparkPlan> planFunction, scala.Function1<org.apache.spark.sql.execution.SparkPlan, org.apache.spark.sql.execution.SparkPlan> expectedPlanFunction, boolean sortAnswers, org.apache.spark.sql.SQLContext _sqlContext) { throw new RuntimeException(); }
  /**
   * Runs the plan and makes sure the answer matches the expected result.
   * &#x8fd0;&#x884c;&#x8ba1;&#x5212;,&#x786e;&#x4fdd;&#x7b54;&#x6848;&#x7b26;&#x5408;&#x9884;&#x671f;&#x7684;&#x7ed3;&#x679c;
   * @param input the input data to be used.&#x4f7f;&#x7528;&#x7684;&#x8f93;&#x5165;&#x6570;&#x636e;
   * @param planFunction a function which accepts the input SparkPlan and uses it to instantiate
   *                     the physical operator that's being tested.
   *                     &#x4e00;&#x4e2a;&#x51fd;&#x6570;&#x63a5;&#x53d7;&#x8f93;&#x5165;sparkplan&#x548c;&#x7528;&#x5b83;&#x6765;&#x5b9e;&#x4f8b;&#x5316;&#x6b63;&#x5728;&#x6d4b;&#x8bd5;&#x7684;&#x7269;&#x7406;&#x8fd0;&#x7b97;&#x7b26;
   * @param expectedAnswer the expected result in a {@link Seq} of {@link Row}s.
   * 										&#x9884;&#x671f;&#x7684;&#x7ed3;&#x679c;&#x5728;&#x4e00;&#x4e2a;[&#x5e8f;&#x5217;]&#x7684;&#x7684;[&#x884c;]
   * @param sortAnswers if true, the answers will be sorted by their toString representations prior
   *                    to being compared.
   *                    &#x5982;&#x679c;&#x4e3a;true,&#x7b54;&#x6848;&#x5c06;&#x7531;&#x4ed6;&#x4eec;&#x7684;toString&#x8868;&#x793a;&#x76f8;&#x6bd4;&#x4e4b;&#x524d;&#x7684;&#x6392;&#x5e8f;
   * @param _sqlContext (undocumented)
   * @return (undocumented)
   */
  public  scala.Option<java.lang.String> checkAnswer (scala.collection.Seq<org.apache.spark.sql.DataFrame> input, scala.Function1<scala.collection.Seq<org.apache.spark.sql.execution.SparkPlan>, org.apache.spark.sql.execution.SparkPlan> planFunction, scala.collection.Seq<org.apache.spark.sql.Row> expectedAnswer, boolean sortAnswers, org.apache.spark.sql.SQLContext _sqlContext) { throw new RuntimeException(); }
  private  scala.Option<java.lang.String> compareAnswers (scala.collection.Seq<org.apache.spark.sql.Row> sparkAnswer, scala.collection.Seq<org.apache.spark.sql.Row> expectedAnswer, boolean sort) { throw new RuntimeException(); }
  private  scala.collection.Seq<org.apache.spark.sql.Row> executePlan (org.apache.spark.sql.execution.SparkPlan outputPlan, org.apache.spark.sql.SQLContext _sqlContext) { throw new RuntimeException(); }
}
