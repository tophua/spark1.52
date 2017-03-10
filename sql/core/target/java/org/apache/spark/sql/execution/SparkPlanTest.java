package org.apache.spark.sql.execution;
/**
 * Base class for writing tests for individual physical operators. For an example of how this
 * class's test helper methods can be used, see {@link SortSuite}.
 */
 abstract class SparkPlanTest extends org.apache.spark.SparkFunSuite {
  static private  scala.Option<java.lang.String> compareAnswers (scala.collection.Seq<org.apache.spark.sql.Row> sparkAnswer, scala.collection.Seq<org.apache.spark.sql.Row> expectedAnswer, boolean sort) { throw new RuntimeException(); }
  static private  scala.collection.Seq<org.apache.spark.sql.Row> executePlan (org.apache.spark.sql.execution.SparkPlan outputPlan, org.apache.spark.sql.SQLContext _sqlContext) { throw new RuntimeException(); }
  public   SparkPlanTest () { throw new RuntimeException(); }
  protected abstract  org.apache.spark.sql.SQLContext _sqlContext () ;
  /**
   * Creates a DataFrame from a local Seq of Product.
   * &#x4ece;&#x521b;&#x5efa;&#x4ea7;&#x54c1;&#x5c40;&#x90e8;&#x5e8f;&#x5217;&#x7684;&#x4e00;&#x4e2a;&#x6570;&#x636e;&#x6846;&#x3002;
   * @param data (undocumented)
   * @param evidence$1 (undocumented)
   * @return (undocumented)
   */
  public <A extends scala.Product> org.apache.spark.sql.DataFrameHolder localSeqToDataFrameHolder (scala.collection.Seq<A> data, scala.reflect.api.TypeTags.TypeTag<A> evidence$1) { throw new RuntimeException(); }
  /**
   * Runs the plan and makes sure the answer matches the expected result.
   * &#x8fd0;&#x884c;&#x8ba1;&#x5212;,&#x786e;&#x4fdd;&#x7b54;&#x6848;&#x7b26;&#x5408;&#x9884;&#x671f;&#x7684;&#x7ed3;&#x679c;
   * @param input the input data to be used.&#x4f7f;&#x7528;&#x7684;&#x8f93;&#x5165;&#x6570;&#x636e;
   * @param planFunction a function which accepts the input SparkPlan and uses it to instantiate
   *                     the physical operator that's being tested.
   *                      &#x4e00;&#x4e2a;&#x51fd;&#x6570;&#x63a5;&#x53d7;&#x8f93;&#x5165;&#x7684;Spark&#x8ba1;&#x5212;&#x548c;&#x4f7f;&#x7528;&#x5b83;&#x6765;&#x5b9e;&#x4f8b;&#x5316;&#x6b63;&#x5728;&#x6d4b;&#x8bd5;&#x7684;&#x7269;&#x7406;&#x8fd0;&#x7b97;&#x7b26;
   * @param expectedAnswer the expected result in a {@link Seq} of {@link Row}s.
   * 											&#x9884;&#x671f;&#x7684;&#x7ed3;&#x679c;&#x5728;&#x4e00;&#x4e2a;[&#x5e8f;&#x5217;]&#x7684;&#x7684;[&#x884c;]
   * @param sortAnswers if true, the answers will be sorted by their toString representations prior
   *                    to being compared.
   */
  protected  void checkAnswer (org.apache.spark.sql.DataFrame input, scala.Function1<org.apache.spark.sql.execution.SparkPlan, org.apache.spark.sql.execution.SparkPlan> planFunction, scala.collection.Seq<org.apache.spark.sql.Row> expectedAnswer, boolean sortAnswers) { throw new RuntimeException(); }
  /**
   * Runs the plan and makes sure the answer matches the expected result.
   * &#x8fd0;&#x884c;&#x8ba1;&#x5212;,&#x786e;&#x4fdd;&#x7b54;&#x6848;&#x7b26;&#x5408;&#x9884;&#x671f;&#x7684;&#x7ed3;&#x679c;
   * @param left the left input data to be used. &#x4f7f;&#x7528;&#x7684;&#x5de6;&#x8f93;&#x5165;&#x6570;&#x636e;
   * @param right the right input data to be used.&#x4f7f;&#x7528;&#x7684;&#x53f3;&#x8f93;&#x5165;&#x6570;&#x636e;
   * @param planFunction a function which accepts the input SparkPlan and uses it to instantiate
   *                     the physical operator that's being tested.
   * @param expectedAnswer the expected result in a {@link Seq} of {@link Row}s.
   * @param sortAnswers if true, the answers will be sorted by their toString representations prior
   *                    to being compared.
   */
  protected  void checkAnswer2 (org.apache.spark.sql.DataFrame left, org.apache.spark.sql.DataFrame right, scala.Function2<org.apache.spark.sql.execution.SparkPlan, org.apache.spark.sql.execution.SparkPlan, org.apache.spark.sql.execution.SparkPlan> planFunction, scala.collection.Seq<org.apache.spark.sql.Row> expectedAnswer, boolean sortAnswers) { throw new RuntimeException(); }
  /**
   * Runs the plan and makes sure the answer matches the expected result.
   * &#x8fd0;&#x884c;&#x8ba1;&#x5212;,&#x786e;&#x4fdd;&#x7b54;&#x6848;&#x7b26;&#x5408;&#x9884;&#x671f;&#x7684;&#x7ed3;&#x679c;
   * @param input the input data to be used. &#x4f7f;&#x7528;&#x7684;&#x8f93;&#x5165;&#x6570;&#x636e;
   * @param planFunction a function which accepts a sequence of input SparkPlans and uses them to
   *                     instantiate the physical operator that's being tested.
   *                     &#x4e00;&#x4e2a;&#x51fd;&#x6570;&#x63a5;&#x53d7;&#x4e00;&#x4e2a;&#x8f93;&#x5165;&#x5e8f;&#x5217;Spark&#x8ba1;&#x5212;&#x5e76;&#x4f7f;&#x7528;&#x5b83;&#x4eec;&#x6765;&#x5b9e;&#x4f8b;&#x5316;&#x7269;&#x7406;&#x8fd0;&#x7b97;&#x7b26;&#x88ab;&#x68c0;&#x6d4b;
   * @param expectedAnswer the expected result in a {@link Seq} of {@link Row}s.
   * @param sortAnswers if true, the answers will be sorted by their toString representations prior
   *                    to being compared.
   */
  protected  void doCheckAnswer (scala.collection.Seq<org.apache.spark.sql.DataFrame> input, scala.Function1<scala.collection.Seq<org.apache.spark.sql.execution.SparkPlan>, org.apache.spark.sql.execution.SparkPlan> planFunction, scala.collection.Seq<org.apache.spark.sql.Row> expectedAnswer, boolean sortAnswers) { throw new RuntimeException(); }
  /**
   * Runs the plan and makes sure the answer matches the result produced by a reference plan.
   * &#x8fd0;&#x884c;&#x8ba1;&#x5212;,&#x786e;&#x4fdd;&#x7b54;&#x6848;&#x5339;&#x914d;&#x53c2;&#x8003;&#x8ba1;&#x5212;&#x7684;&#x7ed3;&#x679c;
   * @param input the input data to be used. &#x4f7f;&#x7528;&#x7684;&#x8f93;&#x5165;&#x6570;&#x636e;
   * @param planFunction a function which accepts the input SparkPlan and uses it to instantiate
   *                     the physical operator that's being tested.
   *                     &#x4e00;&#x4e2a;&#x51fd;&#x6570;&#x63a5;&#x53d7;&#x8f93;&#x5165;&#x7684;Spark&#x8ba1;&#x5212;&#x548c;&#x4f7f;&#x7528;&#x5b83;&#x6765;&#x5b9e;&#x4f8b;&#x5316;&#x6b63;&#x5728;&#x6d4b;&#x8bd5;&#x7684;&#x7269;&#x7406;&#x8fd0;&#x7b97;&#x7b26;
   * @param expectedPlanFunction a function which accepts the input SparkPlan and uses it to
   *                             instantiate a reference implementation of the physical operator
   *                             that's being tested. The result of executing this plan will be
   *                             treated as the source-of-truth for the test.
   *                             &#x4e00;&#x4e2a;&#x51fd;&#x6570;&#x63a5;&#x53d7;&#x8f93;&#x5165;sparkplan&#x548c;&#x7528;&#x5b83;&#x6765;&#x5b9e;&#x4f8b;&#x5316;&#x4e00;&#x4e2a;&#x7269;&#x7406;&#x8fd0;&#x7b97;&#x7b26;&#x88ab;&#x68c0;&#x6d4b;&#x7684;&#x53c2;&#x8003;&#x5b9e;&#x73b0;&#x3002;
   *                             &#x6267;&#x884c;&#x6b64;&#x8ba1;&#x5212;&#x7684;&#x7ed3;&#x679c;&#x5c06;&#x88ab;&#x89c6;&#x4e3a;&#x6d4b;&#x8bd5;&#x7684;&#x771f;&#x5b9e;&#x6765;&#x6e90;&#x3002;
   * @param sortAnswers if true, the answers will be sorted by their toString representations prior
   *                    to being compared. &#x5982;&#x679c;&#x4e3a;true,&#x7b54;&#x6848;&#x5c06;&#x7531;&#x4ed6;&#x4eec;&#x7684;toString&#x8868;&#x793a;,&#x76f8;&#x6bd4;&#x4e4b;&#x524d;&#x7684;&#x6392;&#x5e8f;
   */
  protected  void checkThatPlansAgree (org.apache.spark.sql.DataFrame input, scala.Function1<org.apache.spark.sql.execution.SparkPlan, org.apache.spark.sql.execution.SparkPlan> planFunction, scala.Function1<org.apache.spark.sql.execution.SparkPlan, org.apache.spark.sql.execution.SparkPlan> expectedPlanFunction, boolean sortAnswers) { throw new RuntimeException(); }
}
