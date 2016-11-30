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
   * @param input the input data to be used.
   * @param planFunction a function which accepts the input SparkPlan and uses it to instantiate
   *                     the physical operator that's being tested.
   * @param expectedAnswer the expected result in a {@link Seq} of {@link Row}s.
   * @param sortAnswers if true, the answers will be sorted by their toString representations prior
   *                    to being compared.
   */
  protected  void checkAnswer (org.apache.spark.sql.DataFrame input, scala.Function1<org.apache.spark.sql.execution.SparkPlan, org.apache.spark.sql.execution.SparkPlan> planFunction, scala.collection.Seq<org.apache.spark.sql.Row> expectedAnswer, boolean sortAnswers) { throw new RuntimeException(); }
  /**
   * Runs the plan and makes sure the answer matches the expected result.
   * @param left the left input data to be used.
   * @param right the right input data to be used.
   * @param planFunction a function which accepts the input SparkPlan and uses it to instantiate
   *                     the physical operator that's being tested.
   * @param expectedAnswer the expected result in a {@link Seq} of {@link Row}s.
   * @param sortAnswers if true, the answers will be sorted by their toString representations prior
   *                    to being compared.
   */
  protected  void checkAnswer2 (org.apache.spark.sql.DataFrame left, org.apache.spark.sql.DataFrame right, scala.Function2<org.apache.spark.sql.execution.SparkPlan, org.apache.spark.sql.execution.SparkPlan, org.apache.spark.sql.execution.SparkPlan> planFunction, scala.collection.Seq<org.apache.spark.sql.Row> expectedAnswer, boolean sortAnswers) { throw new RuntimeException(); }
  /**
   * Runs the plan and makes sure the answer matches the expected result.
   * @param input the input data to be used.
   * @param planFunction a function which accepts a sequence of input SparkPlans and uses them to
   *                     instantiate the physical operator that's being tested.
   * @param expectedAnswer the expected result in a {@link Seq} of {@link Row}s.
   * @param sortAnswers if true, the answers will be sorted by their toString representations prior
   *                    to being compared.
   */
  protected  void doCheckAnswer (scala.collection.Seq<org.apache.spark.sql.DataFrame> input, scala.Function1<scala.collection.Seq<org.apache.spark.sql.execution.SparkPlan>, org.apache.spark.sql.execution.SparkPlan> planFunction, scala.collection.Seq<org.apache.spark.sql.Row> expectedAnswer, boolean sortAnswers) { throw new RuntimeException(); }
  /**
   * Runs the plan and makes sure the answer matches the result produced by a reference plan.
   * @param input the input data to be used.
   * @param planFunction a function which accepts the input SparkPlan and uses it to instantiate
   *                     the physical operator that's being tested.
   * @param expectedPlanFunction a function which accepts the input SparkPlan and uses it to
   *                             instantiate a reference implementation of the physical operator
   *                             that's being tested. The result of executing this plan will be
   *                             treated as the source-of-truth for the test.
   * @param sortAnswers if true, the answers will be sorted by their toString representations prior
   *                    to being compared.
   */
  protected  void checkThatPlansAgree (org.apache.spark.sql.DataFrame input, scala.Function1<org.apache.spark.sql.execution.SparkPlan, org.apache.spark.sql.execution.SparkPlan> planFunction, scala.Function1<org.apache.spark.sql.execution.SparkPlan, org.apache.spark.sql.execution.SparkPlan> expectedPlanFunction, boolean sortAnswers) { throw new RuntimeException(); }
}
