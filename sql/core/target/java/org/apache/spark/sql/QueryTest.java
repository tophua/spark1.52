package org.apache.spark.sql;
public  class QueryTest extends org.apache.spark.sql.catalyst.plans.PlanTest {
  public   QueryTest () { throw new RuntimeException(); }
  /**
   * Runs the plan and makes sure the answer contains all of the keywords, or the
   * none of keywords are listed in the answer
   * &#x8fd0;&#x884c;&#x8be5;&#x8ba1;&#x5212;,&#x5e76;&#x786e;&#x4fdd;&#x7b54;&#x6848;&#x5305;&#x542b;&#x6240;&#x6709;&#x7684;&#x5173;&#x952e;&#x5b57;
   * @param df the {@link DataFrame} to be executed
   * @param exists true for make sure the keywords are listed in the output, otherwise
   * 				&#x786e;&#x5b9e;&#x4e3a;&#x786e;&#x4fdd;&#x5173;&#x952e;&#x5b57;&#x5728;&#x8f93;&#x51fa;&#x4e2d;&#x5217;&#x51fa;,&#x5426;&#x5219;,&#x786e;&#x4fdd;&#x5173;&#x952e;&#x5b57;&#x6ca1;&#x6709;&#x5728;&#x8f93;&#x51fa;&#x4e2d;&#x5217;&#x51fa;
   *               to make sure none of the keyword are not listed in the output
   * @param keywords keyword in string array &#x5b57;&#x7b26;&#x4e32;&#x6570;&#x7ec4;&#x4e2d;&#x7684;&#x5173;&#x952e;&#x5b57;
   */
  public  void checkExistence (org.apache.spark.sql.DataFrame df, boolean exists, scala.collection.Seq<java.lang.String> keywords) { throw new RuntimeException(); }
  /**
   * Runs the plan and makes sure the answer matches the expected result.
   * &#x8fd0;&#x884c;&#x8be5;&#x8ba1;&#x5212;&#x5e76;&#x786e;&#x4fdd;&#x7b54;&#x6848;&#x4e0e;&#x9884;&#x671f;&#x7ed3;&#x679c;&#x76f8;&#x5339;&#x914d;
   * @param df the {@link DataFrame} to be executed &#x88ab;&#x6267;&#x884c;DataFrame
   * @param expectedAnswer the expected result in a {@link Seq} of {@link Row}s.
   * 											 &#x9884;&#x671f;&#x7684;&#x7ed3;&#x679c;&#x5728;&#x4e00;&#x5e8f;&#x5217;&#x7684;&#x884c;
   */
  protected  void checkAnswer (org.apache.spark.sql.DataFrame df, scala.collection.Seq<org.apache.spark.sql.Row> expectedAnswer) { throw new RuntimeException(); }
  protected  void checkAnswer (org.apache.spark.sql.DataFrame df, org.apache.spark.sql.Row expectedAnswer) { throw new RuntimeException(); }
  protected  void checkAnswer (org.apache.spark.sql.DataFrame df, org.apache.spark.sql.DataFrame expectedAnswer) { throw new RuntimeException(); }
  /**
   * Asserts that a given {@link DataFrame} will be executed using the given number of cached results.
   * &#x65ad;&#x8a00;&#x4e00;&#x4e2a;&#x7ed9;&#x5b9a;&#x7684;DataFrame, &#x5c06;&#x4f7f;&#x7528;&#x7ed9;&#x5b9a;&#x6570;&#x91cf;&#x7684;&#x7f13;&#x5b58;&#x7ed3;&#x679c;&#x6267;&#x884c;
   * <p>
   * @param query (undocumented)
   * @param numCachedTables (undocumented)
   */
  public  void assertCached (org.apache.spark.sql.DataFrame query, int numCachedTables) { throw new RuntimeException(); }
}
