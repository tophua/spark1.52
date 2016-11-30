package org.apache.spark.sql;
public  class QueryTest extends org.apache.spark.sql.catalyst.plans.PlanTest {
  public   QueryTest () { throw new RuntimeException(); }
  /**
   * Runs the plan and makes sure the answer contains all of the keywords, or the
   * none of keywords are listed in the answer
   * @param df the {@link DataFrame} to be executed
   * @param exists true for make sure the keywords are listed in the output, otherwise
   *               to make sure none of the keyword are not listed in the output
   * @param keywords keyword in string array
   */
  public  void checkExistence (org.apache.spark.sql.DataFrame df, boolean exists, scala.collection.Seq<java.lang.String> keywords) { throw new RuntimeException(); }
  /**
   * Runs the plan and makes sure the answer matches the expected result.
   * @param df the {@link DataFrame} to be executed
   * @param expectedAnswer the expected result in a {@link Seq} of {@link Row}s.
   */
  protected  void checkAnswer (org.apache.spark.sql.DataFrame df, scala.collection.Seq<org.apache.spark.sql.Row> expectedAnswer) { throw new RuntimeException(); }
  protected  void checkAnswer (org.apache.spark.sql.DataFrame df, org.apache.spark.sql.Row expectedAnswer) { throw new RuntimeException(); }
  protected  void checkAnswer (org.apache.spark.sql.DataFrame df, org.apache.spark.sql.DataFrame expectedAnswer) { throw new RuntimeException(); }
  /**
   * Asserts that a given {@link DataFrame} will be executed using the given number of cached results.
   * @param query (undocumented)
   * @param numCachedTables (undocumented)
   */
  public  void assertCached (org.apache.spark.sql.DataFrame query, int numCachedTables) { throw new RuntimeException(); }
}
