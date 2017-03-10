package org.apache.spark.sql.sources;
public  class PrunedScanSuite extends org.apache.spark.sql.sources.DataSourceTest implements org.apache.spark.sql.test.SharedSQLContext {
  public   PrunedScanSuite () { throw new RuntimeException(); }
  protected  scala.Function1<java.lang.String, org.apache.spark.sql.DataFrame> sql () { throw new RuntimeException(); }
  public  void beforeAll () { throw new RuntimeException(); }
  /**
   * &#x6d4b;&#x8bd5;&#x526a;&#x679d;
   * @param sqlString (undocumented)
   * @param expectedColumns (undocumented)
   */
  public  void testPruning (java.lang.String sqlString, scala.collection.Seq<java.lang.String> expectedColumns) { throw new RuntimeException(); }
}
