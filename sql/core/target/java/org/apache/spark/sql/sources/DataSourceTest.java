package org.apache.spark.sql.sources;
 abstract class DataSourceTest extends org.apache.spark.sql.QueryTest {
  public   DataSourceTest () { throw new RuntimeException(); }
  protected abstract  org.apache.spark.sql.SQLContext _sqlContext () ;
  protected  org.apache.spark.sql.SQLContext caseInsensitiveContext () { throw new RuntimeException(); }
  /**
   * expectedAnswer &#x9884;&#x671f;&#x7684;&#x7b54;&#x6848; 
   * @param sqlString (undocumented)
   * @param expectedAnswer (undocumented)
   */
  protected  void sqlTest (java.lang.String sqlString, scala.collection.Seq<org.apache.spark.sql.Row> expectedAnswer) { throw new RuntimeException(); }
}
