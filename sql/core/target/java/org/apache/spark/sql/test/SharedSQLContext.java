package org.apache.spark.sql.test;
/**
 * Helper trait for SQL test suites where all tests share a single {@link TestSQLContext}.
 * SQL&#x7684;&#x6d4b;&#x8bd5;&#x5957;&#x4ef6;,&#x6d4b;&#x8bd5;&#x5171;&#x4eab;&#x4e00;&#x4e2a;&#x5355;&#x4e00;&#x7684;testsqlcontext&#x8f85;&#x52a9;&#x63a5;&#x53e3;
 */
public  interface SharedSQLContext extends org.apache.spark.sql.test.SQLTestUtils {
  /**
   * The {@link TestSQLContext} to use for all tests in this suite.
   * testsqlcontext &#x4f7f;&#x7528;&#x7684;&#x6240;&#x6709;&#x6d4b;&#x8bd5;&#x7684;&#x5957;&#x4ef6;
   * By default, the underlying {@link org.apache.spark.SparkContext} will be run in local
   * mode with the default test configurations.
   * &#x5c06;&#x5728;&#x672c;&#x5730;&#x6a21;&#x5f0f;&#x4e0b;&#x8fd0;&#x884c;,&#x7528;&#x9ed8;&#x8ba4;&#x7684;&#x6d4b;&#x8bd5;&#x914d;&#x7f6e;
   * @return (undocumented)
   */
  public  org.apache.spark.sql.test.TestSQLContext _ctx () ;
  /**
   * The {@link TestSQLContext} to use for all tests in this suite.
   * testsqlcontext&#x7684;&#x4f7f;&#x7528;&#x7684;&#x6240;&#x6709;&#x6d4b;&#x8bd5;&#x5957;&#x4ef6;
   * @return (undocumented)
   */
  public  org.apache.spark.sql.test.TestSQLContext ctx () ;
  public  org.apache.spark.sql.test.TestSQLContext sqlContext () ;
  public  org.apache.spark.sql.SQLContext _sqlContext () ;
  /**
   * Initialize the {@link TestSQLContext}.
   * &#x521d;&#x59cb;&#x5316;TestSQLContext
   */
  public  void beforeAll () ;
  /**
   * Stop the underlying {@link org.apache.spark.SparkContext}, if any.
   * &#x6682;&#x505c;&#x4e0b;&#x5217;SparkContext,&#x5982;&#x679c;&#x4efb;&#x4f55;
   */
  public  void afterAll () ;
  /**
   * Converts $"col name" into an {@link Column}.
   * &#x8f6c;&#x6362;$"col name"&#x5230;{@link Column}
   * @since 1.3.0
   */
  public  class StringToColumn {
    public  scala.StringContext sc () { throw new RuntimeException(); }
    // not preceding
    public   StringToColumn (scala.StringContext sc) { throw new RuntimeException(); }
  }
}
