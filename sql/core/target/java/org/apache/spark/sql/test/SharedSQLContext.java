package org.apache.spark.sql.test;
/**
 * Helper trait for SQL test suites where all tests share a single {@link TestSQLContext}.
 */
public  interface SharedSQLContext extends org.apache.spark.sql.test.SQLTestUtils {
  /**
   * The {@link TestSQLContext} to use for all tests in this suite.
   * <p>
   * By default, the underlying {@link org.apache.spark.SparkContext} will be run in local
   * mode with the default test configurations.
   * @return (undocumented)
   */
  public  org.apache.spark.sql.test.TestSQLContext _ctx () ;
  /**
   * The {@link TestSQLContext} to use for all tests in this suite.
   * @return (undocumented)
   */
  public  org.apache.spark.sql.test.TestSQLContext ctx () ;
  public  org.apache.spark.sql.test.TestSQLContext sqlContext () ;
  public  org.apache.spark.sql.SQLContext _sqlContext () ;
  /**
   * Initialize the {@link TestSQLContext}.
   */
  public  void beforeAll () ;
  /**
   * Stop the underlying {@link org.apache.spark.SparkContext}, if any.
   */
  public  void afterAll () ;
  /**
   * Converts $"col name" into an {@link Column}.
   * @since 1.3.0
   */
  public  class StringToColumn {
    public  scala.StringContext sc () { throw new RuntimeException(); }
    // not preceding
    public   StringToColumn (scala.StringContext sc) { throw new RuntimeException(); }
  }
}
