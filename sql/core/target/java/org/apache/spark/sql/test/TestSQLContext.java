package org.apache.spark.sql.test;
/**
 * A special {@link SQLContext} prepared for testing.
 */
  class TestSQLContext extends org.apache.spark.sql.SQLContext {
  public   TestSQLContext (org.apache.spark.SparkContext sc) { throw new RuntimeException(); }
  public   TestSQLContext () { throw new RuntimeException(); }
  protected  org.apache.spark.sql.test.TestSQLContext.SQLSession createSession () { throw new RuntimeException(); }
  /** A special {@link SQLSession} that uses fewer shuffle partitions than normal. */
  protected  class SQLSession extends org.apache.spark.sql.SQLContext.SQLSession {
    public   SQLSession () { throw new RuntimeException(); }
    protected  org.apache.spark.sql.SQLConf conf () { throw new RuntimeException(); }
  }
  public  void loadTestData () { throw new RuntimeException(); }
  // no position
  private  class testData implements org.apache.spark.sql.test.SQLTestData {
    public   testData () { throw new RuntimeException(); }
    protected  org.apache.spark.sql.SQLContext _sqlContext () { throw new RuntimeException(); }
  }
  private  org.apache.spark.sql.test.TestSQLContext.testData$ testData () { throw new RuntimeException(); }
}
