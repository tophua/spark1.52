package org.apache.spark.sql.test;
/**
 * A special {@link SQLContext} prepared for testing.
 * &#x4e00;&#x4e2a;&#x7279;&#x6b8a;&#x7684;sqlcontext&#x51c6;&#x5907;&#x6d4b;&#x8bd5;
 */
  class TestSQLContext extends org.apache.spark.sql.SQLContext {
  public   TestSQLContext (org.apache.spark.SparkContext sc) { throw new RuntimeException(); }
  public   TestSQLContext () { throw new RuntimeException(); }
  protected  org.apache.spark.sql.test.TestSQLContext.SQLSession createSession () { throw new RuntimeException(); }
  /** 
   *  A special {@link SQLSession} that uses fewer shuffle partitions than normal.
   *  &#x4e00;&#x4e2a;&#x7279;&#x6b8a;&#x7684;sqlsession,&#x4f7f;&#x7528;&#x66f4;&#x5c11;&#x7684;&#x6d17;&#x724c;&#x5c06;&#x6bd4;&#x8f83;&#x6b63;&#x5e38;
   *   */
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
