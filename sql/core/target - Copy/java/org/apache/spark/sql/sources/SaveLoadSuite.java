package org.apache.spark.sql.sources;
public  class SaveLoadSuite extends org.apache.spark.sql.sources.DataSourceTest implements org.apache.spark.sql.test.SharedSQLContext, org.scalatest.BeforeAndAfter {
  public   SaveLoadSuite () { throw new RuntimeException(); }
  protected  scala.Function1<java.lang.String, org.apache.spark.sql.DataFrame> sql () { throw new RuntimeException(); }
  private  org.apache.spark.SparkContext sparkContext () { throw new RuntimeException(); }
  private  java.lang.String originalDefaultSource () { throw new RuntimeException(); }
  private  java.io.File path () { throw new RuntimeException(); }
  private  org.apache.spark.sql.DataFrame df () { throw new RuntimeException(); }
  public  void beforeAll () { throw new RuntimeException(); }
  public  void afterAll () { throw new RuntimeException(); }
  public  void checkLoad (org.apache.spark.sql.DataFrame expectedDF, java.lang.String tbl) { throw new RuntimeException(); }
}
