package org.apache.spark.sql.execution.datasources.parquet;
/**
 * A test suite that tests basic Parquet I/O.
 * &#x4e00;&#x4e2a;&#x6d4b;&#x8bd5;&#x5957;&#x4ef6;&#x7684;&#x57fa;&#x672c;Parquet I/O
 */
public  class ParquetIOSuite extends org.apache.spark.sql.QueryTest implements org.apache.spark.sql.execution.datasources.parquet.ParquetTest, org.apache.spark.sql.test.SharedSQLContext {
  public   ParquetIOSuite () { throw new RuntimeException(); }
  /**
   * Writes <code>data</code> to a Parquet file, reads it back and check file contents.
   * &#x5199;&#x6570;&#x636e;&#x5230;&#x6587;&#x4ef6;Parquet,&#x8bfb;&#x53d6;&#x5b83;&#x5e76;&#x68c0;&#x67e5;&#x6587;&#x4ef6;&#x5185;&#x5bb9;
   * @param data (undocumented)
   * @param evidence$1 (undocumented)
   * @param evidence$2 (undocumented)
   */
  protected <T extends scala.Product> void checkParquetFile (scala.collection.Seq<T> data, scala.reflect.ClassTag<T> evidence$1, scala.reflect.api.TypeTags.TypeTag<T> evidence$2) { throw new RuntimeException(); }
}
