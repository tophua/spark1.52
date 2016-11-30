package org.apache.spark.sql.execution.datasources.parquet;
/**
 * A test suite that tests basic Parquet I/O.
 */
public  class ParquetIOSuite extends org.apache.spark.sql.QueryTest implements org.apache.spark.sql.execution.datasources.parquet.ParquetTest, org.apache.spark.sql.test.SharedSQLContext {
  public   ParquetIOSuite () { throw new RuntimeException(); }
  /**
   * Writes <code>data</code> to a Parquet file, reads it back and check file contents.
   * @param data (undocumented)
   * @param evidence$1 (undocumented)
   * @param evidence$2 (undocumented)
   */
  protected <T extends scala.Product> void checkParquetFile (scala.collection.Seq<T> data, scala.reflect.ClassTag<T> evidence$1, scala.reflect.api.TypeTags.TypeTag<T> evidence$2) { throw new RuntimeException(); }
}
