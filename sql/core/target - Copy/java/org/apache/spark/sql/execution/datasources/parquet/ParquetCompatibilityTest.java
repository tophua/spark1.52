package org.apache.spark.sql.execution.datasources.parquet;
/**
 * Helper class for testing Parquet compatibility.
 */
 abstract class ParquetCompatibilityTest extends org.apache.spark.sql.QueryTest implements org.apache.spark.sql.execution.datasources.parquet.ParquetTest {
  static public <T extends java.lang.Object> T makeNullable (int i, scala.Function0<T> f) { throw new RuntimeException(); }
  public   ParquetCompatibilityTest () { throw new RuntimeException(); }
  protected  org.apache.parquet.schema.MessageType readParquetSchema (java.lang.String path) { throw new RuntimeException(); }
  protected  org.apache.parquet.schema.MessageType readParquetSchema (java.lang.String path, scala.Function1<org.apache.hadoop.fs.Path, java.lang.Object> pathFilter) { throw new RuntimeException(); }
  protected  void logParquetSchema (java.lang.String path) { throw new RuntimeException(); }
}
