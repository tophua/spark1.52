package org.apache.spark.sql.columnar.compression;
public  class TestCompressibleColumnBuilder<T extends org.apache.spark.sql.types.AtomicType> extends org.apache.spark.sql.columnar.NativeColumnBuilder<T> implements org.apache.spark.sql.columnar.NullableColumnBuilder, org.apache.spark.sql.columnar.compression.CompressibleColumnBuilder<T> {
  static public <T extends org.apache.spark.sql.types.AtomicType> org.apache.spark.sql.columnar.compression.TestCompressibleColumnBuilder<T> apply (org.apache.spark.sql.columnar.ColumnStats columnStats, org.apache.spark.sql.columnar.NativeColumnType<T> columnType, org.apache.spark.sql.columnar.compression.CompressionScheme scheme) { throw new RuntimeException(); }
  public  org.apache.spark.sql.columnar.ColumnStats columnStats () { throw new RuntimeException(); }
  public  org.apache.spark.sql.columnar.NativeColumnType<T> columnType () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.columnar.compression.CompressionScheme> schemes () { throw new RuntimeException(); }
  // not preceding
  public   TestCompressibleColumnBuilder (org.apache.spark.sql.columnar.ColumnStats columnStats, org.apache.spark.sql.columnar.NativeColumnType<T> columnType, scala.collection.Seq<org.apache.spark.sql.columnar.compression.CompressionScheme> schemes) { throw new RuntimeException(); }
  protected  boolean isWorthCompressing (org.apache.spark.sql.columnar.compression.Encoder<T> encoder) { throw new RuntimeException(); }
}
