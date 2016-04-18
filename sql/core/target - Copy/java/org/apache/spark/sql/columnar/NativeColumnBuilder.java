package org.apache.spark.sql.columnar;
 abstract class NativeColumnBuilder<T extends org.apache.spark.sql.types.AtomicType> extends org.apache.spark.sql.columnar.BasicColumnBuilder<java.lang.Object> implements org.apache.spark.sql.columnar.NullableColumnBuilder, org.apache.spark.sql.columnar.compression.AllCompressionSchemes, org.apache.spark.sql.columnar.compression.CompressibleColumnBuilder<T> {
  public  org.apache.spark.sql.columnar.ColumnStats columnStats () { throw new RuntimeException(); }
  public  org.apache.spark.sql.columnar.NativeColumnType<T> columnType () { throw new RuntimeException(); }
  // not preceding
  public   NativeColumnBuilder (org.apache.spark.sql.columnar.ColumnStats columnStats, org.apache.spark.sql.columnar.NativeColumnType<T> columnType) { throw new RuntimeException(); }
}
