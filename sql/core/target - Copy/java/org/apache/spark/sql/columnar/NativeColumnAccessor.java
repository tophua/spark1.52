package org.apache.spark.sql.columnar;
 abstract class NativeColumnAccessor<T extends org.apache.spark.sql.types.AtomicType> extends org.apache.spark.sql.columnar.BasicColumnAccessor<java.lang.Object> implements org.apache.spark.sql.columnar.NullableColumnAccessor, org.apache.spark.sql.columnar.compression.CompressibleColumnAccessor<T> {
  protected  java.nio.ByteBuffer buffer () { throw new RuntimeException(); }
  protected  org.apache.spark.sql.columnar.NativeColumnType<T> columnType () { throw new RuntimeException(); }
  // not preceding
  public   NativeColumnAccessor (java.nio.ByteBuffer buffer, org.apache.spark.sql.columnar.NativeColumnType<T> columnType) { throw new RuntimeException(); }
}
