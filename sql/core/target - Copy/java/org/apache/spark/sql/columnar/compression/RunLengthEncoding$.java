package org.apache.spark.sql.columnar.compression;
// no position
  class RunLengthEncoding$ implements org.apache.spark.sql.columnar.compression.CompressionScheme, scala.Product, scala.Serializable {
  /**
   * Static reference to the singleton instance of this Scala object.
   */
  public static final RunLengthEncoding$ MODULE$ = null;
  public   RunLengthEncoding$ () { throw new RuntimeException(); }
  public  int typeId () { throw new RuntimeException(); }
  public <T extends org.apache.spark.sql.types.AtomicType> org.apache.spark.sql.columnar.compression.RunLengthEncoding.Encoder<T> encoder (org.apache.spark.sql.columnar.NativeColumnType<T> columnType) { throw new RuntimeException(); }
  public <T extends org.apache.spark.sql.types.AtomicType> org.apache.spark.sql.columnar.compression.RunLengthEncoding.Decoder<T> decoder (java.nio.ByteBuffer buffer, org.apache.spark.sql.columnar.NativeColumnType<T> columnType) { throw new RuntimeException(); }
  public  boolean supports (org.apache.spark.sql.columnar.ColumnType<?> columnType) { throw new RuntimeException(); }
}
