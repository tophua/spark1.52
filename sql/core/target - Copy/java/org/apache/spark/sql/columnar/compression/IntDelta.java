package org.apache.spark.sql.columnar.compression;
// no position
  class IntDelta implements org.apache.spark.sql.columnar.compression.CompressionScheme, scala.Product, scala.Serializable {
  static public  class Encoder implements org.apache.spark.sql.columnar.compression.Encoder<org.apache.spark.sql.types.IntegerType$> {
    public   Encoder () { throw new RuntimeException(); }
    protected  int _compressedSize () { throw new RuntimeException(); }
    protected  int _uncompressedSize () { throw new RuntimeException(); }
    public  int compressedSize () { throw new RuntimeException(); }
    public  int uncompressedSize () { throw new RuntimeException(); }
    private  int prevValue () { throw new RuntimeException(); }
    public  void gatherCompressibilityStats (org.apache.spark.sql.catalyst.InternalRow row, int ordinal) { throw new RuntimeException(); }
    public  java.nio.ByteBuffer compress (java.nio.ByteBuffer from, java.nio.ByteBuffer to) { throw new RuntimeException(); }
  }
  static public  class Decoder implements org.apache.spark.sql.columnar.compression.Decoder<org.apache.spark.sql.types.IntegerType$> {
    public   Decoder (java.nio.ByteBuffer buffer, org.apache.spark.sql.columnar.NativeColumnType<org.apache.spark.sql.types.IntegerType$> columnType) { throw new RuntimeException(); }
    private  int prev () { throw new RuntimeException(); }
    public  boolean hasNext () { throw new RuntimeException(); }
    public  void next (org.apache.spark.sql.catalyst.expressions.MutableRow row, int ordinal) { throw new RuntimeException(); }
  }
  static public  int typeId () { throw new RuntimeException(); }
  static public <T extends org.apache.spark.sql.types.AtomicType> org.apache.spark.sql.columnar.compression.Decoder<T> decoder (java.nio.ByteBuffer buffer, org.apache.spark.sql.columnar.NativeColumnType<T> columnType) { throw new RuntimeException(); }
  static public <T extends org.apache.spark.sql.types.AtomicType> org.apache.spark.sql.columnar.compression.Encoder<T> encoder (org.apache.spark.sql.columnar.NativeColumnType<T> columnType) { throw new RuntimeException(); }
  static public  boolean supports (org.apache.spark.sql.columnar.ColumnType<?> columnType) { throw new RuntimeException(); }
}
