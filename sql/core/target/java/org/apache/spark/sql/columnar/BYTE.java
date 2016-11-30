package org.apache.spark.sql.columnar;
// no position
  class BYTE extends org.apache.spark.sql.columnar.NativeColumnType<org.apache.spark.sql.types.ByteType$> {
  static public  void append (byte v, java.nio.ByteBuffer buffer) { throw new RuntimeException(); }
  static public  void append (org.apache.spark.sql.catalyst.InternalRow row, int ordinal, java.nio.ByteBuffer buffer) { throw new RuntimeException(); }
  static public  byte extract (java.nio.ByteBuffer buffer) { throw new RuntimeException(); }
  static public  void extract (java.nio.ByteBuffer buffer, org.apache.spark.sql.catalyst.expressions.MutableRow row, int ordinal) { throw new RuntimeException(); }
  static public  void setField (org.apache.spark.sql.catalyst.expressions.MutableRow row, int ordinal, byte value) { throw new RuntimeException(); }
  static public  byte getField (org.apache.spark.sql.catalyst.InternalRow row, int ordinal) { throw new RuntimeException(); }
  static public  void copyField (org.apache.spark.sql.catalyst.InternalRow from, int fromOrdinal, org.apache.spark.sql.catalyst.expressions.MutableRow to, int toOrdinal) { throw new RuntimeException(); }
}
