package org.apache.spark.sql.columnar;
// no position
  class LONG extends org.apache.spark.sql.columnar.NativeColumnType<org.apache.spark.sql.types.LongType$> {
  static public  void append (long v, java.nio.ByteBuffer buffer) { throw new RuntimeException(); }
  static public  void append (org.apache.spark.sql.catalyst.InternalRow row, int ordinal, java.nio.ByteBuffer buffer) { throw new RuntimeException(); }
  static public  long extract (java.nio.ByteBuffer buffer) { throw new RuntimeException(); }
  static public  void extract (java.nio.ByteBuffer buffer, org.apache.spark.sql.catalyst.expressions.MutableRow row, int ordinal) { throw new RuntimeException(); }
  static public  void setField (org.apache.spark.sql.catalyst.expressions.MutableRow row, int ordinal, long value) { throw new RuntimeException(); }
  static public  long getField (org.apache.spark.sql.catalyst.InternalRow row, int ordinal) { throw new RuntimeException(); }
  static public  void copyField (org.apache.spark.sql.catalyst.InternalRow from, int fromOrdinal, org.apache.spark.sql.catalyst.expressions.MutableRow to, int toOrdinal) { throw new RuntimeException(); }
}
