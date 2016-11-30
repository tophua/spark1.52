package org.apache.spark.sql.columnar;
// no position
  class INT$ extends org.apache.spark.sql.columnar.NativeColumnType<org.apache.spark.sql.types.IntegerType$> {
  /**
   * Static reference to the singleton instance of this Scala object.
   */
  public static final INT$ MODULE$ = null;
  public   INT$ () { throw new RuntimeException(); }
  public  void append (int v, java.nio.ByteBuffer buffer) { throw new RuntimeException(); }
  public  void append (org.apache.spark.sql.catalyst.InternalRow row, int ordinal, java.nio.ByteBuffer buffer) { throw new RuntimeException(); }
  public  int extract (java.nio.ByteBuffer buffer) { throw new RuntimeException(); }
  public  void extract (java.nio.ByteBuffer buffer, org.apache.spark.sql.catalyst.expressions.MutableRow row, int ordinal) { throw new RuntimeException(); }
  public  void setField (org.apache.spark.sql.catalyst.expressions.MutableRow row, int ordinal, int value) { throw new RuntimeException(); }
  public  int getField (org.apache.spark.sql.catalyst.InternalRow row, int ordinal) { throw new RuntimeException(); }
  public  void copyField (org.apache.spark.sql.catalyst.InternalRow from, int fromOrdinal, org.apache.spark.sql.catalyst.expressions.MutableRow to, int toOrdinal) { throw new RuntimeException(); }
}
