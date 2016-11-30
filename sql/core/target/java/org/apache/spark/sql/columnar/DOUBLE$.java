package org.apache.spark.sql.columnar;
// no position
  class DOUBLE$ extends org.apache.spark.sql.columnar.NativeColumnType<org.apache.spark.sql.types.DoubleType$> {
  /**
   * Static reference to the singleton instance of this Scala object.
   */
  public static final DOUBLE$ MODULE$ = null;
  public   DOUBLE$ () { throw new RuntimeException(); }
  public  void append (double v, java.nio.ByteBuffer buffer) { throw new RuntimeException(); }
  public  void append (org.apache.spark.sql.catalyst.InternalRow row, int ordinal, java.nio.ByteBuffer buffer) { throw new RuntimeException(); }
  public  double extract (java.nio.ByteBuffer buffer) { throw new RuntimeException(); }
  public  void extract (java.nio.ByteBuffer buffer, org.apache.spark.sql.catalyst.expressions.MutableRow row, int ordinal) { throw new RuntimeException(); }
  public  void setField (org.apache.spark.sql.catalyst.expressions.MutableRow row, int ordinal, double value) { throw new RuntimeException(); }
  public  double getField (org.apache.spark.sql.catalyst.InternalRow row, int ordinal) { throw new RuntimeException(); }
  public  void copyField (org.apache.spark.sql.catalyst.InternalRow from, int fromOrdinal, org.apache.spark.sql.catalyst.expressions.MutableRow to, int toOrdinal) { throw new RuntimeException(); }
}
