package org.apache.spark.sql.columnar;
 abstract class ByteArrayColumnType extends org.apache.spark.sql.columnar.ColumnType<byte[]> {
  public  int typeId () { throw new RuntimeException(); }
  public  int defaultSize () { throw new RuntimeException(); }
  // not preceding
  public   ByteArrayColumnType (int typeId, int defaultSize) { throw new RuntimeException(); }
  public  int actualSize (org.apache.spark.sql.catalyst.InternalRow row, int ordinal) { throw new RuntimeException(); }
  public  void append (byte[] v, java.nio.ByteBuffer buffer) { throw new RuntimeException(); }
  public  byte[] extract (java.nio.ByteBuffer buffer) { throw new RuntimeException(); }
}
