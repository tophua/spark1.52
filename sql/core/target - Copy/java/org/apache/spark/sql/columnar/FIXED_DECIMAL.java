package org.apache.spark.sql.columnar;
  class FIXED_DECIMAL extends org.apache.spark.sql.columnar.NativeColumnType<org.apache.spark.sql.types.DecimalType> implements scala.Product, scala.Serializable {
  static public  int defaultSize () { throw new RuntimeException(); }
  public  int precision () { throw new RuntimeException(); }
  public  int scale () { throw new RuntimeException(); }
  // not preceding
  public   FIXED_DECIMAL (int precision, int scale) { throw new RuntimeException(); }
  public  org.apache.spark.sql.types.Decimal extract (java.nio.ByteBuffer buffer) { throw new RuntimeException(); }
  public  void append (org.apache.spark.sql.types.Decimal v, java.nio.ByteBuffer buffer) { throw new RuntimeException(); }
  public  org.apache.spark.sql.types.Decimal getField (org.apache.spark.sql.catalyst.InternalRow row, int ordinal) { throw new RuntimeException(); }
  public  void setField (org.apache.spark.sql.catalyst.expressions.MutableRow row, int ordinal, org.apache.spark.sql.types.Decimal value) { throw new RuntimeException(); }
  public  void copyField (org.apache.spark.sql.catalyst.InternalRow from, int fromOrdinal, org.apache.spark.sql.catalyst.expressions.MutableRow to, int toOrdinal) { throw new RuntimeException(); }
}
