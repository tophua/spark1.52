package org.apache.spark.sql.columnar;
  interface NullableColumnAccessor extends org.apache.spark.sql.columnar.ColumnAccessor {
  public  java.nio.ByteBuffer nullsBuffer () ;
  public  int nullCount () ;
  public  int seenNulls () ;
  public  int nextNullIndex () ;
  public  int pos () ;
  public  void initialize () ;
  public  void extractTo (org.apache.spark.sql.catalyst.expressions.MutableRow row, int ordinal) ;
  public  boolean hasNext () ;
}
