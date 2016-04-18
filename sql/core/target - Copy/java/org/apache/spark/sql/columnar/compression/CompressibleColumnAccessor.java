package org.apache.spark.sql.columnar.compression;
  interface CompressibleColumnAccessor<T extends org.apache.spark.sql.types.AtomicType> extends org.apache.spark.sql.columnar.ColumnAccessor {
  public  org.apache.spark.sql.columnar.compression.Decoder<T> decoder () ;
  public  void initialize () ;
  public  boolean hasNext () ;
  public  void extractSingle (org.apache.spark.sql.catalyst.expressions.MutableRow row, int ordinal) ;
}
