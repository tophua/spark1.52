package org.apache.spark.sql.columnar.compression;
  interface Encoder<T extends org.apache.spark.sql.types.AtomicType> {
  public  void gatherCompressibilityStats (org.apache.spark.sql.catalyst.InternalRow row, int ordinal) ;
  public  int compressedSize () ;
  public  int uncompressedSize () ;
  public  double compressionRatio () ;
  public  java.nio.ByteBuffer compress (java.nio.ByteBuffer from, java.nio.ByteBuffer to) ;
}
