package org.apache.spark.sql.execution;
/**
 * used to test close InputStream in UnsafeRowSerializer
 */
public  class ClosableByteArrayInputStream extends java.io.ByteArrayInputStream {
  public   ClosableByteArrayInputStream (byte[] buf) { throw new RuntimeException(); }
  public  boolean closed () { throw new RuntimeException(); }
  public  void close () { throw new RuntimeException(); }
}
