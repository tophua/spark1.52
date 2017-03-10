package org.apache.spark.sql.execution;
/**
 * used to test close InputStream in UnsafeRowSerializer 
 * &#x7528;&#x4e8e;&#x6d4b;&#x8bd5;&#x5728;&#x5173;&#x95ed;&#x8f93;&#x5165;&#x6d41;&#x4e0d;&#x5b89;&#x5168;&#x7684;&#x884c;&#x5e8f;&#x5217;&#x5316;
 * &#x5173;&#x95ed;&#x8f93;&#x5165;&#x6d41;&#x7684;&#x5b57;&#x8282;&#x6570;&#x7ec4;
 */
public  class ClosableByteArrayInputStream extends java.io.ByteArrayInputStream {
  public   ClosableByteArrayInputStream (byte[] buf) { throw new RuntimeException(); }
  public  boolean closed () { throw new RuntimeException(); }
  public  void close () { throw new RuntimeException(); }
}
