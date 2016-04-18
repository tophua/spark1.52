package org.apache.spark.sql.columnar;
 final class CustomClass implements scala.Product, scala.Serializable {
  public  int a () { throw new RuntimeException(); }
  public  long b () { throw new RuntimeException(); }
  // not preceding
  public   CustomClass (int a, long b) { throw new RuntimeException(); }
}
