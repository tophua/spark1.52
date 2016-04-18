package org.apache.spark.sql;
  class MyDenseVector implements scala.Serializable {
  public  double[] data () { throw new RuntimeException(); }
  // not preceding
  public   MyDenseVector (double[] data) { throw new RuntimeException(); }
  public  boolean equals (Object other) { throw new RuntimeException(); }
}
