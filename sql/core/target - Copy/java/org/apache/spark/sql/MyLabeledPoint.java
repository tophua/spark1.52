package org.apache.spark.sql;
  class MyLabeledPoint implements scala.Product, scala.Serializable {
  public  double label () { throw new RuntimeException(); }
  public  org.apache.spark.sql.MyDenseVector features () { throw new RuntimeException(); }
  // not preceding
  public   MyLabeledPoint (double label, org.apache.spark.sql.MyDenseVector features) { throw new RuntimeException(); }
  public  double getLabel () { throw new RuntimeException(); }
  public  org.apache.spark.sql.MyDenseVector getFeatures () { throw new RuntimeException(); }
}
