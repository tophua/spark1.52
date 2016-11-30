package org.apache.spark.sql;
public  class FunctionResult implements scala.Product, scala.Serializable {
  public  java.lang.String f1 () { throw new RuntimeException(); }
  public  java.lang.String f2 () { throw new RuntimeException(); }
  // not preceding
  public   FunctionResult (java.lang.String f1, java.lang.String f2) { throw new RuntimeException(); }
}
