package org.apache.spark.sql.test;
/**
 * An example class to demonstrate UDT in Scala, Java, and Python.
 * param:  x x coordinate
 * param:  y y coordinate
 */
  class ExamplePoint implements scala.Serializable {
  public  double x () { throw new RuntimeException(); }
  public  double y () { throw new RuntimeException(); }
  // not preceding
  public   ExamplePoint (double x, double y) { throw new RuntimeException(); }
}
