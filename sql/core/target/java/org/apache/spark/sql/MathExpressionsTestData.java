package org.apache.spark.sql;
// no position
public  class MathExpressionsTestData {
  static public  class DoubleData implements scala.Product, scala.Serializable {
    public  java.lang.Double a () { throw new RuntimeException(); }
    public  java.lang.Double b () { throw new RuntimeException(); }
    // not preceding
    public   DoubleData (java.lang.Double a, java.lang.Double b) { throw new RuntimeException(); }
  }
  // no position
  static public  class DoubleData$ extends scala.runtime.AbstractFunction2<java.lang.Double, java.lang.Double, org.apache.spark.sql.MathExpressionsTestData.DoubleData> implements scala.Serializable {
    /**
     * Static reference to the singleton instance of this Scala object.
     */
    public static final DoubleData$ MODULE$ = null;
    public   DoubleData$ () { throw new RuntimeException(); }
  }
  static public  class NullDoubles implements scala.Product, scala.Serializable {
    public  java.lang.Double a () { throw new RuntimeException(); }
    // not preceding
    public   NullDoubles (java.lang.Double a) { throw new RuntimeException(); }
  }
  // no position
  static public  class NullDoubles$ extends scala.runtime.AbstractFunction1<java.lang.Double, org.apache.spark.sql.MathExpressionsTestData.NullDoubles> implements scala.Serializable {
    /**
     * Static reference to the singleton instance of this Scala object.
     */
    public static final NullDoubles$ MODULE$ = null;
    public   NullDoubles$ () { throw new RuntimeException(); }
  }
}
