package org.apache.spark.sql.execution.datasources.parquet;
// no position
public  class TestingUDT {
  static public  class NestedStruct implements scala.Product, scala.Serializable {
    public  java.lang.Integer a () { throw new RuntimeException(); }
    public  long b () { throw new RuntimeException(); }
    public  double c () { throw new RuntimeException(); }
    // not preceding
    public   NestedStruct (java.lang.Integer a, long b, double c) { throw new RuntimeException(); }
  }
  // no position
  static public  class NestedStruct$ extends scala.runtime.AbstractFunction3<java.lang.Integer, java.lang.Object, java.lang.Object, org.apache.spark.sql.execution.datasources.parquet.TestingUDT.NestedStruct> implements scala.Serializable {
    /**
     * Static reference to the singleton instance of this Scala object.
     */
    public static final NestedStruct$ MODULE$ = null;
    public   NestedStruct$ () { throw new RuntimeException(); }
  }
  static public  class NestedStructUDT extends org.apache.spark.sql.types.UserDefinedType<org.apache.spark.sql.execution.datasources.parquet.TestingUDT.NestedStruct> {
    public   NestedStructUDT () { throw new RuntimeException(); }
    public  org.apache.spark.sql.types.DataType sqlType () { throw new RuntimeException(); }
    public  Object serialize (Object obj) { throw new RuntimeException(); }
    public  java.lang.Class<org.apache.spark.sql.execution.datasources.parquet.TestingUDT.NestedStruct> userClass () { throw new RuntimeException(); }
    public  org.apache.spark.sql.execution.datasources.parquet.TestingUDT.NestedStruct deserialize (Object datum) { throw new RuntimeException(); }
  }
}
