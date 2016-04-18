package org.apache.spark.sql.test;
/**
 * User-defined type for {@link ExamplePoint}.
 */
  class ExamplePointUDT extends org.apache.spark.sql.types.UserDefinedType<org.apache.spark.sql.test.ExamplePoint> {
  public   ExamplePointUDT () { throw new RuntimeException(); }
  public  org.apache.spark.sql.types.DataType sqlType () { throw new RuntimeException(); }
  public  java.lang.String pyUDT () { throw new RuntimeException(); }
  public  org.apache.spark.sql.types.GenericArrayData serialize (Object obj) { throw new RuntimeException(); }
  public  org.apache.spark.sql.test.ExamplePoint deserialize (Object datum) { throw new RuntimeException(); }
  public  java.lang.Class<org.apache.spark.sql.test.ExamplePoint> userClass () { throw new RuntimeException(); }
    org.apache.spark.sql.test.ExamplePointUDT asNullable () { throw new RuntimeException(); }
}
