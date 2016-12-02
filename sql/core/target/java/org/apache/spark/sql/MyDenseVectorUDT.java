package org.apache.spark.sql;
/**
 * &#x81ea;&#x5b9a;&#x4e49;&#x6570;&#x636e;&#x7c7b;&#x578b;
 */
  class MyDenseVectorUDT extends org.apache.spark.sql.types.UserDefinedType<org.apache.spark.sql.MyDenseVector> {
  public   MyDenseVectorUDT () { throw new RuntimeException(); }
  public  org.apache.spark.sql.types.DataType sqlType () { throw new RuntimeException(); }
  public  org.apache.spark.sql.types.ArrayData serialize (Object obj) { throw new RuntimeException(); }
  public  org.apache.spark.sql.MyDenseVector deserialize (Object datum) { throw new RuntimeException(); }
  public  java.lang.Class<org.apache.spark.sql.MyDenseVector> userClass () { throw new RuntimeException(); }
    org.apache.spark.sql.MyDenseVectorUDT asNullable () { throw new RuntimeException(); }
}
