package org.apache.spark.sql;
  class MyDenseVectorUDT extends org.apache.spark.sql.types.UserDefinedType<org.apache.spark.sql.MyDenseVector> {
  public   MyDenseVectorUDT () { throw new RuntimeException(); }
  public  org.apache.spark.sql.types.DataType sqlType () { throw new RuntimeException(); }
  public  org.apache.spark.sql.types.ArrayData serialize (Object obj) { throw new RuntimeException(); }
  public  org.apache.spark.sql.MyDenseVector deserialize (Object datum) { throw new RuntimeException(); }
  public  java.lang.Class<org.apache.spark.sql.MyDenseVector> userClass () { throw new RuntimeException(); }
    org.apache.spark.sql.MyDenseVectorUDT asNullable () { throw new RuntimeException(); }
}
