package org.apache.spark.sql.execution;
  class SparkSqlSerializer extends org.apache.spark.serializer.KryoSerializer {
  static public  org.apache.spark.sql.execution.KryoResourcePool resourcePool () { throw new RuntimeException(); }
  static private <O extends java.lang.Object> O acquireRelease (scala.Function1<org.apache.spark.serializer.SerializerInstance, O> fn) { throw new RuntimeException(); }
  static public <T extends java.lang.Object> byte[] serialize (T o, scala.reflect.ClassTag<T> evidence$1) { throw new RuntimeException(); }
  static public <T extends java.lang.Object> T deserialize (byte[] bytes, scala.reflect.ClassTag<T> evidence$2) { throw new RuntimeException(); }
  public   SparkSqlSerializer (org.apache.spark.SparkConf conf) { throw new RuntimeException(); }
  public  com.esotericsoftware.kryo.Kryo newKryo () { throw new RuntimeException(); }
}
