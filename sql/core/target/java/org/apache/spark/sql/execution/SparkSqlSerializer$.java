package org.apache.spark.sql.execution;
// no position
  class SparkSqlSerializer$ implements scala.Serializable {
  /**
   * Static reference to the singleton instance of this Scala object.
   */
  public static final SparkSqlSerializer$ MODULE$ = null;
  public   SparkSqlSerializer$ () { throw new RuntimeException(); }
  public  org.apache.spark.sql.execution.KryoResourcePool resourcePool () { throw new RuntimeException(); }
  private <O extends java.lang.Object> O acquireRelease (scala.Function1<org.apache.spark.serializer.SerializerInstance, O> fn) { throw new RuntimeException(); }
  public <T extends java.lang.Object> byte[] serialize (T o, scala.reflect.ClassTag<T> evidence$1) { throw new RuntimeException(); }
  public <T extends java.lang.Object> T deserialize (byte[] bytes, scala.reflect.ClassTag<T> evidence$2) { throw new RuntimeException(); }
}
