package org.apache.spark.sql.execution;
  class KryoResourcePool extends com.twitter.chill.ResourcePool<org.apache.spark.serializer.SerializerInstance> {
  public   KryoResourcePool (int size) { throw new RuntimeException(); }
  public  org.apache.spark.sql.execution.SparkSqlSerializer ser () { throw new RuntimeException(); }
  public  org.apache.spark.serializer.SerializerInstance newInstance () { throw new RuntimeException(); }
}
