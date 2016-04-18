package org.apache.spark.sql.execution.datasources.jdbc;
// no position
/**
 * java.sql.DriverManager is always loaded by bootstrap classloader,
 * so it can't load JDBC drivers accessible by Spark ClassLoader.
 * <p>
 * To solve the problem, drivers from user-supplied jars are wrapped into thin wrapper.
 */
public  class DriverRegistry implements org.apache.spark.Logging {
  static private  scala.collection.mutable.Map<java.lang.String, org.apache.spark.sql.execution.datasources.jdbc.DriverWrapper> wrapperMap () { throw new RuntimeException(); }
  static public  void register (java.lang.String className) { throw new RuntimeException(); }
  static public  java.lang.String getDriverClassName (java.lang.String url) { throw new RuntimeException(); }
}
