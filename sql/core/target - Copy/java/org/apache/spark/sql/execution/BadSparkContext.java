package org.apache.spark.sql.execution;
/**
 * A bad {@link SparkContext} that does not clone the inheritable thread local properties
 * when passing them to children threads.
 */
public  class BadSparkContext extends org.apache.spark.SparkContext {
  public   BadSparkContext (org.apache.spark.SparkConf conf) { throw new RuntimeException(); }
  protected  java.lang.InheritableThreadLocal<java.util.Properties> localProperties () { throw new RuntimeException(); }
}
