package org.apache.spark.sql.execution;
/**
 * A bad {@link SparkContext} that does not clone the inheritable thread local properties
 * when passing them to children threads.
 * &#x4e00;&#x4e2a;&#x574f;&#x7684;[sparkcontext]&#x4e0d;&#x514b;&#x9686;&#x9057;&#x4f20;&#x7ebf;&#x7a0b;&#x5c40;&#x90e8;&#x6027;&#x8d28;&#x65f6;,&#x4f20;&#x9012;&#x7ed9;&#x5b50;&#x7684;&#x7ebf;&#x7a0b;,
 */
public  class BadSparkContext extends org.apache.spark.SparkContext {
  public   BadSparkContext (org.apache.spark.SparkConf conf) { throw new RuntimeException(); }
  protected  java.lang.InheritableThreadLocal<java.util.Properties> localProperties () { throw new RuntimeException(); }
}
