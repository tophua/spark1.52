package org.apache.spark.sql;
// no position
/**
 * This SQLContext object contains utility functions to create a singleton SQLContext instance,
 * or to get the last created SQLContext instance.
 */
public  class SQLContext$ implements scala.Serializable {
  /**
   * Static reference to the singleton instance of this Scala object.
   */
  public static final SQLContext$ MODULE$ = null;
  public   SQLContext$ () { throw new RuntimeException(); }
  private  java.lang.Object INSTANTIATION_LOCK () { throw new RuntimeException(); }
  /**
   * Reference to the last created SQLContext.
   * @return (undocumented)
   */
  private  java.util.concurrent.atomic.AtomicReference<org.apache.spark.sql.SQLContext> lastInstantiatedContext () { throw new RuntimeException(); }
  /**
   * Get the singleton SQLContext if it exists or create a new one using the given SparkContext.
   * This function can be used to create a singleton SQLContext object that can be shared across
   * the JVM.
   * @param sparkContext (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.SQLContext getOrCreate (org.apache.spark.SparkContext sparkContext) { throw new RuntimeException(); }
    void clearLastInstantiatedContext () { throw new RuntimeException(); }
    void setLastInstantiatedContext (org.apache.spark.sql.SQLContext sqlContext) { throw new RuntimeException(); }
}
