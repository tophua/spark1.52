package org.apache.spark.sql.sources;
/**
 * A filter that evaluates to <code>true</code> iff the attribute evaluates to a non-null value.
 * <p>
 * @since 1.3.0
 */
public  class IsNotNull extends org.apache.spark.sql.sources.Filter implements scala.Product, scala.Serializable {
  public  java.lang.String attribute () { throw new RuntimeException(); }
  // not preceding
  public   IsNotNull (java.lang.String attribute) { throw new RuntimeException(); }
}
