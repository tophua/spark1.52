package org.apache.spark.sql.sources;
/**
 * A filter that evaluates to <code>true</code> iff the attribute evaluates to a value
 * greater than or equal to <code>value</code>.
 * <p>
 * @since 1.3.0
 */
public  class GreaterThanOrEqual extends org.apache.spark.sql.sources.Filter implements scala.Product, scala.Serializable {
  public  java.lang.String attribute () { throw new RuntimeException(); }
  public  Object value () { throw new RuntimeException(); }
  // not preceding
  public   GreaterThanOrEqual (java.lang.String attribute, Object value) { throw new RuntimeException(); }
}
