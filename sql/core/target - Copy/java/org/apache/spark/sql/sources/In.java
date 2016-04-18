package org.apache.spark.sql.sources;
/**
 * A filter that evaluates to <code>true</code> iff the attribute evaluates to one of the values in the array.
 * <p>
 * @since 1.3.0
 */
public  class In extends org.apache.spark.sql.sources.Filter implements scala.Product, scala.Serializable {
  public  java.lang.String attribute () { throw new RuntimeException(); }
  public  java.lang.Object[] values () { throw new RuntimeException(); }
  // not preceding
  public   In (java.lang.String attribute, java.lang.Object[] values) { throw new RuntimeException(); }
}
