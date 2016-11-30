package org.apache.spark.sql.sources;
/**
 * A filter that evaluates to <code>true</code> iff the attribute evaluates to
 * a string that starts with <code>value</code>.
 * <p>
 * @since 1.3.1
 */
public  class StringStartsWith extends org.apache.spark.sql.sources.Filter implements scala.Product, scala.Serializable {
  public  java.lang.String attribute () { throw new RuntimeException(); }
  public  java.lang.String value () { throw new RuntimeException(); }
  // not preceding
  public   StringStartsWith (java.lang.String attribute, java.lang.String value) { throw new RuntimeException(); }
}
