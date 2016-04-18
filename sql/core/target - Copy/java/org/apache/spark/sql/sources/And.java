package org.apache.spark.sql.sources;
/**
 * A filter that evaluates to <code>true</code> iff both <code>left</code> or <code>right</code> evaluate to <code>true</code>.
 * <p>
 * @since 1.3.0
 */
public  class And extends org.apache.spark.sql.sources.Filter implements scala.Product, scala.Serializable {
  public  org.apache.spark.sql.sources.Filter left () { throw new RuntimeException(); }
  public  org.apache.spark.sql.sources.Filter right () { throw new RuntimeException(); }
  // not preceding
  public   And (org.apache.spark.sql.sources.Filter left, org.apache.spark.sql.sources.Filter right) { throw new RuntimeException(); }
}
