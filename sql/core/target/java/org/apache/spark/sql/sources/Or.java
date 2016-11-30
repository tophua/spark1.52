package org.apache.spark.sql.sources;
/**
 * A filter that evaluates to <code>true</code> iff at least one of <code>left</code> or <code>right</code> evaluates to <code>true</code>.
 * <p>
 * @since 1.3.0
 */
public  class Or extends org.apache.spark.sql.sources.Filter implements scala.Product, scala.Serializable {
  public  org.apache.spark.sql.sources.Filter left () { throw new RuntimeException(); }
  public  org.apache.spark.sql.sources.Filter right () { throw new RuntimeException(); }
  // not preceding
  public   Or (org.apache.spark.sql.sources.Filter left, org.apache.spark.sql.sources.Filter right) { throw new RuntimeException(); }
}
