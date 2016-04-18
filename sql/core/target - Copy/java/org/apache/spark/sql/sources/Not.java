package org.apache.spark.sql.sources;
/**
 * A filter that evaluates to <code>true</code> iff <code>child</code> is evaluated to <code>false</code>.
 * <p>
 * @since 1.3.0
 */
public  class Not extends org.apache.spark.sql.sources.Filter implements scala.Product, scala.Serializable {
  public  org.apache.spark.sql.sources.Filter child () { throw new RuntimeException(); }
  // not preceding
  public   Not (org.apache.spark.sql.sources.Filter child) { throw new RuntimeException(); }
}
