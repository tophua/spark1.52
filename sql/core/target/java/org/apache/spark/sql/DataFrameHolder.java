package org.apache.spark.sql;
/**
 * A container for a {@link DataFrame}, used for implicit conversions.
 * &#x4e00;&#x4e2a;&#x5bb9;&#x5668;&#x4e3a;DataFrame,&#x7528;&#x4e8e;&#x9690;&#x5f0f;&#x8f6c;&#x6362;&#x3002;
 * <p>
 * @since 1.3.0
 */
  class DataFrameHolder implements scala.Product, scala.Serializable {
  public  org.apache.spark.sql.DataFrame df () { throw new RuntimeException(); }
  // not preceding
  public   DataFrameHolder (org.apache.spark.sql.DataFrame df) { throw new RuntimeException(); }
  public  org.apache.spark.sql.DataFrame toDF () { throw new RuntimeException(); }
  public  org.apache.spark.sql.DataFrame toDF (scala.collection.Seq<java.lang.String> colNames) { throw new RuntimeException(); }
}
