package org.apache.spark.sql;
/**
 * A container for a {@link DataFrame}, used for implicit conversions.
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
