package org.apache.spark.sql;
/**
 * A collection of implicit methods for converting common Scala objects into {@link DataFrame}s.
 */
 abstract class SQLImplicits {
  public   SQLImplicits () { throw new RuntimeException(); }
  protected abstract  org.apache.spark.sql.SQLContext _sqlContext () ;
  /**
   * An implicit conversion that turns a Scala <code>Symbol</code> into a {@link Column}.
   * @since 1.3.0
   * @param s (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.ColumnName symbolToColumn (scala.Symbol s) { throw new RuntimeException(); }
  /**
   * Creates a DataFrame from an RDD of Product (e.g. case classes, tuples).
   * @since 1.3.0
   * @param rdd (undocumented)
   * @param evidence$1 (undocumented)
   * @return (undocumented)
   */
  public <A extends scala.Product> org.apache.spark.sql.DataFrameHolder rddToDataFrameHolder (org.apache.spark.rdd.RDD<A> rdd, scala.reflect.api.TypeTags.TypeTag<A> evidence$1) { throw new RuntimeException(); }
  /**
   * Creates a DataFrame from a local Seq of Product.
   * @since 1.3.0
   * @param data (undocumented)
   * @param evidence$2 (undocumented)
   * @return (undocumented)
   */
  public <A extends scala.Product> org.apache.spark.sql.DataFrameHolder localSeqToDataFrameHolder (scala.collection.Seq<A> data, scala.reflect.api.TypeTags.TypeTag<A> evidence$2) { throw new RuntimeException(); }
  /**
   * Creates a single column DataFrame from an RDD[Int].
   * @since 1.3.0
   * @param data (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.DataFrameHolder intRddToDataFrameHolder (org.apache.spark.rdd.RDD<java.lang.Object> data) { throw new RuntimeException(); }
  /**
   * Creates a single column DataFrame from an RDD[Long].
   * @since 1.3.0
   * @param data (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.DataFrameHolder longRddToDataFrameHolder (org.apache.spark.rdd.RDD<java.lang.Object> data) { throw new RuntimeException(); }
  /**
   * Creates a single column DataFrame from an RDD[String].
   * @since 1.3.0
   * @param data (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.DataFrameHolder stringRddToDataFrameHolder (org.apache.spark.rdd.RDD<java.lang.String> data) { throw new RuntimeException(); }
}
