package org.apache.spark.sql;
/**
 * A collection of implicit methods for converting common Scala objects into {@link DataFrame}s.
 * &#x4e00;&#x4e2a;&#x96c6;&#x5408;&#x9690;&#x5f0f;&#x8f6c;&#x6362;&#x7684;&#x65b9;&#x6cd5;&#x5e38;&#x89c1;&#x7684;Scala&#x5bf9;&#x8c61;&#x8f6c;&#x6362;DataFrame
 * <p>
 */
 abstract class SQLImplicits {
  public   SQLImplicits () { throw new RuntimeException(); }
  protected abstract  org.apache.spark.sql.SQLContext _sqlContext () ;
  /**
   * An implicit conversion that turns a Scala <code>Symbol</code> into a {@link Column}.
   * &#x9690;&#x5f0f;&#x8f6c;&#x6362;,&#x53d8;&#x6210;&#x4e00;&#x4e2a;Scala <code>&#x7b26;&#x53f7;</code>&#x4e3a;[&#x5217;]
   * <p>
   * @since 1.3.0
   * @param s (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.ColumnName symbolToColumn (scala.Symbol s) { throw new RuntimeException(); }
  /**
   * Creates a DataFrame from an RDD of Product (e.g. case classes, tuples).
   * &#x521b;&#x5efa;&#x4e00;&#x4e2a;DataFrame&#x6765;&#x81ea;RDD &#x7ed3;&#x679c;&#x4f8b;&#x5982;(&#x5982;&#x7c7b;&#x3001;&#x5143;&#x7ec4;)
   * @since 1.3.0
   * @param rdd (undocumented)
   * @param evidence$1 (undocumented)
   * @return (undocumented)
   */
  public <A extends scala.Product> org.apache.spark.sql.DataFrameHolder rddToDataFrameHolder (org.apache.spark.rdd.RDD<A> rdd, scala.reflect.api.TypeTags.TypeTag<A> evidence$1) { throw new RuntimeException(); }
  /**
   * Creates a DataFrame from a local Seq of Product.
   * &#x521b;&#x5efa;&#x4e00;&#x4e2a;DataFrame&#x6765;&#x81ea;&#x4e00;&#x4e2a;&#x672c;&#x5730;&#x5e8f;&#x5217;&#x7ed3;&#x679c;
   * @since 1.3.0
   * @param data (undocumented)
   * @param evidence$2 (undocumented)
   * @return (undocumented)
   */
  public <A extends scala.Product> org.apache.spark.sql.DataFrameHolder localSeqToDataFrameHolder (scala.collection.Seq<A> data, scala.reflect.api.TypeTags.TypeTag<A> evidence$2) { throw new RuntimeException(); }
  /**
   * Creates a single column DataFrame from an RDD[Int].
   * &#x4ece;RDD[Int]&#x521b;&#x5efa;&#x4e00;&#x5217;&#x7684;DataFrame
   * @since 1.3.0
   * @param data (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.DataFrameHolder intRddToDataFrameHolder (org.apache.spark.rdd.RDD<java.lang.Object> data) { throw new RuntimeException(); }
  /**
   * Creates a single column DataFrame from an RDD[Long].
   * &#x4ece;RDD[Long]&#x521b;&#x5efa;&#x4e00;&#x5217;&#x7684;DataFrame
   * @since 1.3.0
   * @param data (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.DataFrameHolder longRddToDataFrameHolder (org.apache.spark.rdd.RDD<java.lang.Object> data) { throw new RuntimeException(); }
  /**
   * Creates a single column DataFrame from an RDD[String].
   * &#x4ece;RDD[String]&#x521b;&#x5efa;&#x4e00;&#x5217;&#x7684;DataFrame
   * @since 1.3.0
   * @param data (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.DataFrameHolder stringRddToDataFrameHolder (org.apache.spark.rdd.RDD<java.lang.String> data) { throw new RuntimeException(); }
}
