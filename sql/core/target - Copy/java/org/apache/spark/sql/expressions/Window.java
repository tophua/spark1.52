package org.apache.spark.sql.expressions;
/**
 * :: Experimental ::
 * Utility functions for defining window in DataFrames.
 * <p>
 * <pre><code>
 *   // PARTITION BY country ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
 *   Window.partitionBy("country").orderBy("date").rowsBetween(Long.MinValue, 0)
 *
 *   // PARTITION BY country ORDER BY date ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING
 *   Window.partitionBy("country").orderBy("date").rowsBetween(-3, 3)
 * </code></pre>
 * <p>
 * @since 1.4.0
 */
public  class Window {
  /**
   * Creates a {@link WindowSpec} with the partitioning defined.
   * @since 1.4.0
   * @param colName (undocumented)
   * @param colNames (undocumented)
   * @return (undocumented)
   */
  static public  org.apache.spark.sql.expressions.WindowSpec partitionBy (java.lang.String colName, java.lang.String... colNames) { throw new RuntimeException(); }
  /**
   * Creates a {@link WindowSpec} with the partitioning defined.
   * @since 1.4.0
   * @param cols (undocumented)
   * @return (undocumented)
   */
  static public  org.apache.spark.sql.expressions.WindowSpec partitionBy (org.apache.spark.sql.Column... cols) { throw new RuntimeException(); }
  /**
   * Creates a {@link WindowSpec} with the ordering defined.
   * @since 1.4.0
   * @param colName (undocumented)
   * @param colNames (undocumented)
   * @return (undocumented)
   */
  static public  org.apache.spark.sql.expressions.WindowSpec orderBy (java.lang.String colName, java.lang.String... colNames) { throw new RuntimeException(); }
  /**
   * Creates a {@link WindowSpec} with the ordering defined.
   * @since 1.4.0
   * @param cols (undocumented)
   * @return (undocumented)
   */
  static public  org.apache.spark.sql.expressions.WindowSpec orderBy (org.apache.spark.sql.Column... cols) { throw new RuntimeException(); }
  /**
   * Creates a {@link WindowSpec} with the partitioning defined.
   * @since 1.4.0
   * @param colName (undocumented)
   * @param colNames (undocumented)
   * @return (undocumented)
   */
  static public  org.apache.spark.sql.expressions.WindowSpec partitionBy (java.lang.String colName, scala.collection.Seq<java.lang.String> colNames) { throw new RuntimeException(); }
  /**
   * Creates a {@link WindowSpec} with the partitioning defined.
   * @since 1.4.0
   * @param cols (undocumented)
   * @return (undocumented)
   */
  static public  org.apache.spark.sql.expressions.WindowSpec partitionBy (scala.collection.Seq<org.apache.spark.sql.Column> cols) { throw new RuntimeException(); }
  /**
   * Creates a {@link WindowSpec} with the ordering defined.
   * @since 1.4.0
   * @param colName (undocumented)
   * @param colNames (undocumented)
   * @return (undocumented)
   */
  static public  org.apache.spark.sql.expressions.WindowSpec orderBy (java.lang.String colName, scala.collection.Seq<java.lang.String> colNames) { throw new RuntimeException(); }
  /**
   * Creates a {@link WindowSpec} with the ordering defined.
   * @since 1.4.0
   * @param cols (undocumented)
   * @return (undocumented)
   */
  static public  org.apache.spark.sql.expressions.WindowSpec orderBy (scala.collection.Seq<org.apache.spark.sql.Column> cols) { throw new RuntimeException(); }
  static private  org.apache.spark.sql.expressions.WindowSpec spec () { throw new RuntimeException(); }
  private   Window () { throw new RuntimeException(); }
}
