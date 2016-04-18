package org.apache.spark.sql.expressions;
/**
 * :: Experimental ::
 * A window specification that defines the partitioning, ordering, and frame boundaries.
 * <p>
 * Use the static methods in {@link Window} to create a {@link WindowSpec}.
 * <p>
 * @since 1.4.0
 */
public  class WindowSpec {
  /**
   * Defines the partitioning columns in a {@link WindowSpec}.
   * @since 1.4.0
   * @param colName (undocumented)
   * @param colNames (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.expressions.WindowSpec partitionBy (java.lang.String colName, java.lang.String... colNames) { throw new RuntimeException(); }
  /**
   * Defines the partitioning columns in a {@link WindowSpec}.
   * @since 1.4.0
   * @param cols (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.expressions.WindowSpec partitionBy (org.apache.spark.sql.Column... cols) { throw new RuntimeException(); }
  /**
   * Defines the ordering columns in a {@link WindowSpec}.
   * @since 1.4.0
   * @param colName (undocumented)
   * @param colNames (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.expressions.WindowSpec orderBy (java.lang.String colName, java.lang.String... colNames) { throw new RuntimeException(); }
  /**
   * Defines the ordering columns in a {@link WindowSpec}.
   * @since 1.4.0
   * @param cols (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.expressions.WindowSpec orderBy (org.apache.spark.sql.Column... cols) { throw new RuntimeException(); }
  // not preceding
     WindowSpec (scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression> partitionSpec, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.SortOrder> orderSpec, org.apache.spark.sql.catalyst.expressions.WindowFrame frame) { throw new RuntimeException(); }
  /**
   * Defines the partitioning columns in a {@link WindowSpec}.
   * @since 1.4.0
   * @param colName (undocumented)
   * @param colNames (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.expressions.WindowSpec partitionBy (java.lang.String colName, scala.collection.Seq<java.lang.String> colNames) { throw new RuntimeException(); }
  /**
   * Defines the partitioning columns in a {@link WindowSpec}.
   * @since 1.4.0
   * @param cols (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.expressions.WindowSpec partitionBy (scala.collection.Seq<org.apache.spark.sql.Column> cols) { throw new RuntimeException(); }
  /**
   * Defines the ordering columns in a {@link WindowSpec}.
   * @since 1.4.0
   * @param colName (undocumented)
   * @param colNames (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.expressions.WindowSpec orderBy (java.lang.String colName, scala.collection.Seq<java.lang.String> colNames) { throw new RuntimeException(); }
  /**
   * Defines the ordering columns in a {@link WindowSpec}.
   * @since 1.4.0
   * @param cols (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.expressions.WindowSpec orderBy (scala.collection.Seq<org.apache.spark.sql.Column> cols) { throw new RuntimeException(); }
  /**
   * Defines the frame boundaries, from <code>start</code> (inclusive) to <code>end</code> (inclusive).
   * <p>
   * Both <code>start</code> and <code>end</code> are relative positions from the current row. For example, "0" means
   * "current row", while "-1" means the row before the current row, and "5" means the fifth row
   * after the current row.
   * <p>
   * @param start boundary start, inclusive.
   *              The frame is unbounded if this is the minimum long value.
   * @param end boundary end, inclusive.
   *            The frame is unbounded if this is the maximum long value.
   * @since 1.4.0
   * @return (undocumented)
   */
  public  org.apache.spark.sql.expressions.WindowSpec rowsBetween (long start, long end) { throw new RuntimeException(); }
  /**
   * Defines the frame boundaries, from <code>start</code> (inclusive) to <code>end</code> (inclusive).
   * <p>
   * Both <code>start</code> and <code>end</code> are relative from the current row. For example, "0" means "current row",
   * while "-1" means one off before the current row, and "5" means the five off after the
   * current row.
   * <p>
   * @param start boundary start, inclusive.
   *              The frame is unbounded if this is the minimum long value.
   * @param end boundary end, inclusive.
   *            The frame is unbounded if this is the maximum long value.
   * @since 1.4.0
   * @return (undocumented)
   */
  public  org.apache.spark.sql.expressions.WindowSpec rangeBetween (long start, long end) { throw new RuntimeException(); }
  private  org.apache.spark.sql.expressions.WindowSpec between (org.apache.spark.sql.catalyst.expressions.FrameType typ, long start, long end) { throw new RuntimeException(); }
  /**
   * Converts this {@link WindowSpec} into a {@link Column} with an aggregate expression.
   * @param aggregate (undocumented)
   * @return (undocumented)
   */
    org.apache.spark.sql.Column withAggregate (org.apache.spark.sql.Column aggregate) { throw new RuntimeException(); }
}
