package org.apache.spark.sql;
// no position
// not preceding
public  class functions$ {
  /**
   * Static reference to the singleton instance of this Scala object.
   */
  public static final functions$ MODULE$ = null;
  /**
   * Aggregate function: returns the number of distinct items in a group.
   * <p>
   * @group agg_funcs
   * @since 1.3.0
   * @param expr (undocumented)
   * @param exprs (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column countDistinct (org.apache.spark.sql.Column expr, org.apache.spark.sql.Column... exprs) { throw new RuntimeException(); }
  /**
   * Aggregate function: returns the number of distinct items in a group.
   * <p>
   * @group agg_funcs
   * @since 1.3.0
   * @param columnName (undocumented)
   * @param columnNames (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column countDistinct (java.lang.String columnName, java.lang.String... columnNames) { throw new RuntimeException(); }
  /**
   * Creates a new array column. The input columns must all have the same data type.
   * <p>
   * @group normal_funcs
   * @since 1.4.0
   * @param cols (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column array (org.apache.spark.sql.Column... cols) { throw new RuntimeException(); }
  /**
   * Returns the first column that is not null, or null if all inputs are null.
   * <p>
   * For example, <code>coalesce(a, b, c)</code> will return a if a is not null,
   * or b if a is null and b is not null, or c if both a and b are null but c is not null.
   * <p>
   * @group normal_funcs
   * @since 1.3.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column coalesce (org.apache.spark.sql.Column... e) { throw new RuntimeException(); }
  /**
   * Creates a new struct column.
   * If the input column is a column in a {@link DataFrame}, or a derived column expression
   * that is named (i.e. aliased), its name would be remained as the StructField's name,
   * otherwise, the newly generated StructField's name would be auto generated as col${index + 1},
   * i.e. col1, col2, col3, ...
   * <p>
   * @group normal_funcs
   * @since 1.4.0
   * @param cols (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column struct (org.apache.spark.sql.Column... cols) { throw new RuntimeException(); }
  /**
   * Returns the greatest value of the list of values, skipping null values.
   * This function takes at least 2 parameters. It will return null iff all parameters are null.
   * <p>
   * @group normal_funcs
   * @since 1.5.0
   * @param exprs (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column greatest (org.apache.spark.sql.Column... exprs) { throw new RuntimeException(); }
  /**
   * Returns the greatest value of the list of column names, skipping null values.
   * This function takes at least 2 parameters. It will return null iff all parameters are null.
   * <p>
   * @group normal_funcs
   * @since 1.5.0
   * @param columnName (undocumented)
   * @param columnNames (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column greatest (java.lang.String columnName, java.lang.String... columnNames) { throw new RuntimeException(); }
  /**
   * Returns the least value of the list of values, skipping null values.
   * This function takes at least 2 parameters. It will return null iff all parameters are null.
   * <p>
   * @group normal_funcs
   * @since 1.5.0
   * @param exprs (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column least (org.apache.spark.sql.Column... exprs) { throw new RuntimeException(); }
  /**
   * Returns the least value of the list of column names, skipping null values.
   * This function takes at least 2 parameters. It will return null iff all parameters are null.
   * <p>
   * @group normal_funcs
   * @since 1.5.0
   * @param columnName (undocumented)
   * @param columnNames (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column least (java.lang.String columnName, java.lang.String... columnNames) { throw new RuntimeException(); }
  /**
   * Concatenates multiple input string columns together into a single string column.
   * <p>
   * @group string_funcs
   * @since 1.5.0
   * @param exprs (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column concat (org.apache.spark.sql.Column... exprs) { throw new RuntimeException(); }
  /**
   * Concatenates multiple input string columns together into a single string column,
   * using the given separator.
   * <p>
   * @group string_funcs
   * @since 1.5.0
   * @param sep (undocumented)
   * @param exprs (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column concat_ws (java.lang.String sep, org.apache.spark.sql.Column... exprs) { throw new RuntimeException(); }
  /**
   * Formats the arguments in printf-style and returns the result as a string column.
   * <p>
   * @group string_funcs
   * @since 1.5.0
   * @param format (undocumented)
   * @param arguments (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column format_string (java.lang.String format, org.apache.spark.sql.Column... arguments) { throw new RuntimeException(); }
  /**
   * Call an user-defined function.
   * Example:
   * <pre><code>
   *  import org.apache.spark.sql._
   *
   *  val df = Seq(("id1", 1), ("id2", 4), ("id3", 5)).toDF("id", "value")
   *  val sqlContext = df.sqlContext
   *  sqlContext.udf.register("simpleUDF", (v: Int) =&gt; v * v)
   *  df.select($"id", callUDF("simpleUDF", $"value"))
   * </code></pre>
   * <p>
   * @group udf_funcs
   * @since 1.5.0
   * @param udfName (undocumented)
   * @param cols (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column callUDF (java.lang.String udfName, org.apache.spark.sql.Column... cols) { throw new RuntimeException(); }
  public   functions$ () { throw new RuntimeException(); }
  private  org.apache.spark.sql.Column toColumn (org.apache.spark.sql.catalyst.expressions.Expression expr) { throw new RuntimeException(); }
  /**
   * Returns a {@link Column} based on the given column name.
   * <p>
   * @group normal_funcs
   * @since 1.3.0
   * @param colName (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column col (java.lang.String colName) { throw new RuntimeException(); }
  /**
   * Returns a {@link Column} based on the given column name. Alias of {@link col}.
   * <p>
   * @group normal_funcs
   * @since 1.3.0
   * @param colName (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column column (java.lang.String colName) { throw new RuntimeException(); }
  /**
   * Creates a {@link Column} of literal value.
   * <p>
   * The passed in object is returned directly if it is already a {@link Column}.
   * If the object is a Scala Symbol, it is converted into a {@link Column} also.
   * Otherwise, a new {@link Column} is created to represent the literal value.
   * <p>
   * @group normal_funcs
   * @since 1.3.0
   * @param literal (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column lit (Object literal) { throw new RuntimeException(); }
  /**
   * Returns a sort expression based on ascending order of the column.
   * <pre><code>
   *   // Sort by dept in ascending order, and then age in descending order.
   *   df.sort(asc("dept"), desc("age"))
   * </code></pre>
   * <p>
   * @group sort_funcs
   * @since 1.3.0
   * @param columnName (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column asc (java.lang.String columnName) { throw new RuntimeException(); }
  /**
   * Returns a sort expression based on the descending order of the column.
   * <pre><code>
   *   // Sort by dept in ascending order, and then age in descending order.
   *   df.sort(asc("dept"), desc("age"))
   * </code></pre>
   * <p>
   * @group sort_funcs
   * @since 1.3.0
   * @param columnName (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column desc (java.lang.String columnName) { throw new RuntimeException(); }
  /**
   * Aggregate function: returns the approximate number of distinct items in a group.
   * <p>
   * @group agg_funcs
   * @since 1.3.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column approxCountDistinct (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Aggregate function: returns the approximate number of distinct items in a group.
   * <p>
   * @group agg_funcs
   * @since 1.3.0
   * @param columnName (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column approxCountDistinct (java.lang.String columnName) { throw new RuntimeException(); }
  /**
   * Aggregate function: returns the approximate number of distinct items in a group.
   * <p>
   * @group agg_funcs
   * @since 1.3.0
   * @param e (undocumented)
   * @param rsd (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column approxCountDistinct (org.apache.spark.sql.Column e, double rsd) { throw new RuntimeException(); }
  /**
   * Aggregate function: returns the approximate number of distinct items in a group.
   * <p>
   * @group agg_funcs
   * @since 1.3.0
   * @param columnName (undocumented)
   * @param rsd (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column approxCountDistinct (java.lang.String columnName, double rsd) { throw new RuntimeException(); }
  /**
   * Aggregate function: returns the average of the values in a group.
   * <p>
   * @group agg_funcs
   * @since 1.3.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column avg (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Aggregate function: returns the average of the values in a group.
   * <p>
   * @group agg_funcs
   * @since 1.3.0
   * @param columnName (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column avg (java.lang.String columnName) { throw new RuntimeException(); }
  /**
   * Aggregate function: returns the number of items in a group.
   * <p>
   * @group agg_funcs
   * @since 1.3.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column count (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Aggregate function: returns the number of items in a group.
   * <p>
   * @group agg_funcs
   * @since 1.3.0
   * @param columnName (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column count (java.lang.String columnName) { throw new RuntimeException(); }
  /**
   * Aggregate function: returns the number of distinct items in a group.
   * <p>
   * @group agg_funcs
   * @since 1.3.0
   * @param expr (undocumented)
   * @param exprs (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column countDistinct (org.apache.spark.sql.Column expr, scala.collection.Seq<org.apache.spark.sql.Column> exprs) { throw new RuntimeException(); }
  /**
   * Aggregate function: returns the number of distinct items in a group.
   * <p>
   * @group agg_funcs
   * @since 1.3.0
   * @param columnName (undocumented)
   * @param columnNames (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column countDistinct (java.lang.String columnName, scala.collection.Seq<java.lang.String> columnNames) { throw new RuntimeException(); }
  /**
   * Aggregate function: returns the first value in a group.
   * <p>
   * @group agg_funcs
   * @since 1.3.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column first (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Aggregate function: returns the first value of a column in a group.
   * <p>
   * @group agg_funcs
   * @since 1.3.0
   * @param columnName (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column first (java.lang.String columnName) { throw new RuntimeException(); }
  /**
   * Aggregate function: returns the last value in a group.
   * <p>
   * @group agg_funcs
   * @since 1.3.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column last (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Aggregate function: returns the last value of the column in a group.
   * <p>
   * @group agg_funcs
   * @since 1.3.0
   * @param columnName (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column last (java.lang.String columnName) { throw new RuntimeException(); }
  /**
   * Aggregate function: returns the maximum value of the expression in a group.
   * <p>
   * @group agg_funcs
   * @since 1.3.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column max (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Aggregate function: returns the maximum value of the column in a group.
   * <p>
   * @group agg_funcs
   * @since 1.3.0
   * @param columnName (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column max (java.lang.String columnName) { throw new RuntimeException(); }
  /**
   * Aggregate function: returns the average of the values in a group.
   * Alias for avg.
   * <p>
   * @group agg_funcs
   * @since 1.4.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column mean (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Aggregate function: returns the average of the values in a group.
   * Alias for avg.
   * <p>
   * @group agg_funcs
   * @since 1.4.0
   * @param columnName (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column mean (java.lang.String columnName) { throw new RuntimeException(); }
  /**
   * Aggregate function: returns the minimum value of the expression in a group.
   * <p>
   * @group agg_funcs
   * @since 1.3.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column min (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Aggregate function: returns the minimum value of the column in a group.
   * <p>
   * @group agg_funcs
   * @since 1.3.0
   * @param columnName (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column min (java.lang.String columnName) { throw new RuntimeException(); }
  /**
   * Aggregate function: returns the sum of all values in the expression.
   * <p>
   * @group agg_funcs
   * @since 1.3.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column sum (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Aggregate function: returns the sum of all values in the given column.
   * <p>
   * @group agg_funcs
   * @since 1.3.0
   * @param columnName (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column sum (java.lang.String columnName) { throw new RuntimeException(); }
  /**
   * Aggregate function: returns the sum of distinct values in the expression.
   * <p>
   * @group agg_funcs
   * @since 1.3.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column sumDistinct (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Aggregate function: returns the sum of distinct values in the expression.
   * <p>
   * @group agg_funcs
   * @since 1.3.0
   * @param columnName (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column sumDistinct (java.lang.String columnName) { throw new RuntimeException(); }
  /**
   * Window function: returns the cumulative distribution of values within a window partition,
   * i.e. the fraction of rows that are below the current row.
   * <p>
   * <pre><code>
   *   N = total number of rows in the partition
   *   cumeDist(x) = number of values before (and including) x / N
   * </code></pre>
   * <p>
   * This is equivalent to the CUME_DIST function in SQL.
   * <p>
   * @group window_funcs
   * @since 1.4.0
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column cumeDist () { throw new RuntimeException(); }
  /**
   * Window function: returns the rank of rows within a window partition, without any gaps.
   * <p>
   * The difference between rank and denseRank is that denseRank leaves no gaps in ranking
   * sequence when there are ties. That is, if you were ranking a competition using denseRank
   * and had three people tie for second place, you would say that all three were in second
   * place and that the next person came in third.
   * <p>
   * This is equivalent to the DENSE_RANK function in SQL.
   * <p>
   * @group window_funcs
   * @since 1.4.0
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column denseRank () { throw new RuntimeException(); }
  /**
   * Window function: returns the value that is <code>offset</code> rows before the current row, and
   * <code>null</code> if there is less than <code>offset</code> rows before the current row. For example,
   * an <code>offset</code> of one will return the previous row at any given point in the window partition.
   * <p>
   * This is equivalent to the LAG function in SQL.
   * <p>
   * @group window_funcs
   * @since 1.4.0
   * @param e (undocumented)
   * @param offset (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column lag (org.apache.spark.sql.Column e, int offset) { throw new RuntimeException(); }
  /**
   * Window function: returns the value that is <code>offset</code> rows before the current row, and
   * <code>null</code> if there is less than <code>offset</code> rows before the current row. For example,
   * an <code>offset</code> of one will return the previous row at any given point in the window partition.
   * <p>
   * This is equivalent to the LAG function in SQL.
   * <p>
   * @group window_funcs
   * @since 1.4.0
   * @param columnName (undocumented)
   * @param offset (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column lag (java.lang.String columnName, int offset) { throw new RuntimeException(); }
  /**
   * Window function: returns the value that is <code>offset</code> rows before the current row, and
   * <code>defaultValue</code> if there is less than <code>offset</code> rows before the current row. For example,
   * an <code>offset</code> of one will return the previous row at any given point in the window partition.
   * <p>
   * This is equivalent to the LAG function in SQL.
   * <p>
   * @group window_funcs
   * @since 1.4.0
   * @param columnName (undocumented)
   * @param offset (undocumented)
   * @param defaultValue (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column lag (java.lang.String columnName, int offset, Object defaultValue) { throw new RuntimeException(); }
  /**
   * Window function: returns the value that is <code>offset</code> rows before the current row, and
   * <code>defaultValue</code> if there is less than <code>offset</code> rows before the current row. For example,
   * an <code>offset</code> of one will return the previous row at any given point in the window partition.
   * <p>
   * This is equivalent to the LAG function in SQL.
   * <p>
   * @group window_funcs
   * @since 1.4.0
   * @param e (undocumented)
   * @param offset (undocumented)
   * @param defaultValue (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column lag (org.apache.spark.sql.Column e, int offset, Object defaultValue) { throw new RuntimeException(); }
  /**
   * Window function: returns the value that is <code>offset</code> rows after the current row, and
   * <code>null</code> if there is less than <code>offset</code> rows after the current row. For example,
   * an <code>offset</code> of one will return the next row at any given point in the window partition.
   * <p>
   * This is equivalent to the LEAD function in SQL.
   * <p>
   * @group window_funcs
   * @since 1.4.0
   * @param columnName (undocumented)
   * @param offset (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column lead (java.lang.String columnName, int offset) { throw new RuntimeException(); }
  /**
   * Window function: returns the value that is <code>offset</code> rows after the current row, and
   * <code>null</code> if there is less than <code>offset</code> rows after the current row. For example,
   * an <code>offset</code> of one will return the next row at any given point in the window partition.
   * <p>
   * This is equivalent to the LEAD function in SQL.
   * <p>
   * @group window_funcs
   * @since 1.4.0
   * @param e (undocumented)
   * @param offset (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column lead (org.apache.spark.sql.Column e, int offset) { throw new RuntimeException(); }
  /**
   * Window function: returns the value that is <code>offset</code> rows after the current row, and
   * <code>defaultValue</code> if there is less than <code>offset</code> rows after the current row. For example,
   * an <code>offset</code> of one will return the next row at any given point in the window partition.
   * <p>
   * This is equivalent to the LEAD function in SQL.
   * <p>
   * @group window_funcs
   * @since 1.4.0
   * @param columnName (undocumented)
   * @param offset (undocumented)
   * @param defaultValue (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column lead (java.lang.String columnName, int offset, Object defaultValue) { throw new RuntimeException(); }
  /**
   * Window function: returns the value that is <code>offset</code> rows after the current row, and
   * <code>defaultValue</code> if there is less than <code>offset</code> rows after the current row. For example,
   * an <code>offset</code> of one will return the next row at any given point in the window partition.
   * <p>
   * This is equivalent to the LEAD function in SQL.
   * <p>
   * @group window_funcs
   * @since 1.4.0
   * @param e (undocumented)
   * @param offset (undocumented)
   * @param defaultValue (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column lead (org.apache.spark.sql.Column e, int offset, Object defaultValue) { throw new RuntimeException(); }
  /**
   * Window function: returns the ntile group id (from 1 to <code>n</code> inclusive) in an ordered window
   * partition. Fow example, if <code>n</code> is 4, the first quarter of the rows will get value 1, the second
   * quarter will get 2, the third quarter will get 3, and the last quarter will get 4.
   * <p>
   * This is equivalent to the NTILE function in SQL.
   * <p>
   * @group window_funcs
   * @since 1.4.0
   * @param n (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column ntile (int n) { throw new RuntimeException(); }
  /**
   * Window function: returns the relative rank (i.e. percentile) of rows within a window partition.
   * <p>
   * This is computed by:
   * <pre><code>
   *   (rank of row in its partition - 1) / (number of rows in the partition - 1)
   * </code></pre>
   * <p>
   * This is equivalent to the PERCENT_RANK function in SQL.
   * <p>
   * @group window_funcs
   * @since 1.4.0
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column percentRank () { throw new RuntimeException(); }
  /**
   * Window function: returns the rank of rows within a window partition.
   * <p>
   * The difference between rank and denseRank is that denseRank leaves no gaps in ranking
   * sequence when there are ties. That is, if you were ranking a competition using denseRank
   * and had three people tie for second place, you would say that all three were in second
   * place and that the next person came in third.
   * <p>
   * This is equivalent to the RANK function in SQL.
   * <p>
   * @group window_funcs
   * @since 1.4.0
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column rank () { throw new RuntimeException(); }
  /**
   * Window function: returns a sequential number starting at 1 within a window partition.
   * <p>
   * This is equivalent to the ROW_NUMBER function in SQL.
   * <p>
   * @group window_funcs
   * @since 1.4.0
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column rowNumber () { throw new RuntimeException(); }
  /**
   * Computes the absolute value.
   * <p>
   * @group normal_funcs
   * @since 1.3.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column abs (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Creates a new array column. The input columns must all have the same data type.
   * <p>
   * @group normal_funcs
   * @since 1.4.0
   * @param cols (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column array (scala.collection.Seq<org.apache.spark.sql.Column> cols) { throw new RuntimeException(); }
  /**
   * Creates a new array column. The input columns must all have the same data type.
   * <p>
   * @group normal_funcs
   * @since 1.4.0
   * @param colName (undocumented)
   * @param colNames (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column array (java.lang.String colName, scala.collection.Seq<java.lang.String> colNames) { throw new RuntimeException(); }
  /**
   * Marks a DataFrame as small enough for use in broadcast joins.
   * <p>
   * The following example marks the right DataFrame for broadcast hash join using <code>joinKey</code>.
   * <pre><code>
   *   // left and right are DataFrames
   *   left.join(broadcast(right), "joinKey")
   * </code></pre>
   * <p>
   * @group normal_funcs
   * @since 1.5.0
   * @param df (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.DataFrame broadcast (org.apache.spark.sql.DataFrame df) { throw new RuntimeException(); }
  /**
   * Returns the first column that is not null, or null if all inputs are null.
   * <p>
   * For example, <code>coalesce(a, b, c)</code> will return a if a is not null,
   * or b if a is null and b is not null, or c if both a and b are null but c is not null.
   * <p>
   * @group normal_funcs
   * @since 1.3.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column coalesce (scala.collection.Seq<org.apache.spark.sql.Column> e) { throw new RuntimeException(); }
  /**
   * Creates a string column for the file name of the current Spark task.
   * <p>
   * @group normal_funcs
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column inputFileName () { throw new RuntimeException(); }
  /**
   * Return true iff the column is NaN.
   * <p>
   * @group normal_funcs
   * @since 1.5.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column isNaN (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * A column expression that generates monotonically increasing 64-bit integers.
   * <p>
   * The generated ID is guaranteed to be monotonically increasing and unique, but not consecutive.
   * The current implementation puts the partition ID in the upper 31 bits, and the record number
   * within each partition in the lower 33 bits. The assumption is that the data frame has
   * less than 1 billion partitions, and each partition has less than 8 billion records.
   * <p>
   * As an example, consider a {@link DataFrame} with two partitions, each with 3 records.
   * This expression would return the following IDs:
   * 0, 1, 2, 8589934592 (1L << 33), 8589934593, 8589934594.
   * <p>
   * @group normal_funcs
   * @since 1.4.0
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column monotonicallyIncreasingId () { throw new RuntimeException(); }
  /**
   * Returns col1 if it is not NaN, or col2 if col1 is NaN.
   * <p>
   * Both inputs should be floating point columns (DoubleType or FloatType).
   * <p>
   * @group normal_funcs
   * @since 1.5.0
   * @param col1 (undocumented)
   * @param col2 (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column nanvl (org.apache.spark.sql.Column col1, org.apache.spark.sql.Column col2) { throw new RuntimeException(); }
  /**
   * Unary minus, i.e. negate the expression.
   * <pre><code>
   *   // Select the amount column and negates all values.
   *   // Scala:
   *   df.select( -df("amount") )
   *
   *   // Java:
   *   df.select( negate(df.col("amount")) );
   * </code></pre>
   * <p>
   * @group normal_funcs
   * @since 1.3.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column negate (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Inversion of boolean expression, i.e. NOT.
   * <pre><code>
   *   // Scala: select rows that are not active (isActive === false)
   *   df.filter( !df("isActive") )
   *
   *   // Java:
   *   df.filter( not(df.col("isActive")) );
   * </code></pre>
   * <p>
   * @group normal_funcs
   * @since 1.3.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column not (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Generate a random column with i.i.d. samples from U[0.0, 1.0].
   * <p>
   * @group normal_funcs
   * @since 1.4.0
   * @param seed (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column rand (long seed) { throw new RuntimeException(); }
  /**
   * Generate a random column with i.i.d. samples from U[0.0, 1.0].
   * <p>
   * @group normal_funcs
   * @since 1.4.0
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column rand () { throw new RuntimeException(); }
  /**
   * Generate a column with i.i.d. samples from the standard normal distribution.
   * <p>
   * @group normal_funcs
   * @since 1.4.0
   * @param seed (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column randn (long seed) { throw new RuntimeException(); }
  /**
   * Generate a column with i.i.d. samples from the standard normal distribution.
   * <p>
   * @group normal_funcs
   * @since 1.4.0
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column randn () { throw new RuntimeException(); }
  /**
   * Partition ID of the Spark task.
   * <p>
   * Note that this is indeterministic because it depends on data partitioning and task scheduling.
   * <p>
   * @group normal_funcs
   * @since 1.4.0
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column sparkPartitionId () { throw new RuntimeException(); }
  /**
   * Computes the square root of the specified float value.
   * <p>
   * @group math_funcs
   * @since 1.3.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column sqrt (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Computes the square root of the specified float value.
   * <p>
   * @group math_funcs
   * @since 1.5.0
   * @param colName (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column sqrt (java.lang.String colName) { throw new RuntimeException(); }
  /**
   * Creates a new struct column.
   * If the input column is a column in a {@link DataFrame}, or a derived column expression
   * that is named (i.e. aliased), its name would be remained as the StructField's name,
   * otherwise, the newly generated StructField's name would be auto generated as col${index + 1},
   * i.e. col1, col2, col3, ...
   * <p>
   * @group normal_funcs
   * @since 1.4.0
   * @param cols (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column struct (scala.collection.Seq<org.apache.spark.sql.Column> cols) { throw new RuntimeException(); }
  /**
   * Creates a new struct column that composes multiple input columns.
   * <p>
   * @group normal_funcs
   * @since 1.4.0
   * @param colName (undocumented)
   * @param colNames (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column struct (java.lang.String colName, scala.collection.Seq<java.lang.String> colNames) { throw new RuntimeException(); }
  /**
   * Evaluates a list of conditions and returns one of multiple possible result expressions.
   * If otherwise is not defined at the end, null is returned for unmatched conditions.
   * <p>
   * <pre><code>
   *   // Example: encoding gender string column into integer.
   *
   *   // Scala:
   *   people.select(when(people("gender") === "male", 0)
   *     .when(people("gender") === "female", 1)
   *     .otherwise(2))
   *
   *   // Java:
   *   people.select(when(col("gender").equalTo("male"), 0)
   *     .when(col("gender").equalTo("female"), 1)
   *     .otherwise(2))
   * </code></pre>
   * <p>
   * @group normal_funcs
   * @since 1.4.0
   * @param condition (undocumented)
   * @param value (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column when (org.apache.spark.sql.Column condition, Object value) { throw new RuntimeException(); }
  /**
   * Computes bitwise NOT.
   * <p>
   * @group normal_funcs
   * @since 1.4.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column bitwiseNOT (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Parses the expression string into the column that it represents, similar to
   * DataFrame.selectExpr
   * <pre><code>
   *   // get the number of words of each length
   *   df.groupBy(expr("length(word)")).count()
   * </code></pre>
   * <p>
   * @group normal_funcs
   * @param expr (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column expr (java.lang.String expr) { throw new RuntimeException(); }
  /**
   * Computes the cosine inverse of the given value; the returned angle is in the range
   * 0.0 through pi.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column acos (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Computes the cosine inverse of the given column; the returned angle is in the range
   * 0.0 through pi.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param columnName (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column acos (java.lang.String columnName) { throw new RuntimeException(); }
  /**
   * Computes the sine inverse of the given value; the returned angle is in the range
   * -pi/2 through pi/2.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column asin (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Computes the sine inverse of the given column; the returned angle is in the range
   * -pi/2 through pi/2.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param columnName (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column asin (java.lang.String columnName) { throw new RuntimeException(); }
  /**
   * Computes the tangent inverse of the given value.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column atan (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Computes the tangent inverse of the given column.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param columnName (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column atan (java.lang.String columnName) { throw new RuntimeException(); }
  /**
   * Returns the angle theta from the conversion of rectangular coordinates (x, y) to
   * polar coordinates (r, theta).
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param l (undocumented)
   * @param r (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column atan2 (org.apache.spark.sql.Column l, org.apache.spark.sql.Column r) { throw new RuntimeException(); }
  /**
   * Returns the angle theta from the conversion of rectangular coordinates (x, y) to
   * polar coordinates (r, theta).
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param l (undocumented)
   * @param rightName (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column atan2 (org.apache.spark.sql.Column l, java.lang.String rightName) { throw new RuntimeException(); }
  /**
   * Returns the angle theta from the conversion of rectangular coordinates (x, y) to
   * polar coordinates (r, theta).
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param leftName (undocumented)
   * @param r (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column atan2 (java.lang.String leftName, org.apache.spark.sql.Column r) { throw new RuntimeException(); }
  /**
   * Returns the angle theta from the conversion of rectangular coordinates (x, y) to
   * polar coordinates (r, theta).
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param leftName (undocumented)
   * @param rightName (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column atan2 (java.lang.String leftName, java.lang.String rightName) { throw new RuntimeException(); }
  /**
   * Returns the angle theta from the conversion of rectangular coordinates (x, y) to
   * polar coordinates (r, theta).
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param l (undocumented)
   * @param r (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column atan2 (org.apache.spark.sql.Column l, double r) { throw new RuntimeException(); }
  /**
   * Returns the angle theta from the conversion of rectangular coordinates (x, y) to
   * polar coordinates (r, theta).
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param leftName (undocumented)
   * @param r (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column atan2 (java.lang.String leftName, double r) { throw new RuntimeException(); }
  /**
   * Returns the angle theta from the conversion of rectangular coordinates (x, y) to
   * polar coordinates (r, theta).
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param l (undocumented)
   * @param r (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column atan2 (double l, org.apache.spark.sql.Column r) { throw new RuntimeException(); }
  /**
   * Returns the angle theta from the conversion of rectangular coordinates (x, y) to
   * polar coordinates (r, theta).
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param l (undocumented)
   * @param rightName (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column atan2 (double l, java.lang.String rightName) { throw new RuntimeException(); }
  /**
   * An expression that returns the string representation of the binary value of the given long
   * column. For example, bin("12") returns "1100".
   * <p>
   * @group math_funcs
   * @since 1.5.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column bin (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * An expression that returns the string representation of the binary value of the given long
   * column. For example, bin("12") returns "1100".
   * <p>
   * @group math_funcs
   * @since 1.5.0
   * @param columnName (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column bin (java.lang.String columnName) { throw new RuntimeException(); }
  /**
   * Computes the cube-root of the given value.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column cbrt (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Computes the cube-root of the given column.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param columnName (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column cbrt (java.lang.String columnName) { throw new RuntimeException(); }
  /**
   * Computes the ceiling of the given value.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column ceil (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Computes the ceiling of the given column.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param columnName (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column ceil (java.lang.String columnName) { throw new RuntimeException(); }
  /**
   * Convert a number in a string column from one base to another.
   * <p>
   * @group math_funcs
   * @since 1.5.0
   * @param num (undocumented)
   * @param fromBase (undocumented)
   * @param toBase (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column conv (org.apache.spark.sql.Column num, int fromBase, int toBase) { throw new RuntimeException(); }
  /**
   * Computes the cosine of the given value.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column cos (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Computes the cosine of the given column.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param columnName (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column cos (java.lang.String columnName) { throw new RuntimeException(); }
  /**
   * Computes the hyperbolic cosine of the given value.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column cosh (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Computes the hyperbolic cosine of the given column.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param columnName (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column cosh (java.lang.String columnName) { throw new RuntimeException(); }
  /**
   * Computes the exponential of the given value.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column exp (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Computes the exponential of the given column.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param columnName (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column exp (java.lang.String columnName) { throw new RuntimeException(); }
  /**
   * Computes the exponential of the given value minus one.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column expm1 (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Computes the exponential of the given column.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param columnName (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column expm1 (java.lang.String columnName) { throw new RuntimeException(); }
  /**
   * Computes the factorial of the given value.
   * <p>
   * @group math_funcs
   * @since 1.5.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column factorial (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Computes the floor of the given value.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column floor (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Computes the floor of the given column.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param columnName (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column floor (java.lang.String columnName) { throw new RuntimeException(); }
  /**
   * Returns the greatest value of the list of values, skipping null values.
   * This function takes at least 2 parameters. It will return null iff all parameters are null.
   * <p>
   * @group normal_funcs
   * @since 1.5.0
   * @param exprs (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column greatest (scala.collection.Seq<org.apache.spark.sql.Column> exprs) { throw new RuntimeException(); }
  /**
   * Returns the greatest value of the list of column names, skipping null values.
   * This function takes at least 2 parameters. It will return null iff all parameters are null.
   * <p>
   * @group normal_funcs
   * @since 1.5.0
   * @param columnName (undocumented)
   * @param columnNames (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column greatest (java.lang.String columnName, scala.collection.Seq<java.lang.String> columnNames) { throw new RuntimeException(); }
  /**
   * Computes hex value of the given column.
   * <p>
   * @group math_funcs
   * @since 1.5.0
   * @param column (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column hex (org.apache.spark.sql.Column column) { throw new RuntimeException(); }
  /**
   * Inverse of hex. Interprets each pair of characters as a hexadecimal number
   * and converts to the byte representation of number.
   * <p>
   * @group math_funcs
   * @since 1.5.0
   * @param column (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column unhex (org.apache.spark.sql.Column column) { throw new RuntimeException(); }
  /**
   * Computes <code>sqrt(a^2^ + b^2^)</code> without intermediate overflow or underflow.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param l (undocumented)
   * @param r (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column hypot (org.apache.spark.sql.Column l, org.apache.spark.sql.Column r) { throw new RuntimeException(); }
  /**
   * Computes <code>sqrt(a^2^ + b^2^)</code> without intermediate overflow or underflow.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param l (undocumented)
   * @param rightName (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column hypot (org.apache.spark.sql.Column l, java.lang.String rightName) { throw new RuntimeException(); }
  /**
   * Computes <code>sqrt(a^2^ + b^2^)</code> without intermediate overflow or underflow.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param leftName (undocumented)
   * @param r (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column hypot (java.lang.String leftName, org.apache.spark.sql.Column r) { throw new RuntimeException(); }
  /**
   * Computes <code>sqrt(a^2^ + b^2^)</code> without intermediate overflow or underflow.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param leftName (undocumented)
   * @param rightName (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column hypot (java.lang.String leftName, java.lang.String rightName) { throw new RuntimeException(); }
  /**
   * Computes <code>sqrt(a^2^ + b^2^)</code> without intermediate overflow or underflow.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param l (undocumented)
   * @param r (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column hypot (org.apache.spark.sql.Column l, double r) { throw new RuntimeException(); }
  /**
   * Computes <code>sqrt(a^2^ + b^2^)</code> without intermediate overflow or underflow.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param leftName (undocumented)
   * @param r (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column hypot (java.lang.String leftName, double r) { throw new RuntimeException(); }
  /**
   * Computes <code>sqrt(a^2^ + b^2^)</code> without intermediate overflow or underflow.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param l (undocumented)
   * @param r (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column hypot (double l, org.apache.spark.sql.Column r) { throw new RuntimeException(); }
  /**
   * Computes <code>sqrt(a^2^ + b^2^)</code> without intermediate overflow or underflow.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param l (undocumented)
   * @param rightName (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column hypot (double l, java.lang.String rightName) { throw new RuntimeException(); }
  /**
   * Returns the least value of the list of values, skipping null values.
   * This function takes at least 2 parameters. It will return null iff all parameters are null.
   * <p>
   * @group normal_funcs
   * @since 1.5.0
   * @param exprs (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column least (scala.collection.Seq<org.apache.spark.sql.Column> exprs) { throw new RuntimeException(); }
  /**
   * Returns the least value of the list of column names, skipping null values.
   * This function takes at least 2 parameters. It will return null iff all parameters are null.
   * <p>
   * @group normal_funcs
   * @since 1.5.0
   * @param columnName (undocumented)
   * @param columnNames (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column least (java.lang.String columnName, scala.collection.Seq<java.lang.String> columnNames) { throw new RuntimeException(); }
  /**
   * Computes the natural logarithm of the given value.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column log (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Computes the natural logarithm of the given column.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param columnName (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column log (java.lang.String columnName) { throw new RuntimeException(); }
  /**
   * Returns the first argument-base logarithm of the second argument.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param base (undocumented)
   * @param a (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column log (double base, org.apache.spark.sql.Column a) { throw new RuntimeException(); }
  /**
   * Returns the first argument-base logarithm of the second argument.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param base (undocumented)
   * @param columnName (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column log (double base, java.lang.String columnName) { throw new RuntimeException(); }
  /**
   * Computes the logarithm of the given value in base 10.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column log10 (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Computes the logarithm of the given value in base 10.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param columnName (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column log10 (java.lang.String columnName) { throw new RuntimeException(); }
  /**
   * Computes the natural logarithm of the given value plus one.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column log1p (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Computes the natural logarithm of the given column plus one.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param columnName (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column log1p (java.lang.String columnName) { throw new RuntimeException(); }
  /**
   * Computes the logarithm of the given column in base 2.
   * <p>
   * @group math_funcs
   * @since 1.5.0
   * @param expr (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column log2 (org.apache.spark.sql.Column expr) { throw new RuntimeException(); }
  /**
   * Computes the logarithm of the given value in base 2.
   * <p>
   * @group math_funcs
   * @since 1.5.0
   * @param columnName (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column log2 (java.lang.String columnName) { throw new RuntimeException(); }
  /**
   * Returns the value of the first argument raised to the power of the second argument.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param l (undocumented)
   * @param r (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column pow (org.apache.spark.sql.Column l, org.apache.spark.sql.Column r) { throw new RuntimeException(); }
  /**
   * Returns the value of the first argument raised to the power of the second argument.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param l (undocumented)
   * @param rightName (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column pow (org.apache.spark.sql.Column l, java.lang.String rightName) { throw new RuntimeException(); }
  /**
   * Returns the value of the first argument raised to the power of the second argument.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param leftName (undocumented)
   * @param r (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column pow (java.lang.String leftName, org.apache.spark.sql.Column r) { throw new RuntimeException(); }
  /**
   * Returns the value of the first argument raised to the power of the second argument.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param leftName (undocumented)
   * @param rightName (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column pow (java.lang.String leftName, java.lang.String rightName) { throw new RuntimeException(); }
  /**
   * Returns the value of the first argument raised to the power of the second argument.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param l (undocumented)
   * @param r (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column pow (org.apache.spark.sql.Column l, double r) { throw new RuntimeException(); }
  /**
   * Returns the value of the first argument raised to the power of the second argument.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param leftName (undocumented)
   * @param r (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column pow (java.lang.String leftName, double r) { throw new RuntimeException(); }
  /**
   * Returns the value of the first argument raised to the power of the second argument.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param l (undocumented)
   * @param r (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column pow (double l, org.apache.spark.sql.Column r) { throw new RuntimeException(); }
  /**
   * Returns the value of the first argument raised to the power of the second argument.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param l (undocumented)
   * @param rightName (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column pow (double l, java.lang.String rightName) { throw new RuntimeException(); }
  /**
   * Returns the positive value of dividend mod divisor.
   * <p>
   * @group math_funcs
   * @since 1.5.0
   * @param dividend (undocumented)
   * @param divisor (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column pmod (org.apache.spark.sql.Column dividend, org.apache.spark.sql.Column divisor) { throw new RuntimeException(); }
  /**
   * Returns the double value that is closest in value to the argument and
   * is equal to a mathematical integer.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column rint (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Returns the double value that is closest in value to the argument and
   * is equal to a mathematical integer.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param columnName (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column rint (java.lang.String columnName) { throw new RuntimeException(); }
  /**
   * Returns the value of the column <code>e</code> rounded to 0 decimal places.
   * <p>
   * @group math_funcs
   * @since 1.5.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column round (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Round the value of <code>e</code> to <code>scale</code> decimal places if <code>scale</code> >= 0
   * or at integral part when <code>scale</code> < 0.
   * <p>
   * @group math_funcs
   * @since 1.5.0
   * @param e (undocumented)
   * @param scale (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column round (org.apache.spark.sql.Column e, int scale) { throw new RuntimeException(); }
  /**
   * Shift the the given value numBits left. If the given value is a long value, this function
   * will return a long value else it will return an integer value.
   * <p>
   * @group math_funcs
   * @since 1.5.0
   * @param e (undocumented)
   * @param numBits (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column shiftLeft (org.apache.spark.sql.Column e, int numBits) { throw new RuntimeException(); }
  /**
   * Shift the the given value numBits right. If the given value is a long value, it will return
   * a long value else it will return an integer value.
   * <p>
   * @group math_funcs
   * @since 1.5.0
   * @param e (undocumented)
   * @param numBits (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column shiftRight (org.apache.spark.sql.Column e, int numBits) { throw new RuntimeException(); }
  /**
   * Unsigned shift the the given value numBits right. If the given value is a long value,
   * it will return a long value else it will return an integer value.
   * <p>
   * @group math_funcs
   * @since 1.5.0
   * @param e (undocumented)
   * @param numBits (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column shiftRightUnsigned (org.apache.spark.sql.Column e, int numBits) { throw new RuntimeException(); }
  /**
   * Computes the signum of the given value.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column signum (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Computes the signum of the given column.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param columnName (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column signum (java.lang.String columnName) { throw new RuntimeException(); }
  /**
   * Computes the sine of the given value.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column sin (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Computes the sine of the given column.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param columnName (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column sin (java.lang.String columnName) { throw new RuntimeException(); }
  /**
   * Computes the hyperbolic sine of the given value.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column sinh (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Computes the hyperbolic sine of the given column.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param columnName (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column sinh (java.lang.String columnName) { throw new RuntimeException(); }
  /**
   * Computes the tangent of the given value.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column tan (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Computes the tangent of the given column.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param columnName (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column tan (java.lang.String columnName) { throw new RuntimeException(); }
  /**
   * Computes the hyperbolic tangent of the given value.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column tanh (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Computes the hyperbolic tangent of the given column.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param columnName (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column tanh (java.lang.String columnName) { throw new RuntimeException(); }
  /**
   * Converts an angle measured in radians to an approximately equivalent angle measured in degrees.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column toDegrees (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Converts an angle measured in radians to an approximately equivalent angle measured in degrees.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param columnName (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column toDegrees (java.lang.String columnName) { throw new RuntimeException(); }
  /**
   * Converts an angle measured in degrees to an approximately equivalent angle measured in radians.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column toRadians (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Converts an angle measured in degrees to an approximately equivalent angle measured in radians.
   * <p>
   * @group math_funcs
   * @since 1.4.0
   * @param columnName (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column toRadians (java.lang.String columnName) { throw new RuntimeException(); }
  /**
   * Calculates the MD5 digest of a binary column and returns the value
   * as a 32 character hex string.
   * <p>
   * @group misc_funcs
   * @since 1.5.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column md5 (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Calculates the SHA-1 digest of a binary column and returns the value
   * as a 40 character hex string.
   * <p>
   * @group misc_funcs
   * @since 1.5.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column sha1 (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Calculates the SHA-2 family of hash functions of a binary column and
   * returns the value as a hex string.
   * <p>
   * @param e column to compute SHA-2 on.
   * @param numBits one of 224, 256, 384, or 512.
   * <p>
   * @group misc_funcs
   * @since 1.5.0
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column sha2 (org.apache.spark.sql.Column e, int numBits) { throw new RuntimeException(); }
  /**
   * Calculates the cyclic redundancy check value  (CRC32) of a binary column and
   * returns the value as a bigint.
   * <p>
   * @group misc_funcs
   * @since 1.5.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column crc32 (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Computes the numeric value of the first character of the string column, and returns the
   * result as a int column.
   * <p>
   * @group string_funcs
   * @since 1.5.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column ascii (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Computes the BASE64 encoding of a binary column and returns it as a string column.
   * This is the reverse of unbase64.
   * <p>
   * @group string_funcs
   * @since 1.5.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column base64 (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Concatenates multiple input string columns together into a single string column.
   * <p>
   * @group string_funcs
   * @since 1.5.0
   * @param exprs (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column concat (scala.collection.Seq<org.apache.spark.sql.Column> exprs) { throw new RuntimeException(); }
  /**
   * Concatenates multiple input string columns together into a single string column,
   * using the given separator.
   * <p>
   * @group string_funcs
   * @since 1.5.0
   * @param sep (undocumented)
   * @param exprs (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column concat_ws (java.lang.String sep, scala.collection.Seq<org.apache.spark.sql.Column> exprs) { throw new RuntimeException(); }
  /**
   * Computes the first argument into a string from a binary using the provided character set
   * (one of 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16').
   * If either argument is null, the result will also be null.
   * <p>
   * @group string_funcs
   * @since 1.5.0
   * @param value (undocumented)
   * @param charset (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column decode (org.apache.spark.sql.Column value, java.lang.String charset) { throw new RuntimeException(); }
  /**
   * Computes the first argument into a binary from a string using the provided character set
   * (one of 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16').
   * If either argument is null, the result will also be null.
   * <p>
   * @group string_funcs
   * @since 1.5.0
   * @param value (undocumented)
   * @param charset (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column encode (org.apache.spark.sql.Column value, java.lang.String charset) { throw new RuntimeException(); }
  /**
   * Formats numeric column x to a format like '#,###,###.##', rounded to d decimal places,
   * and returns the result as a string column.
   * <p>
   * If d is 0, the result has no decimal point or fractional part.
   * If d < 0, the result will be null.
   * <p>
   * @group string_funcs
   * @since 1.5.0
   * @param x (undocumented)
   * @param d (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column format_number (org.apache.spark.sql.Column x, int d) { throw new RuntimeException(); }
  /**
   * Formats the arguments in printf-style and returns the result as a string column.
   * <p>
   * @group string_funcs
   * @since 1.5.0
   * @param format (undocumented)
   * @param arguments (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column format_string (java.lang.String format, scala.collection.Seq<org.apache.spark.sql.Column> arguments) { throw new RuntimeException(); }
  /**
   * Returns a new string column by converting the first letter of each word to uppercase.
   * Words are delimited by whitespace.
   * <p>
   * For example, "hello world" will become "Hello World".
   * <p>
   * @group string_funcs
   * @since 1.5.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column initcap (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Locate the position of the first occurrence of substr column in the given string.
   * Returns null if either of the arguments are null.
   * <p>
   * NOTE: The position is not zero based, but 1 based index, returns 0 if substr
   * could not be found in str.
   * <p>
   * @group string_funcs
   * @since 1.5.0
   * @param str (undocumented)
   * @param substring (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column instr (org.apache.spark.sql.Column str, java.lang.String substring) { throw new RuntimeException(); }
  /**
   * Computes the length of a given string or binary column.
   * <p>
   * @group string_funcs
   * @since 1.5.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column length (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Converts a string column to lower case.
   * <p>
   * @group string_funcs
   * @since 1.3.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column lower (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Computes the Levenshtein distance of the two given string columns.
   * @group string_funcs
   * @since 1.5.0
   * @param l (undocumented)
   * @param r (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column levenshtein (org.apache.spark.sql.Column l, org.apache.spark.sql.Column r) { throw new RuntimeException(); }
  /**
   * Locate the position of the first occurrence of substr.
   * NOTE: The position is not zero based, but 1 based index, returns 0 if substr
   * could not be found in str.
   * <p>
   * @group string_funcs
   * @since 1.5.0
   * @param substr (undocumented)
   * @param str (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column locate (java.lang.String substr, org.apache.spark.sql.Column str) { throw new RuntimeException(); }
  /**
   * Locate the position of the first occurrence of substr in a string column, after position pos.
   * <p>
   * NOTE: The position is not zero based, but 1 based index. returns 0 if substr
   * could not be found in str.
   * <p>
   * @group string_funcs
   * @since 1.5.0
   * @param substr (undocumented)
   * @param str (undocumented)
   * @param pos (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column locate (java.lang.String substr, org.apache.spark.sql.Column str, int pos) { throw new RuntimeException(); }
  /**
   * Left-pad the string column with
   * <p>
   * @group string_funcs
   * @since 1.5.0
   * @param str (undocumented)
   * @param len (undocumented)
   * @param pad (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column lpad (org.apache.spark.sql.Column str, int len, java.lang.String pad) { throw new RuntimeException(); }
  /**
   * Trim the spaces from left end for the specified string value.
   * <p>
   * @group string_funcs
   * @since 1.5.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column ltrim (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Extract a specific(idx) group identified by a java regex, from the specified string column.
   * <p>
   * @group string_funcs
   * @since 1.5.0
   * @param e (undocumented)
   * @param exp (undocumented)
   * @param groupIdx (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column regexp_extract (org.apache.spark.sql.Column e, java.lang.String exp, int groupIdx) { throw new RuntimeException(); }
  /**
   * Replace all substrings of the specified string value that match regexp with rep.
   * <p>
   * @group string_funcs
   * @since 1.5.0
   * @param e (undocumented)
   * @param pattern (undocumented)
   * @param replacement (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column regexp_replace (org.apache.spark.sql.Column e, java.lang.String pattern, java.lang.String replacement) { throw new RuntimeException(); }
  /**
   * Decodes a BASE64 encoded string column and returns it as a binary column.
   * This is the reverse of base64.
   * <p>
   * @group string_funcs
   * @since 1.5.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column unbase64 (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Right-padded with pad to a length of len.
   * <p>
   * @group string_funcs
   * @since 1.5.0
   * @param str (undocumented)
   * @param len (undocumented)
   * @param pad (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column rpad (org.apache.spark.sql.Column str, int len, java.lang.String pad) { throw new RuntimeException(); }
  /**
   * Repeats a string column n times, and returns it as a new string column.
   * <p>
   * @group string_funcs
   * @since 1.5.0
   * @param str (undocumented)
   * @param n (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column repeat (org.apache.spark.sql.Column str, int n) { throw new RuntimeException(); }
  /**
   * Reverses the string column and returns it as a new string column.
   * <p>
   * @group string_funcs
   * @since 1.5.0
   * @param str (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column reverse (org.apache.spark.sql.Column str) { throw new RuntimeException(); }
  /**
   * Trim the spaces from right end for the specified string value.
   * <p>
   * @group string_funcs
   * @since 1.5.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column rtrim (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * * Return the soundex code for the specified expression.
   * <p>
   * @group string_funcs
   * @since 1.5.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column soundex (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Splits str around pattern (pattern is a regular expression).
   * NOTE: pattern is a string represent the regular expression.
   * <p>
   * @group string_funcs
   * @since 1.5.0
   * @param str (undocumented)
   * @param pattern (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column split (org.apache.spark.sql.Column str, java.lang.String pattern) { throw new RuntimeException(); }
  /**
   * Substring starts at <code>pos</code> and is of length <code>len</code> when str is String type or
   * returns the slice of byte array that starts at <code>pos</code> in byte and is of length <code>len</code>
   * when str is Binary type
   * <p>
   * @group string_funcs
   * @since 1.5.0
   * @param str (undocumented)
   * @param pos (undocumented)
   * @param len (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column substring (org.apache.spark.sql.Column str, int pos, int len) { throw new RuntimeException(); }
  /**
   * Returns the substring from string str before count occurrences of the delimiter delim.
   * If count is positive, everything the left of the final delimiter (counting from left) is
   * returned. If count is negative, every to the right of the final delimiter (counting from the
   * right) is returned. substring_index performs a case-sensitive match when searching for delim.
   * <p>
   * @group string_funcs
   * @param str (undocumented)
   * @param delim (undocumented)
   * @param count (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column substring_index (org.apache.spark.sql.Column str, java.lang.String delim, int count) { throw new RuntimeException(); }
  /**
   * Translate any character in the src by a character in replaceString.
   * The characters in replaceString is corresponding to the characters in matchingString.
   * The translate will happen when any character in the string matching with the character
   * in the matchingString.
   * <p>
   * @group string_funcs
   * @since 1.5.0
   * @param src (undocumented)
   * @param matchingString (undocumented)
   * @param replaceString (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column translate (org.apache.spark.sql.Column src, java.lang.String matchingString, java.lang.String replaceString) { throw new RuntimeException(); }
  /**
   * Trim the spaces from both ends for the specified string column.
   * <p>
   * @group string_funcs
   * @since 1.5.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column trim (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Converts a string column to upper case.
   * <p>
   * @group string_funcs
   * @since 1.3.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column upper (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Returns the date that is numMonths after startDate.
   * <p>
   * @group datetime_funcs
   * @since 1.5.0
   * @param startDate (undocumented)
   * @param numMonths (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column add_months (org.apache.spark.sql.Column startDate, int numMonths) { throw new RuntimeException(); }
  /**
   * Returns the current date as a date column.
   * <p>
   * @group datetime_funcs
   * @since 1.5.0
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column current_date () { throw new RuntimeException(); }
  /**
   * Returns the current timestamp as a timestamp column.
   * <p>
   * @group datetime_funcs
   * @since 1.5.0
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column current_timestamp () { throw new RuntimeException(); }
  /**
   * Converts a date/timestamp/string to a value of string in the format specified by the date
   * format given by the second argument.
   * <p>
   * A pattern could be for instance <code>dd.MM.yyyy</code> and could return a string like '18.03.1993'. All
   * pattern letters of {@link java.text.SimpleDateFormat} can be used.
   * <p>
   * NOTE: Use when ever possible specialized functions like {@link year}. These benefit from a
   * specialized implementation.
   * <p>
   * @group datetime_funcs
   * @since 1.5.0
   * @param dateExpr (undocumented)
   * @param format (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column date_format (org.apache.spark.sql.Column dateExpr, java.lang.String format) { throw new RuntimeException(); }
  /**
   * Returns the date that is <code>days</code> days after <code>start</code>
   * @group datetime_funcs
   * @since 1.5.0
   * @param start (undocumented)
   * @param days (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column date_add (org.apache.spark.sql.Column start, int days) { throw new RuntimeException(); }
  /**
   * Returns the date that is <code>days</code> days before <code>start</code>
   * @group datetime_funcs
   * @since 1.5.0
   * @param start (undocumented)
   * @param days (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column date_sub (org.apache.spark.sql.Column start, int days) { throw new RuntimeException(); }
  /**
   * Returns the number of days from <code>start</code> to <code>end</code>.
   * @group datetime_funcs
   * @since 1.5.0
   * @param end (undocumented)
   * @param start (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column datediff (org.apache.spark.sql.Column end, org.apache.spark.sql.Column start) { throw new RuntimeException(); }
  /**
   * Extracts the year as an integer from a given date/timestamp/string.
   * @group datetime_funcs
   * @since 1.5.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column year (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Extracts the quarter as an integer from a given date/timestamp/string.
   * @group datetime_funcs
   * @since 1.5.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column quarter (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Extracts the month as an integer from a given date/timestamp/string.
   * @group datetime_funcs
   * @since 1.5.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column month (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Extracts the day of the month as an integer from a given date/timestamp/string.
   * @group datetime_funcs
   * @since 1.5.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column dayofmonth (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Extracts the day of the year as an integer from a given date/timestamp/string.
   * @group datetime_funcs
   * @since 1.5.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column dayofyear (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Extracts the hours as an integer from a given date/timestamp/string.
   * @group datetime_funcs
   * @since 1.5.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column hour (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Given a date column, returns the last day of the month which the given date belongs to.
   * For example, input "2015-07-27" returns "2015-07-31" since July 31 is the last day of the
   * month in July 2015.
   * <p>
   * @group datetime_funcs
   * @since 1.5.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column last_day (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Extracts the minutes as an integer from a given date/timestamp/string.
   * @group datetime_funcs
   * @since 1.5.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column minute (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  public  org.apache.spark.sql.Column months_between (org.apache.spark.sql.Column date1, org.apache.spark.sql.Column date2) { throw new RuntimeException(); }
  /**
   * Given a date column, returns the first date which is later than the value of the date column
   * that is on the specified day of the week.
   * <p>
   * For example, <code>next_day('2015-07-27', "Sunday")</code> returns 2015-08-02 because that is the first
   * Sunday after 2015-07-27.
   * <p>
   * Day of the week parameter is case insensitive, and accepts:
   * "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun".
   * <p>
   * @group datetime_funcs
   * @since 1.5.0
   * @param date (undocumented)
   * @param dayOfWeek (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column next_day (org.apache.spark.sql.Column date, java.lang.String dayOfWeek) { throw new RuntimeException(); }
  /**
   * Extracts the seconds as an integer from a given date/timestamp/string.
   * @group datetime_funcs
   * @since 1.5.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column second (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Extracts the week number as an integer from a given date/timestamp/string.
   * @group datetime_funcs
   * @since 1.5.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column weekofyear (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a string
   * representing the timestamp of that moment in the current system time zone in the given
   * format.
   * @group datetime_funcs
   * @since 1.5.0
   * @param ut (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column from_unixtime (org.apache.spark.sql.Column ut) { throw new RuntimeException(); }
  /**
   * Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a string
   * representing the timestamp of that moment in the current system time zone in the given
   * format.
   * @group datetime_funcs
   * @since 1.5.0
   * @param ut (undocumented)
   * @param f (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column from_unixtime (org.apache.spark.sql.Column ut, java.lang.String f) { throw new RuntimeException(); }
  /**
   * Gets current Unix timestamp in seconds.
   * @group datetime_funcs
   * @since 1.5.0
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column unix_timestamp () { throw new RuntimeException(); }
  /**
   * Converts time string in format yyyy-MM-dd HH:mm:ss to Unix timestamp (in seconds),
   * using the default timezone and the default locale, return null if fail.
   * @group datetime_funcs
   * @since 1.5.0
   * @param s (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column unix_timestamp (org.apache.spark.sql.Column s) { throw new RuntimeException(); }
  /**
   * Convert time string with given pattern
   * (see [http://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html])
   * to Unix time stamp (in seconds), return null if fail.
   * @group datetime_funcs
   * @since 1.5.0
   * @param s (undocumented)
   * @param p (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column unix_timestamp (org.apache.spark.sql.Column s, java.lang.String p) { throw new RuntimeException(); }
  /**
   * Converts the column into DateType.
   * <p>
   * @group datetime_funcs
   * @since 1.5.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column to_date (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Returns date truncated to the unit specified by the format.
   * <p>
   * @param format: 'year', 'yyyy', 'yy' for truncate by year,
   *               or 'month', 'mon', 'mm' for truncate by month
   * <p>
   * @group datetime_funcs
   * @since 1.5.0
   * @param date (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column trunc (org.apache.spark.sql.Column date, java.lang.String format) { throw new RuntimeException(); }
  /**
   * Assumes given timestamp is UTC and converts to given timezone.
   * @group datetime_funcs
   * @since 1.5.0
   * @param ts (undocumented)
   * @param tz (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column from_utc_timestamp (org.apache.spark.sql.Column ts, java.lang.String tz) { throw new RuntimeException(); }
  /**
   * Assumes given timestamp is in given timezone and converts to UTC.
   * @group datetime_funcs
   * @since 1.5.0
   * @param ts (undocumented)
   * @param tz (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column to_utc_timestamp (org.apache.spark.sql.Column ts, java.lang.String tz) { throw new RuntimeException(); }
  /**
   * Returns true if the array contain the value
   * @group collection_funcs
   * @since 1.5.0
   * @param column (undocumented)
   * @param value (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column array_contains (org.apache.spark.sql.Column column, Object value) { throw new RuntimeException(); }
  /**
   * Creates a new row for each element in the given array or map column.
   * <p>
   * @group collection_funcs
   * @since 1.3.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column explode (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Returns length of array or map.
   * <p>
   * @group collection_funcs
   * @since 1.5.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column size (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Sorts the input array for the given column in ascending order,
   * according to the natural ordering of the array elements.
   * <p>
   * @group collection_funcs
   * @since 1.5.0
   * @param e (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column sort_array (org.apache.spark.sql.Column e) { throw new RuntimeException(); }
  /**
   * Sorts the input array for the given column in ascending / descending order,
   * according to the natural ordering of the array elements.
   * <p>
   * @group collection_funcs
   * @since 1.5.0
   * @param e (undocumented)
   * @param asc (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column sort_array (org.apache.spark.sql.Column e, boolean asc) { throw new RuntimeException(); }
  /**
   * Defines a user-defined function of 0 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the function's signature.
   * <p>
   * @group udf_funcs
   * @since 1.3.0
   * @param f (undocumented)
   * @param evidence$1 (undocumented)
   * @return (undocumented)
   */
  public <RT extends java.lang.Object> org.apache.spark.sql.UserDefinedFunction udf (scala.Function0<RT> f, scala.reflect.api.TypeTags.TypeTag<RT> evidence$1) { throw new RuntimeException(); }
  /**
   * Defines a user-defined function of 1 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the function's signature.
   * <p>
   * @group udf_funcs
   * @since 1.3.0
   * @param f (undocumented)
   * @param evidence$2 (undocumented)
   * @param evidence$3 (undocumented)
   * @return (undocumented)
   */
  public <RT extends java.lang.Object, A1 extends java.lang.Object> org.apache.spark.sql.UserDefinedFunction udf (scala.Function1<A1, RT> f, scala.reflect.api.TypeTags.TypeTag<RT> evidence$2, scala.reflect.api.TypeTags.TypeTag<A1> evidence$3) { throw new RuntimeException(); }
  /**
   * Defines a user-defined function of 2 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the function's signature.
   * <p>
   * @group udf_funcs
   * @since 1.3.0
   * @param f (undocumented)
   * @param evidence$4 (undocumented)
   * @param evidence$5 (undocumented)
   * @param evidence$6 (undocumented)
   * @return (undocumented)
   */
  public <RT extends java.lang.Object, A1 extends java.lang.Object, A2 extends java.lang.Object> org.apache.spark.sql.UserDefinedFunction udf (scala.Function2<A1, A2, RT> f, scala.reflect.api.TypeTags.TypeTag<RT> evidence$4, scala.reflect.api.TypeTags.TypeTag<A1> evidence$5, scala.reflect.api.TypeTags.TypeTag<A2> evidence$6) { throw new RuntimeException(); }
  /**
   * Defines a user-defined function of 3 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the function's signature.
   * <p>
   * @group udf_funcs
   * @since 1.3.0
   * @param f (undocumented)
   * @param evidence$7 (undocumented)
   * @param evidence$8 (undocumented)
   * @param evidence$9 (undocumented)
   * @param evidence$10 (undocumented)
   * @return (undocumented)
   */
  public <RT extends java.lang.Object, A1 extends java.lang.Object, A2 extends java.lang.Object, A3 extends java.lang.Object> org.apache.spark.sql.UserDefinedFunction udf (scala.Function3<A1, A2, A3, RT> f, scala.reflect.api.TypeTags.TypeTag<RT> evidence$7, scala.reflect.api.TypeTags.TypeTag<A1> evidence$8, scala.reflect.api.TypeTags.TypeTag<A2> evidence$9, scala.reflect.api.TypeTags.TypeTag<A3> evidence$10) { throw new RuntimeException(); }
  /**
   * Defines a user-defined function of 4 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the function's signature.
   * <p>
   * @group udf_funcs
   * @since 1.3.0
   * @param f (undocumented)
   * @param evidence$11 (undocumented)
   * @param evidence$12 (undocumented)
   * @param evidence$13 (undocumented)
   * @param evidence$14 (undocumented)
   * @param evidence$15 (undocumented)
   * @return (undocumented)
   */
  public <RT extends java.lang.Object, A1 extends java.lang.Object, A2 extends java.lang.Object, A3 extends java.lang.Object, A4 extends java.lang.Object> org.apache.spark.sql.UserDefinedFunction udf (scala.Function4<A1, A2, A3, A4, RT> f, scala.reflect.api.TypeTags.TypeTag<RT> evidence$11, scala.reflect.api.TypeTags.TypeTag<A1> evidence$12, scala.reflect.api.TypeTags.TypeTag<A2> evidence$13, scala.reflect.api.TypeTags.TypeTag<A3> evidence$14, scala.reflect.api.TypeTags.TypeTag<A4> evidence$15) { throw new RuntimeException(); }
  /**
   * Defines a user-defined function of 5 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the function's signature.
   * <p>
   * @group udf_funcs
   * @since 1.3.0
   * @param f (undocumented)
   * @param evidence$16 (undocumented)
   * @param evidence$17 (undocumented)
   * @param evidence$18 (undocumented)
   * @param evidence$19 (undocumented)
   * @param evidence$20 (undocumented)
   * @param evidence$21 (undocumented)
   * @return (undocumented)
   */
  public <RT extends java.lang.Object, A1 extends java.lang.Object, A2 extends java.lang.Object, A3 extends java.lang.Object, A4 extends java.lang.Object, A5 extends java.lang.Object> org.apache.spark.sql.UserDefinedFunction udf (scala.Function5<A1, A2, A3, A4, A5, RT> f, scala.reflect.api.TypeTags.TypeTag<RT> evidence$16, scala.reflect.api.TypeTags.TypeTag<A1> evidence$17, scala.reflect.api.TypeTags.TypeTag<A2> evidence$18, scala.reflect.api.TypeTags.TypeTag<A3> evidence$19, scala.reflect.api.TypeTags.TypeTag<A4> evidence$20, scala.reflect.api.TypeTags.TypeTag<A5> evidence$21) { throw new RuntimeException(); }
  /**
   * Defines a user-defined function of 6 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the function's signature.
   * <p>
   * @group udf_funcs
   * @since 1.3.0
   * @param f (undocumented)
   * @param evidence$22 (undocumented)
   * @param evidence$23 (undocumented)
   * @param evidence$24 (undocumented)
   * @param evidence$25 (undocumented)
   * @param evidence$26 (undocumented)
   * @param evidence$27 (undocumented)
   * @param evidence$28 (undocumented)
   * @return (undocumented)
   */
  public <RT extends java.lang.Object, A1 extends java.lang.Object, A2 extends java.lang.Object, A3 extends java.lang.Object, A4 extends java.lang.Object, A5 extends java.lang.Object, A6 extends java.lang.Object> org.apache.spark.sql.UserDefinedFunction udf (scala.Function6<A1, A2, A3, A4, A5, A6, RT> f, scala.reflect.api.TypeTags.TypeTag<RT> evidence$22, scala.reflect.api.TypeTags.TypeTag<A1> evidence$23, scala.reflect.api.TypeTags.TypeTag<A2> evidence$24, scala.reflect.api.TypeTags.TypeTag<A3> evidence$25, scala.reflect.api.TypeTags.TypeTag<A4> evidence$26, scala.reflect.api.TypeTags.TypeTag<A5> evidence$27, scala.reflect.api.TypeTags.TypeTag<A6> evidence$28) { throw new RuntimeException(); }
  /**
   * Defines a user-defined function of 7 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the function's signature.
   * <p>
   * @group udf_funcs
   * @since 1.3.0
   * @param f (undocumented)
   * @param evidence$29 (undocumented)
   * @param evidence$30 (undocumented)
   * @param evidence$31 (undocumented)
   * @param evidence$32 (undocumented)
   * @param evidence$33 (undocumented)
   * @param evidence$34 (undocumented)
   * @param evidence$35 (undocumented)
   * @param evidence$36 (undocumented)
   * @return (undocumented)
   */
  public <RT extends java.lang.Object, A1 extends java.lang.Object, A2 extends java.lang.Object, A3 extends java.lang.Object, A4 extends java.lang.Object, A5 extends java.lang.Object, A6 extends java.lang.Object, A7 extends java.lang.Object> org.apache.spark.sql.UserDefinedFunction udf (scala.Function7<A1, A2, A3, A4, A5, A6, A7, RT> f, scala.reflect.api.TypeTags.TypeTag<RT> evidence$29, scala.reflect.api.TypeTags.TypeTag<A1> evidence$30, scala.reflect.api.TypeTags.TypeTag<A2> evidence$31, scala.reflect.api.TypeTags.TypeTag<A3> evidence$32, scala.reflect.api.TypeTags.TypeTag<A4> evidence$33, scala.reflect.api.TypeTags.TypeTag<A5> evidence$34, scala.reflect.api.TypeTags.TypeTag<A6> evidence$35, scala.reflect.api.TypeTags.TypeTag<A7> evidence$36) { throw new RuntimeException(); }
  /**
   * Defines a user-defined function of 8 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the function's signature.
   * <p>
   * @group udf_funcs
   * @since 1.3.0
   * @param f (undocumented)
   * @param evidence$37 (undocumented)
   * @param evidence$38 (undocumented)
   * @param evidence$39 (undocumented)
   * @param evidence$40 (undocumented)
   * @param evidence$41 (undocumented)
   * @param evidence$42 (undocumented)
   * @param evidence$43 (undocumented)
   * @param evidence$44 (undocumented)
   * @param evidence$45 (undocumented)
   * @return (undocumented)
   */
  public <RT extends java.lang.Object, A1 extends java.lang.Object, A2 extends java.lang.Object, A3 extends java.lang.Object, A4 extends java.lang.Object, A5 extends java.lang.Object, A6 extends java.lang.Object, A7 extends java.lang.Object, A8 extends java.lang.Object> org.apache.spark.sql.UserDefinedFunction udf (scala.Function8<A1, A2, A3, A4, A5, A6, A7, A8, RT> f, scala.reflect.api.TypeTags.TypeTag<RT> evidence$37, scala.reflect.api.TypeTags.TypeTag<A1> evidence$38, scala.reflect.api.TypeTags.TypeTag<A2> evidence$39, scala.reflect.api.TypeTags.TypeTag<A3> evidence$40, scala.reflect.api.TypeTags.TypeTag<A4> evidence$41, scala.reflect.api.TypeTags.TypeTag<A5> evidence$42, scala.reflect.api.TypeTags.TypeTag<A6> evidence$43, scala.reflect.api.TypeTags.TypeTag<A7> evidence$44, scala.reflect.api.TypeTags.TypeTag<A8> evidence$45) { throw new RuntimeException(); }
  /**
   * Defines a user-defined function of 9 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the function's signature.
   * <p>
   * @group udf_funcs
   * @since 1.3.0
   * @param f (undocumented)
   * @param evidence$46 (undocumented)
   * @param evidence$47 (undocumented)
   * @param evidence$48 (undocumented)
   * @param evidence$49 (undocumented)
   * @param evidence$50 (undocumented)
   * @param evidence$51 (undocumented)
   * @param evidence$52 (undocumented)
   * @param evidence$53 (undocumented)
   * @param evidence$54 (undocumented)
   * @param evidence$55 (undocumented)
   * @return (undocumented)
   */
  public <RT extends java.lang.Object, A1 extends java.lang.Object, A2 extends java.lang.Object, A3 extends java.lang.Object, A4 extends java.lang.Object, A5 extends java.lang.Object, A6 extends java.lang.Object, A7 extends java.lang.Object, A8 extends java.lang.Object, A9 extends java.lang.Object> org.apache.spark.sql.UserDefinedFunction udf (scala.Function9<A1, A2, A3, A4, A5, A6, A7, A8, A9, RT> f, scala.reflect.api.TypeTags.TypeTag<RT> evidence$46, scala.reflect.api.TypeTags.TypeTag<A1> evidence$47, scala.reflect.api.TypeTags.TypeTag<A2> evidence$48, scala.reflect.api.TypeTags.TypeTag<A3> evidence$49, scala.reflect.api.TypeTags.TypeTag<A4> evidence$50, scala.reflect.api.TypeTags.TypeTag<A5> evidence$51, scala.reflect.api.TypeTags.TypeTag<A6> evidence$52, scala.reflect.api.TypeTags.TypeTag<A7> evidence$53, scala.reflect.api.TypeTags.TypeTag<A8> evidence$54, scala.reflect.api.TypeTags.TypeTag<A9> evidence$55) { throw new RuntimeException(); }
  /**
   * Defines a user-defined function of 10 arguments as user-defined function (UDF).
   * The data types are automatically inferred based on the function's signature.
   * <p>
   * @group udf_funcs
   * @since 1.3.0
   * @param f (undocumented)
   * @param evidence$56 (undocumented)
   * @param evidence$57 (undocumented)
   * @param evidence$58 (undocumented)
   * @param evidence$59 (undocumented)
   * @param evidence$60 (undocumented)
   * @param evidence$61 (undocumented)
   * @param evidence$62 (undocumented)
   * @param evidence$63 (undocumented)
   * @param evidence$64 (undocumented)
   * @param evidence$65 (undocumented)
   * @param evidence$66 (undocumented)
   * @return (undocumented)
   */
  public <RT extends java.lang.Object, A1 extends java.lang.Object, A2 extends java.lang.Object, A3 extends java.lang.Object, A4 extends java.lang.Object, A5 extends java.lang.Object, A6 extends java.lang.Object, A7 extends java.lang.Object, A8 extends java.lang.Object, A9 extends java.lang.Object, A10 extends java.lang.Object> org.apache.spark.sql.UserDefinedFunction udf (scala.Function10<A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, RT> f, scala.reflect.api.TypeTags.TypeTag<RT> evidence$56, scala.reflect.api.TypeTags.TypeTag<A1> evidence$57, scala.reflect.api.TypeTags.TypeTag<A2> evidence$58, scala.reflect.api.TypeTags.TypeTag<A3> evidence$59, scala.reflect.api.TypeTags.TypeTag<A4> evidence$60, scala.reflect.api.TypeTags.TypeTag<A5> evidence$61, scala.reflect.api.TypeTags.TypeTag<A6> evidence$62, scala.reflect.api.TypeTags.TypeTag<A7> evidence$63, scala.reflect.api.TypeTags.TypeTag<A8> evidence$64, scala.reflect.api.TypeTags.TypeTag<A9> evidence$65, scala.reflect.api.TypeTags.TypeTag<A10> evidence$66) { throw new RuntimeException(); }
  /**
   * Call a Scala function of 0 arguments as user-defined function (UDF). This requires
   * you to specify the return data type.
   * <p>
   * @group udf_funcs
   * @since 1.3.0
   * @deprecated As of 1.5.0, since it's redundant with udf()
   * @param f (undocumented)
   * @param returnType (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column callUDF (scala.Function0<?> f, org.apache.spark.sql.types.DataType returnType) { throw new RuntimeException(); }
  /**
   * Call a Scala function of 1 arguments as user-defined function (UDF). This requires
   * you to specify the return data type.
   * <p>
   * @group udf_funcs
   * @since 1.3.0
   * @deprecated As of 1.5.0, since it's redundant with udf()
   * @param f (undocumented)
   * @param returnType (undocumented)
   * @param arg1 (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column callUDF (scala.Function1<?, ?> f, org.apache.spark.sql.types.DataType returnType, org.apache.spark.sql.Column arg1) { throw new RuntimeException(); }
  /**
   * Call a Scala function of 2 arguments as user-defined function (UDF). This requires
   * you to specify the return data type.
   * <p>
   * @group udf_funcs
   * @since 1.3.0
   * @deprecated As of 1.5.0, since it's redundant with udf()
   * @param f (undocumented)
   * @param returnType (undocumented)
   * @param arg1 (undocumented)
   * @param arg2 (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column callUDF (scala.Function2<?, ?, ?> f, org.apache.spark.sql.types.DataType returnType, org.apache.spark.sql.Column arg1, org.apache.spark.sql.Column arg2) { throw new RuntimeException(); }
  /**
   * Call a Scala function of 3 arguments as user-defined function (UDF). This requires
   * you to specify the return data type.
   * <p>
   * @group udf_funcs
   * @since 1.3.0
   * @deprecated As of 1.5.0, since it's redundant with udf()
   * @param f (undocumented)
   * @param returnType (undocumented)
   * @param arg1 (undocumented)
   * @param arg2 (undocumented)
   * @param arg3 (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column callUDF (scala.Function3<?, ?, ?, ?> f, org.apache.spark.sql.types.DataType returnType, org.apache.spark.sql.Column arg1, org.apache.spark.sql.Column arg2, org.apache.spark.sql.Column arg3) { throw new RuntimeException(); }
  /**
   * Call a Scala function of 4 arguments as user-defined function (UDF). This requires
   * you to specify the return data type.
   * <p>
   * @group udf_funcs
   * @since 1.3.0
   * @deprecated As of 1.5.0, since it's redundant with udf()
   * @param f (undocumented)
   * @param returnType (undocumented)
   * @param arg1 (undocumented)
   * @param arg2 (undocumented)
   * @param arg3 (undocumented)
   * @param arg4 (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column callUDF (scala.Function4<?, ?, ?, ?, ?> f, org.apache.spark.sql.types.DataType returnType, org.apache.spark.sql.Column arg1, org.apache.spark.sql.Column arg2, org.apache.spark.sql.Column arg3, org.apache.spark.sql.Column arg4) { throw new RuntimeException(); }
  /**
   * Call a Scala function of 5 arguments as user-defined function (UDF). This requires
   * you to specify the return data type.
   * <p>
   * @group udf_funcs
   * @since 1.3.0
   * @deprecated As of 1.5.0, since it's redundant with udf()
   * @param f (undocumented)
   * @param returnType (undocumented)
   * @param arg1 (undocumented)
   * @param arg2 (undocumented)
   * @param arg3 (undocumented)
   * @param arg4 (undocumented)
   * @param arg5 (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column callUDF (scala.Function5<?, ?, ?, ?, ?, ?> f, org.apache.spark.sql.types.DataType returnType, org.apache.spark.sql.Column arg1, org.apache.spark.sql.Column arg2, org.apache.spark.sql.Column arg3, org.apache.spark.sql.Column arg4, org.apache.spark.sql.Column arg5) { throw new RuntimeException(); }
  /**
   * Call a Scala function of 6 arguments as user-defined function (UDF). This requires
   * you to specify the return data type.
   * <p>
   * @group udf_funcs
   * @since 1.3.0
   * @deprecated As of 1.5.0, since it's redundant with udf()
   * @param f (undocumented)
   * @param returnType (undocumented)
   * @param arg1 (undocumented)
   * @param arg2 (undocumented)
   * @param arg3 (undocumented)
   * @param arg4 (undocumented)
   * @param arg5 (undocumented)
   * @param arg6 (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column callUDF (scala.Function6<?, ?, ?, ?, ?, ?, ?> f, org.apache.spark.sql.types.DataType returnType, org.apache.spark.sql.Column arg1, org.apache.spark.sql.Column arg2, org.apache.spark.sql.Column arg3, org.apache.spark.sql.Column arg4, org.apache.spark.sql.Column arg5, org.apache.spark.sql.Column arg6) { throw new RuntimeException(); }
  /**
   * Call a Scala function of 7 arguments as user-defined function (UDF). This requires
   * you to specify the return data type.
   * <p>
   * @group udf_funcs
   * @since 1.3.0
   * @deprecated As of 1.5.0, since it's redundant with udf()
   * @param f (undocumented)
   * @param returnType (undocumented)
   * @param arg1 (undocumented)
   * @param arg2 (undocumented)
   * @param arg3 (undocumented)
   * @param arg4 (undocumented)
   * @param arg5 (undocumented)
   * @param arg6 (undocumented)
   * @param arg7 (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column callUDF (scala.Function7<?, ?, ?, ?, ?, ?, ?, ?> f, org.apache.spark.sql.types.DataType returnType, org.apache.spark.sql.Column arg1, org.apache.spark.sql.Column arg2, org.apache.spark.sql.Column arg3, org.apache.spark.sql.Column arg4, org.apache.spark.sql.Column arg5, org.apache.spark.sql.Column arg6, org.apache.spark.sql.Column arg7) { throw new RuntimeException(); }
  /**
   * Call a Scala function of 8 arguments as user-defined function (UDF). This requires
   * you to specify the return data type.
   * <p>
   * @group udf_funcs
   * @since 1.3.0
   * @deprecated As of 1.5.0, since it's redundant with udf()
   * @param f (undocumented)
   * @param returnType (undocumented)
   * @param arg1 (undocumented)
   * @param arg2 (undocumented)
   * @param arg3 (undocumented)
   * @param arg4 (undocumented)
   * @param arg5 (undocumented)
   * @param arg6 (undocumented)
   * @param arg7 (undocumented)
   * @param arg8 (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column callUDF (scala.Function8<?, ?, ?, ?, ?, ?, ?, ?, ?> f, org.apache.spark.sql.types.DataType returnType, org.apache.spark.sql.Column arg1, org.apache.spark.sql.Column arg2, org.apache.spark.sql.Column arg3, org.apache.spark.sql.Column arg4, org.apache.spark.sql.Column arg5, org.apache.spark.sql.Column arg6, org.apache.spark.sql.Column arg7, org.apache.spark.sql.Column arg8) { throw new RuntimeException(); }
  /**
   * Call a Scala function of 9 arguments as user-defined function (UDF). This requires
   * you to specify the return data type.
   * <p>
   * @group udf_funcs
   * @since 1.3.0
   * @deprecated As of 1.5.0, since it's redundant with udf()
   * @param f (undocumented)
   * @param returnType (undocumented)
   * @param arg1 (undocumented)
   * @param arg2 (undocumented)
   * @param arg3 (undocumented)
   * @param arg4 (undocumented)
   * @param arg5 (undocumented)
   * @param arg6 (undocumented)
   * @param arg7 (undocumented)
   * @param arg8 (undocumented)
   * @param arg9 (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column callUDF (scala.Function9<?, ?, ?, ?, ?, ?, ?, ?, ?, ?> f, org.apache.spark.sql.types.DataType returnType, org.apache.spark.sql.Column arg1, org.apache.spark.sql.Column arg2, org.apache.spark.sql.Column arg3, org.apache.spark.sql.Column arg4, org.apache.spark.sql.Column arg5, org.apache.spark.sql.Column arg6, org.apache.spark.sql.Column arg7, org.apache.spark.sql.Column arg8, org.apache.spark.sql.Column arg9) { throw new RuntimeException(); }
  /**
   * Call a Scala function of 10 arguments as user-defined function (UDF). This requires
   * you to specify the return data type.
   * <p>
   * @group udf_funcs
   * @since 1.3.0
   * @deprecated As of 1.5.0, since it's redundant with udf()
   * @param f (undocumented)
   * @param returnType (undocumented)
   * @param arg1 (undocumented)
   * @param arg2 (undocumented)
   * @param arg3 (undocumented)
   * @param arg4 (undocumented)
   * @param arg5 (undocumented)
   * @param arg6 (undocumented)
   * @param arg7 (undocumented)
   * @param arg8 (undocumented)
   * @param arg9 (undocumented)
   * @param arg10 (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column callUDF (scala.Function10<?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?> f, org.apache.spark.sql.types.DataType returnType, org.apache.spark.sql.Column arg1, org.apache.spark.sql.Column arg2, org.apache.spark.sql.Column arg3, org.apache.spark.sql.Column arg4, org.apache.spark.sql.Column arg5, org.apache.spark.sql.Column arg6, org.apache.spark.sql.Column arg7, org.apache.spark.sql.Column arg8, org.apache.spark.sql.Column arg9, org.apache.spark.sql.Column arg10) { throw new RuntimeException(); }
  /**
   * Call an user-defined function.
   * Example:
   * <pre><code>
   *  import org.apache.spark.sql._
   *
   *  val df = Seq(("id1", 1), ("id2", 4), ("id3", 5)).toDF("id", "value")
   *  val sqlContext = df.sqlContext
   *  sqlContext.udf.register("simpleUDF", (v: Int) =&gt; v * v)
   *  df.select($"id", callUDF("simpleUDF", $"value"))
   * </code></pre>
   * <p>
   * @group udf_funcs
   * @since 1.5.0
   * @param udfName (undocumented)
   * @param cols (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column callUDF (java.lang.String udfName, scala.collection.Seq<org.apache.spark.sql.Column> cols) { throw new RuntimeException(); }
  /**
   * Call an user-defined function.
   * Example:
   * <pre><code>
   *  import org.apache.spark.sql._
   *
   *  val df = Seq(("id1", 1), ("id2", 4), ("id3", 5)).toDF("id", "value")
   *  val sqlContext = df.sqlContext
   *  sqlContext.udf.register("simpleUDF", (v: Int) =&gt; v * v)
   *  df.select($"id", callUdf("simpleUDF", $"value"))
   * </code></pre>
   * <p>
   * @group udf_funcs
   * @since 1.4.0
   * @deprecated As of 1.5.0, since it was not coherent to have two functions callUdf and callUDF
   * @param udfName (undocumented)
   * @param cols (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column callUdf (java.lang.String udfName, scala.collection.Seq<org.apache.spark.sql.Column> cols) { throw new RuntimeException(); }
}
