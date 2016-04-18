package org.apache.spark.sql;
/**
 * :: Experimental ::
 * A set of methods for aggregations on a {@link DataFrame}, created by {@link DataFrame.groupBy}.
 * <p>
 * @since 1.3.0
 */
public  class GroupedData {
  // no position
  /**
   * To indicate it's the GroupBy
   */
  static   class GroupByType$ implements org.apache.spark.sql.GroupedData.GroupType {
    /**
     * Static reference to the singleton instance of this Scala object.
     */
    public static final GroupByType$ MODULE$ = null;
    public   GroupByType$ () { throw new RuntimeException(); }
  }
  // no position
  /**
   * To indicate it's the CUBE
   */
  static   class CubeType$ implements org.apache.spark.sql.GroupedData.GroupType {
    /**
     * Static reference to the singleton instance of this Scala object.
     */
    public static final CubeType$ MODULE$ = null;
    public   CubeType$ () { throw new RuntimeException(); }
  }
  // no position
  /**
   * To indicate it's the ROLLUP
   */
  static   class RollupType$ implements org.apache.spark.sql.GroupedData.GroupType {
    /**
     * Static reference to the singleton instance of this Scala object.
     */
    public static final RollupType$ MODULE$ = null;
    public   RollupType$ () { throw new RuntimeException(); }
  }
  /**
   * The Grouping Type
   */
  static   interface GroupType {
  }
  static public  org.apache.spark.sql.GroupedData apply (org.apache.spark.sql.DataFrame df, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression> groupingExprs, org.apache.spark.sql.GroupedData.GroupType groupType) { throw new RuntimeException(); }
  /**
   * Compute aggregates by specifying a series of aggregate columns. Note that this function by
   * default retains the grouping columns in its output. To not retain grouping columns, set
   * <code>spark.sql.retainGroupColumns</code> to false.
   * <p>
   * The available aggregate methods are defined in {@link org.apache.spark.sql.functions}.
   * <p>
   * <pre><code>
   *   // Selects the age of the oldest employee and the aggregate expense for each department
   *
   *   // Scala:
   *   import org.apache.spark.sql.functions._
   *   df.groupBy("department").agg(max("age"), sum("expense"))
   *
   *   // Java:
   *   import static org.apache.spark.sql.functions.*;
   *   df.groupBy("department").agg(max("age"), sum("expense"));
   * </code></pre>
   * <p>
   * Note that before Spark 1.4, the default behavior is to NOT retain grouping columns. To change
   * to that behavior, set config variable <code>spark.sql.retainGroupColumns</code> to <code>false</code>.
   * <pre><code>
   *   // Scala, 1.3.x:
   *   df.groupBy("department").agg($"department", max("age"), sum("expense"))
   *
   *   // Java, 1.3.x:
   *   df.groupBy("department").agg(col("department"), max("age"), sum("expense"));
   * </code></pre>
   * <p>
   * @since 1.3.0
   * @param expr (undocumented)
   * @param exprs (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.DataFrame agg (org.apache.spark.sql.Column expr, org.apache.spark.sql.Column... exprs) { throw new RuntimeException(); }
  /**
   * Compute the average value for each numeric columns for each group. This is an alias for <code>avg</code>.
   * The resulting {@link DataFrame} will also contain the grouping columns.
   * When specified columns are given, only compute the average values for them.
   * <p>
   * @since 1.3.0
   * @param colNames (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.DataFrame mean (java.lang.String... colNames) { throw new RuntimeException(); }
  /**
   * Compute the max value for each numeric columns for each group.
   * The resulting {@link DataFrame} will also contain the grouping columns.
   * When specified columns are given, only compute the max values for them.
   * <p>
   * @since 1.3.0
   * @param colNames (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.DataFrame max (java.lang.String... colNames) { throw new RuntimeException(); }
  /**
   * Compute the mean value for each numeric columns for each group.
   * The resulting {@link DataFrame} will also contain the grouping columns.
   * When specified columns are given, only compute the mean values for them.
   * <p>
   * @since 1.3.0
   * @param colNames (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.DataFrame avg (java.lang.String... colNames) { throw new RuntimeException(); }
  /**
   * Compute the min value for each numeric column for each group.
   * The resulting {@link DataFrame} will also contain the grouping columns.
   * When specified columns are given, only compute the min values for them.
   * <p>
   * @since 1.3.0
   * @param colNames (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.DataFrame min (java.lang.String... colNames) { throw new RuntimeException(); }
  /**
   * Compute the sum for each numeric columns for each group.
   * The resulting {@link DataFrame} will also contain the grouping columns.
   * When specified columns are given, only compute the sum for them.
   * <p>
   * @since 1.3.0
   * @param colNames (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.DataFrame sum (java.lang.String... colNames) { throw new RuntimeException(); }
  // not preceding
  private  org.apache.spark.sql.GroupedData.GroupType groupType () { throw new RuntimeException(); }
  // not preceding
  protected   GroupedData (org.apache.spark.sql.DataFrame df, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression> groupingExprs, org.apache.spark.sql.GroupedData.GroupType groupType) { throw new RuntimeException(); }
  private  org.apache.spark.sql.DataFrame toDF (scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression> aggExprs) { throw new RuntimeException(); }
  private  org.apache.spark.sql.DataFrame aggregateNumericColumns (scala.collection.Seq<java.lang.String> colNames, scala.Function1<org.apache.spark.sql.catalyst.expressions.Expression, org.apache.spark.sql.catalyst.expressions.Expression> f) { throw new RuntimeException(); }
  private  scala.Function1<org.apache.spark.sql.catalyst.expressions.Expression, org.apache.spark.sql.catalyst.expressions.Expression> strToExpr (java.lang.String expr) { throw new RuntimeException(); }
  /**
   * (Scala-specific) Compute aggregates by specifying a map from column name to
   * aggregate methods. The resulting {@link DataFrame} will also contain the grouping columns.
   * <p>
   * The available aggregate methods are <code>avg</code>, <code>max</code>, <code>min</code>, <code>sum</code>, <code>count</code>.
   * <pre><code>
   *   // Selects the age of the oldest employee and the aggregate expense for each department
   *   df.groupBy("department").agg(
   *     "age" -&gt; "max",
   *     "expense" -&gt; "sum"
   *   )
   * </code></pre>
   * <p>
   * @since 1.3.0
   * @param aggExpr (undocumented)
   * @param aggExprs (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.DataFrame agg (scala.Tuple2<java.lang.String, java.lang.String> aggExpr, scala.collection.Seq<scala.Tuple2<java.lang.String, java.lang.String>> aggExprs) { throw new RuntimeException(); }
  /**
   * (Scala-specific) Compute aggregates by specifying a map from column name to
   * aggregate methods. The resulting {@link DataFrame} will also contain the grouping columns.
   * <p>
   * The available aggregate methods are <code>avg</code>, <code>max</code>, <code>min</code>, <code>sum</code>, <code>count</code>.
   * <pre><code>
   *   // Selects the age of the oldest employee and the aggregate expense for each department
   *   df.groupBy("department").agg(Map(
   *     "age" -&gt; "max",
   *     "expense" -&gt; "sum"
   *   ))
   * </code></pre>
   * <p>
   * @since 1.3.0
   * @param exprs (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.DataFrame agg (scala.collection.immutable.Map<java.lang.String, java.lang.String> exprs) { throw new RuntimeException(); }
  /**
   * (Java-specific) Compute aggregates by specifying a map from column name to
   * aggregate methods. The resulting {@link DataFrame} will also contain the grouping columns.
   * <p>
   * The available aggregate methods are <code>avg</code>, <code>max</code>, <code>min</code>, <code>sum</code>, <code>count</code>.
   * <pre><code>
   *   // Selects the age of the oldest employee and the aggregate expense for each department
   *   import com.google.common.collect.ImmutableMap;
   *   df.groupBy("department").agg(ImmutableMap.of("age", "max", "expense", "sum"));
   * </code></pre>
   * <p>
   * @since 1.3.0
   * @param exprs (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.DataFrame agg (java.util.Map<java.lang.String, java.lang.String> exprs) { throw new RuntimeException(); }
  /**
   * Compute aggregates by specifying a series of aggregate columns. Note that this function by
   * default retains the grouping columns in its output. To not retain grouping columns, set
   * <code>spark.sql.retainGroupColumns</code> to false.
   * <p>
   * The available aggregate methods are defined in {@link org.apache.spark.sql.functions}.
   * <p>
   * <pre><code>
   *   // Selects the age of the oldest employee and the aggregate expense for each department
   *
   *   // Scala:
   *   import org.apache.spark.sql.functions._
   *   df.groupBy("department").agg(max("age"), sum("expense"))
   *
   *   // Java:
   *   import static org.apache.spark.sql.functions.*;
   *   df.groupBy("department").agg(max("age"), sum("expense"));
   * </code></pre>
   * <p>
   * Note that before Spark 1.4, the default behavior is to NOT retain grouping columns. To change
   * to that behavior, set config variable <code>spark.sql.retainGroupColumns</code> to <code>false</code>.
   * <pre><code>
   *   // Scala, 1.3.x:
   *   df.groupBy("department").agg($"department", max("age"), sum("expense"))
   *
   *   // Java, 1.3.x:
   *   df.groupBy("department").agg(col("department"), max("age"), sum("expense"));
   * </code></pre>
   * <p>
   * @since 1.3.0
   * @param expr (undocumented)
   * @param exprs (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.DataFrame agg (org.apache.spark.sql.Column expr, scala.collection.Seq<org.apache.spark.sql.Column> exprs) { throw new RuntimeException(); }
  /**
   * Count the number of rows for each group.
   * The resulting {@link DataFrame} will also contain the grouping columns.
   * <p>
   * @since 1.3.0
   * @return (undocumented)
   */
  public  org.apache.spark.sql.DataFrame count () { throw new RuntimeException(); }
  /**
   * Compute the average value for each numeric columns for each group. This is an alias for <code>avg</code>.
   * The resulting {@link DataFrame} will also contain the grouping columns.
   * When specified columns are given, only compute the average values for them.
   * <p>
   * @since 1.3.0
   * @param colNames (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.DataFrame mean (scala.collection.Seq<java.lang.String> colNames) { throw new RuntimeException(); }
  /**
   * Compute the max value for each numeric columns for each group.
   * The resulting {@link DataFrame} will also contain the grouping columns.
   * When specified columns are given, only compute the max values for them.
   * <p>
   * @since 1.3.0
   * @param colNames (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.DataFrame max (scala.collection.Seq<java.lang.String> colNames) { throw new RuntimeException(); }
  /**
   * Compute the mean value for each numeric columns for each group.
   * The resulting {@link DataFrame} will also contain the grouping columns.
   * When specified columns are given, only compute the mean values for them.
   * <p>
   * @since 1.3.0
   * @param colNames (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.DataFrame avg (scala.collection.Seq<java.lang.String> colNames) { throw new RuntimeException(); }
  /**
   * Compute the min value for each numeric column for each group.
   * The resulting {@link DataFrame} will also contain the grouping columns.
   * When specified columns are given, only compute the min values for them.
   * <p>
   * @since 1.3.0
   * @param colNames (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.DataFrame min (scala.collection.Seq<java.lang.String> colNames) { throw new RuntimeException(); }
  /**
   * Compute the sum for each numeric columns for each group.
   * The resulting {@link DataFrame} will also contain the grouping columns.
   * When specified columns are given, only compute the sum for them.
   * <p>
   * @since 1.3.0
   * @param colNames (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.DataFrame sum (scala.collection.Seq<java.lang.String> colNames) { throw new RuntimeException(); }
}
