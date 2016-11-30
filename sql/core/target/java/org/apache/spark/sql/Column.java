package org.apache.spark.sql;
/**
 * :: Experimental ::
 * A column in a {@link DataFrame}.
 * <p>
 * @groupname java_expr_ops Java-specific expression operators
 * @groupname expr_ops Expression operators
 * @groupname df_ops DataFrame functions
 * @groupname Ungrouped Support functions for DataFrames
 * <p>
 * @since 1.3.0
 */
public  class Column implements org.apache.spark.Logging {
  static public  scala.Option<org.apache.spark.sql.catalyst.expressions.Expression> unapply (org.apache.spark.sql.Column col) { throw new RuntimeException(); }
  /**
   * A boolean expression that is evaluated to true if the value of this expression is contained
   * by the evaluated values of the arguments.
   * <p>
   * @group expr_ops
   * @since 1.3.0
   * @param list (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column in (java.lang.Object... list) { throw new RuntimeException(); }
  /**
   * A boolean expression that is evaluated to true if the value of this expression is contained
   * by the evaluated values of the arguments.
   * <p>
   * @group expr_ops
   * @since 1.5.0
   * @param list (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column isin (java.lang.Object... list) { throw new RuntimeException(); }
  // not preceding
  protected  org.apache.spark.sql.catalyst.expressions.Expression expr () { throw new RuntimeException(); }
  // not preceding
  public   Column (org.apache.spark.sql.catalyst.expressions.Expression expr) { throw new RuntimeException(); }
  public   Column (java.lang.String name) { throw new RuntimeException(); }
  /** Creates a column based on the given expression. */
  private  org.apache.spark.sql.Column exprToColumn (org.apache.spark.sql.catalyst.expressions.Expression newExpr) { throw new RuntimeException(); }
  public  java.lang.String toString () { throw new RuntimeException(); }
  public  boolean equals (Object that) { throw new RuntimeException(); }
  public  int hashCode () { throw new RuntimeException(); }
  /**
   * Extracts a value or values from a complex type.
   * The following types of extraction are supported:
   * - Given an Array, an integer ordinal can be used to retrieve a single value.
   * - Given a Map, a key of the correct type can be used to retrieve an individual value.
   * - Given a Struct, a string fieldName can be used to extract that field.
   * - Given an Array of Structs, a string fieldName can be used to extract filed
   *   of every struct in that array, and return an Array of fields
   * <p>
   * @group expr_ops
   * @since 1.4.0
   * @param extraction (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column apply (Object extraction) { throw new RuntimeException(); }
  /**
   * Equality test.
   * <pre><code>
   *   // Scala:
   *   df.filter( df("colA") === df("colB") )
   *
   *   // Java
   *   import static org.apache.spark.sql.functions.*;
   *   df.filter( col("colA").equalTo(col("colB")) );
   * </code></pre>
   * <p>
   * @group expr_ops
   * @since 1.3.0
   * @param other (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column equalTo (Object other) { throw new RuntimeException(); }
  /**
   * Inequality test.
   * <pre><code>
   *   // Scala:
   *   df.select( df("colA") !== df("colB") )
   *   df.select( !(df("colA") === df("colB")) )
   *
   *   // Java:
   *   import static org.apache.spark.sql.functions.*;
   *   df.filter( col("colA").notEqual(col("colB")) );
   * </code></pre>
   * <p>
   * @group java_expr_ops
   * @since 1.3.0
   * @param other (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column notEqual (Object other) { throw new RuntimeException(); }
  /**
   * Greater than.
   * <pre><code>
   *   // Scala: The following selects people older than 21.
   *   people.select( people("age") &gt; lit(21) )
   *
   *   // Java:
   *   import static org.apache.spark.sql.functions.*;
   *   people.select( people("age").gt(21) );
   * </code></pre>
   * <p>
   * @group java_expr_ops
   * @since 1.3.0
   * @param other (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column gt (Object other) { throw new RuntimeException(); }
  /**
   * Less than.
   * <pre><code>
   *   // Scala: The following selects people younger than 21.
   *   people.select( people("age") &lt; 21 )
   *
   *   // Java:
   *   people.select( people("age").lt(21) );
   * </code></pre>
   * <p>
   * @group java_expr_ops
   * @since 1.3.0
   * @param other (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column lt (Object other) { throw new RuntimeException(); }
  /**
   * Less than or equal to.
   * <pre><code>
   *   // Scala: The following selects people age 21 or younger than 21.
   *   people.select( people("age") &lt;= 21 )
   *
   *   // Java:
   *   people.select( people("age").leq(21) );
   * </code></pre>
   * <p>
   * @group java_expr_ops
   * @since 1.3.0
   * @param other (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column leq (Object other) { throw new RuntimeException(); }
  /**
   * Greater than or equal to an expression.
   * <pre><code>
   *   // Scala: The following selects people age 21 or older than 21.
   *   people.select( people("age") &gt;= 21 )
   *
   *   // Java:
   *   people.select( people("age").geq(21) )
   * </code></pre>
   * <p>
   * @group java_expr_ops
   * @since 1.3.0
   * @param other (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column geq (Object other) { throw new RuntimeException(); }
  /**
   * Equality test that is safe for null values.
   * <p>
   * @group java_expr_ops
   * @since 1.3.0
   * @param other (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column eqNullSafe (Object other) { throw new RuntimeException(); }
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
   * @group expr_ops
   * @since 1.4.0
   * @param condition (undocumented)
   * @param value (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column when (org.apache.spark.sql.Column condition, Object value) { throw new RuntimeException(); }
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
   * @group expr_ops
   * @since 1.4.0
   * @param value (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column otherwise (Object value) { throw new RuntimeException(); }
  /**
   * True if the current column is between the lower bound and upper bound, inclusive.
   * <p>
   * @group java_expr_ops
   * @since 1.4.0
   * @param lowerBound (undocumented)
   * @param upperBound (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column between (Object lowerBound, Object upperBound) { throw new RuntimeException(); }
  /**
   * True if the current expression is NaN.
   * <p>
   * @group expr_ops
   * @since 1.5.0
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column isNaN () { throw new RuntimeException(); }
  /**
   * True if the current expression is null.
   * <p>
   * @group expr_ops
   * @since 1.3.0
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column isNull () { throw new RuntimeException(); }
  /**
   * True if the current expression is NOT null.
   * <p>
   * @group expr_ops
   * @since 1.3.0
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column isNotNull () { throw new RuntimeException(); }
  /**
   * Boolean OR.
   * <pre><code>
   *   // Scala: The following selects people that are in school or employed.
   *   people.filter( people("inSchool") || people("isEmployed") )
   *
   *   // Java:
   *   people.filter( people("inSchool").or(people("isEmployed")) );
   * </code></pre>
   * <p>
   * @group java_expr_ops
   * @since 1.3.0
   * @param other (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column or (org.apache.spark.sql.Column other) { throw new RuntimeException(); }
  /**
   * Boolean AND.
   * <pre><code>
   *   // Scala: The following selects people that are in school and employed at the same time.
   *   people.select( people("inSchool") &amp;&amp; people("isEmployed") )
   *
   *   // Java:
   *   people.select( people("inSchool").and(people("isEmployed")) );
   * </code></pre>
   * <p>
   * @group java_expr_ops
   * @since 1.3.0
   * @param other (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column and (org.apache.spark.sql.Column other) { throw new RuntimeException(); }
  /**
   * Sum of this expression and another expression.
   * <pre><code>
   *   // Scala: The following selects the sum of a person's height and weight.
   *   people.select( people("height") + people("weight") )
   *
   *   // Java:
   *   people.select( people("height").plus(people("weight")) );
   * </code></pre>
   * <p>
   * @group java_expr_ops
   * @since 1.3.0
   * @param other (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column plus (Object other) { throw new RuntimeException(); }
  /**
   * Subtraction. Subtract the other expression from this expression.
   * <pre><code>
   *   // Scala: The following selects the difference between people's height and their weight.
   *   people.select( people("height") - people("weight") )
   *
   *   // Java:
   *   people.select( people("height").minus(people("weight")) );
   * </code></pre>
   * <p>
   * @group java_expr_ops
   * @since 1.3.0
   * @param other (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column minus (Object other) { throw new RuntimeException(); }
  /**
   * Multiplication of this expression and another expression.
   * <pre><code>
   *   // Scala: The following multiplies a person's height by their weight.
   *   people.select( people("height") * people("weight") )
   *
   *   // Java:
   *   people.select( people("height").multiply(people("weight")) );
   * </code></pre>
   * <p>
   * @group java_expr_ops
   * @since 1.3.0
   * @param other (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column multiply (Object other) { throw new RuntimeException(); }
  /**
   * Division this expression by another expression.
   * <pre><code>
   *   // Scala: The following divides a person's height by their weight.
   *   people.select( people("height") / people("weight") )
   *
   *   // Java:
   *   people.select( people("height").divide(people("weight")) );
   * </code></pre>
   * <p>
   * @group java_expr_ops
   * @since 1.3.0
   * @param other (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column divide (Object other) { throw new RuntimeException(); }
  /**
   * Modulo (a.k.a. remainder) expression.
   * <p>
   * @group java_expr_ops
   * @since 1.3.0
   * @param other (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column mod (Object other) { throw new RuntimeException(); }
  /**
   * A boolean expression that is evaluated to true if the value of this expression is contained
   * by the evaluated values of the arguments.
   * <p>
   * @group expr_ops
   * @since 1.3.0
   * @param list (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column in (scala.collection.Seq<java.lang.Object> list) { throw new RuntimeException(); }
  /**
   * A boolean expression that is evaluated to true if the value of this expression is contained
   * by the evaluated values of the arguments.
   * <p>
   * @group expr_ops
   * @since 1.5.0
   * @param list (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column isin (scala.collection.Seq<java.lang.Object> list) { throw new RuntimeException(); }
  /**
   * SQL like expression.
   * <p>
   * @group expr_ops
   * @since 1.3.0
   * @param literal (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column like (java.lang.String literal) { throw new RuntimeException(); }
  /**
   * SQL RLIKE expression (LIKE with Regex).
   * <p>
   * @group expr_ops
   * @since 1.3.0
   * @param literal (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column rlike (java.lang.String literal) { throw new RuntimeException(); }
  /**
   * An expression that gets an item at position <code>ordinal</code> out of an array,
   * or gets a value by key <code>key</code> in a {@link MapType}.
   * <p>
   * @group expr_ops
   * @since 1.3.0
   * @param key (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column getItem (Object key) { throw new RuntimeException(); }
  /**
   * An expression that gets a field by name in a {@link StructType}.
   * <p>
   * @group expr_ops
   * @since 1.3.0
   * @param fieldName (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column getField (java.lang.String fieldName) { throw new RuntimeException(); }
  /**
   * An expression that returns a substring.
   * @param startPos expression for the starting position.
   * @param len expression for the length of the substring.
   * <p>
   * @group expr_ops
   * @since 1.3.0
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column substr (org.apache.spark.sql.Column startPos, org.apache.spark.sql.Column len) { throw new RuntimeException(); }
  /**
   * An expression that returns a substring.
   * @param startPos starting position.
   * @param len length of the substring.
   * <p>
   * @group expr_ops
   * @since 1.3.0
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column substr (int startPos, int len) { throw new RuntimeException(); }
  /**
   * Contains the other element.
   * <p>
   * @group expr_ops
   * @since 1.3.0
   * @param other (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column contains (Object other) { throw new RuntimeException(); }
  /**
   * String starts with.
   * <p>
   * @group expr_ops
   * @since 1.3.0
   * @param other (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column startsWith (org.apache.spark.sql.Column other) { throw new RuntimeException(); }
  /**
   * String starts with another string literal.
   * <p>
   * @group expr_ops
   * @since 1.3.0
   * @param literal (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column startsWith (java.lang.String literal) { throw new RuntimeException(); }
  /**
   * String ends with.
   * <p>
   * @group expr_ops
   * @since 1.3.0
   * @param other (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column endsWith (org.apache.spark.sql.Column other) { throw new RuntimeException(); }
  /**
   * String ends with another string literal.
   * <p>
   * @group expr_ops
   * @since 1.3.0
   * @param literal (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column endsWith (java.lang.String literal) { throw new RuntimeException(); }
  /**
   * Gives the column an alias. Same as <code>as</code>.
   * <pre><code>
   *   // Renames colA to colB in select output.
   *   df.select($"colA".alias("colB"))
   * </code></pre>
   * <p>
   * @group expr_ops
   * @since 1.4.0
   * @param alias (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column alias (java.lang.String alias) { throw new RuntimeException(); }
  /**
   * Gives the column an alias.
   * <pre><code>
   *   // Renames colA to colB in select output.
   *   df.select($"colA".as("colB"))
   * </code></pre>
   * <p>
   * If the current column has metadata associated with it, this metadata will be propagated
   * to the new column.  If this not desired, use <code>as</code> with explicitly empty metadata.
   * <p>
   * @group expr_ops
   * @since 1.3.0
   * @param alias (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column as (java.lang.String alias) { throw new RuntimeException(); }
  /**
   * (Scala-specific) Assigns the given aliases to the results of a table generating function.
   * <pre><code>
   *   // Renames colA to colB in select output.
   *   df.select(explode($"myMap").as("key" :: "value" :: Nil))
   * </code></pre>
   * <p>
   * @group expr_ops
   * @since 1.4.0
   * @param aliases (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column as (scala.collection.Seq<java.lang.String> aliases) { throw new RuntimeException(); }
  /**
   * Assigns the given aliases to the results of a table generating function.
   * <pre><code>
   *   // Renames colA to colB in select output.
   *   df.select(explode($"myMap").as("key" :: "value" :: Nil))
   * </code></pre>
   * <p>
   * @group expr_ops
   * @since 1.4.0
   * @param aliases (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column as (java.lang.String[] aliases) { throw new RuntimeException(); }
  /**
   * Gives the column an alias.
   * <pre><code>
   *   // Renames colA to colB in select output.
   *   df.select($"colA".as('colB))
   * </code></pre>
   * <p>
   * If the current column has metadata associated with it, this metadata will be propagated
   * to the new column.  If this not desired, use <code>as</code> with explicitly empty metadata.
   * <p>
   * @group expr_ops
   * @since 1.3.0
   * @param alias (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column as (scala.Symbol alias) { throw new RuntimeException(); }
  /**
   * Gives the column an alias with metadata.
   * <pre><code>
   *   val metadata: Metadata = ...
   *   df.select($"colA".as("colB", metadata))
   * </code></pre>
   * <p>
   * @group expr_ops
   * @since 1.3.0
   * @param alias (undocumented)
   * @param metadata (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column as (java.lang.String alias, org.apache.spark.sql.types.Metadata metadata) { throw new RuntimeException(); }
  /**
   * Casts the column to a different data type.
   * <pre><code>
   *   // Casts colA to IntegerType.
   *   import org.apache.spark.sql.types.IntegerType
   *   df.select(df("colA").cast(IntegerType))
   *
   *   // equivalent to
   *   df.select(df("colA").cast("int"))
   * </code></pre>
   * <p>
   * @group expr_ops
   * @since 1.3.0
   * @param to (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column cast (org.apache.spark.sql.types.DataType to) { throw new RuntimeException(); }
  /**
   * Casts the column to a different data type, using the canonical string representation
   * of the type. The supported types are: <code>string</code>, <code>boolean</code>, <code>byte</code>, <code>short</code>, <code>int</code>, <code>long</code>,
   * <code>float</code>, <code>double</code>, <code>decimal</code>, <code>date</code>, <code>timestamp</code>.
   * <pre><code>
   *   // Casts colA to integer.
   *   df.select(df("colA").cast("int"))
   * </code></pre>
   * <p>
   * @group expr_ops
   * @since 1.3.0
   * @param to (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column cast (java.lang.String to) { throw new RuntimeException(); }
  /**
   * Returns an ordering used in sorting.
   * <pre><code>
   *   // Scala: sort a DataFrame by age column in descending order.
   *   df.sort(df("age").desc)
   *
   *   // Java
   *   df.sort(df.col("age").desc());
   * </code></pre>
   * <p>
   * @group expr_ops
   * @since 1.3.0
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column desc () { throw new RuntimeException(); }
  /**
   * Returns an ordering used in sorting.
   * <pre><code>
   *   // Scala: sort a DataFrame by age column in ascending order.
   *   df.sort(df("age").asc)
   *
   *   // Java
   *   df.sort(df.col("age").asc());
   * </code></pre>
   * <p>
   * @group expr_ops
   * @since 1.3.0
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column asc () { throw new RuntimeException(); }
  /**
   * Prints the expression to the console for debugging purpose.
   * <p>
   * @group df_ops
   * @since 1.3.0
   * @param extended (undocumented)
   */
  public  void explain (boolean extended) { throw new RuntimeException(); }
  /**
   * Compute bitwise OR of this expression with another expression.
   * <pre><code>
   *   df.select($"colA".bitwiseOR($"colB"))
   * </code></pre>
   * <p>
   * @group expr_ops
   * @since 1.4.0
   * @param other (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column bitwiseOR (Object other) { throw new RuntimeException(); }
  /**
   * Compute bitwise AND of this expression with another expression.
   * <pre><code>
   *   df.select($"colA".bitwiseAND($"colB"))
   * </code></pre>
   * <p>
   * @group expr_ops
   * @since 1.4.0
   * @param other (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column bitwiseAND (Object other) { throw new RuntimeException(); }
  /**
   * Compute bitwise XOR of this expression with another expression.
   * <pre><code>
   *   df.select($"colA".bitwiseXOR($"colB"))
   * </code></pre>
   * <p>
   * @group expr_ops
   * @since 1.4.0
   * @param other (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column bitwiseXOR (Object other) { throw new RuntimeException(); }
  /**
   * Define a windowing column.
   * <p>
   * <pre><code>
   *   val w = Window.partitionBy("name").orderBy("id")
   *   df.select(
   *     sum("price").over(w.rangeBetween(Long.MinValue, 2)),
   *     avg("price").over(w.rowsBetween(0, 4))
   *   )
   * </code></pre>
   * <p>
   * @group expr_ops
   * @since 1.4.0
   * @param window (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column over (org.apache.spark.sql.expressions.WindowSpec window) { throw new RuntimeException(); }
}
