package org.apache.spark.sql;
/**
 * A user-defined function. To create one, use the <code>udf</code> functions in {@link functions}.
 * As an example:
 * <pre><code>
 *   // Defined a UDF that returns true or false based on some numeric score.
 *   val predict = udf((score: Double) =&gt; if (score &gt; 0.5) true else false)
 *
 *   // Projects a column that adds a prediction column based on the score column.
 *   df.select( predict(df("score")) )
 * </code></pre>
 * <p>
 * @since 1.3.0
 */
public  class UserDefinedFunction implements scala.Product, scala.Serializable {
  public  java.lang.Object f () { throw new RuntimeException(); }
  public  org.apache.spark.sql.types.DataType dataType () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.types.DataType> inputTypes () { throw new RuntimeException(); }
  // not preceding
  protected   UserDefinedFunction (java.lang.Object f, org.apache.spark.sql.types.DataType dataType, scala.collection.Seq<org.apache.spark.sql.types.DataType> inputTypes) { throw new RuntimeException(); }
  public  org.apache.spark.sql.Column apply (scala.collection.Seq<org.apache.spark.sql.Column> exprs) { throw new RuntimeException(); }
}
