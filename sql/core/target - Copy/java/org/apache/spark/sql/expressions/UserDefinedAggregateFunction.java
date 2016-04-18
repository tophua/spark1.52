package org.apache.spark.sql.expressions;
/**
 * :: Experimental ::
 * The base class for implementing user-defined aggregate functions (UDAF).
 */
public abstract class UserDefinedAggregateFunction implements scala.Serializable {
  /**
   * Creates a {@link Column} for this UDAF using given {@link Column}s as input arguments.
   * @param exprs (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column apply (org.apache.spark.sql.Column... exprs) { throw new RuntimeException(); }
  /**
   * Creates a {@link Column} for this UDAF using the distinct values of the given
   * {@link Column}s as input arguments.
   * @param exprs (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column distinct (org.apache.spark.sql.Column... exprs) { throw new RuntimeException(); }
  // not preceding
  public   UserDefinedAggregateFunction () { throw new RuntimeException(); }
  /**
   * A {@link StructType} represents data types of input arguments of this aggregate function.
   * For example, if a {@link UserDefinedAggregateFunction} expects two input arguments
   * with type of {@link DoubleType} and {@link LongType}, the returned {@link StructType} will look like
   * <p>
   * <code></code><code>
   *   new StructType()
   *    .add("doubleInput", DoubleType)
   *    .add("longInput", LongType)
   * </code><code></code>
   * <p>
   * The name of a field of this {@link StructType} is only used to identify the corresponding
   * input argument. Users can choose names to identify the input arguments.
   * @return (undocumented)
   */
  public abstract  org.apache.spark.sql.types.StructType inputSchema () ;
  /**
   * A {@link StructType} represents data types of values in the aggregation buffer.
   * For example, if a {@link UserDefinedAggregateFunction}'s buffer has two values
   * (i.e. two intermediate values) with type of {@link DoubleType} and {@link LongType},
   * the returned {@link StructType} will look like
   * <p>
   * <code></code><code>
   *   new StructType()
   *    .add("doubleInput", DoubleType)
   *    .add("longInput", LongType)
   * </code><code></code>
   * <p>
   * The name of a field of this {@link StructType} is only used to identify the corresponding
   * buffer value. Users can choose names to identify the input arguments.
   * @return (undocumented)
   */
  public abstract  org.apache.spark.sql.types.StructType bufferSchema () ;
  /**
   * The {@link DataType} of the returned value of this {@link UserDefinedAggregateFunction}.
   * @return (undocumented)
   */
  public abstract  org.apache.spark.sql.types.DataType dataType () ;
  /**
   * Returns true iff this function is deterministic, i.e. given the same input,
   * always return the same output.
   * @return (undocumented)
   */
  public abstract  boolean deterministic () ;
  /**
   * Initializes the given aggregation buffer, i.e. the zero value of the aggregation buffer.
   * <p>
   * The contract should be that applying the merge function on two initial buffers should just
   * return the initial buffer itself, i.e.
   * <code>merge(initialBuffer, initialBuffer)</code> should equal <code>initialBuffer</code>.
   * @param buffer (undocumented)
   */
  public abstract  void initialize (org.apache.spark.sql.expressions.MutableAggregationBuffer buffer) ;
  /**
   * Updates the given aggregation buffer <code>buffer</code> with new input data from <code>input</code>.
   * <p>
   * This is called once per input row.
   * @param buffer (undocumented)
   * @param input (undocumented)
   */
  public abstract  void update (org.apache.spark.sql.expressions.MutableAggregationBuffer buffer, org.apache.spark.sql.Row input) ;
  /**
   * Merges two aggregation buffers and stores the updated buffer values back to <code>buffer1</code>.
   * <p>
   * This is called when we merge two partially aggregated data together.
   * @param buffer1 (undocumented)
   * @param buffer2 (undocumented)
   */
  public abstract  void merge (org.apache.spark.sql.expressions.MutableAggregationBuffer buffer1, org.apache.spark.sql.Row buffer2) ;
  /**
   * Calculates the final result of this {@link UserDefinedAggregateFunction} based on the given
   * aggregation buffer.
   * @param buffer (undocumented)
   * @return (undocumented)
   */
  public abstract  Object evaluate (org.apache.spark.sql.Row buffer) ;
  /**
   * Creates a {@link Column} for this UDAF using given {@link Column}s as input arguments.
   * @param exprs (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column apply (scala.collection.Seq<org.apache.spark.sql.Column> exprs) { throw new RuntimeException(); }
  /**
   * Creates a {@link Column} for this UDAF using the distinct values of the given
   * {@link Column}s as input arguments.
   * @param exprs (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Column distinct (scala.collection.Seq<org.apache.spark.sql.Column> exprs) { throw new RuntimeException(); }
}
