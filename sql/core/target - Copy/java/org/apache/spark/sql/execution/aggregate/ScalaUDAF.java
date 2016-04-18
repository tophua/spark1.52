package org.apache.spark.sql.execution.aggregate;
/**
 * The internal wrapper used to hook a {@link UserDefinedAggregateFunction} <code>udaf</code> in the
 * internal aggregation code path.
 * param:  children
 * param:  udaf
 */
  class ScalaUDAF extends org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction2 implements org.apache.spark.Logging, scala.Product, scala.Serializable {
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression> children () { throw new RuntimeException(); }
  public  org.apache.spark.sql.expressions.UserDefinedAggregateFunction udaf () { throw new RuntimeException(); }
  // not preceding
  public   ScalaUDAF (scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression> children, org.apache.spark.sql.expressions.UserDefinedAggregateFunction udaf) { throw new RuntimeException(); }
  public  boolean nullable () { throw new RuntimeException(); }
  public  org.apache.spark.sql.types.DataType dataType () { throw new RuntimeException(); }
  public  boolean deterministic () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.types.DataType> inputTypes () { throw new RuntimeException(); }
  public  org.apache.spark.sql.types.StructType bufferSchema () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.AttributeReference> bufferAttributes () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.AttributeReference> cloneBufferAttributes () { throw new RuntimeException(); }
  private  org.apache.spark.sql.types.StructType childrenSchema () { throw new RuntimeException(); }
  private  org.apache.spark.sql.catalyst.expressions.MutableProjection inputProjection () { throw new RuntimeException(); }
  private  scala.Function1<java.lang.Object, java.lang.Object> inputToScalaConverters () { throw new RuntimeException(); }
  private  scala.Function1<java.lang.Object, java.lang.Object>[] bufferValuesToCatalystConverters () { throw new RuntimeException(); }
  private  scala.Function1<java.lang.Object, java.lang.Object>[] bufferValuesToScalaConverters () { throw new RuntimeException(); }
  private  scala.Function1<java.lang.Object, java.lang.Object> outputToCatalystConverter () { throw new RuntimeException(); }
  /**
   * Sets the inputBufferOffset to newInputBufferOffset and then create a new instance of
   * <code>inputAggregateBuffer</code> based on this new inputBufferOffset.
   * @param newInputBufferOffset (undocumented)
   */
  public  void withNewInputBufferOffset (int newInputBufferOffset) { throw new RuntimeException(); }
  /**
   * Sets the mutableBufferOffset to newMutableBufferOffset and then create a new instance of
   * <code>mutableAggregateBuffer</code> and <code>evalAggregateBuffer</code> based on this new mutableBufferOffset.
   * @param newMutableBufferOffset (undocumented)
   */
  public  void withNewMutableBufferOffset (int newMutableBufferOffset) { throw new RuntimeException(); }
  public  void initialize (org.apache.spark.sql.catalyst.expressions.MutableRow buffer) { throw new RuntimeException(); }
  public  void update (org.apache.spark.sql.catalyst.expressions.MutableRow buffer, org.apache.spark.sql.catalyst.InternalRow input) { throw new RuntimeException(); }
  public  void merge (org.apache.spark.sql.catalyst.expressions.MutableRow buffer1, org.apache.spark.sql.catalyst.InternalRow buffer2) { throw new RuntimeException(); }
  public  Object eval (org.apache.spark.sql.catalyst.InternalRow buffer) { throw new RuntimeException(); }
  public  java.lang.String toString () { throw new RuntimeException(); }
  public  java.lang.String nodeName () { throw new RuntimeException(); }
}
