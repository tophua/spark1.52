package org.apache.spark.sql.execution;
/**
 * Compare the value of the input index to the value bound of the output index.
 */
 final class RangeBoundOrdering extends org.apache.spark.sql.execution.BoundOrdering implements scala.Product, scala.Serializable {
  public  scala.math.Ordering<org.apache.spark.sql.catalyst.InternalRow> ordering () { throw new RuntimeException(); }
  public  org.apache.spark.sql.catalyst.expressions.Projection current () { throw new RuntimeException(); }
  public  org.apache.spark.sql.catalyst.expressions.Projection bound () { throw new RuntimeException(); }
  // not preceding
  public   RangeBoundOrdering (scala.math.Ordering<org.apache.spark.sql.catalyst.InternalRow> ordering, org.apache.spark.sql.catalyst.expressions.Projection current, org.apache.spark.sql.catalyst.expressions.Projection bound) { throw new RuntimeException(); }
  public  int compare (scala.collection.Seq<org.apache.spark.sql.catalyst.InternalRow> input, int inputIndex, int outputIndex) { throw new RuntimeException(); }
}
