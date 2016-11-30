package org.apache.spark.sql.execution;
/**
 * Compare the input index to the bound of the output index.
 */
 final class RowBoundOrdering extends org.apache.spark.sql.execution.BoundOrdering implements scala.Product, scala.Serializable {
  public  int offset () { throw new RuntimeException(); }
  // not preceding
  public   RowBoundOrdering (int offset) { throw new RuntimeException(); }
  public  int compare (scala.collection.Seq<org.apache.spark.sql.catalyst.InternalRow> input, int inputIndex, int outputIndex) { throw new RuntimeException(); }
}
