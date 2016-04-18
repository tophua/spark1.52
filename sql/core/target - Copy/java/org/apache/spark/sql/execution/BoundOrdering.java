package org.apache.spark.sql.execution;
/**
 * Function for comparing boundary values.
 */
 abstract class BoundOrdering {
  public   BoundOrdering () { throw new RuntimeException(); }
  public abstract  int compare (scala.collection.Seq<org.apache.spark.sql.catalyst.InternalRow> input, int inputIndex, int outputIndex) ;
}
