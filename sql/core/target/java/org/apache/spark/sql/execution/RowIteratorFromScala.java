package org.apache.spark.sql.execution;
public final class RowIteratorFromScala extends org.apache.spark.sql.execution.RowIterator {
  public   RowIteratorFromScala (scala.collection.Iterator<org.apache.spark.sql.catalyst.InternalRow> scalaIter) { throw new RuntimeException(); }
  public  boolean advanceNext () { throw new RuntimeException(); }
  public  org.apache.spark.sql.catalyst.InternalRow getRow () { throw new RuntimeException(); }
  public  scala.collection.Iterator<org.apache.spark.sql.catalyst.InternalRow> toScala () { throw new RuntimeException(); }
}
