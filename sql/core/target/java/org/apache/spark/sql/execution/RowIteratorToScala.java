package org.apache.spark.sql.execution;
public final class RowIteratorToScala implements scala.collection.Iterator<org.apache.spark.sql.catalyst.InternalRow> {
  public  org.apache.spark.sql.execution.RowIterator rowIter () { throw new RuntimeException(); }
  // not preceding
  public   RowIteratorToScala (org.apache.spark.sql.execution.RowIterator rowIter) { throw new RuntimeException(); }
  public  boolean hasNext () { throw new RuntimeException(); }
  public  org.apache.spark.sql.catalyst.InternalRow next () { throw new RuntimeException(); }
}
