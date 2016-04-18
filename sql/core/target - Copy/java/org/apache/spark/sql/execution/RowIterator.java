package org.apache.spark.sql.execution;
/**
 * An internal iterator interface which presents a more restrictive API than
 * {@link scala.collection.Iterator}.
 * <p>
 * One major departure from the Scala iterator API is the fusing of the <code>hasNext()</code> and <code>next()</code>
 * calls: Scala's iterator allows users to call <code>hasNext()</code> without immediately advancing the
 * iterator to consume the next row, whereas RowIterator combines these calls into a single
 * {@link advanceNext()} method.
 */
 abstract class RowIterator {
  static public  org.apache.spark.sql.execution.RowIterator fromScala (scala.collection.Iterator<org.apache.spark.sql.catalyst.InternalRow> scalaIter) { throw new RuntimeException(); }
  public   RowIterator () { throw new RuntimeException(); }
  /**
   * Advance this iterator by a single row. Returns <code>false</code> if this iterator has no more rows
   * and <code>true</code> otherwise. If this returns <code>true</code>, then the new row can be retrieved by calling
   * {@link getRow}.
   * @return (undocumented)
   */
  public abstract  boolean advanceNext () ;
  /**
   * Retrieve the row from this iterator. This method is idempotent. It is illegal to call this
   * method after {@link advanceNext()} has returned <code>false</code>.
   * @return (undocumented)
   */
  public abstract  org.apache.spark.sql.catalyst.InternalRow getRow () ;
  /**
   * Convert this RowIterator into a {@link scala.collection.Iterator}.
   * @return (undocumented)
   */
  public  scala.collection.Iterator<org.apache.spark.sql.catalyst.InternalRow> toScala () { throw new RuntimeException(); }
}
