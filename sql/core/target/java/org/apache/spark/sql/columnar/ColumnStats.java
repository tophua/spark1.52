package org.apache.spark.sql.columnar;
/**
 * Used to collect statistical information when building in-memory columns.
 * <p>
 * NOTE: we intentionally avoid using <code>Ordering[T]</code> to compare values here because <code>Ordering[T]</code>
 * brings significant performance penalty.
 */
  interface ColumnStats extends scala.Serializable {
  public  int count () ;
  public  int nullCount () ;
  public  long sizeInBytes () ;
  /**
   * Gathers statistics information from <code>row(ordinal)</code>.
   * @param row (undocumented)
   * @param ordinal (undocumented)
   */
  public  void gatherStats (org.apache.spark.sql.catalyst.InternalRow row, int ordinal) ;
  /**
   * Column statistics represented as a single row, currently including closed lower bound, closed
   * upper bound and null count.
   * @return (undocumented)
   */
  public  org.apache.spark.sql.catalyst.expressions.GenericInternalRow collectedStatistics () ;
}
