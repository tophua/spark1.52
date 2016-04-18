package org.apache.spark.sql.columnar;
/**
 * A no-op ColumnStats only used for testing purposes.
 */
  class NoopColumnStats implements org.apache.spark.sql.columnar.ColumnStats {
  public   NoopColumnStats () { throw new RuntimeException(); }
  public  void gatherStats (org.apache.spark.sql.catalyst.InternalRow row, int ordinal) { throw new RuntimeException(); }
  public  org.apache.spark.sql.catalyst.expressions.GenericInternalRow collectedStatistics () { throw new RuntimeException(); }
}
