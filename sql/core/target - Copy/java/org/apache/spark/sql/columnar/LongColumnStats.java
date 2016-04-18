package org.apache.spark.sql.columnar;
  class LongColumnStats implements org.apache.spark.sql.columnar.ColumnStats {
  public   LongColumnStats () { throw new RuntimeException(); }
  protected  long upper () { throw new RuntimeException(); }
  protected  long lower () { throw new RuntimeException(); }
  public  void gatherStats (org.apache.spark.sql.catalyst.InternalRow row, int ordinal) { throw new RuntimeException(); }
  public  org.apache.spark.sql.catalyst.expressions.GenericInternalRow collectedStatistics () { throw new RuntimeException(); }
}
