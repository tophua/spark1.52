package org.apache.spark.sql.columnar;
  class DoubleColumnStats implements org.apache.spark.sql.columnar.ColumnStats {
  public   DoubleColumnStats () { throw new RuntimeException(); }
  protected  double upper () { throw new RuntimeException(); }
  protected  double lower () { throw new RuntimeException(); }
  public  void gatherStats (org.apache.spark.sql.catalyst.InternalRow row, int ordinal) { throw new RuntimeException(); }
  public  org.apache.spark.sql.catalyst.expressions.GenericInternalRow collectedStatistics () { throw new RuntimeException(); }
}
