package org.apache.spark.sql.columnar;
  class BooleanColumnStats implements org.apache.spark.sql.columnar.ColumnStats {
  public   BooleanColumnStats () { throw new RuntimeException(); }
  protected  boolean upper () { throw new RuntimeException(); }
  protected  boolean lower () { throw new RuntimeException(); }
  public  void gatherStats (org.apache.spark.sql.catalyst.InternalRow row, int ordinal) { throw new RuntimeException(); }
  public  org.apache.spark.sql.catalyst.expressions.GenericInternalRow collectedStatistics () { throw new RuntimeException(); }
}
