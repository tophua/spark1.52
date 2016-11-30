package org.apache.spark.sql.columnar;
  class StringColumnStats implements org.apache.spark.sql.columnar.ColumnStats {
  public   StringColumnStats () { throw new RuntimeException(); }
  protected  org.apache.spark.unsafe.types.UTF8String upper () { throw new RuntimeException(); }
  protected  org.apache.spark.unsafe.types.UTF8String lower () { throw new RuntimeException(); }
  public  void gatherStats (org.apache.spark.sql.catalyst.InternalRow row, int ordinal) { throw new RuntimeException(); }
  public  org.apache.spark.sql.catalyst.expressions.GenericInternalRow collectedStatistics () { throw new RuntimeException(); }
}
