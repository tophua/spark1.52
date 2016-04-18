package org.apache.spark.sql.columnar;
  class FixedDecimalColumnStats implements org.apache.spark.sql.columnar.ColumnStats {
  public   FixedDecimalColumnStats (int precision, int scale) { throw new RuntimeException(); }
  protected  org.apache.spark.sql.types.Decimal upper () { throw new RuntimeException(); }
  protected  org.apache.spark.sql.types.Decimal lower () { throw new RuntimeException(); }
  public  void gatherStats (org.apache.spark.sql.catalyst.InternalRow row, int ordinal) { throw new RuntimeException(); }
  public  org.apache.spark.sql.catalyst.expressions.GenericInternalRow collectedStatistics () { throw new RuntimeException(); }
}
