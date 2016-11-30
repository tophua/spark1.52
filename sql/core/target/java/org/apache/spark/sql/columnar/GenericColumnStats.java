package org.apache.spark.sql.columnar;
  class GenericColumnStats implements org.apache.spark.sql.columnar.ColumnStats {
  public   GenericColumnStats (org.apache.spark.sql.types.DataType dataType) { throw new RuntimeException(); }
  public  org.apache.spark.sql.columnar.GENERIC columnType () { throw new RuntimeException(); }
  public  void gatherStats (org.apache.spark.sql.catalyst.InternalRow row, int ordinal) { throw new RuntimeException(); }
  public  org.apache.spark.sql.catalyst.expressions.GenericInternalRow collectedStatistics () { throw new RuntimeException(); }
}
