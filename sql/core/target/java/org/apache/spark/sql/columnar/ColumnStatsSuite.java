package org.apache.spark.sql.columnar;
public  class ColumnStatsSuite extends org.apache.spark.SparkFunSuite {
  public   ColumnStatsSuite () { throw new RuntimeException(); }
  public  org.apache.spark.sql.catalyst.expressions.GenericInternalRow createRow (scala.collection.Seq<java.lang.Object> values) { throw new RuntimeException(); }
  public <T extends org.apache.spark.sql.types.AtomicType, U extends org.apache.spark.sql.columnar.ColumnStats> void testColumnStats (java.lang.Class<U> columnStatsClass, org.apache.spark.sql.columnar.NativeColumnType<T> columnType, org.apache.spark.sql.catalyst.expressions.GenericInternalRow initialStatistics) { throw new RuntimeException(); }
  public <T extends org.apache.spark.sql.types.AtomicType, U extends org.apache.spark.sql.columnar.ColumnStats> void testDecimalColumnStats (org.apache.spark.sql.catalyst.expressions.GenericInternalRow initialStatistics) { throw new RuntimeException(); }
}
