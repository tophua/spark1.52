package org.apache.spark.sql.columnar;
 abstract class ComplexColumnBuilder<JvmType extends java.lang.Object> extends org.apache.spark.sql.columnar.BasicColumnBuilder<JvmType> implements org.apache.spark.sql.columnar.NullableColumnBuilder {
  // not preceding
  // TypeTree().setOriginal(TypeBoundsTree(TypeTree().setOriginal(Select(Select(Ident(_root_), scala), scala.Nothing)), TypeTree().setOriginal(Select(Select(Ident(_root_), scala), scala.Any))))
  public   ComplexColumnBuilder (org.apache.spark.sql.columnar.ColumnStats columnStats, org.apache.spark.sql.columnar.ColumnType<JvmType> columnType) { throw new RuntimeException(); }
}
