package org.apache.spark.sql.columnar;
// no position
public  class ColumnarTestUtils$ {
  /**
   * Static reference to the singleton instance of this Scala object.
   */
  public static final ColumnarTestUtils$ MODULE$ = null;
  public   ColumnarTestUtils$ () { throw new RuntimeException(); }
  public  org.apache.spark.sql.catalyst.expressions.GenericMutableRow makeNullRow (int length) { throw new RuntimeException(); }
  public <JvmType extends java.lang.Object> JvmType makeRandomValue (org.apache.spark.sql.columnar.ColumnType<JvmType> columnType) { throw new RuntimeException(); }
  public  scala.collection.Seq<java.lang.Object> makeRandomValues (org.apache.spark.sql.columnar.ColumnType<?> head, scala.collection.Seq<org.apache.spark.sql.columnar.ColumnType<?>> tail) { throw new RuntimeException(); }
  public  scala.collection.Seq<java.lang.Object> makeRandomValues (scala.collection.Seq<org.apache.spark.sql.columnar.ColumnType<?>> columnTypes) { throw new RuntimeException(); }
  public <JvmType extends java.lang.Object> scala.collection.Seq<JvmType> makeUniqueRandomValues (org.apache.spark.sql.columnar.ColumnType<JvmType> columnType, int count) { throw new RuntimeException(); }
  public  org.apache.spark.sql.catalyst.InternalRow makeRandomRow (org.apache.spark.sql.columnar.ColumnType<?> head, scala.collection.Seq<org.apache.spark.sql.columnar.ColumnType<?>> tail) { throw new RuntimeException(); }
  public  org.apache.spark.sql.catalyst.InternalRow makeRandomRow (scala.collection.Seq<org.apache.spark.sql.columnar.ColumnType<?>> columnTypes) { throw new RuntimeException(); }
  public <T extends org.apache.spark.sql.types.AtomicType> scala.Tuple2<scala.collection.Seq<java.lang.Object>, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.GenericMutableRow>> makeUniqueValuesAndSingleValueRows (org.apache.spark.sql.columnar.NativeColumnType<T> columnType, int count) { throw new RuntimeException(); }
}
