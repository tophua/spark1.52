package org.apache.spark.sql.execution.datasources.parquet;
/**
 * A <code>parquet.hadoop.api.WriteSupport</code> for Row objects.
 */
  class RowWriteSupport extends org.apache.parquet.hadoop.api.WriteSupport<org.apache.spark.sql.catalyst.InternalRow> implements org.apache.spark.Logging {
  static public  java.lang.String SPARK_ROW_SCHEMA () { throw new RuntimeException(); }
  static public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> getSchema (org.apache.hadoop.conf.Configuration configuration) { throw new RuntimeException(); }
  static public  void setSchema (scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> schema, org.apache.hadoop.conf.Configuration configuration) { throw new RuntimeException(); }
  public   RowWriteSupport () { throw new RuntimeException(); }
  public  org.apache.parquet.io.api.RecordConsumer writer () { throw new RuntimeException(); }
  public  org.apache.spark.sql.catalyst.expressions.Attribute[] attributes () { throw new RuntimeException(); }
  public  org.apache.parquet.hadoop.api.WriteSupport.WriteContext init (org.apache.hadoop.conf.Configuration configuration) { throw new RuntimeException(); }
  public  void prepareForWrite (org.apache.parquet.io.api.RecordConsumer recordConsumer) { throw new RuntimeException(); }
  public  void write (org.apache.spark.sql.catalyst.InternalRow record) { throw new RuntimeException(); }
    void writeValue (org.apache.spark.sql.types.DataType schema, Object value) { throw new RuntimeException(); }
    void writePrimitive (org.apache.spark.sql.types.DataType schema, Object value) { throw new RuntimeException(); }
    void writeStruct (org.apache.spark.sql.types.StructType schema, org.apache.spark.sql.catalyst.InternalRow struct) { throw new RuntimeException(); }
    void writeArray (org.apache.spark.sql.types.ArrayType schema, org.apache.spark.sql.types.ArrayData array) { throw new RuntimeException(); }
    void writeMap (org.apache.spark.sql.types.MapType schema, org.apache.spark.sql.types.MapData map) { throw new RuntimeException(); }
    void writeDecimal (org.apache.spark.sql.types.Decimal decimal, int precision) { throw new RuntimeException(); }
    void writeTimestamp (long ts) { throw new RuntimeException(); }
}
