package org.apache.spark.sql.execution.datasources.parquet;
// no position
  class DirectParquetWriter$ {
  /**
   * Static reference to the singleton instance of this Scala object.
   */
  public static final DirectParquetWriter$ MODULE$ = null;
  public   DirectParquetWriter$ () { throw new RuntimeException(); }
  public  void writeDirect (java.lang.String path, java.lang.String schema, scala.collection.immutable.Map<java.lang.String, java.lang.String> metadata, scala.Function1<org.apache.parquet.hadoop.ParquetWriter<scala.Function1<org.apache.parquet.io.api.RecordConsumer, scala.runtime.BoxedUnit>>, scala.runtime.BoxedUnit> f) { throw new RuntimeException(); }
  public  void message (org.apache.parquet.hadoop.ParquetWriter<scala.Function1<org.apache.parquet.io.api.RecordConsumer, scala.runtime.BoxedUnit>> writer, scala.Function1<org.apache.parquet.io.api.RecordConsumer, scala.runtime.BoxedUnit> builder) { throw new RuntimeException(); }
  public  void group (org.apache.parquet.io.api.RecordConsumer consumer, scala.Function0<scala.runtime.BoxedUnit> f) { throw new RuntimeException(); }
  public  void field (org.apache.parquet.io.api.RecordConsumer consumer, java.lang.String name, int index, scala.Function0<scala.runtime.BoxedUnit> f) { throw new RuntimeException(); }
}
