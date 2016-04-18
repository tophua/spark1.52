package org.apache.spark.sql.execution.datasources.parquet;
// no position
  class DirectParquetWriter {
  /**
   * A testing Parquet {@link WriteSupport} implementation used to write manually constructed Parquet
   * records with arbitrary structures.
   */
  static private  class DirectWriteSupport extends org.apache.parquet.hadoop.api.WriteSupport<scala.Function1<org.apache.parquet.io.api.RecordConsumer, scala.runtime.BoxedUnit>> {
    public   DirectWriteSupport (org.apache.parquet.schema.MessageType schema, scala.collection.immutable.Map<java.lang.String, java.lang.String> metadata) { throw new RuntimeException(); }
    private  org.apache.parquet.io.api.RecordConsumer recordConsumer () { throw new RuntimeException(); }
    public  org.apache.parquet.hadoop.api.WriteSupport.WriteContext init (org.apache.hadoop.conf.Configuration configuration) { throw new RuntimeException(); }
    public  void write (scala.Function1<org.apache.parquet.io.api.RecordConsumer, scala.runtime.BoxedUnit> buildRecord) { throw new RuntimeException(); }
    public  void prepareForWrite (org.apache.parquet.io.api.RecordConsumer recordConsumer) { throw new RuntimeException(); }
  }
  static public  void writeDirect (java.lang.String path, java.lang.String schema, scala.collection.immutable.Map<java.lang.String, java.lang.String> metadata, scala.Function1<org.apache.parquet.hadoop.ParquetWriter<scala.Function1<org.apache.parquet.io.api.RecordConsumer, scala.runtime.BoxedUnit>>, scala.runtime.BoxedUnit> f) { throw new RuntimeException(); }
  static public  void message (org.apache.parquet.hadoop.ParquetWriter<scala.Function1<org.apache.parquet.io.api.RecordConsumer, scala.runtime.BoxedUnit>> writer, scala.Function1<org.apache.parquet.io.api.RecordConsumer, scala.runtime.BoxedUnit> builder) { throw new RuntimeException(); }
  static public  void group (org.apache.parquet.io.api.RecordConsumer consumer, scala.Function0<scala.runtime.BoxedUnit> f) { throw new RuntimeException(); }
  static public  void field (org.apache.parquet.io.api.RecordConsumer consumer, java.lang.String name, int index, scala.Function0<scala.runtime.BoxedUnit> f) { throw new RuntimeException(); }
}
