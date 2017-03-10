package org.apache.spark.sql.execution.datasources.parquet;
// no position
/**
 * Parquet&#x662f;&#x4e00;&#x79cd;&#x9762;&#x5411;&#x5217;&#x5b58;&#x5b58;&#x50a8;&#x7684;&#x6587;&#x4ef6;&#x683c;&#x5f0f;
 * &#x76f4;&#x63a5;&#x5199;&#x5165;Parquet
 */
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
