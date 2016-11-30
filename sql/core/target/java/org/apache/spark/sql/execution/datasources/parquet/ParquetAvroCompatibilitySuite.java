package org.apache.spark.sql.execution.datasources.parquet;
public  class ParquetAvroCompatibilitySuite extends org.apache.spark.sql.execution.datasources.parquet.ParquetCompatibilityTest implements org.apache.spark.sql.test.SharedSQLContext {
  public   ParquetAvroCompatibilitySuite () { throw new RuntimeException(); }
  private <T extends org.apache.avro.generic.IndexedRecord> void withWriter (java.lang.String path, org.apache.avro.Schema schema, scala.Function1<org.apache.parquet.avro.AvroParquetWriter<T>, scala.runtime.BoxedUnit> f) { throw new RuntimeException(); }
  public  org.apache.spark.sql.execution.datasources.parquet.test.avro.ParquetAvroCompat makeParquetAvroCompat (int i) { throw new RuntimeException(); }
}
