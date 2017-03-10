package org.apache.spark.sql.execution.datasources.parquet;
public  class ParquetAvroCompatibilitySuite extends org.apache.spark.sql.execution.datasources.parquet.ParquetCompatibilityTest implements org.apache.spark.sql.test.SharedSQLContext {
  public   ParquetAvroCompatibilitySuite () { throw new RuntimeException(); }
  private <T extends org.apache.avro.generic.IndexedRecord> void withWriter (java.lang.String path, org.apache.avro.Schema schema, scala.Function1<org.apache.parquet.avro.AvroParquetWriter<T>, scala.runtime.BoxedUnit> f) { throw new RuntimeException(); }
  /**
   *  +--------------------+--------------------+--------------------+
   |      strings_column|string_to_int_column|      complex_column|
   +--------------------+--------------------+--------------------+
   |[arr_0, arr_1, ar...|Map(0 -> 0, 1 -> ...|Map(0 -> WrappedA...|
   |[arr_1, arr_2, ar...|Map(0 -> 1, 1 -> ...|Map(1 -> WrappedA...|
   |[arr_2, arr_3, ar...|Map(0 -> 2, 1 -> ...|Map(2 -> WrappedA...|
   |[arr_3, arr_4, ar...|Map(0 -> 3, 1 -> ...|Map(3 -> WrappedA...|
   |[arr_4, arr_5, ar...|Map(0 -> 4, 1 -> ...|Map(4 -> WrappedA...|
   |[arr_5, arr_6, ar...|Map(0 -> 5, 1 -> ...|Map(5 -> WrappedA...|
   |[arr_6, arr_7, ar...|Map(0 -> 6, 1 -> ...|Map(6 -> WrappedA...|
   |[arr_7, arr_8, ar...|Map(0 -> 7, 1 -> ...|Map(7 -> WrappedA...|
   |[arr_8, arr_9, ar...|Map(0 -> 8, 1 -> ...|Map(8 -> WrappedA...|
   |[arr_9, arr_10, a...|Map(0 -> 9, 1 -> ...|Map(9 -> WrappedA...|
   +--------------------+--------------------+--------------------+
   * @param i (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.execution.datasources.parquet.test.avro.ParquetAvroCompat makeParquetAvroCompat (int i) { throw new RuntimeException(); }
}
