package org.apache.spark.sql.execution.datasources.parquet;
/**
 * A {@link RecordMaterializer} for Catalyst rows.
 * <p>
 * param:  parquetSchema Parquet schema of the records to be read
 * param:  catalystSchema Catalyst schema of the rows to be constructed
 */
  class CatalystRecordMaterializer extends org.apache.parquet.io.api.RecordMaterializer<org.apache.spark.sql.catalyst.InternalRow> {
  public   CatalystRecordMaterializer (org.apache.parquet.schema.MessageType parquetSchema, org.apache.spark.sql.types.StructType catalystSchema) { throw new RuntimeException(); }
  private  org.apache.spark.sql.execution.datasources.parquet.CatalystRowConverter rootConverter () { throw new RuntimeException(); }
  public  org.apache.spark.sql.catalyst.InternalRow getCurrentRecord () { throw new RuntimeException(); }
  public  org.apache.parquet.io.api.GroupConverter getRootConverter () { throw new RuntimeException(); }
}
