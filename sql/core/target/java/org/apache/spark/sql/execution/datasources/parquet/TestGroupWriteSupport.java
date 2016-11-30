package org.apache.spark.sql.execution.datasources.parquet;
  class TestGroupWriteSupport extends org.apache.parquet.hadoop.api.WriteSupport<org.apache.parquet.example.data.Group> {
  public   TestGroupWriteSupport (org.apache.parquet.schema.MessageType schema) { throw new RuntimeException(); }
  public  org.apache.parquet.example.data.GroupWriter groupWriter () { throw new RuntimeException(); }
  public  void prepareForWrite (org.apache.parquet.io.api.RecordConsumer recordConsumer) { throw new RuntimeException(); }
  public  org.apache.parquet.hadoop.api.WriteSupport.WriteContext init (org.apache.hadoop.conf.Configuration configuration) { throw new RuntimeException(); }
  public  void write (org.apache.parquet.example.data.Group record) { throw new RuntimeException(); }
}
