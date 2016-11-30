package org.apache.spark.sql.execution.datasources.parquet;
  class DefaultSource implements org.apache.spark.sql.sources.HadoopFsRelationProvider, org.apache.spark.sql.sources.DataSourceRegister {
  public   DefaultSource () { throw new RuntimeException(); }
  public  java.lang.String shortName () { throw new RuntimeException(); }
  public  org.apache.spark.sql.sources.HadoopFsRelation createRelation (org.apache.spark.sql.SQLContext sqlContext, java.lang.String[] paths, scala.Option<org.apache.spark.sql.types.StructType> schema, scala.Option<org.apache.spark.sql.types.StructType> partitionColumns, scala.collection.immutable.Map<java.lang.String, java.lang.String> parameters) { throw new RuntimeException(); }
}
