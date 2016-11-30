package org.apache.spark.sql.execution.datasources.json;
  class JSONRelation extends org.apache.spark.sql.sources.HadoopFsRelation {
  public  scala.Option<org.apache.spark.rdd.RDD<java.lang.String>> inputRDD () { throw new RuntimeException(); }
  public  double samplingRatio () { throw new RuntimeException(); }
  public  scala.Option<org.apache.spark.sql.types.StructType> maybeDataSchema () { throw new RuntimeException(); }
  public  scala.Option<org.apache.spark.sql.execution.datasources.PartitionSpec> maybePartitionSpec () { throw new RuntimeException(); }
  public  scala.Option<org.apache.spark.sql.types.StructType> userDefinedPartitionColumns () { throw new RuntimeException(); }
  public  java.lang.String[] paths () { throw new RuntimeException(); }
  public  org.apache.spark.sql.SQLContext sqlContext () { throw new RuntimeException(); }
  // not preceding
  public   JSONRelation (scala.Option<org.apache.spark.rdd.RDD<java.lang.String>> inputRDD, double samplingRatio, scala.Option<org.apache.spark.sql.types.StructType> maybeDataSchema, scala.Option<org.apache.spark.sql.execution.datasources.PartitionSpec> maybePartitionSpec, scala.Option<org.apache.spark.sql.types.StructType> userDefinedPartitionColumns, java.lang.String[] paths, org.apache.spark.sql.SQLContext sqlContext) { throw new RuntimeException(); }
  /** Constraints to be imposed on schema to be stored. */
  private  void checkConstraints (org.apache.spark.sql.types.StructType schema) { throw new RuntimeException(); }
  public  boolean needConversion () { throw new RuntimeException(); }
  private  org.apache.spark.rdd.RDD<java.lang.String> createBaseRdd (org.apache.hadoop.fs.FileStatus[] inputPaths) { throw new RuntimeException(); }
  public  org.apache.spark.sql.types.StructType dataSchema () { throw new RuntimeException(); }
  public  org.apache.spark.rdd.RDD<org.apache.spark.sql.Row> buildScan (java.lang.String[] requiredColumns, org.apache.spark.sql.sources.Filter[] filters, org.apache.hadoop.fs.FileStatus[] inputPaths) { throw new RuntimeException(); }
  public  boolean equals (Object other) { throw new RuntimeException(); }
  public  int hashCode () { throw new RuntimeException(); }
  public  org.apache.spark.sql.sources.OutputWriterFactory prepareJobForWrite (org.apache.hadoop.mapreduce.Job job) { throw new RuntimeException(); }
}
