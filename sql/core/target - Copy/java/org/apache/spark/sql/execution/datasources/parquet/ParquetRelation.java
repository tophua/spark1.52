package org.apache.spark.sql.execution.datasources.parquet;
  class ParquetRelation extends org.apache.spark.sql.sources.HadoopFsRelation implements org.apache.spark.Logging {
  private  class MetadataCache {
    public   MetadataCache () { throw new RuntimeException(); }
    private  org.apache.hadoop.fs.FileStatus[] metadataStatuses () { throw new RuntimeException(); }
    private  org.apache.hadoop.fs.FileStatus[] commonMetadataStatuses () { throw new RuntimeException(); }
    public  org.apache.hadoop.fs.FileStatus[] dataStatuses () { throw new RuntimeException(); }
    public  org.apache.spark.sql.types.StructType dataSchema () { throw new RuntimeException(); }
    public  org.apache.spark.sql.types.StructType schema () { throw new RuntimeException(); }
    public  scala.collection.immutable.Set<org.apache.hadoop.fs.FileStatus> cachedLeaves () { throw new RuntimeException(); }
    /**
     * Refreshes <code>FileStatus</code>es, footers, partition spec, and table schema.
     */
    public  void refresh () { throw new RuntimeException(); }
    private  boolean isSummaryFile (org.apache.hadoop.fs.Path file) { throw new RuntimeException(); }
    private  scala.Option<org.apache.spark.sql.types.StructType> readSchema () { throw new RuntimeException(); }
  }
  static public  java.lang.String MERGE_SCHEMA () { throw new RuntimeException(); }
  static public  java.lang.String METASTORE_SCHEMA () { throw new RuntimeException(); }
  /**
   * If parquet's block size (row group size) setting is larger than the min split size,
   * we use parquet's block size setting as the min split size. Otherwise, we will create
   * tasks processing nothing (because a split does not cover the starting point of a
   * parquet block). See https://issues.apache.org/jira/browse/SPARK-10143 for more information.
   * @param parquetBlockSize (undocumented)
   * @param conf (undocumented)
   */
  static private  void overrideMinSplitSize (long parquetBlockSize, org.apache.hadoop.conf.Configuration conf) { throw new RuntimeException(); }
  /** This closure sets various Parquet configurations at both driver side and executor side. */
  static   void initializeLocalJobFunc (java.lang.String[] requiredColumns, org.apache.spark.sql.sources.Filter[] filters, org.apache.spark.sql.types.StructType dataSchema, long parquetBlockSize, boolean useMetadataCache, boolean parquetFilterPushDown, boolean assumeBinaryIsString, boolean assumeInt96IsTimestamp, boolean followParquetFormatSpec, org.apache.hadoop.mapreduce.Job job) { throw new RuntimeException(); }
  /** This closure sets input paths at the driver side. */
  static   void initializeDriverSideJobFunc (org.apache.hadoop.fs.FileStatus[] inputFiles, long parquetBlockSize, org.apache.hadoop.mapreduce.Job job) { throw new RuntimeException(); }
  static   scala.Option<org.apache.spark.sql.types.StructType> readSchema (scala.collection.Seq<org.apache.parquet.hadoop.Footer> footers, org.apache.spark.sql.SQLContext sqlContext) { throw new RuntimeException(); }
  /**
   * Reconciles Hive Metastore case insensitivity issue and data type conflicts between Metastore
   * schema and Parquet schema.
   * <p>
   * Hive doesn't retain case information, while Parquet is case sensitive. On the other hand, the
   * schema read from Parquet files may be incomplete (e.g. older versions of Parquet doesn't
   * distinguish binary and string).  This method generates a correct schema by merging Metastore
   * schema data types and Parquet schema field names.
   * @param metastoreSchema (undocumented)
   * @param parquetSchema (undocumented)
   * @return (undocumented)
   */
  static   org.apache.spark.sql.types.StructType mergeMetastoreParquetSchema (org.apache.spark.sql.types.StructType metastoreSchema, org.apache.spark.sql.types.StructType parquetSchema) { throw new RuntimeException(); }
  /**
   * Returns the original schema from the Parquet file with any missing nullable fields from the
   * Hive Metastore schema merged in.
   * <p>
   * When constructing a DataFrame from a collection of structured data, the resulting object has
   * a schema corresponding to the union of the fields present in each element of the collection.
   * Spark SQL simply assigns a null value to any field that isn't present for a particular row.
   * In some cases, it is possible that a given table partition stored as a Parquet file doesn't
   * contain a particular nullable field in its schema despite that field being present in the
   * table schema obtained from the Hive Metastore. This method returns a schema representing the
   * Parquet file schema along with any additional nullable fields from the Metastore schema
   * merged in.
   * @param metastoreSchema (undocumented)
   * @param parquetSchema (undocumented)
   * @return (undocumented)
   */
  static   org.apache.spark.sql.types.StructType mergeMissingNullableFields (org.apache.spark.sql.types.StructType metastoreSchema, org.apache.spark.sql.types.StructType parquetSchema) { throw new RuntimeException(); }
  /**
   * Figures out a merged Parquet schema with a distributed Spark job.
   * <p>
   * Note that locality is not taken into consideration here because:
   * <p>
   *  1. For a single Parquet part-file, in most cases the footer only resides in the last block of
   *     that file.  Thus we only need to retrieve the location of the last block.  However, Hadoop
   *     <code>FileSystem</code> only provides API to retrieve locations of all blocks, which can be
   *     potentially expensive.
   * <p>
   *  2. This optimization is mainly useful for S3, where file metadata operations can be pretty
   *     slow.  And basically locality is not available when using S3 (you can't run computation on
   *     S3 nodes).
   * @param filesToTouch (undocumented)
   * @param sqlContext (undocumented)
   * @return (undocumented)
   */
  static public  scala.Option<org.apache.spark.sql.types.StructType> mergeSchemasInParallel (scala.collection.Seq<org.apache.hadoop.fs.FileStatus> filesToTouch, org.apache.spark.sql.SQLContext sqlContext) { throw new RuntimeException(); }
  /**
   * Reads Spark SQL schema from a Parquet footer.  If a valid serialized Spark SQL schema string
   * can be found in the file metadata, returns the deserialized {@link StructType}, otherwise, returns
   * a {@link StructType} converted from the {@link MessageType} stored in this footer.
   * @param footer (undocumented)
   * @param converter (undocumented)
   * @return (undocumented)
   */
  static public  org.apache.spark.sql.types.StructType readSchemaFromFooter (org.apache.parquet.hadoop.Footer footer, org.apache.spark.sql.execution.datasources.parquet.CatalystSchemaConverter converter) { throw new RuntimeException(); }
  static private  scala.Option<org.apache.spark.sql.types.StructType> deserializeSchemaString (java.lang.String schemaString) { throw new RuntimeException(); }
  static public  java.util.logging.Logger apacheParquetLogger () { throw new RuntimeException(); }
  static public  java.util.logging.Logger parquetLogger () { throw new RuntimeException(); }
  static public  void redirectParquetLogsViaSLF4J () { throw new RuntimeException(); }
  static public  scala.collection.immutable.Map<java.lang.String, org.apache.parquet.hadoop.metadata.CompressionCodecName> shortParquetCompressionCodecNames () { throw new RuntimeException(); }
  public  java.lang.String[] paths () { throw new RuntimeException(); }
  private  scala.Option<org.apache.spark.sql.types.StructType> maybeDataSchema () { throw new RuntimeException(); }
  private  scala.Option<org.apache.spark.sql.execution.datasources.PartitionSpec> maybePartitionSpec () { throw new RuntimeException(); }
  public  scala.Option<org.apache.spark.sql.types.StructType> userDefinedPartitionColumns () { throw new RuntimeException(); }
  public  org.apache.spark.sql.SQLContext sqlContext () { throw new RuntimeException(); }
  // not preceding
  public   ParquetRelation (java.lang.String[] paths, scala.Option<org.apache.spark.sql.types.StructType> maybeDataSchema, scala.Option<org.apache.spark.sql.execution.datasources.PartitionSpec> maybePartitionSpec, scala.Option<org.apache.spark.sql.types.StructType> userDefinedPartitionColumns, scala.collection.immutable.Map<java.lang.String, java.lang.String> parameters, org.apache.spark.sql.SQLContext sqlContext) { throw new RuntimeException(); }
     ParquetRelation (java.lang.String[] paths, scala.Option<org.apache.spark.sql.types.StructType> maybeDataSchema, scala.Option<org.apache.spark.sql.execution.datasources.PartitionSpec> maybePartitionSpec, scala.collection.immutable.Map<java.lang.String, java.lang.String> parameters, org.apache.spark.sql.SQLContext sqlContext) { throw new RuntimeException(); }
  private  boolean shouldMergeSchemas () { throw new RuntimeException(); }
  private  boolean mergeRespectSummaries () { throw new RuntimeException(); }
  private  scala.Option<org.apache.spark.sql.types.StructType> maybeMetastoreSchema () { throw new RuntimeException(); }
  private  org.apache.spark.sql.execution.datasources.parquet.ParquetRelation.MetadataCache metadataCache () { throw new RuntimeException(); }
  public  boolean equals (Object other) { throw new RuntimeException(); }
  public  int hashCode () { throw new RuntimeException(); }
  /** Constraints on schema of dataframe to be stored. */
  private  void checkConstraints (org.apache.spark.sql.types.StructType schema) { throw new RuntimeException(); }
  public  org.apache.spark.sql.types.StructType dataSchema () { throw new RuntimeException(); }
    void refresh () { throw new RuntimeException(); }
  public  boolean needConversion () { throw new RuntimeException(); }
  public  long sizeInBytes () { throw new RuntimeException(); }
  public  org.apache.spark.sql.sources.OutputWriterFactory prepareJobForWrite (org.apache.hadoop.mapreduce.Job job) { throw new RuntimeException(); }
  public  org.apache.spark.rdd.RDD<org.apache.spark.sql.Row> buildScan (java.lang.String[] requiredColumns, org.apache.spark.sql.sources.Filter[] filters, org.apache.hadoop.fs.FileStatus[] inputFiles, org.apache.spark.broadcast.Broadcast<org.apache.spark.util.SerializableConfiguration> broadcastedConf) { throw new RuntimeException(); }
}
