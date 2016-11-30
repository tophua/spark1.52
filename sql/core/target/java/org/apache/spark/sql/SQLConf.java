package org.apache.spark.sql;
/**
 * A class that enables the setting and getting of mutable config parameters/hints.
 * <p>
 * In the presence of a SQLContext, these can be set and queried by passing SET commands
 * into Spark SQL's query functions (i.e. sql()). Otherwise, users of this class can
 * modify the hints by programmatically calling the setters and getters of this class.
 * <p>
 * SQLConf is thread-safe (internally synchronized, so safe to be used in multiple threads).
 */
  class SQLConf implements scala.Serializable, org.apache.spark.sql.catalyst.CatalystConf {
  /**
   * An entry contains all meta information for a configuration.
   * <p>
   * param:  key the key for the configuration
   * param:  defaultValue the default value for the configuration
   * param:  valueConverter how to convert a string to the value. It should throw an exception if the
   *                       string does not have the required format.
   * param:  stringConverter how to convert a value to a string that the user can use it as a valid
   *                        string value. It's usually <code>toString</code>. But sometimes, a custom converter
   *                        is necessary. E.g., if T is List[String], <code>a, b, c</code> is better than
   *                        <code>List(a, b, c)</code>.
   * param:  doc the document for the configuration
   * param:  isPublic if this configuration is public to the user. If it's <code>false</code>, this
   *                 configuration is only used internally and we should not expose it to the user.
   * @tparam T the value type
   */
  static   class SQLConfEntry<T extends java.lang.Object> {
    public  java.lang.String key () { throw new RuntimeException(); }
    public  scala.Option<T> defaultValue () { throw new RuntimeException(); }
    public  scala.Function1<java.lang.String, T> valueConverter () { throw new RuntimeException(); }
    public  scala.Function1<T, java.lang.String> stringConverter () { throw new RuntimeException(); }
    public  java.lang.String doc () { throw new RuntimeException(); }
    public  boolean isPublic () { throw new RuntimeException(); }
    // not preceding
    private   SQLConfEntry (java.lang.String key, scala.Option<T> defaultValue, scala.Function1<java.lang.String, T> valueConverter, scala.Function1<T, java.lang.String> stringConverter, java.lang.String doc, boolean isPublic) { throw new RuntimeException(); }
    public  java.lang.String defaultValueString () { throw new RuntimeException(); }
    public  java.lang.String toString () { throw new RuntimeException(); }
  }
  // no position
  static   class SQLConfEntry$ {
    /**
     * Static reference to the singleton instance of this Scala object.
     */
    public static final SQLConfEntry$ MODULE$ = null;
    public   SQLConfEntry$ () { throw new RuntimeException(); }
    private <T extends java.lang.Object> org.apache.spark.sql.SQLConf.SQLConfEntry<T> apply (java.lang.String key, scala.Option<T> defaultValue, scala.Function1<java.lang.String, T> valueConverter, scala.Function1<T, java.lang.String> stringConverter, java.lang.String doc, boolean isPublic) { throw new RuntimeException(); }
    public  org.apache.spark.sql.SQLConf.SQLConfEntry<java.lang.Object> intConf (java.lang.String key, scala.Option<java.lang.Object> defaultValue, java.lang.String doc, boolean isPublic) { throw new RuntimeException(); }
    public  org.apache.spark.sql.SQLConf.SQLConfEntry<java.lang.Object> longConf (java.lang.String key, scala.Option<java.lang.Object> defaultValue, java.lang.String doc, boolean isPublic) { throw new RuntimeException(); }
    public  org.apache.spark.sql.SQLConf.SQLConfEntry<java.lang.Object> doubleConf (java.lang.String key, scala.Option<java.lang.Object> defaultValue, java.lang.String doc, boolean isPublic) { throw new RuntimeException(); }
    public  org.apache.spark.sql.SQLConf.SQLConfEntry<java.lang.Object> booleanConf (java.lang.String key, scala.Option<java.lang.Object> defaultValue, java.lang.String doc, boolean isPublic) { throw new RuntimeException(); }
    public  org.apache.spark.sql.SQLConf.SQLConfEntry<java.lang.String> stringConf (java.lang.String key, scala.Option<java.lang.String> defaultValue, java.lang.String doc, boolean isPublic) { throw new RuntimeException(); }
    public <T extends java.lang.Object> org.apache.spark.sql.SQLConf.SQLConfEntry<T> enumConf (java.lang.String key, scala.Function1<java.lang.String, T> valueConverter, scala.collection.immutable.Set<T> validValues, scala.Option<T> defaultValue, java.lang.String doc, boolean isPublic) { throw new RuntimeException(); }
    public <T extends java.lang.Object> org.apache.spark.sql.SQLConf.SQLConfEntry<scala.collection.Seq<T>> seqConf (java.lang.String key, scala.Function1<java.lang.String, T> valueConverter, scala.Option<scala.collection.Seq<T>> defaultValue, java.lang.String doc, boolean isPublic) { throw new RuntimeException(); }
    public  org.apache.spark.sql.SQLConf.SQLConfEntry<scala.collection.Seq<java.lang.String>> stringSeqConf (java.lang.String key, scala.Option<scala.collection.Seq<java.lang.String>> defaultValue, java.lang.String doc, boolean isPublic) { throw new RuntimeException(); }
  }
  // no position
  static public  class Deprecated$ {
    /**
     * Static reference to the singleton instance of this Scala object.
     */
    public static final Deprecated$ MODULE$ = null;
    public   Deprecated$ () { throw new RuntimeException(); }
    public  java.lang.String MAPRED_REDUCE_TASKS () { throw new RuntimeException(); }
  }
  static private  java.util.Map<java.lang.String, org.apache.spark.sql.SQLConf.SQLConfEntry<?>> sqlConfEntries () { throw new RuntimeException(); }
  static public  org.apache.spark.sql.SQLConf.SQLConfEntry<java.lang.Object> COMPRESS_CACHED () { throw new RuntimeException(); }
  static public  org.apache.spark.sql.SQLConf.SQLConfEntry<java.lang.Object> COLUMN_BATCH_SIZE () { throw new RuntimeException(); }
  static public  org.apache.spark.sql.SQLConf.SQLConfEntry<java.lang.Object> IN_MEMORY_PARTITION_PRUNING () { throw new RuntimeException(); }
  static public  org.apache.spark.sql.SQLConf.SQLConfEntry<java.lang.Object> AUTO_BROADCASTJOIN_THRESHOLD () { throw new RuntimeException(); }
  static public  org.apache.spark.sql.SQLConf.SQLConfEntry<java.lang.Object> DEFAULT_SIZE_IN_BYTES () { throw new RuntimeException(); }
  static public  org.apache.spark.sql.SQLConf.SQLConfEntry<java.lang.Object> SHUFFLE_PARTITIONS () { throw new RuntimeException(); }
  static public  org.apache.spark.sql.SQLConf.SQLConfEntry<java.lang.Object> TUNGSTEN_ENABLED () { throw new RuntimeException(); }
  static public  org.apache.spark.sql.SQLConf.SQLConfEntry<java.lang.Object> CODEGEN_ENABLED () { throw new RuntimeException(); }
  static public  org.apache.spark.sql.SQLConf.SQLConfEntry<java.lang.Object> UNSAFE_ENABLED () { throw new RuntimeException(); }
  static public  org.apache.spark.sql.SQLConf.SQLConfEntry<java.lang.String> DIALECT () { throw new RuntimeException(); }
  static public  org.apache.spark.sql.SQLConf.SQLConfEntry<java.lang.Object> CASE_SENSITIVE () { throw new RuntimeException(); }
  static public  org.apache.spark.sql.SQLConf.SQLConfEntry<java.lang.Object> PARQUET_SCHEMA_MERGING_ENABLED () { throw new RuntimeException(); }
  static public  org.apache.spark.sql.SQLConf.SQLConfEntry<java.lang.Object> PARQUET_SCHEMA_RESPECT_SUMMARIES () { throw new RuntimeException(); }
  static public  org.apache.spark.sql.SQLConf.SQLConfEntry<java.lang.Object> PARQUET_BINARY_AS_STRING () { throw new RuntimeException(); }
  static public  org.apache.spark.sql.SQLConf.SQLConfEntry<java.lang.Object> PARQUET_INT96_AS_TIMESTAMP () { throw new RuntimeException(); }
  static public  org.apache.spark.sql.SQLConf.SQLConfEntry<java.lang.Object> PARQUET_CACHE_METADATA () { throw new RuntimeException(); }
  static public  org.apache.spark.sql.SQLConf.SQLConfEntry<java.lang.String> PARQUET_COMPRESSION () { throw new RuntimeException(); }
  static public  org.apache.spark.sql.SQLConf.SQLConfEntry<java.lang.Object> PARQUET_FILTER_PUSHDOWN_ENABLED () { throw new RuntimeException(); }
  static public  org.apache.spark.sql.SQLConf.SQLConfEntry<java.lang.Object> PARQUET_FOLLOW_PARQUET_FORMAT_SPEC () { throw new RuntimeException(); }
  static public  org.apache.spark.sql.SQLConf.SQLConfEntry<java.lang.String> PARQUET_OUTPUT_COMMITTER_CLASS () { throw new RuntimeException(); }
  static public  org.apache.spark.sql.SQLConf.SQLConfEntry<java.lang.Object> ORC_FILTER_PUSHDOWN_ENABLED () { throw new RuntimeException(); }
  static public  org.apache.spark.sql.SQLConf.SQLConfEntry<java.lang.Object> HIVE_VERIFY_PARTITION_PATH () { throw new RuntimeException(); }
  static public  org.apache.spark.sql.SQLConf.SQLConfEntry<java.lang.Object> HIVE_METASTORE_PARTITION_PRUNING () { throw new RuntimeException(); }
  static public  org.apache.spark.sql.SQLConf.SQLConfEntry<java.lang.String> COLUMN_NAME_OF_CORRUPT_RECORD () { throw new RuntimeException(); }
  static public  org.apache.spark.sql.SQLConf.SQLConfEntry<java.lang.Object> BROADCAST_TIMEOUT () { throw new RuntimeException(); }
  static public  org.apache.spark.sql.SQLConf.SQLConfEntry<java.lang.Object> EXTERNAL_SORT () { throw new RuntimeException(); }
  static public  org.apache.spark.sql.SQLConf.SQLConfEntry<java.lang.Object> SORTMERGE_JOIN () { throw new RuntimeException(); }
  static public  org.apache.spark.sql.SQLConf.SQLConfEntry<java.lang.String> THRIFTSERVER_POOL () { throw new RuntimeException(); }
  static public  org.apache.spark.sql.SQLConf.SQLConfEntry<java.lang.Object> THRIFTSERVER_UI_STATEMENT_LIMIT () { throw new RuntimeException(); }
  static public  org.apache.spark.sql.SQLConf.SQLConfEntry<java.lang.Object> THRIFTSERVER_UI_SESSION_LIMIT () { throw new RuntimeException(); }
  static public  org.apache.spark.sql.SQLConf.SQLConfEntry<java.lang.String> DEFAULT_DATA_SOURCE_NAME () { throw new RuntimeException(); }
  static public  org.apache.spark.sql.SQLConf.SQLConfEntry<java.lang.Object> SCHEMA_STRING_LENGTH_THRESHOLD () { throw new RuntimeException(); }
  static public  org.apache.spark.sql.SQLConf.SQLConfEntry<java.lang.Object> PARTITION_DISCOVERY_ENABLED () { throw new RuntimeException(); }
  static public  org.apache.spark.sql.SQLConf.SQLConfEntry<java.lang.Object> PARTITION_COLUMN_TYPE_INFERENCE () { throw new RuntimeException(); }
  static public  org.apache.spark.sql.SQLConf.SQLConfEntry<java.lang.Object> PARTITION_MAX_FILES () { throw new RuntimeException(); }
  static public  org.apache.spark.sql.SQLConf.SQLConfEntry<java.lang.String> OUTPUT_COMMITTER_CLASS () { throw new RuntimeException(); }
  static public  org.apache.spark.sql.SQLConf.SQLConfEntry<java.lang.Object> PARALLEL_PARTITION_DISCOVERY_THRESHOLD () { throw new RuntimeException(); }
  static public  org.apache.spark.sql.SQLConf.SQLConfEntry<java.lang.Object> DATAFRAME_EAGER_ANALYSIS () { throw new RuntimeException(); }
  static public  org.apache.spark.sql.SQLConf.SQLConfEntry<java.lang.Object> DATAFRAME_SELF_JOIN_AUTO_RESOLVE_AMBIGUITY () { throw new RuntimeException(); }
  static public  org.apache.spark.sql.SQLConf.SQLConfEntry<java.lang.Object> DATAFRAME_RETAIN_GROUP_COLUMNS () { throw new RuntimeException(); }
  static public  org.apache.spark.sql.SQLConf.SQLConfEntry<java.lang.Object> USE_SQL_AGGREGATE2 () { throw new RuntimeException(); }
  public   SQLConf () { throw new RuntimeException(); }
  /** Only low degree of contention is expected for conf, thus NOT using ConcurrentHashMap. */
  protected  java.util.Map<java.lang.String, java.lang.String> settings () { throw new RuntimeException(); }
  /**
   * The SQL dialect that is used when parsing queries.  This defaults to 'sql' which uses
   * a simple SQL parser provided by Spark SQL.  This is currently the only option for users of
   * SQLContext.
   * <p>
   * When using a HiveContext, this value defaults to 'hiveql', which uses the Hive 0.12.0 HiveQL
   * parser.  Users can change this to 'sql' if they want to run queries that aren't supported by
   * HiveQL (e.g., SELECT 1).
   * <p>
   * Note that the choice of dialect does not affect things like what tables are available or
   * how query execution is performed.
   * @return (undocumented)
   */
    java.lang.String dialect () { throw new RuntimeException(); }
    boolean useCompression () { throw new RuntimeException(); }
    java.lang.String parquetCompressionCodec () { throw new RuntimeException(); }
    boolean parquetCacheMetadata () { throw new RuntimeException(); }
    int columnBatchSize () { throw new RuntimeException(); }
    int numShufflePartitions () { throw new RuntimeException(); }
    boolean parquetFilterPushDown () { throw new RuntimeException(); }
    boolean orcFilterPushDown () { throw new RuntimeException(); }
    boolean verifyPartitionPath () { throw new RuntimeException(); }
    boolean metastorePartitionPruning () { throw new RuntimeException(); }
    boolean externalSortEnabled () { throw new RuntimeException(); }
    boolean sortMergeJoinEnabled () { throw new RuntimeException(); }
    boolean codegenEnabled () { throw new RuntimeException(); }
  public  boolean caseSensitiveAnalysis () { throw new RuntimeException(); }
    boolean unsafeEnabled () { throw new RuntimeException(); }
    boolean useSqlAggregate2 () { throw new RuntimeException(); }
    int autoBroadcastJoinThreshold () { throw new RuntimeException(); }
    long defaultSizeInBytes () { throw new RuntimeException(); }
    boolean isParquetBinaryAsString () { throw new RuntimeException(); }
    boolean isParquetINT96AsTimestamp () { throw new RuntimeException(); }
    boolean followParquetFormatSpec () { throw new RuntimeException(); }
    boolean inMemoryPartitionPruning () { throw new RuntimeException(); }
    java.lang.String columnNameOfCorruptRecord () { throw new RuntimeException(); }
    int broadcastTimeout () { throw new RuntimeException(); }
    java.lang.String defaultDataSourceName () { throw new RuntimeException(); }
    boolean partitionDiscoveryEnabled () { throw new RuntimeException(); }
    boolean partitionColumnTypeInferenceEnabled () { throw new RuntimeException(); }
    int parallelPartitionDiscoveryThreshold () { throw new RuntimeException(); }
    int schemaStringLengthThreshold () { throw new RuntimeException(); }
    boolean dataFrameEagerAnalysis () { throw new RuntimeException(); }
    boolean dataFrameSelfJoinAutoResolveAmbiguity () { throw new RuntimeException(); }
    boolean dataFrameRetainGroupColumns () { throw new RuntimeException(); }
  /** Set Spark SQL configuration properties. */
  public  void setConf (java.util.Properties props) { throw new RuntimeException(); }
  /** Set the given Spark SQL configuration property using a `string` value. */
  public  void setConfString (java.lang.String key, java.lang.String value) { throw new RuntimeException(); }
  /** Set the given Spark SQL configuration property. */
  public <T extends java.lang.Object> void setConf (org.apache.spark.sql.SQLConf.SQLConfEntry<T> entry, T value) { throw new RuntimeException(); }
  /** Return the value of Spark SQL configuration property for the given key. */
  public  java.lang.String getConfString (java.lang.String key) { throw new RuntimeException(); }
  /**
   * Return the value of Spark SQL configuration property for the given key. If the key is not set
   * yet, return <code>defaultValue</code>. This is useful when <code>defaultValue</code> in SQLConfEntry is not the
   * desired one.
   * @param entry (undocumented)
   * @param defaultValue (undocumented)
   * @return (undocumented)
   */
  public <T extends java.lang.Object> T getConf (org.apache.spark.sql.SQLConf.SQLConfEntry<T> entry, T defaultValue) { throw new RuntimeException(); }
  /**
   * Return the value of Spark SQL configuration property for the given key. If the key is not set
   * yet, return <code>defaultValue</code> in {@link SQLConfEntry}.
   * @param entry (undocumented)
   * @return (undocumented)
   */
  public <T extends java.lang.Object> T getConf (org.apache.spark.sql.SQLConf.SQLConfEntry<T> entry) { throw new RuntimeException(); }
  /**
   * Return the <code>string</code> value of Spark SQL configuration property for the given key. If the key is
   * not set yet, return <code>defaultValue</code>.
   * @param key (undocumented)
   * @param defaultValue (undocumented)
   * @return (undocumented)
   */
  public  java.lang.String getConfString (java.lang.String key, java.lang.String defaultValue) { throw new RuntimeException(); }
  /**
   * Return all the configuration properties that have been set (i.e. not the default).
   * This creates a new copy of the config properties in the form of a Map.
   * @return (undocumented)
   */
  public  scala.collection.immutable.Map<java.lang.String, java.lang.String> getAllConfs () { throw new RuntimeException(); }
  /**
   * Return all the configuration definitions that have been defined in {@link SQLConf}. Each
   * definition contains key, defaultValue and doc.
   * @return (undocumented)
   */
  public  scala.collection.Seq<scala.Tuple3<java.lang.String, java.lang.String, java.lang.String>> getAllDefinedConfs () { throw new RuntimeException(); }
    void unsetConf (java.lang.String key) { throw new RuntimeException(); }
    void unsetConf (org.apache.spark.sql.SQLConf.SQLConfEntry<?> entry) { throw new RuntimeException(); }
    void clear () { throw new RuntimeException(); }
}
