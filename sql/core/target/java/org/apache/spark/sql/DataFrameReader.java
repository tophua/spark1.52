package org.apache.spark.sql;
/**
 * :: Experimental ::
 * Interface used to load a {@link DataFrame} from external storage systems (e.g. file systems,
 * key-value stores, etc). Use {@link SQLContext.read} to access this.
 * <p>
 * @since 1.4.0
 */
public  class DataFrameReader implements org.apache.spark.Logging {
  /**
   * Loads a Parquet file, returning the result as a {@link DataFrame}. This function returns an empty
   * {@link DataFrame} if no paths are passed in.
   * <p>
   * @since 1.4.0
   * @param paths (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.DataFrame parquet (java.lang.String... paths) { throw new RuntimeException(); }
  // not preceding
     DataFrameReader (org.apache.spark.sql.SQLContext sqlContext) { throw new RuntimeException(); }
  /**
   * Specifies the input data source format.
   * <p>
   * @since 1.4.0
   * @param source (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.DataFrameReader format (java.lang.String source) { throw new RuntimeException(); }
  /**
   * Specifies the input schema. Some data sources (e.g. JSON) can infer the input schema
   * automatically from data. By specifying the schema here, the underlying data source can
   * skip the schema inference step, and thus speed up data loading.
   * <p>
   * @since 1.4.0
   * @param schema (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.DataFrameReader schema (org.apache.spark.sql.types.StructType schema) { throw new RuntimeException(); }
  /**
   * Adds an input option for the underlying data source.
   * <p>
   * @since 1.4.0
   * @param key (undocumented)
   * @param value (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.DataFrameReader option (java.lang.String key, java.lang.String value) { throw new RuntimeException(); }
  /**
   * (Scala-specific) Adds input options for the underlying data source.
   * <p>
   * @since 1.4.0
   * @param options (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.DataFrameReader options (scala.collection.Map<java.lang.String, java.lang.String> options) { throw new RuntimeException(); }
  /**
   * Adds input options for the underlying data source.
   * <p>
   * @since 1.4.0
   * @param options (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.DataFrameReader options (java.util.Map<java.lang.String, java.lang.String> options) { throw new RuntimeException(); }
  /**
   * Loads input in as a {@link DataFrame}, for data sources that require a path (e.g. data backed by
   * a local or distributed file system).
   * <p>
   * @since 1.4.0
   * @param path (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.DataFrame load (java.lang.String path) { throw new RuntimeException(); }
  /**
   * Loads input in as a {@link DataFrame}, for data sources that don't require a path (e.g. external
   * key-value stores).
   * <p>
   * @since 1.4.0
   * @return (undocumented)
   */
  public  org.apache.spark.sql.DataFrame load () { throw new RuntimeException(); }
  /**
   * Construct a {@link DataFrame} representing the database table accessible via JDBC URL
   * url named table and connection properties.
   * <p>
   * @since 1.4.0
   * @param url (undocumented)
   * @param table (undocumented)
   * @param properties (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.DataFrame jdbc (java.lang.String url, java.lang.String table, java.util.Properties properties) { throw new RuntimeException(); }
  /**
   * Construct a {@link DataFrame} representing the database table accessible via JDBC URL
   * url named table. Partitions of the table will be retrieved in parallel based on the parameters
   * passed to this function.
   * <p>
   * Don't create too many partitions in parallel on a large cluster; otherwise Spark might crash
   * your external database systems.
   * <p>
   * @param url JDBC database url of the form <code>jdbc:subprotocol:subname</code>
   * @param table Name of the table in the external database.
   * @param columnName the name of a column of integral type that will be used for partitioning.
   * @param lowerBound the minimum value of <code>columnName</code> used to decide partition stride
   * @param upperBound the maximum value of <code>columnName</code> used to decide partition stride
   * @param numPartitions the number of partitions.  the range <code>minValue</code>-<code>maxValue</code> will be split
   *                      evenly into this many partitions
   * @param connectionProperties JDBC database connection arguments, a list of arbitrary string
   *                             tag/value. Normally at least a "user" and "password" property
   *                             should be included.
   * <p>
   * @since 1.4.0
   * @return (undocumented)
   */
  public  org.apache.spark.sql.DataFrame jdbc (java.lang.String url, java.lang.String table, java.lang.String columnName, long lowerBound, long upperBound, int numPartitions, java.util.Properties connectionProperties) { throw new RuntimeException(); }
  /**
   * Construct a {@link DataFrame} representing the database table accessible via JDBC URL
   * url named table using connection properties. The <code>predicates</code> parameter gives a list
   * expressions suitable for inclusion in WHERE clauses; each one defines one partition
   * of the {@link DataFrame}.
   * <p>
   * Don't create too many partitions in parallel on a large cluster; otherwise Spark might crash
   * your external database systems.
   * <p>
   * @param url JDBC database url of the form <code>jdbc:subprotocol:subname</code>
   * @param table Name of the table in the external database.
   * @param predicates Condition in the where clause for each partition.
   * @param connectionProperties JDBC database connection arguments, a list of arbitrary string
   *                             tag/value. Normally at least a "user" and "password" property
   *                             should be included.
   * @since 1.4.0
   * @return (undocumented)
   */
  public  org.apache.spark.sql.DataFrame jdbc (java.lang.String url, java.lang.String table, java.lang.String[] predicates, java.util.Properties connectionProperties) { throw new RuntimeException(); }
  private  org.apache.spark.sql.DataFrame jdbc (java.lang.String url, java.lang.String table, org.apache.spark.Partition[] parts, java.util.Properties connectionProperties) { throw new RuntimeException(); }
  /**
   * Loads a JSON file (one object per line) and returns the result as a {@link DataFrame}.
   * <p>
   * This function goes through the input once to determine the input schema. If you know the
   * schema in advance, use the version that specifies the schema to avoid the extra scan.
   * <p>
   * @param path input path
   * @since 1.4.0
   * @return (undocumented)
   */
  public  org.apache.spark.sql.DataFrame json (java.lang.String path) { throw new RuntimeException(); }
  /**
   * Loads an <code>JavaRDD[String]</code> storing JSON objects (one object per record) and
   * returns the result as a {@link DataFrame}.
   * <p>
   * Unless the schema is specified using {@link schema} function, this function goes through the
   * input once to determine the input schema.
   * <p>
   * @param jsonRDD input RDD with one JSON object per record
   * @since 1.4.0
   * @return (undocumented)
   */
  public  org.apache.spark.sql.DataFrame json (org.apache.spark.api.java.JavaRDD<java.lang.String> jsonRDD) { throw new RuntimeException(); }
  /**
   * Loads an <code>RDD[String]</code> storing JSON objects (one object per record) and
   * returns the result as a {@link DataFrame}.
   * <p>
   * Unless the schema is specified using {@link schema} function, this function goes through the
   * input once to determine the input schema.
   * <p>
   * @param jsonRDD input RDD with one JSON object per record
   * @since 1.4.0
   * @return (undocumented)
   */
  public  org.apache.spark.sql.DataFrame json (org.apache.spark.rdd.RDD<java.lang.String> jsonRDD) { throw new RuntimeException(); }
  /**
   * Loads a Parquet file, returning the result as a {@link DataFrame}. This function returns an empty
   * {@link DataFrame} if no paths are passed in.
   * <p>
   * @since 1.4.0
   * @param paths (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.DataFrame parquet (scala.collection.Seq<java.lang.String> paths) { throw new RuntimeException(); }
  /**
   * Loads an ORC file and returns the result as a {@link DataFrame}.
   * <p>
   * @param path input path
   * @since 1.5.0
   * @note Currently, this method can only be used together with <code>HiveContext</code>.
   * @return (undocumented)
   */
  public  org.apache.spark.sql.DataFrame orc (java.lang.String path) { throw new RuntimeException(); }
  /**
   * Returns the specified table as a {@link DataFrame}.
   * <p>
   * @since 1.4.0
   * @param tableName (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.DataFrame table (java.lang.String tableName) { throw new RuntimeException(); }
  private  java.lang.String source () { throw new RuntimeException(); }
  private  scala.Option<org.apache.spark.sql.types.StructType> userSpecifiedSchema () { throw new RuntimeException(); }
  private  scala.collection.mutable.HashMap<java.lang.String, java.lang.String> extraOptions () { throw new RuntimeException(); }
}
