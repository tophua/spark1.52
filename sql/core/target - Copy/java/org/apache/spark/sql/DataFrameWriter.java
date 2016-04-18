package org.apache.spark.sql;
/**
 * :: Experimental ::
 * Interface used to write a {@link DataFrame} to external storage systems (e.g. file systems,
 * key-value stores, etc). Use {@link DataFrame.write} to access this.
 * <p>
 * @since 1.4.0
 */
public final class DataFrameWriter {
  /**
   * Partitions the output by the given columns on the file system. If specified, the output is
   * laid out on the file system similar to Hive's partitioning scheme.
   * <p>
   * This is only applicable for Parquet at the moment.
   * <p>
   * @since 1.4.0
   * @param colNames (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.DataFrameWriter partitionBy (java.lang.String... colNames) { throw new RuntimeException(); }
  // not preceding
     DataFrameWriter (org.apache.spark.sql.DataFrame df) { throw new RuntimeException(); }
  /**
   * Specifies the behavior when data or table already exists. Options include:
   *   - <code>SaveMode.Overwrite</code>: overwrite the existing data.
   *   - <code>SaveMode.Append</code>: append the data.
   *   - <code>SaveMode.Ignore</code>: ignore the operation (i.e. no-op).
   *   - <code>SaveMode.ErrorIfExists</code>: default option, throw an exception at runtime.
   * <p>
   * @since 1.4.0
   * @param saveMode (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.DataFrameWriter mode (org.apache.spark.sql.SaveMode saveMode) { throw new RuntimeException(); }
  /**
   * Specifies the behavior when data or table already exists. Options include:
   *   - <code>overwrite</code>: overwrite the existing data.
   *   - <code>append</code>: append the data.
   *   - <code>ignore</code>: ignore the operation (i.e. no-op).
   *   - <code>error</code>: default option, throw an exception at runtime.
   * <p>
   * @since 1.4.0
   * @param saveMode (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.DataFrameWriter mode (java.lang.String saveMode) { throw new RuntimeException(); }
  /**
   * Specifies the underlying output data source. Built-in options include "parquet", "json", etc.
   * <p>
   * @since 1.4.0
   * @param source (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.DataFrameWriter format (java.lang.String source) { throw new RuntimeException(); }
  /**
   * Adds an output option for the underlying data source.
   * <p>
   * @since 1.4.0
   * @param key (undocumented)
   * @param value (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.DataFrameWriter option (java.lang.String key, java.lang.String value) { throw new RuntimeException(); }
  /**
   * (Scala-specific) Adds output options for the underlying data source.
   * <p>
   * @since 1.4.0
   * @param options (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.DataFrameWriter options (scala.collection.Map<java.lang.String, java.lang.String> options) { throw new RuntimeException(); }
  /**
   * Adds output options for the underlying data source.
   * <p>
   * @since 1.4.0
   * @param options (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.DataFrameWriter options (java.util.Map<java.lang.String, java.lang.String> options) { throw new RuntimeException(); }
  /**
   * Partitions the output by the given columns on the file system. If specified, the output is
   * laid out on the file system similar to Hive's partitioning scheme.
   * <p>
   * This is only applicable for Parquet at the moment.
   * <p>
   * @since 1.4.0
   * @param colNames (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.DataFrameWriter partitionBy (scala.collection.Seq<java.lang.String> colNames) { throw new RuntimeException(); }
  /**
   * Saves the content of the {@link DataFrame} at the specified path.
   * <p>
   * @since 1.4.0
   * @param path (undocumented)
   */
  public  void save (java.lang.String path) { throw new RuntimeException(); }
  /**
   * Saves the content of the {@link DataFrame} as the specified table.
   * <p>
   * @since 1.4.0
   */
  public  void save () { throw new RuntimeException(); }
  /**
   * Inserts the content of the {@link DataFrame} to the specified table. It requires that
   * the schema of the {@link DataFrame} is the same as the schema of the table.
   * <p>
   * Because it inserts data to an existing table, format or options will be ignored.
   * <p>
   * @since 1.4.0
   * @param tableName (undocumented)
   */
  public  void insertInto (java.lang.String tableName) { throw new RuntimeException(); }
  private  void insertInto (org.apache.spark.sql.catalyst.TableIdentifier tableIdent) { throw new RuntimeException(); }
  /**
   * Saves the content of the {@link DataFrame} as the specified table.
   * <p>
   * In the case the table already exists, behavior of this function depends on the
   * save mode, specified by the <code>mode</code> function (default to throwing an exception).
   * When <code>mode</code> is <code>Overwrite</code>, the schema of the {@link DataFrame} does not need to be
   * the same as that of the existing table.
   * When <code>mode</code> is <code>Append</code>, the schema of the {@link DataFrame} need to be
   * the same as that of the existing table, and format or options will be ignored.
   * <p>
   * When the DataFrame is created from a non-partitioned {@link HadoopFsRelation} with a single input
   * path, and the data source provider can be mapped to an existing Hive builtin SerDe (i.e. ORC
   * and Parquet), the table is persisted in a Hive compatible format, which means other systems
   * like Hive will be able to read this table. Otherwise, the table is persisted in a Spark SQL
   * specific format.
   * <p>
   * @since 1.4.0
   * @param tableName (undocumented)
   */
  public  void saveAsTable (java.lang.String tableName) { throw new RuntimeException(); }
  private  void saveAsTable (org.apache.spark.sql.catalyst.TableIdentifier tableIdent) { throw new RuntimeException(); }
  /**
   * Saves the content of the {@link DataFrame} to a external database table via JDBC. In the case the
   * table already exists in the external database, behavior of this function depends on the
   * save mode, specified by the <code>mode</code> function (default to throwing an exception).
   * <p>
   * Don't create too many partitions in parallel on a large cluster; otherwise Spark might crash
   * your external database systems.
   * <p>
   * @param url JDBC database url of the form <code>jdbc:subprotocol:subname</code>
   * @param table Name of the table in the external database.
   * @param connectionProperties JDBC database connection arguments, a list of arbitrary string
   *                             tag/value. Normally at least a "user" and "password" property
   *                             should be included.
   */
  public  void jdbc (java.lang.String url, java.lang.String table, java.util.Properties connectionProperties) { throw new RuntimeException(); }
  /**
   * Saves the content of the {@link DataFrame} in JSON format at the specified path.
   * This is equivalent to:
   * <pre><code>
   *   format("json").save(path)
   * </code></pre>
   * <p>
   * @since 1.4.0
   * @param path (undocumented)
   */
  public  void json (java.lang.String path) { throw new RuntimeException(); }
  /**
   * Saves the content of the {@link DataFrame} in Parquet format at the specified path.
   * This is equivalent to:
   * <pre><code>
   *   format("parquet").save(path)
   * </code></pre>
   * <p>
   * @since 1.4.0
   * @param path (undocumented)
   */
  public  void parquet (java.lang.String path) { throw new RuntimeException(); }
  /**
   * Saves the content of the {@link DataFrame} in ORC format at the specified path.
   * This is equivalent to:
   * <pre><code>
   *   format("orc").save(path)
   * </code></pre>
   * <p>
   * @since 1.5.0
   * @note Currently, this method can only be used together with <code>HiveContext</code>.
   * @param path (undocumented)
   */
  public  void orc (java.lang.String path) { throw new RuntimeException(); }
  private  java.lang.String source () { throw new RuntimeException(); }
  private  org.apache.spark.sql.SaveMode mode () { throw new RuntimeException(); }
  private  scala.collection.mutable.HashMap<java.lang.String, java.lang.String> extraOptions () { throw new RuntimeException(); }
  private  scala.Option<scala.collection.Seq<java.lang.String>> partitioningColumns () { throw new RuntimeException(); }
}
