package org.apache.spark.sql.sources;
/**
 * ::DeveloperApi::
 * Data sources should implement this trait so that they can register an alias to their data source.
 * This allows users to give the data source alias as the format type over the fully qualified
 * class name.
 * <p>
 * A new instance of this class will be instantiated each time a DDL call is made.
 * <p>
 * @since 1.5.0
 */
public  interface DataSourceRegister {
  /**
   * The string that represents the format that this data source provider uses. This is
   * overridden by children to provide a nice alias for the data source. For example:
   * <p>
   * <pre><code>
   *   override def format(): String = "parquet"
   * </code></pre>
   * <p>
   * @since 1.5.0
   * @return (undocumented)
   */
  public  java.lang.String shortName () ;
}
