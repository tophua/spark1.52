package org.apache.spark.sql.jdbc;
// no position
/**
 * :: DeveloperApi ::
 * Registry of dialects that apply to every new jdbc {@link org.apache.spark.sql.DataFrame}.
 * <p>
 * If multiple matching dialects are registered then all matching ones will be
 * tried in reverse order. A user-added dialect will thus be applied first,
 * overwriting the defaults.
 * <p>
 * Note that all new dialects are applied to new jdbc DataFrames only. Make
 * sure to register your dialects first.
 */
public  class JdbcDialects$ {
  /**
   * Static reference to the singleton instance of this Scala object.
   */
  public static final JdbcDialects$ MODULE$ = null;
  public   JdbcDialects$ () { throw new RuntimeException(); }
  private  scala.collection.immutable.List<org.apache.spark.sql.jdbc.JdbcDialect> dialects () { throw new RuntimeException(); }
  /**
   * Register a dialect for use on all new matching jdbc {@link org.apache.spark.sql.DataFrame}.
   * Readding an existing dialect will cause a move-to-front.
   * @param dialect The new dialect.
   */
  public  void registerDialect (org.apache.spark.sql.jdbc.JdbcDialect dialect) { throw new RuntimeException(); }
  /**
   * Unregister a dialect. Does nothing if the dialect is not registered.
   * @param dialect The jdbc dialect.
   */
  public  void unregisterDialect (org.apache.spark.sql.jdbc.JdbcDialect dialect) { throw new RuntimeException(); }
  /**
   * Fetch the JdbcDialect class corresponding to a given database url.
   * @param url (undocumented)
   * @return (undocumented)
   */
    org.apache.spark.sql.jdbc.JdbcDialect get (java.lang.String url) { throw new RuntimeException(); }
}
