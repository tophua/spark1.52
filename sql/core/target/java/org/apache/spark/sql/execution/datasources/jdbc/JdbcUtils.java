package org.apache.spark.sql.execution.datasources.jdbc;
// no position
/**
 * Util functions for JDBC tables.
 */
public  class JdbcUtils implements org.apache.spark.Logging {
  /**
   * Establishes a JDBC connection.
   * @param url (undocumented)
   * @param connectionProperties (undocumented)
   * @return (undocumented)
   */
  static public  java.sql.Connection createConnection (java.lang.String url, java.util.Properties connectionProperties) { throw new RuntimeException(); }
  /**
   * Returns true if the table already exists in the JDBC database.
   * @param conn (undocumented)
   * @param table (undocumented)
   * @return (undocumented)
   */
  static public  boolean tableExists (java.sql.Connection conn, java.lang.String table) { throw new RuntimeException(); }
  /**
   * Drops a table from the JDBC database.
   * @param conn (undocumented)
   * @param table (undocumented)
   */
  static public  void dropTable (java.sql.Connection conn, java.lang.String table) { throw new RuntimeException(); }
  /**
   * Returns a PreparedStatement that inserts a row into table via conn.
   * @param conn (undocumented)
   * @param table (undocumented)
   * @param rddSchema (undocumented)
   * @return (undocumented)
   */
  static public  java.sql.PreparedStatement insertStatement (java.sql.Connection conn, java.lang.String table, org.apache.spark.sql.types.StructType rddSchema) { throw new RuntimeException(); }
  /**
   * Saves a partition of a DataFrame to the JDBC database.  This is done in
   * a single database transaction in order to avoid repeatedly inserting
   * data as much as possible.
   * <p>
   * It is still theoretically possible for rows in a DataFrame to be
   * inserted into the database more than once if a stage somehow fails after
   * the commit occurs but before the stage can return successfully.
   * <p>
   * This is not a closure inside saveTable() because apparently cosmetic
   * implementation changes elsewhere might easily render such a closure
   * non-Serializable.  Instead, we explicitly close over all variables that
   * are used.
   * @param getConnection (undocumented)
   * @param table (undocumented)
   * @param iterator (undocumented)
   * @param rddSchema (undocumented)
   * @param nullTypes (undocumented)
   * @return (undocumented)
   */
  static public  scala.collection.Iterator<java.lang.Object> savePartition (scala.Function0<java.sql.Connection> getConnection, java.lang.String table, scala.collection.Iterator<org.apache.spark.sql.Row> iterator, org.apache.spark.sql.types.StructType rddSchema, int[] nullTypes) { throw new RuntimeException(); }
  /**
   * Compute the schema string for this RDD.
   * @param df (undocumented)
   * @param url (undocumented)
   * @return (undocumented)
   */
  static public  java.lang.String schemaString (org.apache.spark.sql.DataFrame df, java.lang.String url) { throw new RuntimeException(); }
  /**
   * Saves the RDD to the database in a single transaction.
   * @param df (undocumented)
   * @param url (undocumented)
   * @param table (undocumented)
   * @param properties (undocumented)
   */
  static public  void saveTable (org.apache.spark.sql.DataFrame df, java.lang.String url, java.lang.String table, java.util.Properties properties) { throw new RuntimeException(); }
}
