package org.apache.spark.sql.jdbc;
/**
 * :: DeveloperApi ::
 * Encapsulates everything (extensions, workarounds, quirks) to handle the
 * SQL dialect of a certain database or jdbc driver.
 * Lots of databases define types that aren't explicitly supported
 * by the JDBC spec.  Some JDBC drivers also report inaccurate
 * information---for instance, BIT(n>1) being reported as a BIT type is quite
 * common, even though BIT in JDBC is meant for single-bit values.  Also, there
 * does not appear to be a standard name for an unbounded string or binary
 * type; we use BLOB and CLOB by default but override with database-specific
 * alternatives when these are absent or do not behave correctly.
 * <p>
 * Currently, the only thing done by the dialect is type mapping.
 * <code>getCatalystType</code> is used when reading from a JDBC table and <code>getJDBCType</code>
 * is used when writing to a JDBC table.  If <code>getCatalystType</code> returns <code>null</code>,
 * the default type handling is used for the given JDBC type.  Similarly,
 * if <code>getJDBCType</code> returns <code>(null, None)</code>, the default type handling is used
 * for the given Catalyst type.
 */
public abstract class JdbcDialect {
  public   JdbcDialect () { throw new RuntimeException(); }
  /**
   * Check if this dialect instance can handle a certain jdbc url.
   * @param url the jdbc url.
   * @return True if the dialect can be applied on the given jdbc url.
   * @throws NullPointerException if the url is null.
   */
  public abstract  boolean canHandle (java.lang.String url) ;
  /**
   * Get the custom datatype mapping for the given jdbc meta information.
   * @param sqlType The sql type (see java.sql.Types)
   * @param typeName The sql type name (e.g. "BIGINT UNSIGNED")
   * @param size The size of the type.
   * @param md Result metadata associated with this type.
   * @return The actual DataType (subclasses of {@link org.apache.spark.sql.types.DataType})
   *         or null if the default type mapping should be used.
   */
  public  scala.Option<org.apache.spark.sql.types.DataType> getCatalystType (int sqlType, java.lang.String typeName, int size, org.apache.spark.sql.types.MetadataBuilder md) { throw new RuntimeException(); }
  /**
   * Retrieve the jdbc / sql type for a given datatype.
   * @param dt The datatype (e.g. {@link org.apache.spark.sql.types.StringType})
   * @return The new JdbcType if there is an override for this DataType
   */
  public  scala.Option<org.apache.spark.sql.jdbc.JdbcType> getJDBCType (org.apache.spark.sql.types.DataType dt) { throw new RuntimeException(); }
  /**
   * Quotes the identifier. This is used to put quotes around the identifier in case the column
   * name is a reserved keyword, or in case it contains characters that require quotes (e.g. space).
   * @param colName (undocumented)
   * @return (undocumented)
   */
  public  java.lang.String quoteIdentifier (java.lang.String colName) { throw new RuntimeException(); }
}
