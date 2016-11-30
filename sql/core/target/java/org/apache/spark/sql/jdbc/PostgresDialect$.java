package org.apache.spark.sql.jdbc;
// no position
/**
 * :: DeveloperApi ::
 * Default postgres dialect, mapping bit/cidr/inet on read and string/binary/boolean on write.
 */
public  class PostgresDialect$ extends org.apache.spark.sql.jdbc.JdbcDialect implements scala.Product, scala.Serializable {
  /**
   * Static reference to the singleton instance of this Scala object.
   */
  public static final PostgresDialect$ MODULE$ = null;
  public   PostgresDialect$ () { throw new RuntimeException(); }
  public  boolean canHandle (java.lang.String url) { throw new RuntimeException(); }
  public  scala.Option<org.apache.spark.sql.types.DataType> getCatalystType (int sqlType, java.lang.String typeName, int size, org.apache.spark.sql.types.MetadataBuilder md) { throw new RuntimeException(); }
  public  scala.Option<org.apache.spark.sql.jdbc.JdbcType> getJDBCType (org.apache.spark.sql.types.DataType dt) { throw new RuntimeException(); }
}
