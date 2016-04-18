package org.apache.spark.sql.jdbc;
// no position
/**
 * :: DeveloperApi ::
 * Default mysql dialect to read bit/bitsets correctly.
 */
public  class MySQLDialect extends org.apache.spark.sql.jdbc.JdbcDialect implements scala.Product, scala.Serializable {
  static public  boolean canHandle (java.lang.String url) { throw new RuntimeException(); }
  static public  scala.Option<org.apache.spark.sql.types.DataType> getCatalystType (int sqlType, java.lang.String typeName, int size, org.apache.spark.sql.types.MetadataBuilder md) { throw new RuntimeException(); }
  static public  java.lang.String quoteIdentifier (java.lang.String colName) { throw new RuntimeException(); }
}
