package org.apache.spark.sql.jdbc;
// no position
/**
 * :: DeveloperApi ::
 * Default Oracle dialect, mapping a nonspecific numeric type to a general decimal type.
 */
public  class OracleDialect$ extends org.apache.spark.sql.jdbc.JdbcDialect implements scala.Product, scala.Serializable {
  /**
   * Static reference to the singleton instance of this Scala object.
   */
  public static final OracleDialect$ MODULE$ = null;
  public   OracleDialect$ () { throw new RuntimeException(); }
  public  boolean canHandle (java.lang.String url) { throw new RuntimeException(); }
  public  scala.Option<org.apache.spark.sql.types.DataType> getCatalystType (int sqlType, java.lang.String typeName, int size, org.apache.spark.sql.types.MetadataBuilder md) { throw new RuntimeException(); }
}
