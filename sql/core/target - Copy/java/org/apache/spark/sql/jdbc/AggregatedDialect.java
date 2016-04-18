package org.apache.spark.sql.jdbc;
/**
 * :: DeveloperApi ::
 * AggregatedDialect can unify multiple dialects into one virtual Dialect.
 * Dialects are tried in order, and the first dialect that does not return a
 * neutral element will will.
 * param:  dialects List of dialects.
 */
public  class AggregatedDialect extends org.apache.spark.sql.jdbc.JdbcDialect {
  public   AggregatedDialect (scala.collection.immutable.List<org.apache.spark.sql.jdbc.JdbcDialect> dialects) { throw new RuntimeException(); }
  public  boolean canHandle (java.lang.String url) { throw new RuntimeException(); }
  public  scala.Option<org.apache.spark.sql.types.DataType> getCatalystType (int sqlType, java.lang.String typeName, int size, org.apache.spark.sql.types.MetadataBuilder md) { throw new RuntimeException(); }
  public  scala.Option<org.apache.spark.sql.jdbc.JdbcType> getJDBCType (org.apache.spark.sql.types.DataType dt) { throw new RuntimeException(); }
}
