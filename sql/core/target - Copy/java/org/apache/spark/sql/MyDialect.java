package org.apache.spark.sql;
/** A SQL Dialect for testing purpose, and it can not be nested type */
public  class MyDialect extends org.apache.spark.sql.catalyst.DefaultParserDialect {
  public   MyDialect () { throw new RuntimeException(); }
}
