package org.apache.spark.sql;
/** 
 *  A SQL Dialect for testing purpose, and it can not be nested type
 *  &#x7528;&#x4e8e;&#x6d4b;&#x8bd5;&#x76ee;&#x7684;&#x7684;SQL&#x65b9;&#x8a00;,&#x5b83;&#x4e0d;&#x80fd;&#x5d4c;&#x5957;&#x7c7b;&#x578b;
 *  */
public  class MyDialect extends org.apache.spark.sql.catalyst.DefaultParserDialect {
  public   MyDialect () { throw new RuntimeException(); }
}
