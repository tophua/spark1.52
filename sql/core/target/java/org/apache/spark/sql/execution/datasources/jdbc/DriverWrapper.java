package org.apache.spark.sql.execution.datasources.jdbc;
/**
 * A wrapper for a JDBC Driver to work around SPARK-6913.
 * <p>
 * The problem is in <code>java.sql.DriverManager</code> class that can't access drivers loaded by
 * Spark ClassLoader.
 */
public  class DriverWrapper implements java.sql.Driver {
  public  java.sql.Driver wrapped () { throw new RuntimeException(); }
  // not preceding
  public   DriverWrapper (java.sql.Driver wrapped) { throw new RuntimeException(); }
  public  boolean acceptsURL (java.lang.String url) { throw new RuntimeException(); }
  public  boolean jdbcCompliant () { throw new RuntimeException(); }
  public  java.sql.DriverPropertyInfo[] getPropertyInfo (java.lang.String url, java.util.Properties info) { throw new RuntimeException(); }
  public  int getMinorVersion () { throw new RuntimeException(); }
  public  java.util.logging.Logger getParentLogger () { throw new RuntimeException(); }
  public  java.sql.Connection connect (java.lang.String url, java.util.Properties info) { throw new RuntimeException(); }
  public  int getMajorVersion () { throw new RuntimeException(); }
}
