package org.apache.spark.sql.execution.datasources.parquet;
/**
 * Parquet converter for Parquet primitive types.  Note that not all Spark SQL atomic types
 * are handled by this converter.  Parquet primitive types are only a subset of those of Spark
 * SQL.  For example, BYTE, SHORT, and INT in Spark SQL are all covered by INT32 in Parquet.
 */
  class CatalystPrimitiveConverter extends org.apache.parquet.io.api.PrimitiveConverter implements org.apache.spark.sql.execution.datasources.parquet.HasParentContainerUpdater {
  public  org.apache.spark.sql.execution.datasources.parquet.ParentContainerUpdater updater () { throw new RuntimeException(); }
  // not preceding
  public   CatalystPrimitiveConverter (org.apache.spark.sql.execution.datasources.parquet.ParentContainerUpdater updater) { throw new RuntimeException(); }
  public  void addBoolean (boolean value) { throw new RuntimeException(); }
  public  void addInt (int value) { throw new RuntimeException(); }
  public  void addLong (long value) { throw new RuntimeException(); }
  public  void addFloat (float value) { throw new RuntimeException(); }
  public  void addDouble (double value) { throw new RuntimeException(); }
  public  void addBinary (org.apache.parquet.io.api.Binary value) { throw new RuntimeException(); }
}
