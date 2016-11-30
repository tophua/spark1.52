package org.apache.spark.sql.execution.datasources;
// no position
public  class ResolvedDataSource$ implements org.apache.spark.Logging, scala.Serializable {
  /**
   * Static reference to the singleton instance of this Scala object.
   */
  public static final ResolvedDataSource$ MODULE$ = null;
  public   ResolvedDataSource$ () { throw new RuntimeException(); }
  /** A map to maintain backward compatibility in case we move data sources around. */
  private  scala.collection.immutable.Map<java.lang.String, java.lang.String> backwardCompatibilityMap () { throw new RuntimeException(); }
  /** Given a provider name, look up the data source class definition. */
  public  java.lang.Class<?> lookupDataSource (java.lang.String provider0) { throw new RuntimeException(); }
  /** Create a {@link ResolvedDataSource} for reading data in. */
  public  org.apache.spark.sql.execution.datasources.ResolvedDataSource apply (org.apache.spark.sql.SQLContext sqlContext, scala.Option<org.apache.spark.sql.types.StructType> userSpecifiedSchema, java.lang.String[] partitionColumns, java.lang.String provider, scala.collection.immutable.Map<java.lang.String, java.lang.String> options) { throw new RuntimeException(); }
  private  org.apache.spark.sql.types.StructType partitionColumnsSchema (org.apache.spark.sql.types.StructType schema, java.lang.String[] partitionColumns) { throw new RuntimeException(); }
  /** Create a {@link ResolvedDataSource} for saving the content of the given DataFrame. */
  public  org.apache.spark.sql.execution.datasources.ResolvedDataSource apply (org.apache.spark.sql.SQLContext sqlContext, java.lang.String provider, java.lang.String[] partitionColumns, org.apache.spark.sql.SaveMode mode, scala.collection.immutable.Map<java.lang.String, java.lang.String> options, org.apache.spark.sql.DataFrame data) { throw new RuntimeException(); }
}
