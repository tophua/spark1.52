package org.apache.spark.sql.execution.datasources;
public  class DefaultSource implements org.apache.spark.sql.sources.RelationProvider, org.apache.spark.sql.sources.DataSourceRegister {
  public   DefaultSource () { throw new RuntimeException(); }
  public  java.lang.String shortName () { throw new RuntimeException(); }
  /** Returns a new base relation with the given parameters. */
  public  org.apache.spark.sql.sources.BaseRelation createRelation (org.apache.spark.sql.SQLContext sqlContext, scala.collection.immutable.Map<java.lang.String, java.lang.String> parameters) { throw new RuntimeException(); }
}
