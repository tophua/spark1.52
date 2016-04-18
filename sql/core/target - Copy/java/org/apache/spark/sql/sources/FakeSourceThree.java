package org.apache.spark.sql.sources;
public  class FakeSourceThree implements org.apache.spark.sql.sources.RelationProvider, org.apache.spark.sql.sources.DataSourceRegister {
  public   FakeSourceThree () { throw new RuntimeException(); }
  public  java.lang.String shortName () { throw new RuntimeException(); }
  public  org.apache.spark.sql.sources.BaseRelation createRelation (org.apache.spark.sql.SQLContext cont, scala.collection.immutable.Map<java.lang.String, java.lang.String> param) { throw new RuntimeException(); }
}
