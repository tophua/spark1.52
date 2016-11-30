package org.apache.spark.sql.execution.datasources;
/**
 * Builds a map in which keys are case insensitive
 */
public  class CaseInsensitiveMap implements scala.collection.immutable.Map<java.lang.String, java.lang.String>, scala.Serializable {
  public   CaseInsensitiveMap (scala.collection.immutable.Map<java.lang.String, java.lang.String> map) { throw new RuntimeException(); }
  public  scala.collection.immutable.Map<java.lang.String, java.lang.String> baseMap () { throw new RuntimeException(); }
  public  scala.Option<java.lang.String> get (java.lang.String k) { throw new RuntimeException(); }
  public  scala.collection.Iterator<scala.Tuple2<java.lang.String, java.lang.String>> iterator () { throw new RuntimeException(); }
}
