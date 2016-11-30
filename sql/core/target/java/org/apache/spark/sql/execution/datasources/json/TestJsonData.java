package org.apache.spark.sql.execution.datasources.json;
  interface TestJsonData {
  public  org.apache.spark.sql.SQLContext _sqlContext () ;
  public  org.apache.spark.rdd.RDD<java.lang.String> primitiveFieldAndType () ;
  public  org.apache.spark.rdd.RDD<java.lang.String> primitiveFieldValueTypeConflict () ;
  public  org.apache.spark.rdd.RDD<java.lang.String> jsonNullStruct () ;
  public  org.apache.spark.rdd.RDD<java.lang.String> complexFieldValueTypeConflict () ;
  public  org.apache.spark.rdd.RDD<java.lang.String> arrayElementTypeConflict () ;
  public  org.apache.spark.rdd.RDD<java.lang.String> missingFields () ;
  public  org.apache.spark.rdd.RDD<java.lang.String> complexFieldAndType1 () ;
  public  org.apache.spark.rdd.RDD<java.lang.String> complexFieldAndType2 () ;
  public  org.apache.spark.rdd.RDD<java.lang.String> mapType1 () ;
  public  org.apache.spark.rdd.RDD<java.lang.String> mapType2 () ;
  public  org.apache.spark.rdd.RDD<java.lang.String> nullsInArrays () ;
  public  org.apache.spark.rdd.RDD<java.lang.String> jsonArray () ;
  public  org.apache.spark.rdd.RDD<java.lang.String> corruptRecords () ;
  public  org.apache.spark.rdd.RDD<java.lang.String> emptyRecords () ;
  public  org.apache.spark.rdd.RDD<java.lang.String> singleRow () ;
  public  org.apache.spark.rdd.RDD<java.lang.String> empty () ;
}
