package org.apache.spark.sql.api.r;
// no position
  class SQLUtils$ {
  /**
   * Static reference to the singleton instance of this Scala object.
   */
  public static final SQLUtils$ MODULE$ = null;
  public   SQLUtils$ () { throw new RuntimeException(); }
  public  org.apache.spark.sql.SQLContext createSQLContext (org.apache.spark.api.java.JavaSparkContext jsc) { throw new RuntimeException(); }
  public  org.apache.spark.api.java.JavaSparkContext getJavaSparkContext (org.apache.spark.sql.SQLContext sqlCtx) { throw new RuntimeException(); }
  public <T extends java.lang.Object> scala.collection.Seq<T> toSeq (java.lang.Object arr) { throw new RuntimeException(); }
  public  org.apache.spark.sql.types.StructType createStructType (scala.collection.Seq<org.apache.spark.sql.types.StructField> fields) { throw new RuntimeException(); }
  public  org.apache.spark.sql.types.DataType getSQLDataType (java.lang.String dataType) { throw new RuntimeException(); }
  public  org.apache.spark.sql.types.StructField createStructField (java.lang.String name, java.lang.String dataType, boolean nullable) { throw new RuntimeException(); }
  public  org.apache.spark.sql.DataFrame createDF (org.apache.spark.rdd.RDD<byte[]> rdd, org.apache.spark.sql.types.StructType schema, org.apache.spark.sql.SQLContext sqlContext) { throw new RuntimeException(); }
  public  org.apache.spark.api.java.JavaRDD<byte[]> dfToRowRDD (org.apache.spark.sql.DataFrame df) { throw new RuntimeException(); }
  private  java.lang.Object doConversion (java.lang.Object data, org.apache.spark.sql.types.DataType dataType) { throw new RuntimeException(); }
  private  org.apache.spark.sql.Row bytesToRow (byte[] bytes, org.apache.spark.sql.types.StructType schema) { throw new RuntimeException(); }
  private  byte[] rowToRBytes (org.apache.spark.sql.Row row) { throw new RuntimeException(); }
  public  byte[][] dfToCols (org.apache.spark.sql.DataFrame df) { throw new RuntimeException(); }
  public  java.lang.Object[][] convertRowsToColumns (org.apache.spark.sql.Row[] localDF, int numCols) { throw new RuntimeException(); }
  public  byte[] colToRBytes (java.lang.Object[] col) { throw new RuntimeException(); }
  public  org.apache.spark.sql.SaveMode saveMode (java.lang.String mode) { throw new RuntimeException(); }
  public  org.apache.spark.sql.DataFrame loadDF (org.apache.spark.sql.SQLContext sqlContext, java.lang.String source, java.util.Map<java.lang.String, java.lang.String> options) { throw new RuntimeException(); }
  public  org.apache.spark.sql.DataFrame loadDF (org.apache.spark.sql.SQLContext sqlContext, java.lang.String source, org.apache.spark.sql.types.StructType schema, java.util.Map<java.lang.String, java.lang.String> options) { throw new RuntimeException(); }
}
