package org.apache.spark.sql.execution.datasources.json;
// no position
  class InferSchema {
  /**
   * Infer the type of a collection of json records in three stages:
   *   1. Infer the type of each record
   *   2. Merge types by choosing the lowest type necessary to cover equal keys
   *   3. Replace any remaining null fields with string, the top type
   * @param json (undocumented)
   * @param samplingRatio (undocumented)
   * @param columnNameOfCorruptRecords (undocumented)
   * @return (undocumented)
   */
  static public  org.apache.spark.sql.types.StructType apply (org.apache.spark.rdd.RDD<java.lang.String> json, double samplingRatio, java.lang.String columnNameOfCorruptRecords) { throw new RuntimeException(); }
  /**
   * Infer the type of a json document from the parser's token stream
   * @param parser (undocumented)
   * @return (undocumented)
   */
  static private  org.apache.spark.sql.types.DataType inferField (com.fasterxml.jackson.core.JsonParser parser) { throw new RuntimeException(); }
  /**
   * Convert NullType to StringType and remove StructTypes with no fields
   * @return (undocumented)
   */
  static private  scala.Function1<org.apache.spark.sql.types.DataType, scala.Option<org.apache.spark.sql.types.DataType>> canonicalizeType () { throw new RuntimeException(); }
  /**
   * Remove top-level ArrayType wrappers and merge the remaining schemas
   * @return (undocumented)
   */
  static private  scala.Function2<org.apache.spark.sql.types.DataType, org.apache.spark.sql.types.DataType, org.apache.spark.sql.types.DataType> compatibleRootType () { throw new RuntimeException(); }
  /**
   * Returns the most general data type for two given data types.
   * &#x8fd4;&#x56de;&#x4e24;&#x4e2a;&#x7ed9;&#x5b9a;&#x7684;&#x6570;&#x636e;&#x7c7b;&#x578b;&#x6700;&#x5e38;&#x7528;&#x7684;&#x6570;&#x636e;&#x7c7b;&#x578b;
   * @param t1 (undocumented)
   * @param t2 (undocumented)
   * @return (undocumented)
   */
  static   org.apache.spark.sql.types.DataType compatibleType (org.apache.spark.sql.types.DataType t1, org.apache.spark.sql.types.DataType t2) { throw new RuntimeException(); }
}
