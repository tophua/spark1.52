package org.apache.spark.sql.execution.datasources.json;
// no position
  class JacksonParser$ {
  /**
   * Static reference to the singleton instance of this Scala object.
   */
  public static final JacksonParser$ MODULE$ = null;
  public   JacksonParser$ () { throw new RuntimeException(); }
  public  org.apache.spark.rdd.RDD<org.apache.spark.sql.catalyst.InternalRow> apply (org.apache.spark.rdd.RDD<java.lang.String> json, org.apache.spark.sql.types.StructType schema, java.lang.String columnNameOfCorruptRecords) { throw new RuntimeException(); }
  /**
   * Parse the current token (and related children) according to a desired schema
   * @param factory (undocumented)
   * @param parser (undocumented)
   * @param schema (undocumented)
   * @return (undocumented)
   */
    Object convertField (com.fasterxml.jackson.core.JsonFactory factory, com.fasterxml.jackson.core.JsonParser parser, org.apache.spark.sql.types.DataType schema) { throw new RuntimeException(); }
  /**
   * Parse an object from the token stream into a new Row representing the schema.
   * <p>
   * Fields in the json that are not defined in the requested schema will be dropped.
   * @param factory (undocumented)
   * @param parser (undocumented)
   * @param schema (undocumented)
   * @return (undocumented)
   */
  private  org.apache.spark.sql.catalyst.InternalRow convertObject (com.fasterxml.jackson.core.JsonFactory factory, com.fasterxml.jackson.core.JsonParser parser, org.apache.spark.sql.types.StructType schema) { throw new RuntimeException(); }
  /**
   * Parse an object as a Map, preserving all fields
   * @param factory (undocumented)
   * @param parser (undocumented)
   * @param valueType (undocumented)
   * @return (undocumented)
   */
  private  org.apache.spark.sql.types.MapData convertMap (com.fasterxml.jackson.core.JsonFactory factory, com.fasterxml.jackson.core.JsonParser parser, org.apache.spark.sql.types.DataType valueType) { throw new RuntimeException(); }
  private  org.apache.spark.sql.types.ArrayData convertArray (com.fasterxml.jackson.core.JsonFactory factory, com.fasterxml.jackson.core.JsonParser parser, org.apache.spark.sql.types.DataType elementType) { throw new RuntimeException(); }
  private  org.apache.spark.rdd.RDD<org.apache.spark.sql.catalyst.InternalRow> parseJson (org.apache.spark.rdd.RDD<java.lang.String> json, org.apache.spark.sql.types.StructType schema, java.lang.String columnNameOfCorruptRecords) { throw new RuntimeException(); }
}
