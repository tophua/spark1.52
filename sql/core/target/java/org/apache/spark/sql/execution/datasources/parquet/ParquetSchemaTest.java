package org.apache.spark.sql.execution.datasources.parquet;
public abstract class ParquetSchemaTest extends org.apache.spark.SparkFunSuite implements org.apache.spark.sql.execution.datasources.parquet.ParquetTest, org.apache.spark.sql.test.SharedSQLContext {
  public   ParquetSchemaTest () { throw new RuntimeException(); }
  /**
   * Checks whether the reflected Parquet message type for product type <code>T</code> conforms <code>messageType</code>.
   * &#x68c0;&#x67e5;&#x4ea7;&#x54c1;&#x7c7b;&#x578b;<code>T'&#x7684;&#x53cd;&#x5c04;Parquet&#x6d88;&#x606f;&#x7c7b;&#x578b;&#x662f;&#x5426;&#x7b26;&#x5408;</code>messageType<code>
   * @param testName (undocumented)
   * @param messageType (undocumented)
   * @param binaryAsString (undocumented)
   * @param int96AsTimestamp (undocumented)
   * @param followParquetFormatSpec (undocumented)
   * @param isThriftDerived (undocumented)
   * @param evidence$1 (undocumented)
   * @param evidence$2 (undocumented)
   */
  protected <T extends scala.Product> void testSchemaInference (java.lang.String testName, java.lang.String messageType, boolean binaryAsString, boolean int96AsTimestamp, boolean followParquetFormatSpec, boolean isThriftDerived, scala.reflect.ClassTag<T> evidence$1, scala.reflect.api.TypeTags.TypeTag<T> evidence$2) { throw new RuntimeException(); }
  protected  void testParquetToCatalyst (java.lang.String testName, org.apache.spark.sql.types.StructType sqlSchema, java.lang.String parquetSchema, boolean binaryAsString, boolean int96AsTimestamp, boolean followParquetFormatSpec, boolean isThriftDerived) { throw new RuntimeException(); }
  protected  void testCatalystToParquet (java.lang.String testName, org.apache.spark.sql.types.StructType sqlSchema, java.lang.String parquetSchema, boolean binaryAsString, boolean int96AsTimestamp, boolean followParquetFormatSpec, boolean isThriftDerived) { throw new RuntimeException(); }
  protected  void testSchema (java.lang.String testName, org.apache.spark.sql.types.StructType sqlSchema, java.lang.String parquetSchema, boolean binaryAsString, boolean int96AsTimestamp, boolean followParquetFormatSpec, boolean isThriftDerived) { throw new RuntimeException(); }
}
