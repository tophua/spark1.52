package org.apache.spark.sql.execution.datasources.parquet;
// no position
  class ParquetTypesConverter$ implements org.apache.spark.Logging {
  /**
   * Static reference to the singleton instance of this Scala object.
   */
  public static final ParquetTypesConverter$ MODULE$ = null;
  public   ParquetTypesConverter$ () { throw new RuntimeException(); }
  public  boolean isPrimitiveType (org.apache.spark.sql.types.DataType ctype) { throw new RuntimeException(); }
  /**
   * Compute the FIXED_LEN_BYTE_ARRAY length needed to represent a given DECIMAL precision.
   * @return (undocumented)
   */
  public  int[] BYTES_FOR_PRECISION () { throw new RuntimeException(); }
  public  org.apache.parquet.schema.MessageType convertFromAttributes (scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> attributes) { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> convertFromString (java.lang.String string) { throw new RuntimeException(); }
  public  java.lang.String convertToString (scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> schema) { throw new RuntimeException(); }
  public  void writeMetaData (scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> attributes, org.apache.hadoop.fs.Path origPath, org.apache.hadoop.conf.Configuration conf) { throw new RuntimeException(); }
  /**
   * Try to read Parquet metadata at the given Path. We first see if there is a summary file
   * in the parent directory. If so, this is used. Else we read the actual footer at the given
   * location.
   * @param origPath The path at which we expect one (or more) Parquet files.
   * @param configuration The Hadoop configuration to use.
   * @return The <code>ParquetMetadata</code> containing among other things the schema.
   */
  public  org.apache.parquet.hadoop.metadata.ParquetMetadata readMetaData (org.apache.hadoop.fs.Path origPath, scala.Option<org.apache.hadoop.conf.Configuration> configuration) { throw new RuntimeException(); }
}
