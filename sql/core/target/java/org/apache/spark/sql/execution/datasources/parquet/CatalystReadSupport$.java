package org.apache.spark.sql.execution.datasources.parquet;
// no position
  class CatalystReadSupport$ {
  /**
   * Static reference to the singleton instance of this Scala object.
   */
  public static final CatalystReadSupport$ MODULE$ = null;
  public   CatalystReadSupport$ () { throw new RuntimeException(); }
  public  java.lang.String SPARK_ROW_REQUESTED_SCHEMA () { throw new RuntimeException(); }
  public  java.lang.String SPARK_METADATA_KEY () { throw new RuntimeException(); }
  /**
   * Tailors <code>parquetSchema</code> according to <code>catalystSchema</code> by removing column paths don't exist
   * in <code>catalystSchema</code>, and adding those only exist in <code>catalystSchema</code>.
   * @param parquetSchema (undocumented)
   * @param catalystSchema (undocumented)
   * @return (undocumented)
   */
  public  org.apache.parquet.schema.MessageType clipParquetSchema (org.apache.parquet.schema.MessageType parquetSchema, org.apache.spark.sql.types.StructType catalystSchema) { throw new RuntimeException(); }
  private  org.apache.parquet.schema.Type clipParquetType (org.apache.parquet.schema.Type parquetType, org.apache.spark.sql.types.DataType catalystType) { throw new RuntimeException(); }
  /**
   * Whether a Catalyst {@link DataType} is primitive.  Primitive {@link DataType} is not equivalent to
   * {@link AtomicType}.  For example, {@link CalendarIntervalType} is primitive, but it's not an
   * {@link AtomicType}.
   * @param dataType (undocumented)
   * @return (undocumented)
   */
  private  boolean isPrimitiveCatalystType (org.apache.spark.sql.types.DataType dataType) { throw new RuntimeException(); }
  /**
   * Clips a Parquet {@link GroupType} which corresponds to a Catalyst {@link ArrayType}.  The element type
   * of the {@link ArrayType} should also be a nested type, namely an {@link ArrayType}, a {@link MapType}, or a
   * {@link StructType}.
   * @param parquetList (undocumented)
   * @param elementType (undocumented)
   * @return (undocumented)
   */
  private  org.apache.parquet.schema.Type clipParquetListType (org.apache.parquet.schema.GroupType parquetList, org.apache.spark.sql.types.DataType elementType) { throw new RuntimeException(); }
  /**
   * Clips a Parquet {@link GroupType} which corresponds to a Catalyst {@link MapType}.  Either key type or
   * value type of the {@link MapType} must be a nested type, namely an {@link ArrayType}, a {@link MapType}, or
   * a {@link StructType}.
   * @param parquetMap (undocumented)
   * @param keyType (undocumented)
   * @param valueType (undocumented)
   * @return (undocumented)
   */
  private  org.apache.parquet.schema.GroupType clipParquetMapType (org.apache.parquet.schema.GroupType parquetMap, org.apache.spark.sql.types.DataType keyType, org.apache.spark.sql.types.DataType valueType) { throw new RuntimeException(); }
  /**
   * Clips a Parquet {@link GroupType} which corresponds to a Catalyst {@link StructType}.
   * <p>
   * @return A clipped {@link GroupType}, which has at least one field.
   * @note Parquet doesn't allow creating empty {@link GroupType} instances except for empty
   *       {@link MessageType}.  Because it's legal to construct an empty requested schema for column
   *       pruning.
   * @param parquetRecord (undocumented)
   * @param structType (undocumented)
   */
  private  org.apache.parquet.schema.GroupType clipParquetGroup (org.apache.parquet.schema.GroupType parquetRecord, org.apache.spark.sql.types.StructType structType) { throw new RuntimeException(); }
  /**
   * Clips a Parquet {@link GroupType} which corresponds to a Catalyst {@link StructType}.
   * <p>
   * @return A list of clipped {@link GroupType} fields, which can be empty.
   * @param parquetRecord (undocumented)
   * @param structType (undocumented)
   */
  private  scala.collection.Seq<org.apache.parquet.schema.Type> clipParquetGroupFields (org.apache.parquet.schema.GroupType parquetRecord, org.apache.spark.sql.types.StructType structType) { throw new RuntimeException(); }
}
