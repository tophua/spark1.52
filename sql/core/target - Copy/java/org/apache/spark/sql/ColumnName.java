package org.apache.spark.sql;
/**
 * :: Experimental ::
 * A convenient class used for constructing schema.
 * <p>
 * @since 1.3.0
 */
public  class ColumnName extends org.apache.spark.sql.Column {
  public   ColumnName (java.lang.String name) { throw new RuntimeException(); }
  /**
   * Creates a new {@link StructField} of type string.
   * @since 1.3.0
   * @return (undocumented)
   */
  public  org.apache.spark.sql.types.StructField string () { throw new RuntimeException(); }
  /**
   * Creates a new {@link StructField} of type date.
   * @since 1.3.0
   * @return (undocumented)
   */
  public  org.apache.spark.sql.types.StructField date () { throw new RuntimeException(); }
  /**
   * Creates a new {@link StructField} of type decimal.
   * @since 1.3.0
   * @return (undocumented)
   */
  public  org.apache.spark.sql.types.StructField decimal () { throw new RuntimeException(); }
  /**
   * Creates a new {@link StructField} of type decimal.
   * @since 1.3.0
   * @param precision (undocumented)
   * @param scale (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.types.StructField decimal (int precision, int scale) { throw new RuntimeException(); }
  /**
   * Creates a new {@link StructField} of type timestamp.
   * @since 1.3.0
   * @return (undocumented)
   */
  public  org.apache.spark.sql.types.StructField timestamp () { throw new RuntimeException(); }
  /**
   * Creates a new {@link StructField} of type binary.
   * @since 1.3.0
   * @return (undocumented)
   */
  public  org.apache.spark.sql.types.StructField binary () { throw new RuntimeException(); }
  /**
   * Creates a new {@link StructField} of type array.
   * @since 1.3.0
   * @param dataType (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.types.StructField array (org.apache.spark.sql.types.DataType dataType) { throw new RuntimeException(); }
  /**
   * Creates a new {@link StructField} of type map.
   * @since 1.3.0
   * @param keyType (undocumented)
   * @param valueType (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.types.StructField map (org.apache.spark.sql.types.DataType keyType, org.apache.spark.sql.types.DataType valueType) { throw new RuntimeException(); }
  public  org.apache.spark.sql.types.StructField map (org.apache.spark.sql.types.MapType mapType) { throw new RuntimeException(); }
  /**
   * Creates a new {@link StructField} of type struct.
   * @since 1.3.0
   * @param fields (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.types.StructField struct (scala.collection.Seq<org.apache.spark.sql.types.StructField> fields) { throw new RuntimeException(); }
  /**
   * Creates a new {@link StructField} of type struct.
   * @since 1.3.0
   * @param structType (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.types.StructField struct (org.apache.spark.sql.types.StructType structType) { throw new RuntimeException(); }
}
