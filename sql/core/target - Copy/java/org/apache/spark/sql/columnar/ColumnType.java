package org.apache.spark.sql.columnar;
/**
 * An abstract class that represents type of a column. Used to append/extract Java objects into/from
 * the underlying {@link ByteBuffer} of a column.
 * <p>
 * @tparam JvmType Underlying Java type to represent the elements.
 */
 abstract class ColumnType<JvmType extends java.lang.Object> {
  static public  org.apache.spark.sql.columnar.ColumnType<?> apply (org.apache.spark.sql.types.DataType dataType) { throw new RuntimeException(); }
  // not preceding
  // TypeTree().setOriginal(TypeBoundsTree(TypeTree().setOriginal(Select(Select(Ident(_root_), scala), scala.Nothing)), TypeTree().setOriginal(Select(Select(Ident(_root_), scala), scala.Any))))
  public   ColumnType () { throw new RuntimeException(); }
  public abstract  org.apache.spark.sql.types.DataType dataType () ;
  public abstract  int typeId () ;
  public abstract  int defaultSize () ;
  /**
   * Extracts a value out of the buffer at the buffer's current position.
   * @param buffer (undocumented)
   * @return (undocumented)
   */
  public abstract  JvmType extract (java.nio.ByteBuffer buffer) ;
  /**
   * Extracts a value out of the buffer at the buffer's current position and stores in
   * <code>row(ordinal)</code>. Subclasses should override this method to avoid boxing/unboxing costs whenever
   * possible.
   * @param buffer (undocumented)
   * @param row (undocumented)
   * @param ordinal (undocumented)
   */
  public  void extract (java.nio.ByteBuffer buffer, org.apache.spark.sql.catalyst.expressions.MutableRow row, int ordinal) { throw new RuntimeException(); }
  /**
   * Appends the given value v of type T into the given ByteBuffer.
   * @param v (undocumented)
   * @param buffer (undocumented)
   */
  public abstract  void append (JvmType v, java.nio.ByteBuffer buffer) ;
  /**
   * Appends <code>row(ordinal)</code> of type T into the given ByteBuffer. Subclasses should override this
   * method to avoid boxing/unboxing costs whenever possible.
   * @param row (undocumented)
   * @param ordinal (undocumented)
   * @param buffer (undocumented)
   */
  public  void append (org.apache.spark.sql.catalyst.InternalRow row, int ordinal, java.nio.ByteBuffer buffer) { throw new RuntimeException(); }
  /**
   * Returns the size of the value <code>row(ordinal)</code>. This is used to calculate the size of variable
   * length types such as byte arrays and strings.
   * @param row (undocumented)
   * @param ordinal (undocumented)
   * @return (undocumented)
   */
  public  int actualSize (org.apache.spark.sql.catalyst.InternalRow row, int ordinal) { throw new RuntimeException(); }
  /**
   * Returns <code>row(ordinal)</code>. Subclasses should override this method to avoid boxing/unboxing costs
   * whenever possible.
   * @param row (undocumented)
   * @param ordinal (undocumented)
   * @return (undocumented)
   */
  public abstract  JvmType getField (org.apache.spark.sql.catalyst.InternalRow row, int ordinal) ;
  /**
   * Sets <code>row(ordinal)</code> to <code>field</code>. Subclasses should override this method to avoid boxing/unboxing
   * costs whenever possible.
   * @param row (undocumented)
   * @param ordinal (undocumented)
   * @param value (undocumented)
   */
  public abstract  void setField (org.apache.spark.sql.catalyst.expressions.MutableRow row, int ordinal, JvmType value) ;
  /**
   * Copies <code>from(fromOrdinal)</code> to <code>to(toOrdinal)</code>. Subclasses should override this method to avoid
   * boxing/unboxing costs whenever possible.
   * @param from (undocumented)
   * @param fromOrdinal (undocumented)
   * @param to (undocumented)
   * @param toOrdinal (undocumented)
   */
  public  void copyField (org.apache.spark.sql.catalyst.InternalRow from, int fromOrdinal, org.apache.spark.sql.catalyst.expressions.MutableRow to, int toOrdinal) { throw new RuntimeException(); }
  /**
   * Creates a duplicated copy of the value.
   * @param v (undocumented)
   * @return (undocumented)
   */
  public  JvmType clone (JvmType v) { throw new RuntimeException(); }
  public  java.lang.String toString () { throw new RuntimeException(); }
}
