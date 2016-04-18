package org.apache.spark.sql.columnar;
/**
 * A stackable trait used for building byte buffer for a column containing null values.  Memory
 * layout of the final byte buffer is:
 * <pre><code>
 *    .----------------------- Column type ID (4 bytes)
 *    |   .------------------- Null count N (4 bytes)
 *    |   |   .--------------- Null positions (4 x N bytes, empty if null count is zero)
 *    |   |   |     .--------- Non-null elements
 *    V   V   V     V
 *   +---+---+-----+---------+
 *   |   |   | ... | ... ... |
 *   +---+---+-----+---------+
 * </code></pre>
 */
  interface NullableColumnBuilder extends org.apache.spark.sql.columnar.ColumnBuilder {
  public  java.nio.ByteBuffer nulls () ;
  public  int nullCount () ;
  public  int pos () ;
  public  void initialize (int initialSize, java.lang.String columnName, boolean useCompression) ;
  public  void appendFrom (org.apache.spark.sql.catalyst.InternalRow row, int ordinal) ;
  public  java.nio.ByteBuffer build () ;
  public  java.nio.ByteBuffer buildNonNulls () ;
}
