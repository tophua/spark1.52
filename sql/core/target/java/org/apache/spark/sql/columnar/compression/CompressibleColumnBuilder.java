package org.apache.spark.sql.columnar.compression;
/**
 * A stackable trait that builds optionally compressed byte buffer for a column.  Memory layout of
 * the final byte buffer is:
 * <pre><code>
 *    .--------------------------- Column type ID (4 bytes)
 *    |   .----------------------- Null count N (4 bytes)
 *    |   |   .------------------- Null positions (4 x N bytes, empty if null count is zero)
 *    |   |   |     .------------- Compression scheme ID (4 bytes)
 *    |   |   |     |   .--------- Compressed non-null elements
 *    V   V   V     V   V
 *   +---+---+-----+---+---------+
 *   |   |   | ... |   | ... ... |
 *   +---+---+-----+---+---------+
 *    \-----------/ \-----------/
 *       header         body
 * </code></pre>
 */
  interface CompressibleColumnBuilder<T extends org.apache.spark.sql.types.AtomicType> extends org.apache.spark.sql.columnar.ColumnBuilder, org.apache.spark.Logging {
  public  scala.collection.Seq<org.apache.spark.sql.columnar.compression.Encoder<T>> compressionEncoders () ;
  public  void initialize (int initialSize, java.lang.String columnName, boolean useCompression) ;
  public  boolean isWorthCompressing (org.apache.spark.sql.columnar.compression.Encoder<T> encoder) ;
  public  void gatherCompressibilityStats (org.apache.spark.sql.catalyst.InternalRow row, int ordinal) ;
  public  void appendFrom (org.apache.spark.sql.catalyst.InternalRow row, int ordinal) ;
  public  java.nio.ByteBuffer build () ;
}
