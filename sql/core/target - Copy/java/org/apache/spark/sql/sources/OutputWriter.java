package org.apache.spark.sql.sources;
/**
 * ::Experimental::
 * {@link OutputWriter} is used together with {@link HadoopFsRelation} for persisting rows to the
 * underlying file system.  Subclasses of {@link OutputWriter} must provide a zero-argument constructor.
 * An {@link OutputWriter} instance is created and initialized when a new output file is opened on
 * executor side.  This instance is used to persist rows to this single output file.
 * <p>
 * @since 1.4.0
 */
public abstract class OutputWriter {
  public   OutputWriter () { throw new RuntimeException(); }
  /**
   * Persists a single row.  Invoked on the executor side.  When writing to dynamically partitioned
   * tables, dynamic partition columns are not included in rows to be written.
   * <p>
   * @since 1.4.0
   * @param row (undocumented)
   */
  public abstract  void write (org.apache.spark.sql.Row row) ;
  /**
   * Closes the {@link OutputWriter}. Invoked on the executor side after all rows are persisted, before
   * the task output is committed.
   * <p>
   * @since 1.4.0
   */
  public abstract  void close () ;
  private  scala.Function1<org.apache.spark.sql.catalyst.InternalRow, org.apache.spark.sql.Row> converter () { throw new RuntimeException(); }
  protected  void initConverter (org.apache.spark.sql.types.StructType dataSchema) { throw new RuntimeException(); }
  protected  void writeInternal (org.apache.spark.sql.catalyst.InternalRow row) { throw new RuntimeException(); }
}
