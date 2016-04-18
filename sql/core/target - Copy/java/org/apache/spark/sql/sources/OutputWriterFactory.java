package org.apache.spark.sql.sources;
/**
 * ::Experimental::
 * A factory that produces {@link OutputWriter}s.  A new {@link OutputWriterFactory} is created on driver
 * side for each write job issued when writing to a {@link HadoopFsRelation}, and then gets serialized
 * to executor side to create actual {@link OutputWriter}s on the fly.
 * <p>
 * @since 1.4.0
 */
public abstract class OutputWriterFactory implements scala.Serializable {
  public   OutputWriterFactory () { throw new RuntimeException(); }
  /**
   * When writing to a {@link HadoopFsRelation}, this method gets called by each task on executor side
   * to instantiate new {@link OutputWriter}s.
   * <p>
   * @param path Path of the file to which this {@link OutputWriter} is supposed to write.  Note that
   *        this may not point to the final output file.  For example, <code>FileOutputFormat</code> writes to
   *        temporary directories and then merge written files back to the final destination.  In
   *        this case, <code>path</code> points to a temporary output file under the temporary directory.
   * @param dataSchema Schema of the rows to be written. Partition columns are not included in the
   *        schema if the relation being written is partitioned.
   * @param context The Hadoop MapReduce task context.
   * <p>
   * @since 1.4.0
   * @return (undocumented)
   */
  public abstract  org.apache.spark.sql.sources.OutputWriter newInstance (java.lang.String path, org.apache.spark.sql.types.StructType dataSchema, org.apache.hadoop.mapreduce.TaskAttemptContext context) ;
}
