package org.apache.spark.sql.execution.datasources;
// no position
  class PartitioningUtils$ {
  /**
   * Static reference to the singleton instance of this Scala object.
   */
  public static final PartitioningUtils$ MODULE$ = null;
  public   PartitioningUtils$ () { throw new RuntimeException(); }
  public  java.lang.String DEFAULT_PARTITION_NAME () { throw new RuntimeException(); }
  /**
   * Given a group of qualified paths, tries to parse them and returns a partition specification.
   * For example, given:
   * <pre><code>
   *   hdfs://&lt;host&gt;:&lt;port&gt;/path/to/partition/a=1/b=hello/c=3.14
   *   hdfs://&lt;host&gt;:&lt;port&gt;/path/to/partition/a=2/b=world/c=6.28
   * </code></pre>
   * it returns:
   * <pre><code>
   *   PartitionSpec(
   *     partitionColumns = StructType(
   *       StructField(name = "a", dataType = IntegerType, nullable = true),
   *       StructField(name = "b", dataType = StringType, nullable = true),
   *       StructField(name = "c", dataType = DoubleType, nullable = true)),
   *     partitions = Seq(
   *       Partition(
   *         values = Row(1, "hello", 3.14),
   *         path = "hdfs://&lt;host&gt;:&lt;port&gt;/path/to/partition/a=1/b=hello/c=3.14"),
   *       Partition(
   *         values = Row(2, "world", 6.28),
   *         path = "hdfs://&lt;host&gt;:&lt;port&gt;/path/to/partition/a=2/b=world/c=6.28")))
   * </code></pre>
   * @param paths (undocumented)
   * @param defaultPartitionName (undocumented)
   * @param typeInference (undocumented)
   * @return (undocumented)
   */
    org.apache.spark.sql.execution.datasources.PartitionSpec parsePartitions (scala.collection.Seq<org.apache.hadoop.fs.Path> paths, java.lang.String defaultPartitionName, boolean typeInference) { throw new RuntimeException(); }
  /**
   * Parses a single partition, returns column names and values of each partition column.  For
   * example, given:
   * <pre><code>
   *   path = hdfs://&lt;host&gt;:&lt;port&gt;/path/to/partition/a=42/b=hello/c=3.14
   * </code></pre>
   * it returns:
   * <pre><code>
   *   PartitionValues(
   *     Seq("a", "b", "c"),
   *     Seq(
   *       Literal.create(42, IntegerType),
   *       Literal.create("hello", StringType),
   *       Literal.create(3.14, FloatType)))
   * </code></pre>
   * @param path (undocumented)
   * @param defaultPartitionName (undocumented)
   * @param typeInference (undocumented)
   * @return (undocumented)
   */
    scala.Option<org.apache.spark.sql.execution.datasources.PartitioningUtils.PartitionValues> parsePartition (org.apache.hadoop.fs.Path path, java.lang.String defaultPartitionName, boolean typeInference) { throw new RuntimeException(); }
  private  scala.Option<scala.Tuple2<java.lang.String, org.apache.spark.sql.catalyst.expressions.Literal>> parsePartitionColumn (java.lang.String columnSpec, java.lang.String defaultPartitionName, boolean typeInference) { throw new RuntimeException(); }
  /**
   * Resolves possible type conflicts between partitions by up-casting "lower" types.  The up-
   * casting order is:
   * <pre><code>
   *   NullType -&gt;
   *   IntegerType -&gt; LongType -&gt;
   *   DoubleType -&gt; StringType
   * </code></pre>
   * @param pathsWithPartitionValues (undocumented)
   * @return (undocumented)
   */
    scala.collection.Seq<org.apache.spark.sql.execution.datasources.PartitioningUtils.PartitionValues> resolvePartitions (scala.collection.Seq<scala.Tuple2<org.apache.hadoop.fs.Path, org.apache.spark.sql.execution.datasources.PartitioningUtils.PartitionValues>> pathsWithPartitionValues) { throw new RuntimeException(); }
    java.lang.String listConflictingPartitionColumns (scala.collection.Seq<scala.Tuple2<org.apache.hadoop.fs.Path, org.apache.spark.sql.execution.datasources.PartitioningUtils.PartitionValues>> pathWithPartitionValues) { throw new RuntimeException(); }
  /**
   * Converts a string to a {@link Literal} with automatic type inference.  Currently only supports
   * {@link IntegerType}, {@link LongType}, {@link DoubleType}, {@link DecimalType.SYSTEM_DEFAULT}, and
   * {@link StringType}.
   * @param raw (undocumented)
   * @param defaultPartitionName (undocumented)
   * @param typeInference (undocumented)
   * @return (undocumented)
   */
    org.apache.spark.sql.catalyst.expressions.Literal inferPartitionColumnValue (java.lang.String raw, java.lang.String defaultPartitionName, boolean typeInference) { throw new RuntimeException(); }
  private  scala.collection.Seq<org.apache.spark.sql.types.DataType> upCastingOrder () { throw new RuntimeException(); }
  /**
   * Given a collection of {@link Literal}s, resolves possible type conflicts by up-casting "lower"
   * types.
   * @param literals (undocumented)
   * @return (undocumented)
   */
  private  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Literal> resolveTypeConflicts (scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Literal> literals) { throw new RuntimeException(); }
  public  java.util.BitSet charToEscape () { throw new RuntimeException(); }
  /**
   * ASCII 01-1F are HTTP control characters that need to be escaped.
   * <p>
   and  are \n and \r, respectively.
   * @param c (undocumented)
   * @return (undocumented)
   */
  public  boolean needsEscaping (char c) { throw new RuntimeException(); }
  public  java.lang.String escapePathName (java.lang.String path) { throw new RuntimeException(); }
  public  java.lang.String unescapePathName (java.lang.String path) { throw new RuntimeException(); }
}
