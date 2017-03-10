package org.apache.spark.sql.execution.datasources;
// no position
  class PartitioningUtils {
  static   class PartitionValues implements scala.Product, scala.Serializable {
    public  scala.collection.Seq<java.lang.String> columnNames () { throw new RuntimeException(); }
    public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Literal> literals () { throw new RuntimeException(); }
    // not preceding
    public   PartitionValues (scala.collection.Seq<java.lang.String> columnNames, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Literal> literals) { throw new RuntimeException(); }
  }
  // no position
  static   class PartitionValues$ extends scala.runtime.AbstractFunction2<scala.collection.Seq<java.lang.String>, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Literal>, org.apache.spark.sql.execution.datasources.PartitioningUtils.PartitionValues> implements scala.Serializable {
    /**
     * Static reference to the singleton instance of this Scala object.
     */
    public static final PartitionValues$ MODULE$ = null;
    public   PartitionValues$ () { throw new RuntimeException(); }
  }
  static public  java.lang.String DEFAULT_PARTITION_NAME () { throw new RuntimeException(); }
  /**
   * Given a group of qualified paths, tries to parse them and returns a partition specification.
   * &#x7ed9;&#x5b9a;&#x4e00;&#x7ec4;&#x5408;&#x683c;&#x8def;&#x5f84;,&#x5c1d;&#x8bd5;&#x89e3;&#x6790;&#x5b83;&#x4eec;&#x5e76;&#x8fd4;&#x56de;&#x5206;&#x533a;&#x89c4;&#x8303;
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
  static   org.apache.spark.sql.execution.datasources.PartitionSpec parsePartitions (scala.collection.Seq<org.apache.hadoop.fs.Path> paths, java.lang.String defaultPartitionName, boolean typeInference) { throw new RuntimeException(); }
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
  static   scala.Option<org.apache.spark.sql.execution.datasources.PartitioningUtils.PartitionValues> parsePartition (org.apache.hadoop.fs.Path path, java.lang.String defaultPartitionName, boolean typeInference) { throw new RuntimeException(); }
  static private  scala.Option<scala.Tuple2<java.lang.String, org.apache.spark.sql.catalyst.expressions.Literal>> parsePartitionColumn (java.lang.String columnSpec, java.lang.String defaultPartitionName, boolean typeInference) { throw new RuntimeException(); }
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
  static   scala.collection.Seq<org.apache.spark.sql.execution.datasources.PartitioningUtils.PartitionValues> resolvePartitions (scala.collection.Seq<scala.Tuple2<org.apache.hadoop.fs.Path, org.apache.spark.sql.execution.datasources.PartitioningUtils.PartitionValues>> pathsWithPartitionValues) { throw new RuntimeException(); }
  static   java.lang.String listConflictingPartitionColumns (scala.collection.Seq<scala.Tuple2<org.apache.hadoop.fs.Path, org.apache.spark.sql.execution.datasources.PartitioningUtils.PartitionValues>> pathWithPartitionValues) { throw new RuntimeException(); }
  /**
   * Converts a string to a {@link Literal} with automatic type inference.  Currently only supports
   * {@link IntegerType}, {@link LongType}, {@link DoubleType}, {@link DecimalType.SYSTEM_DEFAULT}, and
   * {@link StringType}.
   * @param raw (undocumented)
   * @param defaultPartitionName (undocumented)
   * @param typeInference (undocumented)
   * @return (undocumented)
   */
  static   org.apache.spark.sql.catalyst.expressions.Literal inferPartitionColumnValue (java.lang.String raw, java.lang.String defaultPartitionName, boolean typeInference) { throw new RuntimeException(); }
  static private  scala.collection.Seq<org.apache.spark.sql.types.DataType> upCastingOrder () { throw new RuntimeException(); }
  /**
   * Given a collection of {@link Literal}s, resolves possible type conflicts by up-casting "lower"
   * types.
   * @param literals (undocumented)
   * @return (undocumented)
   */
  static private  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Literal> resolveTypeConflicts (scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Literal> literals) { throw new RuntimeException(); }
  static public  java.util.BitSet charToEscape () { throw new RuntimeException(); }
  /**
   * ASCII 01-1F are HTTP control characters that need to be escaped.
   * <p>
   and  are \n and \r, respectively.
   * @param c (undocumented)
   * @return (undocumented)
   */
  static public  boolean needsEscaping (char c) { throw new RuntimeException(); }
  static public  java.lang.String escapePathName (java.lang.String path) { throw new RuntimeException(); }
  static public  java.lang.String unescapePathName (java.lang.String path) { throw new RuntimeException(); }
}
