package org.apache.spark.sql.execution.datasources.parquet;
// no position
  class ParquetFilters {
  static public  class SetInFilter<T extends java.lang.Comparable<T>> extends org.apache.parquet.filter2.predicate.UserDefinedPredicate<T> implements java.io.Serializable, scala.Product, scala.Serializable {
    public  scala.collection.immutable.Set<T> valueSet () { throw new RuntimeException(); }
    // not preceding
    public   SetInFilter (scala.collection.immutable.Set<T> valueSet) { throw new RuntimeException(); }
    public  boolean keep (T value) { throw new RuntimeException(); }
    public  boolean canDrop (org.apache.parquet.filter2.predicate.Statistics<T> statistics) { throw new RuntimeException(); }
    public  boolean inverseCanDrop (org.apache.parquet.filter2.predicate.Statistics<T> statistics) { throw new RuntimeException(); }
  }
  // no position
  static public  class SetInFilter$ implements scala.Serializable {
    /**
     * Static reference to the singleton instance of this Scala object.
     */
    public static final SetInFilter$ MODULE$ = null;
    public   SetInFilter$ () { throw new RuntimeException(); }
  }
  static public  java.lang.String PARQUET_FILTER_DATA () { throw new RuntimeException(); }
  static private  scala.PartialFunction<org.apache.spark.sql.types.DataType, scala.Function2<java.lang.String, java.lang.Object, org.apache.parquet.filter2.predicate.FilterPredicate>> makeEq () { throw new RuntimeException(); }
  static private  scala.PartialFunction<org.apache.spark.sql.types.DataType, scala.Function2<java.lang.String, java.lang.Object, org.apache.parquet.filter2.predicate.FilterPredicate>> makeNotEq () { throw new RuntimeException(); }
  static private  scala.PartialFunction<org.apache.spark.sql.types.DataType, scala.Function2<java.lang.String, java.lang.Object, org.apache.parquet.filter2.predicate.FilterPredicate>> makeLt () { throw new RuntimeException(); }
  static private  scala.PartialFunction<org.apache.spark.sql.types.DataType, scala.Function2<java.lang.String, java.lang.Object, org.apache.parquet.filter2.predicate.FilterPredicate>> makeLtEq () { throw new RuntimeException(); }
  static private  scala.PartialFunction<org.apache.spark.sql.types.DataType, scala.Function2<java.lang.String, java.lang.Object, org.apache.parquet.filter2.predicate.FilterPredicate>> makeGt () { throw new RuntimeException(); }
  static private  scala.PartialFunction<org.apache.spark.sql.types.DataType, scala.Function2<java.lang.String, java.lang.Object, org.apache.parquet.filter2.predicate.FilterPredicate>> makeGtEq () { throw new RuntimeException(); }
  static private  scala.PartialFunction<org.apache.spark.sql.types.DataType, scala.Function2<java.lang.String, scala.collection.immutable.Set<java.lang.Object>, org.apache.parquet.filter2.predicate.FilterPredicate>> makeInSet () { throw new RuntimeException(); }
  /**
   * Converts data sources filters to Parquet filter predicates.
   * @param schema (undocumented)
   * @param predicate (undocumented)
   * @return (undocumented)
   */
  static public  scala.Option<org.apache.parquet.filter2.predicate.FilterPredicate> createFilter (org.apache.spark.sql.types.StructType schema, org.apache.spark.sql.sources.Filter predicate) { throw new RuntimeException(); }
  static private  void relaxParquetValidTypeMap () { throw new RuntimeException(); }
  /**
   * Note: Inside the Hadoop API we only have access to <code>Configuration</code>, not to
   * {@link org.apache.spark.SparkContext}, so we cannot use broadcasts to convey
   * the actual filter predicate.
   * @param filters (undocumented)
   * @param conf (undocumented)
   */
  static public  void serializeFilterExpressions (scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression> filters, org.apache.hadoop.conf.Configuration conf) { throw new RuntimeException(); }
  /**
   * Note: Inside the Hadoop API we only have access to <code>Configuration</code>, not to
   * {@link org.apache.spark.SparkContext}, so we cannot use broadcasts to convey
   * the actual filter predicate.
   * @param conf (undocumented)
   * @return (undocumented)
   */
  static public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression> deserializeFilterExpressions (org.apache.hadoop.conf.Configuration conf) { throw new RuntimeException(); }
}
