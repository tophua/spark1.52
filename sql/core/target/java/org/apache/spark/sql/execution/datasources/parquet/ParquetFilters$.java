package org.apache.spark.sql.execution.datasources.parquet;
// no position
  class ParquetFilters$ {
  /**
   * Static reference to the singleton instance of this Scala object.
   */
  public static final ParquetFilters$ MODULE$ = null;
  public   ParquetFilters$ () { throw new RuntimeException(); }
  public  java.lang.String PARQUET_FILTER_DATA () { throw new RuntimeException(); }
  private  scala.PartialFunction<org.apache.spark.sql.types.DataType, scala.Function2<java.lang.String, java.lang.Object, org.apache.parquet.filter2.predicate.FilterPredicate>> makeEq () { throw new RuntimeException(); }
  private  scala.PartialFunction<org.apache.spark.sql.types.DataType, scala.Function2<java.lang.String, java.lang.Object, org.apache.parquet.filter2.predicate.FilterPredicate>> makeNotEq () { throw new RuntimeException(); }
  private  scala.PartialFunction<org.apache.spark.sql.types.DataType, scala.Function2<java.lang.String, java.lang.Object, org.apache.parquet.filter2.predicate.FilterPredicate>> makeLt () { throw new RuntimeException(); }
  private  scala.PartialFunction<org.apache.spark.sql.types.DataType, scala.Function2<java.lang.String, java.lang.Object, org.apache.parquet.filter2.predicate.FilterPredicate>> makeLtEq () { throw new RuntimeException(); }
  private  scala.PartialFunction<org.apache.spark.sql.types.DataType, scala.Function2<java.lang.String, java.lang.Object, org.apache.parquet.filter2.predicate.FilterPredicate>> makeGt () { throw new RuntimeException(); }
  private  scala.PartialFunction<org.apache.spark.sql.types.DataType, scala.Function2<java.lang.String, java.lang.Object, org.apache.parquet.filter2.predicate.FilterPredicate>> makeGtEq () { throw new RuntimeException(); }
  private  scala.PartialFunction<org.apache.spark.sql.types.DataType, scala.Function2<java.lang.String, scala.collection.immutable.Set<java.lang.Object>, org.apache.parquet.filter2.predicate.FilterPredicate>> makeInSet () { throw new RuntimeException(); }
  /**
   * Converts data sources filters to Parquet filter predicates.
   * @param schema (undocumented)
   * @param predicate (undocumented)
   * @return (undocumented)
   */
  public  scala.Option<org.apache.parquet.filter2.predicate.FilterPredicate> createFilter (org.apache.spark.sql.types.StructType schema, org.apache.spark.sql.sources.Filter predicate) { throw new RuntimeException(); }
  private  void relaxParquetValidTypeMap () { throw new RuntimeException(); }
  /**
   * Note: Inside the Hadoop API we only have access to <code>Configuration</code>, not to
   * {@link org.apache.spark.SparkContext}, so we cannot use broadcasts to convey
   * the actual filter predicate.
   * @param filters (undocumented)
   * @param conf (undocumented)
   */
  public  void serializeFilterExpressions (scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression> filters, org.apache.hadoop.conf.Configuration conf) { throw new RuntimeException(); }
  /**
   * Note: Inside the Hadoop API we only have access to <code>Configuration</code>, not to
   * {@link org.apache.spark.SparkContext}, so we cannot use broadcasts to convey
   * the actual filter predicate.
   * @param conf (undocumented)
   * @return (undocumented)
   */
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression> deserializeFilterExpressions (org.apache.hadoop.conf.Configuration conf) { throw new RuntimeException(); }
}
