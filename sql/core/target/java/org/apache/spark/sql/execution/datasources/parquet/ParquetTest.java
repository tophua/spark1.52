package org.apache.spark.sql.execution.datasources.parquet;
/**
 * A helper trait that provides convenient facilities for Parquet testing.
 * &#x5e2e;&#x52a9;&#x6027;&#x80fd;,&#x4e3a;Parquet&#x6d4b;&#x8bd5;&#x63d0;&#x4f9b;&#x65b9;&#x4fbf;&#x7684;&#x8bbe;&#x65bd;
 * <p>
 * NOTE: Considering classes <code>Tuple1</code> ... <code>Tuple22</code> all extend <code>Product</code>, it would be more
 * convenient to use tuples rather than special case classes when writing test cases/suites.
 * &#x5728;&#x7f16;&#x5199;&#x6d4b;&#x8bd5;&#x7528;&#x4f8b;/&#x5957;&#x4ef6;&#x65f6;&#x4f7f;&#x7528;&#x5143;&#x7ec4;&#x800c;&#x4e0d;&#x662f;&#x7279;&#x6b8a;&#x60c5;&#x51b5;&#x7c7b;&#x4f1a;&#x66f4;&#x65b9;&#x4fbf;,
 * Especially, <code>Tuple1.apply</code> can be used to easily wrap a single type/value.
 * &#x7279;&#x522b;&#x662f;,<code>Tuple1.apply</code>&#x53ef;&#x4ee5;&#x7528;&#x6765;&#x8f7b;&#x677e;&#x5305;&#x88c5;&#x4e00;&#x4e2a;&#x7c7b;&#x578b;/&#x503c;
 */
  interface ParquetTest extends org.apache.spark.sql.test.SQLTestUtils {
  public  org.apache.spark.sql.SQLContext _sqlContext () ;
  /**
   * Writes <code>data</code> to a Parquet file, which is then passed to <code>f</code> and will be deleted after <code>f</code>
   * returns.
   * &#x5c06;<code>data</code>&#x5199;&#x5165;Parquet&#x6587;&#x4ef6;,&#x7136;&#x540e;&#x5c06;&#x5176;&#x4f20;&#x9012;&#x7ed9;<code>f</code>,&#x5e76;&#x5728;<code>f</code>&#x8fd4;&#x56de;&#x540e;&#x88ab;&#x5220;&#x9664;
   * @param data (undocumented)
   * @param f (undocumented)
   * @param evidence$1 (undocumented)
   * @param evidence$2 (undocumented)
   */
  public <T extends scala.Product> void withParquetFile (scala.collection.Seq<T> data, scala.Function1<java.lang.String, scala.runtime.BoxedUnit> f, scala.reflect.ClassTag<T> evidence$1, scala.reflect.api.TypeTags.TypeTag<T> evidence$2) ;
  /**
   * Writes <code>data</code> to a Parquet file and reads it back as a {@link DataFrame},
   * which is then passed to <code>f</code>. The Parquet file will be deleted after <code>f</code> returns.
   * &#x5c06;<code>data</code>&#x5199;&#x5165;Parquet&#x6587;&#x4ef6;,&#x5e76;&#x5c06;&#x5176;&#x8bfb;&#x56de;{@link DataFrame},&#x7136;&#x540e;&#x4f20;&#x9012;&#x7ed9;<code>f</code>, <code>f</code>&#x8fd4;&#x56de;&#x540e;,Parquet&#x6587;&#x4ef6;&#x5c06;&#x88ab;&#x5220;&#x9664;&#x3002;
   * @param data (undocumented)
   * @param f (undocumented)
   * @param evidence$3 (undocumented)
   * @param evidence$4 (undocumented)
   */
  public <T extends scala.Product> void withParquetDataFrame (scala.collection.Seq<T> data, scala.Function1<org.apache.spark.sql.DataFrame, scala.runtime.BoxedUnit> f, scala.reflect.ClassTag<T> evidence$3, scala.reflect.api.TypeTags.TypeTag<T> evidence$4) ;
  /**
   * Writes <code>data</code> to a Parquet file, reads it back as a {@link DataFrame} and registers it as a
   * temporary table named <code>tableName</code>, then call <code>f</code>. The temporary table together with the
   * Parquet file will be dropped/deleted after <code>f</code> returns.
   * &#x5c06;<code>data</code>&#x5199;&#x5165;Parquet&#x6587;&#x4ef6;,&#x5c06;&#x5176;&#x8bfb;&#x53d6;&#x4e3a;{@link DataFrame}&#x5e76;&#x5c06;&#x5176;&#x6ce8;&#x518c;&#x4e3a;&#x540d;&#x4e3a;<code>tableName</code>&#x7684;&#x4e34;&#x65f6;&#x8868;,&#x7136;&#x540e;&#x8c03;&#x7528;<code>f</code>,
   *  <code>f</code>&#x8fd4;&#x56de;&#x540e;,&#x4e34;&#x65f6;&#x8868;&#x548c;Parquet&#x6587;&#x4ef6;&#x5c06;&#x88ab;&#x5220;&#x9664;/&#x5220;&#x9664;
   * @param data (undocumented)
   * @param tableName (undocumented)
   * @param f (undocumented)
   * @param evidence$5 (undocumented)
   * @param evidence$6 (undocumented)
   */
  public <T extends scala.Product> void withParquetTable (scala.collection.Seq<T> data, java.lang.String tableName, scala.Function0<scala.runtime.BoxedUnit> f, scala.reflect.ClassTag<T> evidence$5, scala.reflect.api.TypeTags.TypeTag<T> evidence$6) ;
  public <T extends scala.Product> void makeParquetFile (scala.collection.Seq<T> data, java.io.File path, scala.reflect.ClassTag<T> evidence$7, scala.reflect.api.TypeTags.TypeTag<T> evidence$8) ;
  public <T extends scala.Product> void makeParquetFile (org.apache.spark.sql.DataFrame df, java.io.File path, scala.reflect.ClassTag<T> evidence$9, scala.reflect.api.TypeTags.TypeTag<T> evidence$10) ;
  public  java.io.File makePartitionDir (java.io.File basePath, java.lang.String defaultPartitionName, scala.collection.Seq<scala.Tuple2<java.lang.String, java.lang.Object>> partitionCols) ;
}
