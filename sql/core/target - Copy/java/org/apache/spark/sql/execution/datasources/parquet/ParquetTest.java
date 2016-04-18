package org.apache.spark.sql.execution.datasources.parquet;
/**
 * A helper trait that provides convenient facilities for Parquet testing.
 * <p>
 * NOTE: Considering classes <code>Tuple1</code> ... <code>Tuple22</code> all extend <code>Product</code>, it would be more
 * convenient to use tuples rather than special case classes when writing test cases/suites.
 * Especially, <code>Tuple1.apply</code> can be used to easily wrap a single type/value.
 */
  interface ParquetTest extends org.apache.spark.sql.test.SQLTestUtils {
  public  org.apache.spark.sql.SQLContext _sqlContext () ;
  /**
   * Writes <code>data</code> to a Parquet file, which is then passed to <code>f</code> and will be deleted after <code>f</code>
   * returns.
   * @param data (undocumented)
   * @param f (undocumented)
   * @param evidence$1 (undocumented)
   * @param evidence$2 (undocumented)
   */
  public <T extends scala.Product> void withParquetFile (scala.collection.Seq<T> data, scala.Function1<java.lang.String, scala.runtime.BoxedUnit> f, scala.reflect.ClassTag<T> evidence$1, scala.reflect.api.TypeTags.TypeTag<T> evidence$2) ;
  /**
   * Writes <code>data</code> to a Parquet file and reads it back as a {@link DataFrame},
   * which is then passed to <code>f</code>. The Parquet file will be deleted after <code>f</code> returns.
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
