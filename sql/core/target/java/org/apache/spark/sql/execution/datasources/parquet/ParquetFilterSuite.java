package org.apache.spark.sql.execution.datasources.parquet;
/**
 * A test suite that tests Parquet filter2 API based filter pushdown optimization.
 * &#x4e00;&#x4e2a;&#x6d4b;&#x8bd5;&#x5957;&#x4ef6;,&#x6d4b;&#x8bd5;&#x57fa;&#x4e8e;Parquet filter2 API&#x7684;&#x8fc7;&#x6ee4;&#x5668;&#x4e0b;&#x63a8;&#x4f18;&#x5316;
 * <p>
 * NOTE:
 * <p>
 * 1. <code>!(a cmp b)</code> is always transformed to its negated form <code>a cmp' b</code> by the
 *    <code>BooleanSimplification</code> optimization rule whenever possible. As a result, predicate <code>!(a < 1)</code>
 *    results in a <code>GtEq</code> filter predicate rather than a <code>Not</code>.
 * <p>
 * 2. <code>Tuple1(Option(x))</code> is used together with <code>AnyVal</code> types like <code>Int</code> to ensure the inferred
 *    data type is nullable.
 *    <code>Tuple1&#xff08;Option&#xff08;x&#xff09;&#xff09;</code>&#x4e0e;<code>AnyVal</code>&#x7c7b;&#x578b;&#x4e00;&#x8d77;&#x4f7f;&#x7528;,&#x7c7b;&#x4f3c;<code>Int</code>,&#x4ee5;&#x786e;&#x4fdd;&#x63a8;&#x65ad;&#x7684;&#x6570;&#x636e;&#x7c7b;&#x578b;&#x662f;&#x53ef;&#x7a7a;&#x7684;
 */
public  class ParquetFilterSuite extends org.apache.spark.sql.QueryTest implements org.apache.spark.sql.execution.datasources.parquet.ParquetTest, org.apache.spark.sql.test.SharedSQLContext {
  public   ParquetFilterSuite () { throw new RuntimeException(); }
  private  void checkFilterPredicate (org.apache.spark.sql.DataFrame df, org.apache.spark.sql.catalyst.expressions.Predicate predicate, java.lang.Class<? extends org.apache.parquet.filter2.predicate.FilterPredicate> filterClass, scala.Function2<org.apache.spark.sql.DataFrame, scala.collection.Seq<org.apache.spark.sql.Row>, scala.runtime.BoxedUnit> checker, scala.collection.Seq<org.apache.spark.sql.Row> expected) { throw new RuntimeException(); }
  private  void checkFilterPredicate (org.apache.spark.sql.catalyst.expressions.Predicate predicate, java.lang.Class<? extends org.apache.parquet.filter2.predicate.FilterPredicate> filterClass, scala.collection.Seq<org.apache.spark.sql.Row> expected, org.apache.spark.sql.DataFrame df) { throw new RuntimeException(); }
  private <T extends java.lang.Object> void checkFilterPredicate (org.apache.spark.sql.catalyst.expressions.Predicate predicate, java.lang.Class<? extends org.apache.parquet.filter2.predicate.FilterPredicate> filterClass, T expected, org.apache.spark.sql.DataFrame df) { throw new RuntimeException(); }
  private  void checkBinaryFilterPredicate (org.apache.spark.sql.catalyst.expressions.Predicate predicate, java.lang.Class<? extends org.apache.parquet.filter2.predicate.FilterPredicate> filterClass, scala.collection.Seq<org.apache.spark.sql.Row> expected, org.apache.spark.sql.DataFrame df) { throw new RuntimeException(); }
  private  void checkBinaryFilterPredicate (org.apache.spark.sql.catalyst.expressions.Predicate predicate, java.lang.Class<? extends org.apache.parquet.filter2.predicate.FilterPredicate> filterClass, byte[] expected, org.apache.spark.sql.DataFrame df) { throw new RuntimeException(); }
}
