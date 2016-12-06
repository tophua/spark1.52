package org.apache.spark.sql.execution.joins;
/**
 * Test various broadcast join operators with unsafe enabled.
 * <p>
 * Tests in this suite we need to run Spark in local-cluster mode. In particular, the use of
 * unsafe map in {@link org.apache.spark.sql.execution.joins.UnsafeHashedRelation} is not triggered
 * without serializing the hashed relation, which does not happen in local mode.
 */
public  class BroadcastJoinSuite extends org.apache.spark.sql.QueryTest implements org.scalatest.BeforeAndAfterAll {
  public   BroadcastJoinSuite () { throw new RuntimeException(); }
  private  org.apache.spark.SparkContext sc () { throw new RuntimeException(); }
  private  org.apache.spark.sql.SQLContext sqlContext () { throw new RuntimeException(); }
  /**
   * Create a new {@link SQLContext} running in local-cluster mode with unsafe and codegen enabled.
   */
  public  void beforeAll () { throw new RuntimeException(); }
  public  void afterAll () { throw new RuntimeException(); }
  /**
   * Test whether the specified broadcast join updates the peak execution memory accumulator.
   * &#x6d4b;&#x8bd5;&#x6307;&#x5b9a;&#x7684;&#x5e7f;&#x64ad;&#x8fde;&#x63a5;&#x662f;&#x5426;&#x66f4;&#x65b0;&#x5cf0;&#x503c;&#x6267;&#x884c;&#x5185;&#x5b58;&#x7d2f;&#x52a0;&#x5668;
   * @param name (undocumented)
   * @param joinType (undocumented)
   * @param evidence$1 (undocumented)
   */
  private <T extends java.lang.Object> void testBroadcastJoin (java.lang.String name, java.lang.String joinType, scala.reflect.ClassTag<T> evidence$1) { throw new RuntimeException(); }
}
