package org.apache.spark.sql.execution.joins;
/**
 * Test various broadcast join operators with unsafe enabled.
 * &#x6d4b;&#x8bd5;&#x5404;&#x79cd;&#x5177;&#x6709;&#x4e0d;&#x5b89;&#x5168;&#x542f;&#x7528;&#x7684;&#x5e7f;&#x64ad;&#x52a0;&#x5165;&#x8fd0;&#x7b97;&#x7b26;
 * <p>
 * Tests in this suite we need to run Spark in local-cluster mode. In particular, the use of
 * &#x5728;&#x8fd9;&#x4e2a;&#x5957;&#x4ef6;&#x4e2d;&#x6d4b;&#x8bd5;&#x6211;&#x4eec;&#x9700;&#x8981;&#x5728;&#x672c;&#x5730;&#x96c6;&#x7fa4;&#x6a21;&#x5f0f;&#x4e0b;&#x8fd0;&#x884c;Spark,&#x7279;&#x522b;,&#x4f7f;&#x7528;&#x4e0d;&#x5b89;&#x5168;map[UnsafeHashedRelation]&#x4e0d;&#x89e6;&#x53d1;
 * unsafe map in {@link org.apache.spark.sql.execution.joins.UnsafeHashedRelation} is not triggered
 * without serializing the hashed relation, which does not happen in local mode.
 * &#x6ca1;&#x6709;&#x8fdb;&#x884c;&#x6563;&#x5217;&#x7684;&#x5173;&#x7cfb;,&#x4e0d;&#x53d1;&#x751f;&#x5728;&#x672c;&#x5730;&#x6a21;&#x5f0f;
 */
public  class BroadcastJoinSuite extends org.apache.spark.sql.QueryTest implements org.scalatest.BeforeAndAfterAll {
  public   BroadcastJoinSuite () { throw new RuntimeException(); }
  private  org.apache.spark.SparkContext sc () { throw new RuntimeException(); }
  private  org.apache.spark.sql.SQLContext sqlContext () { throw new RuntimeException(); }
  /**
   * Create a new {@link SQLContext} running in local-cluster mode with unsafe and codegen enabled.
   * &#x521b;&#x5efa;&#x4e00;&#x4e2a;&#x65b0;&#x7684;[sqlcontext]&#x4e0e;&#x4e0d;&#x5b89;&#x5168;&#x4ee3;&#x7801;&#x751f;&#x6210;&#x672c;&#x5730;&#x96c6;&#x7fa4;&#x8fd0;&#x884c;&#x6a21;&#x5f0f;&#x542f;&#x7528;
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
