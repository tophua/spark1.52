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
   * @param name (undocumented)
   * @param joinType (undocumented)
   * @param evidence$1 (undocumented)
   */
  private <T extends java.lang.Object> void testBroadcastJoin (java.lang.String name, java.lang.String joinType, scala.reflect.ClassTag<T> evidence$1) { throw new RuntimeException(); }
}
