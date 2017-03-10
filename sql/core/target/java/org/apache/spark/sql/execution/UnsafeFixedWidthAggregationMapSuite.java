package org.apache.spark.sql.execution;
/**
 * Test suite for {@link UnsafeFixedWidthAggregationMap}.
 * <p>
 * Use {@link testWithMemoryLeakDetection} rather than {@link test} to construct test cases.
 * &#x4e0d;&#x5b89;&#x5168;&#x56fa;&#x5b9a;&#x5bbd;&#x5ea6;&#x805a;&#x5408;&#x6620;&#x5c04;&#x6d4b;&#x8bd5;&#x5957;&#x4ef6;
 */
public  class UnsafeFixedWidthAggregationMapSuite extends org.apache.spark.SparkFunSuite implements org.scalatest.Matchers, org.apache.spark.sql.test.SharedSQLContext {
  public   UnsafeFixedWidthAggregationMapSuite () { throw new RuntimeException(); }
  private  org.apache.spark.sql.types.StructType groupKeySchema () { throw new RuntimeException(); }
  private  org.apache.spark.sql.types.StructType aggBufferSchema () { throw new RuntimeException(); }
  private  org.apache.spark.sql.catalyst.InternalRow emptyAggregationBuffer () { throw new RuntimeException(); }
  private  long PAGE_SIZE_BYTES () { throw new RuntimeException(); }
  private  org.apache.spark.unsafe.memory.TaskMemoryManager taskMemoryManager () { throw new RuntimeException(); }
  private  org.apache.spark.sql.execution.TestShuffleMemoryManager shuffleMemoryManager () { throw new RuntimeException(); }
  public  void testWithMemoryLeakDetection (java.lang.String name, scala.Function0<scala.runtime.BoxedUnit> f) { throw new RuntimeException(); }
  private  scala.collection.Seq<java.lang.String> randomStrings (int n) { throw new RuntimeException(); }
}
