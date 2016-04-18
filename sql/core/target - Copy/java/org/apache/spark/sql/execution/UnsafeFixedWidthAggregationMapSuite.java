package org.apache.spark.sql.execution;
/**
 * Test suite for {@link UnsafeFixedWidthAggregationMap}.
 * <p>
 * Use {@link testWithMemoryLeakDetection} rather than {@link test} to construct test cases.
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
