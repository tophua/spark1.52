package org.apache.spark.sql.execution;
/**
 * Test suite for {@link UnsafeKVExternalSorter}, with randomly generated test data.
 */
public  class UnsafeKVExternalSorterSuite extends org.apache.spark.SparkFunSuite implements org.apache.spark.sql.test.SharedSQLContext {
  public   UnsafeKVExternalSorterSuite () { throw new RuntimeException(); }
  private  scala.collection.Seq<org.apache.spark.sql.types.AtomicType> keyTypes () { throw new RuntimeException(); }
  private  scala.collection.Seq<org.apache.spark.sql.types.AtomicType> valueTypes () { throw new RuntimeException(); }
  private  scala.util.Random rand () { throw new RuntimeException(); }
  /**
   * Create a test case using randomly generated data for the given key and value schema.
   * <p>
   * The approach works as follows:
   * <p>
   * - Create input by randomly generating data based on the given schema
   * - Run {@link UnsafeKVExternalSorter} on the generated data
   * - Collect the output from the sorter, and make sure the keys are sorted in ascending order
   * - Sort the input by both key and value, and sort the sorter output also by both key and value.
   *   Compare the sorted input and sorted output together to make sure all the key/values match.
   * <p>
   * If spill is set to true, the sorter will spill probabilistically roughly every 100 records.
   * @param keySchema (undocumented)
   * @param valueSchema (undocumented)
   * @param spill (undocumented)
   */
  private  void testKVSorter (org.apache.spark.sql.types.StructType keySchema, org.apache.spark.sql.types.StructType valueSchema, boolean spill) { throw new RuntimeException(); }
  /**
   * Create a test case using the given input data for the given key and value schema.
   * <p>
   * The approach works as follows:
   * <p>
   * - Create input by randomly generating data based on the given schema
   * - Run {@link UnsafeKVExternalSorter} on the input data
   * - Collect the output from the sorter, and make sure the keys are sorted in ascending order
   * - Sort the input by both key and value, and sort the sorter output also by both key and value.
   *   Compare the sorted input and sorted output together to make sure all the key/values match.
   * <p>
   * If spill is set to true, the sorter will spill probabilistically roughly every 100 records.
   * @param keySchema (undocumented)
   * @param valueSchema (undocumented)
   * @param inputData (undocumented)
   * @param pageSize (undocumented)
   * @param spill (undocumented)
   */
  private  void testKVSorter (org.apache.spark.sql.types.StructType keySchema, org.apache.spark.sql.types.StructType valueSchema, scala.collection.Seq<scala.Tuple2<org.apache.spark.sql.catalyst.InternalRow, org.apache.spark.sql.catalyst.InternalRow>> inputData, long pageSize, boolean spill) { throw new RuntimeException(); }
}
