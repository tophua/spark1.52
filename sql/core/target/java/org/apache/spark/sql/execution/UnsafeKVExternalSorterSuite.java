package org.apache.spark.sql.execution;
/**
 * Test suite for {@link UnsafeKVExternalSorter}, with randomly generated test data.
 * &#x6d4b;&#x8bd5;&#x5957;&#x4ef6;[unsafekvexternalsorter],&#x968f;&#x673a;&#x751f;&#x6210;&#x7684;&#x6d4b;&#x8bd5;&#x6570;&#x636e;&#x3002;
 */
public  class UnsafeKVExternalSorterSuite extends org.apache.spark.SparkFunSuite implements org.apache.spark.sql.test.SharedSQLContext {
  public   UnsafeKVExternalSorterSuite () { throw new RuntimeException(); }
  private  scala.collection.Seq<org.apache.spark.sql.types.AtomicType> keyTypes () { throw new RuntimeException(); }
  private  scala.collection.Seq<org.apache.spark.sql.types.AtomicType> valueTypes () { throw new RuntimeException(); }
  private  scala.util.Random rand () { throw new RuntimeException(); }
  /**
   * Create a test case using randomly generated data for the given key and value schema.
   * &#x4f7f;&#x7528;&#x7ed9;&#x5b9a;&#x7684;&#x952e;&#x548c;&#x503c;&#x6a21;&#x5f0f;&#x968f;&#x673a;&#x751f;&#x6210;&#x7684;&#x6570;&#x636e;&#x521b;&#x5efa;&#x4e00;&#x4e2a;&#x6d4b;&#x8bd5;&#x7528;&#x4f8b;
   * The approach works as follows:
   * &#x8be5;&#x65b9;&#x6cd5;&#x7684;&#x5de5;&#x4f5c;&#x539f;&#x7406;&#x5982;&#x4e0b;&#xff1a;
   * - Create input by randomly generating data based on the given schema
   *  &#x7528;&#x8f93;&#x5165;&#x968f;&#x673a;&#x751f;&#x6210;&#x7684;&#x6570;&#x636e;&#x7684;&#x57fa;&#x7840;&#x4e0a;&#x7ed9;&#x51fa;&#x4e86;&#x6a21;&#x5f0f;
   * - Run {@link UnsafeKVExternalSorter} on the generated data
   * 	&#x8fd0;&#x884c;[unsafekvexternalsorter]&#x4e0a;&#x4ea7;&#x751f;&#x7684;&#x6570;&#x636e;
   * - Collect the output from the sorter, and make sure the keys are sorted in ascending order
   *   &#x4ece;&#x5206;&#x7c7b;&#x6536;&#x96c6;&#x8f93;&#x51fa;,&#x5e76;&#x786e;&#x4fdd;&#x94a5;&#x5319;&#x5728;&#x5347;&#x5e8f;&#x6392;&#x5e8f;
   * - Sort the input by both key and value, and sort the sorter output also by both key and value.
   *   Compare the sorted input and sorted output together to make sure all the key/values match.
   *   &#x7531;&#x952e;&#x548c;&#x503c;&#x8f93;&#x5165;&#x6392;&#x5e8f;,&#x6392;&#x5e8f;&#x5206;&#x7c7b;&#x5668;&#x7684;&#x8f93;&#x51fa;&#x4e5f;&#x7531;&#x952e;&#x548c;&#x503c;,&#x6bd4;&#x8f83;&#x6392;&#x5e8f;&#x7684;&#x8f93;&#x5165;&#x548c;&#x6392;&#x5e8f;&#x8f93;&#x51fa;&#x4e00;&#x8d77;&#x786e;&#x4fdd;&#x6240;&#x6709;&#x7684;&#x952e;/&#x503c;&#x5339;&#x914d;&#x3002;
   * <p>
   * If spill is set to true, the sorter will spill probabilistically roughly every 100 records.
   * &#x5982;&#x679c;&#x6cc4;&#x6f0f;&#x88ab;&#x8bbe;&#x7f6e;&#x4e3a;true,&#x5206;&#x7c7b;&#x5c06;&#x6cc4;&#x6f0f;&#x7684;&#x6982;&#x7387;&#x5927;&#x7ea6;&#x6bcf;100&#x6761;&#x8bb0;&#x5f55;
   * @param keySchema (undocumented)
   * @param valueSchema (undocumented)
   * @param spill (undocumented)
   */
  private  void testKVSorter (org.apache.spark.sql.types.StructType keySchema, org.apache.spark.sql.types.StructType valueSchema, boolean spill) { throw new RuntimeException(); }
  /**
   * Create a test case using the given input data for the given key and value schema.
   * &#x4f7f;&#x7528;&#x7ed9;&#x5b9a;&#x7684;&#x952e;&#x548c;&#x503c;&#x6a21;&#x5f0f;&#x7684;&#x7ed9;&#x5b9a;&#x8f93;&#x5165;&#x6570;&#x636e;&#x521b;&#x5efa;&#x4e00;&#x4e2a;&#x6d4b;&#x8bd5;&#x7528;&#x4f8b;
   * The approach works as follows:
   * &#x8be5;&#x65b9;&#x6cd5;&#x7684;&#x5de5;&#x4f5c;&#x539f;&#x7406;&#x5982;&#x4e0b;&#xff1a;
   * - Create input by randomly generating data based on the given schema
   *   &#x6839;&#x636e;&#x7ed9;&#x5b9a;&#x7684;&#x6a21;&#x5f0f;&#x968f;&#x673a;&#x751f;&#x6210;&#x6570;&#x636e;
   * - Run {@link UnsafeKVExternalSorter} on the input data &#x8fd0;&#x884c;[unsafekvexternalsorter]&#x5bf9;&#x8f93;&#x5165;&#x6570;&#x636e;
   * - Collect the output from the sorter, and make sure the keys are sorted in ascending order
   *   &#x4ece;&#x5206;&#x7c7b;&#x6536;&#x96c6;&#x8f93;&#x51fa;,&#x5e76;&#x786e;&#x4fdd;Key&#x5728;&#x5347;&#x5e8f;&#x6392;&#x5e8f;
   * - Sort the input by both key and value, and sort the sorter output also by both key and value.
   * 	 &#x7531;&#x952e;&#x548c;&#x503c;&#x8f93;&#x5165;&#x6392;&#x5e8f;,&#x6392;&#x5e8f;&#x5206;&#x7c7b;&#x7684;&#x8f93;&#x51fa;&#x4e5f;&#x7531;&#x952e;&#x548c;&#x503c;
   *   Compare the sorted input and sorted output together to make sure all the key/values match.
   *   &#x5c06;&#x6392;&#x5e8f;&#x7684;&#x8f93;&#x5165;&#x548c;&#x6392;&#x5e8f;&#x8f93;&#x51fa;&#x8fdb;&#x884c;&#x6bd4;&#x8f83;,&#x4ee5;&#x786e;&#x4fdd;&#x6240;&#x6709;&#x7684;&#x952e;/&#x503c;&#x5339;&#x914d;
   * If spill is set to true, the sorter will spill probabilistically roughly every 100 records.
   *  &#x5982;&#x679c;&#x6cc4;&#x6f0f;&#x88ab;&#x8bbe;&#x7f6e;&#x4e3a;true,&#x5206;&#x7c7b;&#x5c06;&#x6cc4;&#x6f0f;&#x7684;&#x6982;&#x7387;&#x5927;&#x7ea6;&#x6bcf;100&#x6761;&#x8bb0;&#x5f55;
   * @param keySchema (undocumented)
   * @param valueSchema (undocumented)
   * @param inputData (undocumented)
   * @param pageSize (undocumented)
   * @param spill (undocumented)
   */
  private  void testKVSorter (org.apache.spark.sql.types.StructType keySchema, org.apache.spark.sql.types.StructType valueSchema, scala.collection.Seq<scala.Tuple2<org.apache.spark.sql.catalyst.InternalRow, org.apache.spark.sql.catalyst.InternalRow>> inputData, long pageSize, boolean spill) { throw new RuntimeException(); }
}
