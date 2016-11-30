package org.apache.spark.sql.columnar;
public  class PartitionBatchPruningSuite extends org.apache.spark.SparkFunSuite implements org.apache.spark.sql.test.SharedSQLContext {
  public   PartitionBatchPruningSuite () { throw new RuntimeException(); }
  private  int originalColumnBatchSize () { throw new RuntimeException(); }
  private  boolean originalInMemoryPartitionPruning () { throw new RuntimeException(); }
  protected  void beforeAll () { throw new RuntimeException(); }
  protected  void afterAll () { throw new RuntimeException(); }
  public  void checkBatchPruning (java.lang.String query, int expectedReadPartitions, int expectedReadBatches, scala.Function0<scala.collection.Seq<java.lang.Object>> expectedQueryResult) { throw new RuntimeException(); }
}
