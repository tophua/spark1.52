package org.apache.spark.sql.columnar;
  class InMemoryRelation extends org.apache.spark.sql.catalyst.plans.logical.LogicalPlan implements org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation, scala.Product, scala.Serializable {
  static public  org.apache.spark.sql.columnar.InMemoryRelation apply (boolean useCompression, int batchSize, org.apache.spark.storage.StorageLevel storageLevel, org.apache.spark.sql.execution.SparkPlan child, scala.Option<java.lang.String> tableName) { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> output () { throw new RuntimeException(); }
  public  boolean useCompression () { throw new RuntimeException(); }
  public  int batchSize () { throw new RuntimeException(); }
  public  org.apache.spark.storage.StorageLevel storageLevel () { throw new RuntimeException(); }
  public  org.apache.spark.sql.execution.SparkPlan child () { throw new RuntimeException(); }
  public  scala.Option<java.lang.String> tableName () { throw new RuntimeException(); }
  private  org.apache.spark.rdd.RDD<org.apache.spark.sql.columnar.CachedBatch> _cachedColumnBuffers () { throw new RuntimeException(); }
  private  org.apache.spark.sql.catalyst.plans.logical.Statistics _statistics () { throw new RuntimeException(); }
  private  org.apache.spark.Accumulable<scala.collection.mutable.ArrayBuffer<org.apache.spark.sql.catalyst.InternalRow>, org.apache.spark.sql.catalyst.InternalRow> _batchStats () { throw new RuntimeException(); }
  public   InMemoryRelation (scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> output, boolean useCompression, int batchSize, org.apache.spark.storage.StorageLevel storageLevel, org.apache.spark.sql.execution.SparkPlan child, scala.Option<java.lang.String> tableName, org.apache.spark.rdd.RDD<org.apache.spark.sql.columnar.CachedBatch> _cachedColumnBuffers, org.apache.spark.sql.catalyst.plans.logical.Statistics _statistics, org.apache.spark.Accumulable<scala.collection.mutable.ArrayBuffer<org.apache.spark.sql.catalyst.InternalRow>, org.apache.spark.sql.catalyst.InternalRow> _batchStats) { throw new RuntimeException(); }
  private  org.apache.spark.Accumulable<scala.collection.mutable.ArrayBuffer<org.apache.spark.sql.catalyst.InternalRow>, org.apache.spark.sql.catalyst.InternalRow> batchStats () { throw new RuntimeException(); }
  public  org.apache.spark.sql.columnar.PartitionStatistics partitionStatistics () { throw new RuntimeException(); }
  private  long computeSizeInBytes () { throw new RuntimeException(); }
  private  org.apache.spark.sql.catalyst.plans.logical.Statistics statisticsToBePropagated () { throw new RuntimeException(); }
  public  org.apache.spark.sql.catalyst.plans.logical.Statistics statistics () { throw new RuntimeException(); }
  public  void recache () { throw new RuntimeException(); }
  private  void buildBuffers () { throw new RuntimeException(); }
  public  org.apache.spark.sql.columnar.InMemoryRelation withOutput (scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> newOutput) { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.plans.logical.LogicalPlan> children () { throw new RuntimeException(); }
  public  org.apache.spark.sql.columnar.InMemoryRelation newInstance () { throw new RuntimeException(); }
  public  org.apache.spark.rdd.RDD<org.apache.spark.sql.columnar.CachedBatch> cachedColumnBuffers () { throw new RuntimeException(); }
  protected  scala.collection.Seq<java.lang.Object> otherCopyArgs () { throw new RuntimeException(); }
    void uncache (boolean blocking) { throw new RuntimeException(); }
}
