package org.apache.spark.sql.execution;
/**
 * This is a specialized version of {@link org.apache.spark.rdd.ShuffledRDD} that is optimized for
 * shuffling rows instead of Java key-value pairs. Note that something like this should eventually
 * be implemented in Spark core, but that is blocked by some more general refactorings to shuffle
 * interfaces / internals.
 * <p>
 * param:  prev the RDD being shuffled. Elements of this RDD are (partitionId, Row) pairs.
 *             Partition ids should be in the range [0, numPartitions - 1].
 * param:  serializer the serializer used during the shuffle.
 * param:  numPartitions the number of post-shuffle partitions.
 */
public  class ShuffledRowRDD extends org.apache.spark.rdd.RDD<org.apache.spark.sql.catalyst.InternalRow> {
  public  org.apache.spark.rdd.RDD<scala.Product2<java.lang.Object, org.apache.spark.sql.catalyst.InternalRow>> prev () { throw new RuntimeException(); }
  // not preceding
  public   ShuffledRowRDD (org.apache.spark.rdd.RDD<scala.Product2<java.lang.Object, org.apache.spark.sql.catalyst.InternalRow>> prev, org.apache.spark.serializer.Serializer serializer, int numPartitions) { throw new RuntimeException(); }
  private  org.apache.spark.Partitioner part () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.Dependency<?>> getDependencies () { throw new RuntimeException(); }
  public  scala.Some<org.apache.spark.Partitioner> partitioner () { throw new RuntimeException(); }
  public  org.apache.spark.Partition[] getPartitions () { throw new RuntimeException(); }
  public  scala.collection.Iterator<org.apache.spark.sql.catalyst.InternalRow> compute (org.apache.spark.Partition split, org.apache.spark.TaskContext context) { throw new RuntimeException(); }
  public  void clearDependencies () { throw new RuntimeException(); }
}
