package org.apache.spark.sql.execution.joins;
/**
 * A HashedRelation for UnsafeRow, which is backed by HashMap or BytesToBytesMap that maps the key
 * into a sequence of values.
 * <p>
 * When it's created, it uses HashMap. After it's serialized and deserialized, it switch to use
 * BytesToBytesMap for better memory performance (multiple values for the same are stored as a
 * continuous byte array.
 * <p>
 * It's serialized in the following format:
 *  [number of keys]
 *  [size of key] [size of all values in bytes] [key bytes] [bytes for all values]
 *  ...
 * <p>
 * All the values are serialized as following:
 *   [number of fields] [number of bytes] [underlying bytes of UnsafeRow]
 *   ...
 */
 final class UnsafeHashedRelation implements org.apache.spark.sql.execution.joins.HashedRelation, java.io.Externalizable {
  static public  org.apache.spark.sql.execution.joins.HashedRelation apply (scala.collection.Iterator<org.apache.spark.sql.catalyst.InternalRow> input, org.apache.spark.sql.execution.metric.LongSQLMetric numInputRows, org.apache.spark.sql.catalyst.expressions.UnsafeProjection keyGenerator, int sizeEstimate) { throw new RuntimeException(); }
  private  java.util.HashMap<org.apache.spark.sql.catalyst.expressions.UnsafeRow, org.apache.spark.util.collection.CompactBuffer<org.apache.spark.sql.catalyst.expressions.UnsafeRow>> hashTable () { throw new RuntimeException(); }
  // not preceding
  public   UnsafeHashedRelation (java.util.HashMap<org.apache.spark.sql.catalyst.expressions.UnsafeRow, org.apache.spark.util.collection.CompactBuffer<org.apache.spark.sql.catalyst.expressions.UnsafeRow>> hashTable) { throw new RuntimeException(); }
     UnsafeHashedRelation () { throw new RuntimeException(); }
  /**
   * Return the size of the unsafe map on the executors.
   * <p>
   * For broadcast joins, this hashed relation is bigger on the driver because it is
   * represented as a Java hash map there. While serializing the map to the executors,
   * however, we rehash the contents in a binary map to reduce the memory footprint on
   * the executors.
   * <p>
   * For non-broadcast joins or in local mode, return 0.
   * @return (undocumented)
   */
  public  long getUnsafeSize () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.InternalRow> get (org.apache.spark.sql.catalyst.InternalRow key) { throw new RuntimeException(); }
  public  void writeExternal (java.io.ObjectOutput out) { throw new RuntimeException(); }
  public  void readExternal (java.io.ObjectInput in) { throw new RuntimeException(); }
}
