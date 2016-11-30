package org.apache.spark.sql.execution.joins;
/**
 * A general {@link HashedRelation} backed by a hash map that maps the key into a sequence of values.
 */
 final class GeneralHashedRelation implements org.apache.spark.sql.execution.joins.HashedRelation, java.io.Externalizable {
  private  java.util.HashMap<org.apache.spark.sql.catalyst.InternalRow, org.apache.spark.util.collection.CompactBuffer<org.apache.spark.sql.catalyst.InternalRow>> hashTable () { throw new RuntimeException(); }
  // not preceding
  public   GeneralHashedRelation (java.util.HashMap<org.apache.spark.sql.catalyst.InternalRow, org.apache.spark.util.collection.CompactBuffer<org.apache.spark.sql.catalyst.InternalRow>> hashTable) { throw new RuntimeException(); }
  public   GeneralHashedRelation () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.InternalRow> get (org.apache.spark.sql.catalyst.InternalRow key) { throw new RuntimeException(); }
  public  void writeExternal (java.io.ObjectOutput out) { throw new RuntimeException(); }
  public  void readExternal (java.io.ObjectInput in) { throw new RuntimeException(); }
}
