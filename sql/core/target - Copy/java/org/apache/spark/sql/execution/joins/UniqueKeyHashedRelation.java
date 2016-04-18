package org.apache.spark.sql.execution.joins;
/**
 * A specialized {@link HashedRelation} that maps key into a single value. This implementation
 * assumes the key is unique.
 */
 final class UniqueKeyHashedRelation implements org.apache.spark.sql.execution.joins.HashedRelation, java.io.Externalizable {
  private  java.util.HashMap<org.apache.spark.sql.catalyst.InternalRow, org.apache.spark.sql.catalyst.InternalRow> hashTable () { throw new RuntimeException(); }
  // not preceding
  public   UniqueKeyHashedRelation (java.util.HashMap<org.apache.spark.sql.catalyst.InternalRow, org.apache.spark.sql.catalyst.InternalRow> hashTable) { throw new RuntimeException(); }
  public   UniqueKeyHashedRelation () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.InternalRow> get (org.apache.spark.sql.catalyst.InternalRow key) { throw new RuntimeException(); }
  public  org.apache.spark.sql.catalyst.InternalRow getValue (org.apache.spark.sql.catalyst.InternalRow key) { throw new RuntimeException(); }
  public  void writeExternal (java.io.ObjectOutput out) { throw new RuntimeException(); }
  public  void readExternal (java.io.ObjectInput in) { throw new RuntimeException(); }
}
