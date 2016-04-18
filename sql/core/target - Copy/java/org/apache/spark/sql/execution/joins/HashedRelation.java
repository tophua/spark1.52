package org.apache.spark.sql.execution.joins;
/**
 * Interface for a hashed relation by some key. Use {@link HashedRelation.apply} to create a concrete
 * object.
 */
  interface HashedRelation {
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.InternalRow> get (org.apache.spark.sql.catalyst.InternalRow key) ;
  public  void writeBytes (java.io.ObjectOutput out, byte[] serialized) ;
  public  byte[] readBytes (java.io.ObjectInput in) ;
}
