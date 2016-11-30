package org.apache.spark.sql.execution;
/**
 * Serializer for serializing {@link UnsafeRow}s during shuffle. Since UnsafeRows are already stored as
 * bytes, this serializer simply copies those bytes to the underlying output stream. When
 * deserializing a stream of rows, instances of this serializer mutate and return a single UnsafeRow
 * instance that is backed by an on-heap byte array.
 * <p>
 * Note that this serializer implements only the {@link Serializer} methods that are used during
 * shuffle, so certain {@link SerializerInstance} methods will throw UnsupportedOperationException.
 * <p>
 * param:  numFields the number of fields in the row being serialized.
 */
  class UnsafeRowSerializer extends org.apache.spark.serializer.Serializer implements java.io.Serializable {
  public   UnsafeRowSerializer (int numFields) { throw new RuntimeException(); }
  public  org.apache.spark.serializer.SerializerInstance newInstance () { throw new RuntimeException(); }
    boolean supportsRelocationOfSerializedObjects () { throw new RuntimeException(); }
}
