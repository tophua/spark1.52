package org.apache.spark.sql.execution.aggregate;
/**
 * A helper trait used to create specialized setter and getter for types supported by
 * {@link org.apache.spark.sql.execution.UnsafeFixedWidthAggregationMap}'s buffer.
 * (see UnsafeFixedWidthAggregationMap.supportsAggregationBufferSchema).
 */
public  interface BufferSetterGetterUtils {
  public  scala.Function2<org.apache.spark.sql.catalyst.InternalRow, java.lang.Object, java.lang.Object>[] createGetters (org.apache.spark.sql.types.StructType schema) ;
  public  scala.Function3<org.apache.spark.sql.catalyst.expressions.MutableRow, java.lang.Object, java.lang.Object, scala.runtime.BoxedUnit>[] createSetters (org.apache.spark.sql.types.StructType schema) ;
}
