package org.apache.spark.sql.execution;
// no position
public  class EvaluatePython$ implements scala.Serializable {
  /**
   * Static reference to the singleton instance of this Scala object.
   */
  public static final EvaluatePython$ MODULE$ = null;
  public   EvaluatePython$ () { throw new RuntimeException(); }
  public  org.apache.spark.sql.execution.EvaluatePython apply (org.apache.spark.sql.execution.PythonUDF udf, org.apache.spark.sql.catalyst.plans.logical.LogicalPlan child) { throw new RuntimeException(); }
  public  int takeAndServe (org.apache.spark.sql.DataFrame df, int n) { throw new RuntimeException(); }
  /**
   * Helper for converting from Catalyst type to java type suitable for Pyrolite.
   * @param obj (undocumented)
   * @param dataType (undocumented)
   * @return (undocumented)
   */
  public  Object toJava (Object obj, org.apache.spark.sql.types.DataType dataType) { throw new RuntimeException(); }
  /**
   * Converts <code>obj</code> to the type specified by the data type, or returns null if the type of obj is
   * unexpected. Because Python doesn't enforce the type.
   * @param obj (undocumented)
   * @param dataType (undocumented)
   * @return (undocumented)
   */
  public  Object fromJava (Object obj, org.apache.spark.sql.types.DataType dataType) { throw new RuntimeException(); }
  private  java.lang.String module () { throw new RuntimeException(); }
  /**
   * This should be called before trying to serialize any above classes un cluster mode,
   * this should be put in the closure
   */
  public  void registerPicklers () { throw new RuntimeException(); }
  /**
   * Convert an RDD of Java objects to an RDD of serialized Python objects, that is usable by
   * PySpark.
   * @param rdd (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.rdd.RDD<byte[]> javaToPython (org.apache.spark.rdd.RDD<java.lang.Object> rdd) { throw new RuntimeException(); }
}
