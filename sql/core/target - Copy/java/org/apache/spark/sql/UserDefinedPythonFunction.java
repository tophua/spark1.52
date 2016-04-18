package org.apache.spark.sql;
/**
 * A user-defined Python function. To create one, use the <code>pythonUDF</code> functions in {@link functions}.
 * This is used by Python API.
 */
  class UserDefinedPythonFunction implements scala.Product, scala.Serializable {
  public  java.lang.String name () { throw new RuntimeException(); }
  public  byte[] command () { throw new RuntimeException(); }
  public  java.util.Map<java.lang.String, java.lang.String> envVars () { throw new RuntimeException(); }
  public  java.util.List<java.lang.String> pythonIncludes () { throw new RuntimeException(); }
  public  java.lang.String pythonExec () { throw new RuntimeException(); }
  public  java.lang.String pythonVer () { throw new RuntimeException(); }
  public  java.util.List<org.apache.spark.broadcast.Broadcast<org.apache.spark.api.python.PythonBroadcast>> broadcastVars () { throw new RuntimeException(); }
  public  org.apache.spark.Accumulator<java.util.List<byte[]>> accumulator () { throw new RuntimeException(); }
  public  org.apache.spark.sql.types.DataType dataType () { throw new RuntimeException(); }
  // not preceding
  public   UserDefinedPythonFunction (java.lang.String name, byte[] command, java.util.Map<java.lang.String, java.lang.String> envVars, java.util.List<java.lang.String> pythonIncludes, java.lang.String pythonExec, java.lang.String pythonVer, java.util.List<org.apache.spark.broadcast.Broadcast<org.apache.spark.api.python.PythonBroadcast>> broadcastVars, org.apache.spark.Accumulator<java.util.List<byte[]>> accumulator, org.apache.spark.sql.types.DataType dataType) { throw new RuntimeException(); }
  public  org.apache.spark.sql.execution.PythonUDF builder (scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression> e) { throw new RuntimeException(); }
  /** Returns a {@link Column} that will evaluate to calling this UDF with the given input. */
  public  org.apache.spark.sql.Column apply (scala.collection.Seq<org.apache.spark.sql.Column> exprs) { throw new RuntimeException(); }
}
