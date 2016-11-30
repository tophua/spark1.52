package org.apache.spark.sql.execution;
/**
 * :: DeveloperApi ::
 * Uses PythonRDD to evaluate a {@link PythonUDF}, one partition of tuples at a time.
 * <p>
 * Python evaluation works by sending the necessary (projected) input data via a socket to an
 * external Python process, and combine the result from the Python process with the original row.
 * <p>
 * For each row we send to Python, we also put it in a queue. For each output row from Python,
 * we drain the queue to find the original input row. Note that if the Python process is way too
 * slow, this could lead to the queue growing unbounded and eventually run out of memory.
 */
public  class BatchPythonEvaluation extends org.apache.spark.sql.execution.SparkPlan implements scala.Product, scala.Serializable {
  public  org.apache.spark.sql.execution.PythonUDF udf () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> output () { throw new RuntimeException(); }
  public  org.apache.spark.sql.execution.SparkPlan child () { throw new RuntimeException(); }
  // not preceding
  public   BatchPythonEvaluation (org.apache.spark.sql.execution.PythonUDF udf, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> output, org.apache.spark.sql.execution.SparkPlan child) { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.execution.SparkPlan> children () { throw new RuntimeException(); }
  protected  org.apache.spark.rdd.RDD<org.apache.spark.sql.catalyst.InternalRow> doExecute () { throw new RuntimeException(); }
}
