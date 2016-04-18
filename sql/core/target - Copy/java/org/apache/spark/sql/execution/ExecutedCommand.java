package org.apache.spark.sql.execution;
/**
 * A physical operator that executes the run method of a <code>RunnableCommand</code> and
 * saves the result to prevent multiple executions.
 */
  class ExecutedCommand extends org.apache.spark.sql.execution.SparkPlan implements scala.Product, scala.Serializable {
  public  org.apache.spark.sql.execution.RunnableCommand cmd () { throw new RuntimeException(); }
  // not preceding
  public   ExecutedCommand (org.apache.spark.sql.execution.RunnableCommand cmd) { throw new RuntimeException(); }
  /**
   * A concrete command should override this lazy field to wrap up any side effects caused by the
   * command or any other computation that should be evaluated exactly once. The value of this field
   * can be used as the contents of the corresponding RDD generated from the physical plan of this
   * command.
   * <p>
   * The <code>execute()</code> method of all the physical command classes should reference <code>sideEffectResult</code>
   * so that the command can be executed eagerly right after the command query is created.
   * @return (undocumented)
   */
  protected  scala.collection.Seq<org.apache.spark.sql.Row> sideEffectResult () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> output () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.execution.SparkPlan> children () { throw new RuntimeException(); }
  public  org.apache.spark.sql.Row[] executeCollect () { throw new RuntimeException(); }
  public  org.apache.spark.sql.Row[] executeTake (int limit) { throw new RuntimeException(); }
  protected  org.apache.spark.rdd.RDD<org.apache.spark.sql.catalyst.InternalRow> doExecute () { throw new RuntimeException(); }
  public  java.lang.String argString () { throw new RuntimeException(); }
}
