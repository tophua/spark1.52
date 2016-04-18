package org.apache.spark.sql.execution;
/**
 * :: DeveloperApi ::
 * Take the first limit elements as defined by the sortOrder, and do projection if needed.
 * This is logically equivalent to having a {@link Limit} operator after a {@link Sort} operator,
 * or having a {@link Project} operator between them.
 * This could have been named TopK, but Spark's top operator does the opposite in ordering
 * so we name it TakeOrdered to avoid confusion.
 */
public  class TakeOrderedAndProject extends org.apache.spark.sql.execution.SparkPlan implements org.apache.spark.sql.execution.UnaryNode, scala.Product, scala.Serializable {
  public  int limit () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.SortOrder> sortOrder () { throw new RuntimeException(); }
  public  scala.Option<scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.NamedExpression>> projectList () { throw new RuntimeException(); }
  public  org.apache.spark.sql.execution.SparkPlan child () { throw new RuntimeException(); }
  // not preceding
  public   TakeOrderedAndProject (int limit, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.SortOrder> sortOrder, scala.Option<scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.NamedExpression>> projectList, org.apache.spark.sql.execution.SparkPlan child) { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> output () { throw new RuntimeException(); }
  public  org.apache.spark.sql.catalyst.plans.physical.Partitioning outputPartitioning () { throw new RuntimeException(); }
  private  org.apache.spark.sql.catalyst.expressions.InterpretedOrdering ord () { throw new RuntimeException(); }
  private  scala.Option<org.apache.spark.sql.catalyst.expressions.InterpretedProjection> projection () { throw new RuntimeException(); }
  private  org.apache.spark.sql.catalyst.InternalRow[] collectData () { throw new RuntimeException(); }
  public  org.apache.spark.sql.Row[] executeCollect () { throw new RuntimeException(); }
  protected  org.apache.spark.rdd.RDD<org.apache.spark.sql.catalyst.InternalRow> doExecute () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.SortOrder> outputOrdering () { throw new RuntimeException(); }
  public  java.lang.String simpleString () { throw new RuntimeException(); }
}
