package test.org.apache.spark.sql;
public  class FastOperator extends org.apache.spark.sql.execution.SparkPlan implements scala.Product, scala.Serializable {
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> output () { throw new RuntimeException(); }
  // not preceding
  public   FastOperator (scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> output) { throw new RuntimeException(); }
  protected  org.apache.spark.rdd.RDD<org.apache.spark.sql.catalyst.InternalRow> doExecute () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.execution.SparkPlan> children () { throw new RuntimeException(); }
}
