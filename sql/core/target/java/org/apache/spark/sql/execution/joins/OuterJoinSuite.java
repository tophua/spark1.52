package org.apache.spark.sql.execution.joins;
public  class OuterJoinSuite extends org.apache.spark.sql.execution.SparkPlanTest implements org.apache.spark.sql.test.SharedSQLContext {
  public   OuterJoinSuite () { throw new RuntimeException(); }
  private  org.apache.spark.sql.DataFrame left () { throw new RuntimeException(); }
  private  org.apache.spark.sql.DataFrame right () { throw new RuntimeException(); }
  private  org.apache.spark.sql.catalyst.expressions.And condition () { throw new RuntimeException(); }
  private  void testOuterJoin (java.lang.String testName, scala.Function0<org.apache.spark.sql.DataFrame> leftRows, scala.Function0<org.apache.spark.sql.DataFrame> rightRows, org.apache.spark.sql.catalyst.plans.JoinType joinType, scala.Function0<org.apache.spark.sql.catalyst.expressions.Expression> condition, scala.collection.Seq<scala.Product> expectedAnswer) { throw new RuntimeException(); }
}
