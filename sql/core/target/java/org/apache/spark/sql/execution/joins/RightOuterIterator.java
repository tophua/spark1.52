package org.apache.spark.sql.execution.joins;
public  class RightOuterIterator extends org.apache.spark.sql.execution.RowIterator {
  public   RightOuterIterator (org.apache.spark.sql.execution.joins.SortMergeJoinScanner smjScanner, org.apache.spark.sql.catalyst.InternalRow leftNullRow, scala.Function1<org.apache.spark.sql.catalyst.InternalRow, java.lang.Object> boundCondition, scala.Function1<org.apache.spark.sql.catalyst.InternalRow, org.apache.spark.sql.catalyst.InternalRow> resultProj, org.apache.spark.sql.execution.metric.LongSQLMetric numRows) { throw new RuntimeException(); }
  private  boolean advanceRight () { throw new RuntimeException(); }
  private  boolean advanceLeftUntilBoundConditionSatisfied () { throw new RuntimeException(); }
  public  boolean advanceNext () { throw new RuntimeException(); }
  public  org.apache.spark.sql.catalyst.InternalRow getRow () { throw new RuntimeException(); }
}
