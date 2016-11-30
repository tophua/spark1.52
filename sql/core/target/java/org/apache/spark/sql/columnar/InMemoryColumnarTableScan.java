package org.apache.spark.sql.columnar;
  class InMemoryColumnarTableScan extends org.apache.spark.sql.execution.SparkPlan implements org.apache.spark.sql.execution.LeafNode, scala.Product, scala.Serializable {
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> attributes () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression> predicates () { throw new RuntimeException(); }
  public  org.apache.spark.sql.columnar.InMemoryRelation relation () { throw new RuntimeException(); }
  // not preceding
  public   InMemoryColumnarTableScan (scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> attributes, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression> predicates, org.apache.spark.sql.columnar.InMemoryRelation relation) { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> output () { throw new RuntimeException(); }
  private  org.apache.spark.sql.columnar.ColumnStatisticsSchema statsFor (org.apache.spark.sql.catalyst.expressions.Attribute a) { throw new RuntimeException(); }
  public  scala.PartialFunction<org.apache.spark.sql.catalyst.expressions.Expression, org.apache.spark.sql.catalyst.expressions.Expression> buildFilter () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression> partitionFilters () { throw new RuntimeException(); }
  public  boolean enableAccumulators () { throw new RuntimeException(); }
  public  org.apache.spark.Accumulator<java.lang.Object> readPartitions () { throw new RuntimeException(); }
  public  org.apache.spark.Accumulator<java.lang.Object> readBatches () { throw new RuntimeException(); }
  private  boolean inMemoryPartitionPruningEnabled () { throw new RuntimeException(); }
  protected  org.apache.spark.rdd.RDD<org.apache.spark.sql.catalyst.InternalRow> doExecute () { throw new RuntimeException(); }
}
