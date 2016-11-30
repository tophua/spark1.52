package org.apache.spark.sql.execution.datasources;
// no position
/**
 * A Strategy for planning scans over data sources defined using the sources API.
 */
  class DataSourceStrategy$ extends org.apache.spark.sql.catalyst.planning.GenericStrategy<org.apache.spark.sql.execution.SparkPlan> implements org.apache.spark.Logging {
  /**
   * Static reference to the singleton instance of this Scala object.
   */
  public static final DataSourceStrategy$ MODULE$ = null;
  public   DataSourceStrategy$ () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.execution.SparkPlan> apply (org.apache.spark.sql.catalyst.plans.logical.LogicalPlan plan) { throw new RuntimeException(); }
  private  org.apache.spark.sql.execution.SparkPlan buildPartitionedTableScan (org.apache.spark.sql.execution.datasources.LogicalRelation logicalRelation, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.NamedExpression> projections, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression> filters, org.apache.spark.sql.types.StructType partitionColumns, org.apache.spark.sql.execution.datasources.Partition[] partitions) { throw new RuntimeException(); }
  private  org.apache.spark.rdd.RDD<org.apache.spark.sql.catalyst.InternalRow> mergeWithPartitionValues (org.apache.spark.sql.types.StructType schema, java.lang.String[] requiredColumns, java.lang.String[] partitionColumns, org.apache.spark.sql.catalyst.InternalRow partitionValues, org.apache.spark.rdd.RDD<org.apache.spark.sql.catalyst.InternalRow> dataRows) { throw new RuntimeException(); }
  protected  scala.collection.Seq<org.apache.spark.sql.execution.datasources.Partition> prunePartitions (scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression> predicates, org.apache.spark.sql.execution.datasources.PartitionSpec partitionSpec) { throw new RuntimeException(); }
  protected  org.apache.spark.sql.execution.SparkPlan pruneFilterProject (org.apache.spark.sql.execution.datasources.LogicalRelation relation, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.NamedExpression> projects, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression> filterPredicates, scala.Function2<scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute>, org.apache.spark.sql.sources.Filter[], org.apache.spark.rdd.RDD<org.apache.spark.sql.catalyst.InternalRow>> scanBuilder) { throw new RuntimeException(); }
  protected  org.apache.spark.sql.execution.SparkPlan pruneFilterProjectRaw (org.apache.spark.sql.execution.datasources.LogicalRelation relation, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.NamedExpression> projects, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression> filterPredicates, scala.Function2<scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute>, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression>, org.apache.spark.rdd.RDD<org.apache.spark.sql.catalyst.InternalRow>> scanBuilder) { throw new RuntimeException(); }
  /**
   * Convert RDD of Row into RDD of InternalRow with objects in catalyst types
   * @param relation (undocumented)
   * @param output (undocumented)
   * @param rdd (undocumented)
   * @return (undocumented)
   */
  private  org.apache.spark.rdd.RDD<org.apache.spark.sql.catalyst.InternalRow> toCatalystRDD (org.apache.spark.sql.execution.datasources.LogicalRelation relation, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> output, org.apache.spark.rdd.RDD<org.apache.spark.sql.Row> rdd) { throw new RuntimeException(); }
  /**
   * Convert RDD of Row into RDD of InternalRow with objects in catalyst types
   * @param relation (undocumented)
   * @param rdd (undocumented)
   * @return (undocumented)
   */
  private  org.apache.spark.rdd.RDD<org.apache.spark.sql.catalyst.InternalRow> toCatalystRDD (org.apache.spark.sql.execution.datasources.LogicalRelation relation, org.apache.spark.rdd.RDD<org.apache.spark.sql.Row> rdd) { throw new RuntimeException(); }
  /**
   * Selects Catalyst predicate {@link Expression}s which are convertible into data source {@link Filter}s,
   * and convert them.
   * @param filters (undocumented)
   * @return (undocumented)
   */
  protected  scala.collection.Seq<org.apache.spark.sql.sources.Filter> selectFilters (scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression> filters) { throw new RuntimeException(); }
}
