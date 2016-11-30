package org.apache.spark.sql.execution;
/**
 * :: DeveloperApi ::
 * Groups input data by <code>groupingExpressions</code> and computes the <code>aggregateExpressions</code> for each
 * group.
 * <p>
 * param:  partial if true then aggregation is done partially on local data without shuffling to
 *                ensure all values where <code>groupingExpressions</code> are equal are present.
 * param:  groupingExpressions expressions that are evaluated to determine grouping.
 * param:  aggregateExpressions expressions that are computed for each group.
 * param:  child the input data source.
 */
public  class Aggregate extends org.apache.spark.sql.execution.SparkPlan implements org.apache.spark.sql.execution.UnaryNode, scala.Product, scala.Serializable {
  /**
   * An aggregate that needs to be computed for each row in a group.
   * <p>
   * param:  unbound Unbound version of this aggregate, used for result substitution.
   * param:  aggregate A bound copy of this aggregate used to create a new aggregation buffer.
   * param:  resultAttribute An attribute used to refer to the result of this aggregate in the final
   *                        output.
   */
  public  class ComputedAggregate implements scala.Product, scala.Serializable {
    public  org.apache.spark.sql.catalyst.expressions.AggregateExpression1 unbound () { throw new RuntimeException(); }
    public  org.apache.spark.sql.catalyst.expressions.AggregateExpression1 aggregate () { throw new RuntimeException(); }
    public  org.apache.spark.sql.catalyst.expressions.AttributeReference resultAttribute () { throw new RuntimeException(); }
    // not preceding
    public   ComputedAggregate (org.apache.spark.sql.catalyst.expressions.AggregateExpression1 unbound, org.apache.spark.sql.catalyst.expressions.AggregateExpression1 aggregate, org.apache.spark.sql.catalyst.expressions.AttributeReference resultAttribute) { throw new RuntimeException(); }
  }
  // no position
  public  class ComputedAggregate$ extends scala.runtime.AbstractFunction3<org.apache.spark.sql.catalyst.expressions.AggregateExpression1, org.apache.spark.sql.catalyst.expressions.AggregateExpression1, org.apache.spark.sql.catalyst.expressions.AttributeReference, org.apache.spark.sql.execution.Aggregate.ComputedAggregate> implements scala.Serializable {
    public   ComputedAggregate$ () { throw new RuntimeException(); }
  }
  public  boolean partial () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression> groupingExpressions () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.NamedExpression> aggregateExpressions () { throw new RuntimeException(); }
  public  org.apache.spark.sql.execution.SparkPlan child () { throw new RuntimeException(); }
  // not preceding
  public   Aggregate (boolean partial, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression> groupingExpressions, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.NamedExpression> aggregateExpressions, org.apache.spark.sql.execution.SparkPlan child) { throw new RuntimeException(); }
  public  scala.collection.immutable.Map<java.lang.String, org.apache.spark.sql.execution.metric.LongSQLMetric> metrics () { throw new RuntimeException(); }
  public  scala.collection.immutable.List<org.apache.spark.sql.catalyst.plans.physical.Distribution> requiredChildDistribution () { throw new RuntimeException(); }
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> output () { throw new RuntimeException(); }
  /** Creates a new aggregate buffer for a group. */
  private  org.apache.spark.sql.catalyst.expressions.AggregateFunction1[] newAggregateBuffer () { throw new RuntimeException(); }
  /**
   * Substituted version of aggregateExpressions expressions which are used to compute final
   * output rows given a group and the result of all aggregate computations.
   * @return (undocumented)
   */
  protected  org.apache.spark.rdd.RDD<org.apache.spark.sql.catalyst.InternalRow> doExecute () { throw new RuntimeException(); }
}
