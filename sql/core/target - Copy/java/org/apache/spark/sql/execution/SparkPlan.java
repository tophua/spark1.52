package org.apache.spark.sql.execution;
/**
 * :: DeveloperApi ::
 */
public abstract class SparkPlan extends org.apache.spark.sql.catalyst.plans.QueryPlan<org.apache.spark.sql.execution.SparkPlan> implements org.apache.spark.Logging, scala.Serializable {
  static protected  java.lang.ThreadLocal<org.apache.spark.sql.SQLContext> currentContext () { throw new RuntimeException(); }
  public   SparkPlan () { throw new RuntimeException(); }
  /**
   * A handle to the SQL Context that was used to create this plan.   Since many operators need
   * access to the sqlContext for RDD operations or configuration this field is automatically
   * populated by the query planning infrastructure.
   * @return (undocumented)
   */
  protected final  org.apache.spark.sql.SQLContext sqlContext () { throw new RuntimeException(); }
  protected  org.apache.spark.SparkContext sparkContext () { throw new RuntimeException(); }
  public  boolean codegenEnabled () { throw new RuntimeException(); }
  public  boolean unsafeEnabled () { throw new RuntimeException(); }
  /**
   * Whether the "prepare" method is called.
   * @return (undocumented)
   */
  private  java.util.concurrent.atomic.AtomicBoolean prepareCalled () { throw new RuntimeException(); }
  /** Overridden make copy also propogates sqlContext to copied plan. */
  public  org.apache.spark.sql.execution.SparkPlan makeCopy (java.lang.Object[] newArgs) { throw new RuntimeException(); }
  /**
   * Return all metrics containing metrics of this SparkPlan.
   * @return (undocumented)
   */
    scala.collection.immutable.Map<java.lang.String, org.apache.spark.sql.execution.metric.SQLMetric<?, ?>> metrics () { throw new RuntimeException(); }
  /**
   * Return a LongSQLMetric according to the name.
   * @param name (undocumented)
   * @return (undocumented)
   */
    org.apache.spark.sql.execution.metric.LongSQLMetric longMetric (java.lang.String name) { throw new RuntimeException(); }
  /** Specifies how data is partitioned across different nodes in the cluster. */
  public  org.apache.spark.sql.catalyst.plans.physical.Partitioning outputPartitioning () { throw new RuntimeException(); }
  /** Specifies any partition requirements on the input data for this operator. */
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.plans.physical.Distribution> requiredChildDistribution () { throw new RuntimeException(); }
  /** Specifies how data is ordered in each partition. */
  public  scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.SortOrder> outputOrdering () { throw new RuntimeException(); }
  /** Specifies sort order for each partition requirements on the input data for this operator. */
  public  scala.collection.Seq<scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.SortOrder>> requiredChildOrdering () { throw new RuntimeException(); }
  /** Specifies whether this operator outputs UnsafeRows */
  public  boolean outputsUnsafeRows () { throw new RuntimeException(); }
  /** Specifies whether this operator is capable of processing UnsafeRows */
  public  boolean canProcessUnsafeRows () { throw new RuntimeException(); }
  /**
   * Specifies whether this operator is capable of processing Java-object-based Rows (i.e. rows
   * that are not UnsafeRows).
   * @return (undocumented)
   */
  public  boolean canProcessSafeRows () { throw new RuntimeException(); }
  /**
   * Returns the result of this query as an RDD[InternalRow] by delegating to doExecute
   * after adding query plan information to created RDDs for visualization.
   * Concrete implementations of SparkPlan should override doExecute instead.
   * @return (undocumented)
   */
  public final  org.apache.spark.rdd.RDD<org.apache.spark.sql.catalyst.InternalRow> execute () { throw new RuntimeException(); }
  /**
   * Prepare a SparkPlan for execution. It's idempotent.
   */
  public final  void prepare () { throw new RuntimeException(); }
  /**
   * Overridden by concrete implementations of SparkPlan. It is guaranteed to run before any
   * <code>execute</code> of SparkPlan. This is helpful if we want to set up some state before executing the
   * query, e.g., <code>BroadcastHashJoin</code> uses it to broadcast asynchronously.
   * <p>
   * Note: the prepare method has already walked down the tree, so the implementation doesn't need
   * to call children's prepare methods.
   */
  protected  void doPrepare () { throw new RuntimeException(); }
  /**
   * Overridden by concrete implementations of SparkPlan.
   * Produces the result of the query as an RDD[InternalRow]
   * @return (undocumented)
   */
  protected abstract  org.apache.spark.rdd.RDD<org.apache.spark.sql.catalyst.InternalRow> doExecute () ;
  /**
   * Runs this query returning the result as an array.
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Row[] executeCollect () { throw new RuntimeException(); }
  /**
   * Runs this query returning the first <code>n</code> rows as an array.
   * <p>
   * This is modeled after RDD.take but never runs any job locally on the driver.
   * @param n (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.Row[] executeTake (int n) { throw new RuntimeException(); }
  private  boolean isTesting () { throw new RuntimeException(); }
  protected  org.apache.spark.sql.catalyst.expressions.Projection newProjection (scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression> expressions, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> inputSchema) { throw new RuntimeException(); }
  protected  scala.Function0<org.apache.spark.sql.catalyst.expressions.MutableProjection> newMutableProjection (scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Expression> expressions, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> inputSchema) { throw new RuntimeException(); }
  protected  scala.Function1<org.apache.spark.sql.catalyst.InternalRow, java.lang.Object> newPredicate (org.apache.spark.sql.catalyst.expressions.Expression expression, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> inputSchema) { throw new RuntimeException(); }
  protected  scala.math.Ordering<org.apache.spark.sql.catalyst.InternalRow> newOrdering (scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.SortOrder> order, scala.collection.Seq<org.apache.spark.sql.catalyst.expressions.Attribute> inputSchema) { throw new RuntimeException(); }
  /**
   * Creates a row ordering for the given schema, in natural ascending order.
   * @param dataTypes (undocumented)
   * @return (undocumented)
   */
  protected  scala.math.Ordering<org.apache.spark.sql.catalyst.InternalRow> newNaturalAscendingOrdering (scala.collection.Seq<org.apache.spark.sql.types.DataType> dataTypes) { throw new RuntimeException(); }
}
