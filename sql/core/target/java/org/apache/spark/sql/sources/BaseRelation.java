package org.apache.spark.sql.sources;
/**
 * ::DeveloperApi::
 * Represents a collection of tuples with a known schema. Classes that extend BaseRelation must
 * be able to produce the schema of their data in the form of a {@link StructType}. Concrete
 * implementation should inherit from one of the descendant <code>Scan</code> classes, which define various
 * abstract methods for execution.
 * <p>
 * BaseRelations must also define an equality function that only returns true when the two
 * instances will return the same data. This equality function is used when determining when
 * it is safe to substitute cached results for a given relation.
 * <p>
 * @since 1.3.0
 */
public abstract class BaseRelation {
  public   BaseRelation () { throw new RuntimeException(); }
  public abstract  org.apache.spark.sql.SQLContext sqlContext () ;
  public abstract  org.apache.spark.sql.types.StructType schema () ;
  /**
   * Returns an estimated size of this relation in bytes. This information is used by the planner
   * to decide when it is safe to broadcast a relation and can be overridden by sources that
   * know the size ahead of time. By default, the system will assume that tables are too
   * large to broadcast. This method will be called multiple times during query planning
   * and thus should not perform expensive operations for each invocation.
   * <p>
   * Note that it is always better to overestimate size than underestimate, because underestimation
   * could lead to execution plans that are suboptimal (i.e. broadcasting a very large table).
   * <p>
   * @since 1.3.0
   * @return (undocumented)
   */
  public  long sizeInBytes () { throw new RuntimeException(); }
  /**
   * Whether does it need to convert the objects in Row to internal representation, for example:
   *  java.lang.String -> UTF8String
   *  java.lang.Decimal -> Decimal
   * <p>
   * If <code>needConversion</code> is <code>false</code>, buildScan() should return an {@link RDD} of {@link InternalRow}
   * <p>
   * Note: The internal representation is not stable across releases and thus data sources outside
   * of Spark SQL should leave this as true.
   * <p>
   * @since 1.4.0
   * @return (undocumented)
   */
  public  boolean needConversion () { throw new RuntimeException(); }
}
