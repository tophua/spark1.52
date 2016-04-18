package org.apache.spark.sql.sources;
/**
 * ::DeveloperApi::
 * A BaseRelation that can eliminate unneeded columns and filter using selected
 * predicates before producing an RDD containing all matching tuples as Row objects.
 * <p>
 * The actual filter should be the conjunction of all <code>filters</code>,
 * i.e. they should be "and" together.
 * <p>
 * The pushed down filters are currently purely an optimization as they will all be evaluated
 * again.  This means it is safe to use them with methods that produce false positives such
 * as filtering partitions based on a bloom filter.
 * <p>
 * @since 1.3.0
 */
public  interface PrunedFilteredScan {
  public  org.apache.spark.rdd.RDD<org.apache.spark.sql.Row> buildScan (java.lang.String[] requiredColumns, org.apache.spark.sql.sources.Filter[] filters) ;
}
