package org.apache.spark.sql.sources;
/**
 * ::DeveloperApi::
 * A BaseRelation that can eliminate unneeded columns before producing an RDD
 * containing all of its tuples as Row objects.
 * <p>
 * @since 1.3.0
 */
public  interface PrunedScan {
  public  org.apache.spark.rdd.RDD<org.apache.spark.sql.Row> buildScan (java.lang.String[] requiredColumns) ;
}
