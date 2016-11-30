package org.apache.spark.sql.execution.stat;
// no position
  class StatFunctions$ implements org.apache.spark.Logging {
  /**
   * Static reference to the singleton instance of this Scala object.
   */
  public static final StatFunctions$ MODULE$ = null;
  public   StatFunctions$ () { throw new RuntimeException(); }
  /** Calculate the Pearson Correlation Coefficient for the given columns */
    double pearsonCorrelation (org.apache.spark.sql.DataFrame df, scala.collection.Seq<java.lang.String> cols) { throw new RuntimeException(); }
  private  org.apache.spark.sql.execution.stat.StatFunctions.CovarianceCounter collectStatisticalData (org.apache.spark.sql.DataFrame df, scala.collection.Seq<java.lang.String> cols) { throw new RuntimeException(); }
  /**
   * Calculate the covariance of two numerical columns of a DataFrame.
   * @param df The DataFrame
   * @param cols the column names
   * @return the covariance of the two columns.
   */
    double calculateCov (org.apache.spark.sql.DataFrame df, scala.collection.Seq<java.lang.String> cols) { throw new RuntimeException(); }
  /** Generate a table of frequencies for the elements of two columns. */
    org.apache.spark.sql.DataFrame crossTabulate (org.apache.spark.sql.DataFrame df, java.lang.String col1, java.lang.String col2) { throw new RuntimeException(); }
}
