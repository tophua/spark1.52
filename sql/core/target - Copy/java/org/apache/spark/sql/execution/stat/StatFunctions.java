package org.apache.spark.sql.execution.stat;
// no position
  class StatFunctions implements org.apache.spark.Logging {
  /** Helper class to simplify tracking and merging counts. */
  static private  class CovarianceCounter implements scala.Serializable {
    public   CovarianceCounter () { throw new RuntimeException(); }
    public  double xAvg () { throw new RuntimeException(); }
    public  double yAvg () { throw new RuntimeException(); }
    public  double Ck () { throw new RuntimeException(); }
    public  double MkX () { throw new RuntimeException(); }
    public  double MkY () { throw new RuntimeException(); }
    public  long count () { throw new RuntimeException(); }
    public  org.apache.spark.sql.execution.stat.StatFunctions.CovarianceCounter add (double x, double y) { throw new RuntimeException(); }
    public  org.apache.spark.sql.execution.stat.StatFunctions.CovarianceCounter merge (org.apache.spark.sql.execution.stat.StatFunctions.CovarianceCounter other) { throw new RuntimeException(); }
    public  double cov () { throw new RuntimeException(); }
  }
  /** Calculate the Pearson Correlation Coefficient for the given columns */
  static   double pearsonCorrelation (org.apache.spark.sql.DataFrame df, scala.collection.Seq<java.lang.String> cols) { throw new RuntimeException(); }
  static private  org.apache.spark.sql.execution.stat.StatFunctions.CovarianceCounter collectStatisticalData (org.apache.spark.sql.DataFrame df, scala.collection.Seq<java.lang.String> cols) { throw new RuntimeException(); }
  /**
   * Calculate the covariance of two numerical columns of a DataFrame.
   * @param df The DataFrame
   * @param cols the column names
   * @return the covariance of the two columns.
   */
  static   double calculateCov (org.apache.spark.sql.DataFrame df, scala.collection.Seq<java.lang.String> cols) { throw new RuntimeException(); }
  /** Generate a table of frequencies for the elements of two columns. */
  static   org.apache.spark.sql.DataFrame crossTabulate (org.apache.spark.sql.DataFrame df, java.lang.String col1, java.lang.String col2) { throw new RuntimeException(); }
}
