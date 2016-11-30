package org.apache.spark.sql;
// no position
public  class QueryTest$ implements scala.Serializable {
  /**
   * Static reference to the singleton instance of this Scala object.
   */
  public static final QueryTest$ MODULE$ = null;
  public   QueryTest$ () { throw new RuntimeException(); }
  /**
   * Runs the plan and makes sure the answer matches the expected result.
   * If there was exception during the execution or the contents of the DataFrame does not
   * match the expected result, an error message will be returned. Otherwise, a {@link None} will
   * be returned.
   * @param df the {@link DataFrame} to be executed
   * @param expectedAnswer the expected result in a {@link Seq} of {@link Row}s.
   * @return (undocumented)
   */
  public  scala.Option<java.lang.String> checkAnswer (org.apache.spark.sql.DataFrame df, scala.collection.Seq<org.apache.spark.sql.Row> expectedAnswer) { throw new RuntimeException(); }
  public  java.lang.String checkAnswer (org.apache.spark.sql.DataFrame df, java.util.List<org.apache.spark.sql.Row> expectedAnswer) { throw new RuntimeException(); }
}
