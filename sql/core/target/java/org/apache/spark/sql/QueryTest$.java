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
   * &#x8fd0;&#x884c;&#x8be5;&#x8ba1;&#x5212;,&#x5e76;&#x786e;&#x4fdd;&#x7b54;&#x6848;&#x4e0e;&#x9884;&#x671f;&#x7ed3;&#x679c;&#x76f8;&#x5339;&#x914d;
   * If there was exception during the execution or the contents of the DataFrame does not
   * &#x5982;&#x679c;DataFrame&#x6267;&#x884c;&#x6216;&#x5185;&#x5bb9;&#x4e2d;&#x6709;&#x5f02;&#x5e38;,&#x4e0d;&#x7b26;&#x5408;&#x9884;&#x671f;&#x7684;&#x7ed3;&#x679c;,&#x5c06;&#x8fd4;&#x56de;&#x4e00;&#x4e2a;&#x9519;&#x8bef;&#x6d88;&#x606f;,&#x5426;&#x5219;,&#x4e00;&#x4e2a;[&#x6ca1;&#x6709;]&#x5c06;&#x88ab;&#x8fd4;&#x56de;&#x3002;
   * match the expected result, an error message will be returned. Otherwise, a {@link None} will
   * be returned.
   * @param df the {@link DataFrame} to be executed
   * @param expectedAnswer the expected result in a {@link Seq} of {@link Row}s.
   * @return (undocumented)
   */
  public  scala.Option<java.lang.String> checkAnswer (org.apache.spark.sql.DataFrame df, scala.collection.Seq<org.apache.spark.sql.Row> expectedAnswer) { throw new RuntimeException(); }
  public  java.lang.String checkAnswer (org.apache.spark.sql.DataFrame df, java.util.List<org.apache.spark.sql.Row> expectedAnswer) { throw new RuntimeException(); }
}
