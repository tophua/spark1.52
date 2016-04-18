package org.apache.spark.sql;
/**
 * :: Experimental ::
 * Statistic functions for {@link DataFrame}s.
 * <p>
 * @since 1.4.0
 */
public final class DataFrameStatFunctions {
     DataFrameStatFunctions (org.apache.spark.sql.DataFrame df) { throw new RuntimeException(); }
  /**
   * Calculate the sample covariance of two numerical columns of a DataFrame.
   * @param col1 the name of the first column
   * @param col2 the name of the second column
   * @return the covariance of the two columns.
   * <p>
   * <pre><code>
   *    val df = sc.parallelize(0 until 10).toDF("id").withColumn("rand1", rand(seed=10))
   *      .withColumn("rand2", rand(seed=27))
   *    df.stat.cov("rand1", "rand2")
   *    res1: Double = 0.065...
   * </code></pre>
   * <p>
   * @since 1.4.0
   */
  public  double cov (java.lang.String col1, java.lang.String col2) { throw new RuntimeException(); }
  /**
   * Calculates the correlation of two columns of a DataFrame. Currently only supports the Pearson
   * Correlation Coefficient. For Spearman Correlation, consider using RDD methods found in
   * MLlib's Statistics.
   * <p>
   * @param col1 the name of the column
   * @param col2 the name of the column to calculate the correlation against
   * @return The Pearson Correlation Coefficient as a Double.
   * <p>
   * <pre><code>
   *    val df = sc.parallelize(0 until 10).toDF("id").withColumn("rand1", rand(seed=10))
   *      .withColumn("rand2", rand(seed=27))
   *    df.stat.corr("rand1", "rand2")
   *    res1: Double = 0.613...
   * </code></pre>
   * <p>
   * @since 1.4.0
   * @param method (undocumented)
   */
  public  double corr (java.lang.String col1, java.lang.String col2, java.lang.String method) { throw new RuntimeException(); }
  /**
   * Calculates the Pearson Correlation Coefficient of two columns of a DataFrame.
   * <p>
   * @param col1 the name of the column
   * @param col2 the name of the column to calculate the correlation against
   * @return The Pearson Correlation Coefficient as a Double.
   * <p>
   * <pre><code>
   *    val df = sc.parallelize(0 until 10).toDF("id").withColumn("rand1", rand(seed=10))
   *      .withColumn("rand2", rand(seed=27))
   *    df.stat.corr("rand1", "rand2", "pearson")
   *    res1: Double = 0.613...
   * </code></pre>
   * <p>
   * @since 1.4.0
   */
  public  double corr (java.lang.String col1, java.lang.String col2) { throw new RuntimeException(); }
  /**
   * Computes a pair-wise frequency table of the given columns. Also known as a contingency table.
   * The number of distinct values for each column should be less than 1e4. At most 1e6 non-zero
   * pair frequencies will be returned.
   * The first column of each row will be the distinct values of <code>col1</code> and the column names will
   * be the distinct values of <code>col2</code>. The name of the first column will be <code>$col1_$col2</code>. Counts
   * will be returned as <code>Long</code>s. Pairs that have no occurrences will have zero as their counts.
   * Null elements will be replaced by "null", and back ticks will be dropped from elements if they
   * exist.
   * <p>
   * @param col1 The name of the first column. Distinct items will make the first item of
   *             each row.
   * @param col2 The name of the second column. Distinct items will make the column names
   *             of the DataFrame.
   * @return A DataFrame containing for the contingency table.
   * <p>
   * <pre><code>
   *    val df = sqlContext.createDataFrame(Seq((1, 1), (1, 2), (2, 1), (2, 1), (2, 3), (3, 2),
   *      (3, 3))).toDF("key", "value")
   *    val ct = df.stat.crosstab("key", "value")
   *    ct.show()
   *    +---------+---+---+---+
   *    |key_value|  1|  2|  3|
   *    +---------+---+---+---+
   *    |        2|  2|  0|  1|
   *    |        1|  1|  1|  0|
   *    |        3|  0|  1|  1|
   *    +---------+---+---+---+
   * </code></pre>
   * <p>
   * @since 1.4.0
   */
  public  org.apache.spark.sql.DataFrame crosstab (java.lang.String col1, java.lang.String col2) { throw new RuntimeException(); }
  /**
   * Finding frequent items for columns, possibly with false positives. Using the
   * frequent element count algorithm described in
   * {@link http://dx.doi.org/10.1145/762471.762473, proposed by Karp, Schenker, and Papadimitriou}.
   * The <code>support</code> should be greater than 1e-4.
   * <p>
   * This function is meant for exploratory data analysis, as we make no guarantee about the
   * backward compatibility of the schema of the resulting {@link DataFrame}.
   * <p>
   * @param cols the names of the columns to search frequent items in.
   * @param support The minimum frequency for an item to be considered <code>frequent</code>. Should be greater
   *                than 1e-4.
   * @return A Local DataFrame with the Array of frequent items for each column.
   * <p>
   * <pre><code>
   *    val rows = Seq.tabulate(100) { i =&gt;
   *      if (i % 2 == 0) (1, -1.0) else (i, i * -1.0)
   *    }
   *    val df = sqlContext.createDataFrame(rows).toDF("a", "b")
   *    // find the items with a frequency greater than 0.4 (observed 40% of the time) for columns
   *    // "a" and "b"
   *    val freqSingles = df.stat.freqItems(Array("a", "b"), 0.4)
   *    freqSingles.show()
   *    +-----------+-------------+
   *    |a_freqItems|  b_freqItems|
   *    +-----------+-------------+
   *    |    [1, 99]|[-1.0, -99.0]|
   *    +-----------+-------------+
   *    // find the pair of items with a frequency greater than 0.1 in columns "a" and "b"
   *    val pairDf = df.select(struct("a", "b").as("a-b"))
   *    val freqPairs = pairDf.stat.freqItems(Array("a-b"), 0.1)
   *    freqPairs.select(explode($"a-b_freqItems").as("freq_ab")).show()
   *    +----------+
   *    |   freq_ab|
   *    +----------+
   *    |  [1,-1.0]|
   *    |   ...    |
   *    +----------+
   * </code></pre>
   * <p>
   * @since 1.4.0
   */
  public  org.apache.spark.sql.DataFrame freqItems (java.lang.String[] cols, double support) { throw new RuntimeException(); }
  /**
   * Finding frequent items for columns, possibly with false positives. Using the
   * frequent element count algorithm described in
   * {@link http://dx.doi.org/10.1145/762471.762473, proposed by Karp, Schenker, and Papadimitriou}.
   * Uses a <code>default</code> support of 1%.
   * <p>
   * This function is meant for exploratory data analysis, as we make no guarantee about the
   * backward compatibility of the schema of the resulting {@link DataFrame}.
   * <p>
   * @param cols the names of the columns to search frequent items in.
   * @return A Local DataFrame with the Array of frequent items for each column.
   * <p>
   * @since 1.4.0
   */
  public  org.apache.spark.sql.DataFrame freqItems (java.lang.String[] cols) { throw new RuntimeException(); }
  /**
   * (Scala-specific) Finding frequent items for columns, possibly with false positives. Using the
   * frequent element count algorithm described in
   * {@link http://dx.doi.org/10.1145/762471.762473, proposed by Karp, Schenker, and Papadimitriou}.
   * <p>
   * This function is meant for exploratory data analysis, as we make no guarantee about the
   * backward compatibility of the schema of the resulting {@link DataFrame}.
   * <p>
   * @param cols the names of the columns to search frequent items in.
   * @return A Local DataFrame with the Array of frequent items for each column.
   * <p>
   * <pre><code>
   *    val rows = Seq.tabulate(100) { i =&gt;
   *      if (i % 2 == 0) (1, -1.0) else (i, i * -1.0)
   *    }
   *    val df = sqlContext.createDataFrame(rows).toDF("a", "b")
   *    // find the items with a frequency greater than 0.4 (observed 40% of the time) for columns
   *    // "a" and "b"
   *    val freqSingles = df.stat.freqItems(Seq("a", "b"), 0.4)
   *    freqSingles.show()
   *    +-----------+-------------+
   *    |a_freqItems|  b_freqItems|
   *    +-----------+-------------+
   *    |    [1, 99]|[-1.0, -99.0]|
   *    +-----------+-------------+
   *    // find the pair of items with a frequency greater than 0.1 in columns "a" and "b"
   *    val pairDf = df.select(struct("a", "b").as("a-b"))
   *    val freqPairs = pairDf.stat.freqItems(Seq("a-b"), 0.1)
   *    freqPairs.select(explode($"a-b_freqItems").as("freq_ab")).show()
   *    +----------+
   *    |   freq_ab|
   *    +----------+
   *    |  [1,-1.0]|
   *    |   ...    |
   *    +----------+
   * </code></pre>
   * <p>
   * @since 1.4.0
   * @param support (undocumented)
   */
  public  org.apache.spark.sql.DataFrame freqItems (scala.collection.Seq<java.lang.String> cols, double support) { throw new RuntimeException(); }
  /**
   * (Scala-specific) Finding frequent items for columns, possibly with false positives. Using the
   * frequent element count algorithm described in
   * {@link http://dx.doi.org/10.1145/762471.762473, proposed by Karp, Schenker, and Papadimitriou}.
   * Uses a <code>default</code> support of 1%.
   * <p>
   * This function is meant for exploratory data analysis, as we make no guarantee about the
   * backward compatibility of the schema of the resulting {@link DataFrame}.
   * <p>
   * @param cols the names of the columns to search frequent items in.
   * @return A Local DataFrame with the Array of frequent items for each column.
   * <p>
   * @since 1.4.0
   */
  public  org.apache.spark.sql.DataFrame freqItems (scala.collection.Seq<java.lang.String> cols) { throw new RuntimeException(); }
  /**
   * Returns a stratified sample without replacement based on the fraction given on each stratum.
   * @param col column that defines strata
   * @param fractions sampling fraction for each stratum. If a stratum is not specified, we treat
   *                  its fraction as zero.
   * @param seed random seed
   * @tparam T stratum type
   * @return a new {@link DataFrame} that represents the stratified sample
   * <p>
   * <pre><code>
   *    val df = sqlContext.createDataFrame(Seq((1, 1), (1, 2), (2, 1), (2, 1), (2, 3), (3, 2),
   *      (3, 3))).toDF("key", "value")
   *    val fractions = Map(1 -&gt; 1.0, 3 -&gt; 0.5)
   *    df.stat.sampleBy("key", fractions, 36L).show()
   *    +---+-----+
   *    |key|value|
   *    +---+-----+
   *    |  1|    1|
   *    |  1|    2|
   *    |  3|    2|
   *    +---+-----+
   * </code></pre>
   * <p>
   * @since 1.5.0
   */
  public <T extends java.lang.Object> org.apache.spark.sql.DataFrame sampleBy (java.lang.String col, scala.collection.immutable.Map<T, java.lang.Object> fractions, long seed) { throw new RuntimeException(); }
  /**
   * Returns a stratified sample without replacement based on the fraction given on each stratum.
   * @param col column that defines strata
   * @param fractions sampling fraction for each stratum. If a stratum is not specified, we treat
   *                  its fraction as zero.
   * @param seed random seed
   * @tparam T stratum type
   * @return a new {@link DataFrame} that represents the stratified sample
   * <p>
   * @since 1.5.0
   */
  public <T extends java.lang.Object> org.apache.spark.sql.DataFrame sampleBy (java.lang.String col, java.util.Map<T, java.lang.Double> fractions, long seed) { throw new RuntimeException(); }
}
