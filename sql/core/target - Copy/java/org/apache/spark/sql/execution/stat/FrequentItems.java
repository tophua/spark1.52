package org.apache.spark.sql.execution.stat;
// no position
  class FrequentItems implements org.apache.spark.Logging {
  /** A helper class wrapping `MutableMap[Any, Long]` for simplicity. */
  static private  class FreqItemCounter implements scala.Serializable {
    public   FreqItemCounter (int size) { throw new RuntimeException(); }
    public  scala.collection.mutable.Map<java.lang.Object, java.lang.Object> baseMap () { throw new RuntimeException(); }
    /**
     * Add a new example to the counts if it exists, otherwise deduct the count
     * from existing items.
     * @param key (undocumented)
     * @param count (undocumented)
     * @return (undocumented)
     */
    public  org.apache.spark.sql.execution.stat.FrequentItems.FreqItemCounter add (Object key, long count) { throw new RuntimeException(); }
    /**
     * Merge two maps of counts.
     * @param other The map containing the counts for that partition
     * @return (undocumented)
     */
    public  org.apache.spark.sql.execution.stat.FrequentItems.FreqItemCounter merge (org.apache.spark.sql.execution.stat.FrequentItems.FreqItemCounter other) { throw new RuntimeException(); }
  }
  /**
   * Finding frequent items for columns, possibly with false positives. Using the
   * frequent element count algorithm described in
   * {@link http://dx.doi.org/10.1145/762471.762473, proposed by Karp, Schenker, and Papadimitriou}.
   * The <code>support</code> should be greater than 1e-4.
   * For Internal use only.
   * <p>
   * @param df The input DataFrame
   * @param cols the names of the columns to search frequent items in
   * @param support The minimum frequency for an item to be considered <code>frequent</code>. Should be greater
   *                than 1e-4.
   * @return A Local DataFrame with the Array of frequent items for each column.
   */
  static   org.apache.spark.sql.DataFrame singlePassFreqItems (org.apache.spark.sql.DataFrame df, scala.collection.Seq<java.lang.String> cols, double support) { throw new RuntimeException(); }
}
