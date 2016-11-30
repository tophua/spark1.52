package org.apache.spark.sql.execution;
// no position
public  class SortPrefixUtils$ {
  /**
   * Static reference to the singleton instance of this Scala object.
   */
  public static final SortPrefixUtils$ MODULE$ = null;
  public   SortPrefixUtils$ () { throw new RuntimeException(); }
  public  org.apache.spark.util.collection.unsafe.sort.PrefixComparator getPrefixComparator (org.apache.spark.sql.catalyst.expressions.SortOrder sortOrder) { throw new RuntimeException(); }
  /**
   * Creates the prefix comparator for the first field in the given schema, in ascending order.
   * @param schema (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.util.collection.unsafe.sort.PrefixComparator getPrefixComparator (org.apache.spark.sql.types.StructType schema) { throw new RuntimeException(); }
  /**
   * Creates the prefix computer for the first field in the given schema, in ascending order.
   * @param schema (undocumented)
   * @return (undocumented)
   */
  public  org.apache.spark.sql.execution.UnsafeExternalRowSorter.PrefixComputer createPrefixGenerator (org.apache.spark.sql.types.StructType schema) { throw new RuntimeException(); }
}
