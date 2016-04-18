package org.apache.spark.sql.execution;
/**
 * Provides support in a SQLContext for caching query results and automatically using these cached
 * results when subsequent queries are executed.  Data is cached using byte buffers stored in an
 * InMemoryRelation.  This relation is automatically substituted query plans that return the
 * <code>sameResult</code> as the originally cached query.
 * <p>
 * Internal to Spark SQL.
 */
  class CacheManager implements org.apache.spark.Logging {
  public   CacheManager (org.apache.spark.sql.SQLContext sqlContext) { throw new RuntimeException(); }
  private  scala.collection.mutable.ArrayBuffer<org.apache.spark.sql.execution.CachedData> cachedData () { throw new RuntimeException(); }
  private  java.util.concurrent.locks.ReentrantReadWriteLock cacheLock () { throw new RuntimeException(); }
  /** Returns true if the table is currently cached in-memory. */
  public  boolean isCached (java.lang.String tableName) { throw new RuntimeException(); }
  /** Caches the specified table in-memory. */
  public  void cacheTable (java.lang.String tableName) { throw new RuntimeException(); }
  /** Removes the specified table from the in-memory cache. */
  public  void uncacheTable (java.lang.String tableName) { throw new RuntimeException(); }
  /** Acquires a read lock on the cache for the duration of `f`. */
  private <A extends java.lang.Object> A readLock (scala.Function0<A> f) { throw new RuntimeException(); }
  /** Acquires a write lock on the cache for the duration of `f`. */
  private <A extends java.lang.Object> A writeLock (scala.Function0<A> f) { throw new RuntimeException(); }
  /** Clears all cached tables. */
    void clearCache () { throw new RuntimeException(); }
  /** Checks if the cache is empty. */
    boolean isEmpty () { throw new RuntimeException(); }
  /**
   * Caches the data produced by the logical representation of the given {@link DataFrame}. Unlike
   * <code>RDD.cache()</code>, the default storage level is set to be <code>MEMORY_AND_DISK</code> because recomputing
   * the in-memory columnar representation of the underlying table is expensive.
   * @param query (undocumented)
   * @param tableName (undocumented)
   * @param storageLevel (undocumented)
   */
    void cacheQuery (org.apache.spark.sql.DataFrame query, scala.Option<java.lang.String> tableName, org.apache.spark.storage.StorageLevel storageLevel) { throw new RuntimeException(); }
  /** Removes the data for the given {@link DataFrame} from the cache */
    void uncacheQuery (org.apache.spark.sql.DataFrame query, boolean blocking) { throw new RuntimeException(); }
  /** Tries to remove the data for the given {@link DataFrame} from the cache if it's cached */
    boolean tryUncacheQuery (org.apache.spark.sql.DataFrame query, boolean blocking) { throw new RuntimeException(); }
  /** Optionally returns cached data for the given {@link DataFrame} */
    scala.Option<org.apache.spark.sql.execution.CachedData> lookupCachedData (org.apache.spark.sql.DataFrame query) { throw new RuntimeException(); }
  /** Optionally returns cached data for the given LogicalPlan. */
    scala.Option<org.apache.spark.sql.execution.CachedData> lookupCachedData (org.apache.spark.sql.catalyst.plans.logical.LogicalPlan plan) { throw new RuntimeException(); }
  /** Replaces segments of the given logical plan with cached versions where possible. */
    org.apache.spark.sql.catalyst.plans.logical.LogicalPlan useCachedData (org.apache.spark.sql.catalyst.plans.logical.LogicalPlan plan) { throw new RuntimeException(); }
  /**
   * Invalidates the cache of any data that contains <code>plan</code>. Note that it is possible that this
   * function will over invalidate.
   * @param plan (undocumented)
   */
    void invalidateCache (org.apache.spark.sql.catalyst.plans.logical.LogicalPlan plan) { throw new RuntimeException(); }
}
