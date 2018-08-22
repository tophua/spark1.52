/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution

import java.util.concurrent.locks.ReentrantReadWriteLock

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.columnar.InMemoryRelation
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK

/** Holds a cached logical plan and its data
  * 保存缓存的逻辑计划及其数据*/
private[sql] case class CachedData(plan: LogicalPlan, cachedRepresentation: InMemoryRelation)

/**
 * Provides support in a SQLContext for caching query results and automatically using these cached
 * results when subsequent queries are executed.  Data is cached using byte buffers stored in an
 * InMemoryRelation.  This relation is automatically substituted query plans that return the
 * `sameResult` as the originally cached query.
  * 在Sql上下文中提供对缓存查询结果的支持,并在执行后续查询时自动使用这些缓存结果,
  * 使用存储在非内存关系中的字节缓冲区缓存数据,此关系是自动替换查询计划,将“sAsErthUT”返回为原始缓存的查询
 *
 * Internal to Spark SQL.
 */
private[sql] class CacheManager(sqlContext: SQLContext) extends Logging {

  @transient
  private val cachedData = new scala.collection.mutable.ArrayBuffer[CachedData]

  @transient
  private val cacheLock = new ReentrantReadWriteLock

  /** Returns true if the table is currently cached in-memory.
    * 如果表当前缓存在内存中,则返回true*/
  def isCached(tableName: String): Boolean = lookupCachedData(sqlContext.table(tableName)).nonEmpty

  /** Caches the specified table in-memory.
    * 在内存中缓存指定的表*/
  def cacheTable(tableName: String): Unit = cacheQuery(sqlContext.table(tableName), Some(tableName))

  /** Removes the specified table from the in-memory cache.
    * 从内存缓存中移除指定的表*/
  def uncacheTable(tableName: String): Unit = uncacheQuery(sqlContext.table(tableName))

  /** Acquires a read lock on the cache for the duration of `f`.
    * 在“f”的持续时间内,在缓存中获取读取锁*/
  private def readLock[A](f: => A): A = {
    val lock = cacheLock.readLock()
    lock.lock()
    try f finally {
      lock.unlock()
    }
  }

  /** Acquires a write lock on the cache for the duration of `f`.
    * 在“f”的持续时间内,在缓存中获取写锁*/
  private def writeLock[A](f: => A): A = {
    val lock = cacheLock.writeLock()
    lock.lock()
    try f finally {
      lock.unlock()
    }
  }

  /** Clears all cached tables. 清除所有缓存的表*/
  private[sql] def clearCache(): Unit = writeLock {
    cachedData.foreach(_.cachedRepresentation.cachedColumnBuffers.unpersist())
    cachedData.clear()
  }

  /** Checks if the cache is empty. 检查缓存是否为空*/
  private[sql] def isEmpty: Boolean = readLock {
    cachedData.isEmpty
  }

  /**
   * Caches the data produced by the logical representation of the given [[DataFrame]]. Unlike
   * `RDD.cache()`, the default storage level is set to be `MEMORY_AND_DISK` because recomputing
   * the in-memory columnar representation of the underlying table is expensive.
    * 缓存由给定[[DataFrame]]的逻辑表示产生的数据,与`RDD.cache（）`不同,
    * 默认存储级别设置为`MEMORY_AND_DISK`,因为重新计算基础表的内存中列表示是昂贵的
   */
  private[sql] def cacheQuery(
      query: DataFrame,
      tableName: Option[String] = None,
      storageLevel: StorageLevel = MEMORY_AND_DISK): Unit = writeLock {
    val planToCache = query.queryExecution.analyzed
    if (lookupCachedData(planToCache).nonEmpty) {
      logWarning("Asked to cache already cached data.")
    } else {
      cachedData +=
        CachedData(
          planToCache,
          InMemoryRelation(
            sqlContext.conf.useCompression,
            sqlContext.conf.columnBatchSize,
            storageLevel,
            sqlContext.executePlan(query.logicalPlan).executedPlan,
            tableName))
    }
  }

  /** Removes the data for the given [[DataFrame]] from the cache
    * 从缓存中删除给定[[DataFrame]]的数据*/
  private[sql] def uncacheQuery(query: DataFrame, blocking: Boolean = true): Unit = writeLock {
    val planToCache = query.queryExecution.analyzed
    val dataIndex = cachedData.indexWhere(cd => planToCache.sameResult(cd.plan))
    require(dataIndex >= 0, s"Table $query is not cached.")
    cachedData(dataIndex).cachedRepresentation.uncache(blocking)
    cachedData.remove(dataIndex)
  }

  /** Tries to remove the data for the given [[DataFrame]] from the cache if it's cached
    * 尝试从缓存中删除给定[[DataFrame]]的数据(如果它已缓存)*/
  private[sql] def tryUncacheQuery(
      query: DataFrame,
      blocking: Boolean = true): Boolean = writeLock {
    val planToCache = query.queryExecution.analyzed
    val dataIndex = cachedData.indexWhere(cd => planToCache.sameResult(cd.plan))
    val found = dataIndex >= 0
    if (found) {
      cachedData(dataIndex).cachedRepresentation.cachedColumnBuffers.unpersist(blocking)
      cachedData.remove(dataIndex)
    }
    found
  }

  /** Optionally returns cached data for the given [[DataFrame]]
    * （可选）返回给定[[DataFrame]]的缓存数据*/
  private[sql] def lookupCachedData(query: DataFrame): Option[CachedData] = readLock {
    lookupCachedData(query.queryExecution.analyzed)
  }

  /** Optionally returns cached data for the given LogicalPlan.(可选)返回给定LogicalPlan的缓存数据 */
  private[sql] def lookupCachedData(plan: LogicalPlan): Option[CachedData] = readLock {
    cachedData.find(cd => plan.sameResult(cd.plan))
  }

  /** Replaces segments of the given logical plan with cached versions where possible.
    * 尽可能使用缓存版本替换给定逻辑计划的段*/
  private[sql] def useCachedData(plan: LogicalPlan): LogicalPlan = {
    plan transformDown {
      case currentFragment =>
        lookupCachedData(currentFragment)
          .map(_.cachedRepresentation.withOutput(currentFragment.output))
          .getOrElse(currentFragment)
    }
  }

  /**
   * Invalidates the cache of any data that contains `plan`. Note that it is possible that this
   * function will over invalidate.
    *使包含`plan`的任何数据的缓存无效,请注意,此功能可能会失效
   */
  private[sql] def invalidateCache(plan: LogicalPlan): Unit = writeLock {
    cachedData.foreach {
      case data if data.plan.collect { case p if p.sameResult(plan) => p }.nonEmpty =>
        data.cachedRepresentation.recache()
      case _ =>
    }
  }
}
