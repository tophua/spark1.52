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

package org.apache.spark

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.storage._

/**
 * Spark class responsible for passing RDDs partition contents to the BlockManager and making
 * sure a node doesn't load two copies of an RDD at once.
 * CacheManager 缓存管理用于缓存RDD某个分区计算后的中间结果,缓存计算结果发生在迭代计算的时候
 * cacheManager负责调用BlockManager来管理RDD的缓存,如果当前RDD原来计算过并且把结果缓存起来.
 * 接下来的运行都可以通过BlockManager来直接读取缓存后返回
  */
private[spark] class CacheManager(blockManager: BlockManager) extends Logging {

  /** Keys of RDD partitions that are being computed/loaded.
    * 正在计算/加载的RDD分区的密钥
    * */
  private val loading = new mutable.HashSet[RDDBlockId]

  /** Gets or computes an RDD partition. Used by RDD.iterator() when an RDD is cached. 
   *  获取或计算一个RDD的分区  ,当RDD被缓存时由RDD.iterator（）使用。
   *  */
  def getOrCompute[T](
      rdd: RDD[T],
      partition: Partition,
      context: TaskContext,
      storageLevel: StorageLevel): Iterator[T] = {

    val key = RDDBlockId(rdd.id, partition.index)//获取RDD的BlockID,RDDBlockId扩展BlockID类
    logDebug(s"Looking for partition $key")
    blockManager.get(key) match {//向BlockManager查询是否有缓存,如果有将它封装为InterruptibleIterator并返回
      case Some(blockResult) =>
        //val blockResult: BlockResult
        //缓存命中,更新统计信息,将缓存作为结果返回
        // Partition is already materialized, so just return its values
        //分区已经被物化,所以才返回其值
        val existingMetrics = context.taskMetrics
          .getInputMetricsForReadMethod(blockResult.readMethod)
        existingMetrics.incBytesRead(blockResult.bytes)

        val iter = blockResult.data.asInstanceOf[Iterator[T]]
        //可中断迭代器
        new InterruptibleIterator[T](context, iter) {
          override def next(): T = {
            existingMetrics.incRecordsRead(1)
            delegate.next()
          }
        }
      case None =>  //没有缓存命中,需要重新计算或从CheckPion中获取数据,并调用putInBlockManager方法将数据写入
        //缓存后封装为InterruptibleIterator并返回
        // Acquire a lock for loading this partition
        // If another thread already holds the lock, wait for it to finish return its results
        //判断当前是否有线程在处理当前partition,如果有那么等待它结束后,直接从BlockManager中读取处理结果数据
        //如果没有线程在计算,那么storedvalue就是none,否则就是计算结果
        val storedValues = acquireLockForPartition[T](key)
        if (storedValues.isDefined) {//已经被其他线程处理了,直接返回计算结果
          return new InterruptibleIterator[T](context, storedValues.get)
        }

        // Otherwise, we have to load the partition ourselves
        //否则,我们必须自己加载分区
        //需要计算
        try {
          logInfo(s"Partition $key not found, computing it")
          //如果被Checkpoint过,那么读取Checkpoint的数据,否则调用RDD的compute开始计算
          val computedValues = rdd.computeOrReadCheckpoint(partition, context)
          // Task是在Drver端执行的话不需要缓存结果,这个主要是为了first()或者take()
          //这种仅仅有一个执行的任务的快速执行,这类任务由于没有Shuflle阶段,直接运行Driver端可以可能更省时间
          // If the task is running locally, do not persist the result
          if (context.isRunningLocally) {
            return computedValues
          }

          // Otherwise, cache the values and keep track of any updates in block statuses
          //否则,缓存值并跟踪块状态中的任何更新
          val updatedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]
         //将数据结果写缓存到BlockManager
          val cachedValues = putInBlockManager(key, computedValues, storageLevel, updatedBlocks)
          //更新任务的统计信息
          val metrics = context.taskMetrics
          val lastUpdatedBlocks = metrics.updatedBlocks.getOrElse(Seq[(BlockId, BlockStatus)]())
          metrics.updatedBlocks = Some(lastUpdatedBlocks ++ updatedBlocks.toSeq)
          //最后封装InterruptibleIterator返回
          new InterruptibleIterator(context, cachedValues)

        } finally {
          loading.synchronized {
            loading.remove(key)
            loading.notifyAll()
          }
        }
    }
  }

  /**
   * Acquire a loading lock for the partition identified by the given block ID.
   *  判断当前是否有线程在处理当前partition,如果有那么等待它结束后,直接从BlockManager中读取处理结果数据
                 如果没有线程在计算,那么storedvalue就是none,否则就是计算结果
   * If the lock is free, just acquire it and return None. Otherwise, another thread is already
   * loading the partition, so we wait for it to finish and return the values loaded by the thread.
    * 如果锁是免费的,只需获取它并返回None。 否则,另一个线程已经加载分区,所以我们等待它完成并返回线程加载的值。
   */
  private def acquireLockForPartition[T](id: RDDBlockId): Option[Iterator[T]] = {
    loading.synchronized {
      if (!loading.contains(id)) {
        // If the partition is free, acquire its lock to compute its value
        //如果分区是空闲的,获取其锁来计算其值
        loading.add(id)
        None
      } else {
        // Otherwise, wait for another thread to finish and return its result
        //否则,等待另一个线程完成并返回其结果
        logInfo(s"Another thread is loading $id, waiting for it to finish...")
        while (loading.contains(id)) {
          try {
            loading.wait()
          } catch {
            case e: Exception =>
              logWarning(s"Exception while waiting for another thread to load $id", e)
          }
        }
        logInfo(s"Finished waiting for $id")
        val values = blockManager.get(id)
        if (!values.isDefined) {
          /* The block is not guaranteed to exist even after the other thread has finished.
           * For instance, the block could be evicted after it was put, but before our get.
           * In this case, we still need to load the partition ourselves. */
          /**即使在另一个线程完成之后，块也不能保证存在,例如，该块可以在被放置之后，但在我们得到之前被驱逐,
          *在这种情况下，我们仍然需要自己加载分区。*/
          logInfo(s"Whoever was loading $id failed; we'll try it ourselves")
          loading.add(id)
        }
        values.map(_.data.asInstanceOf[Iterator[T]])
      }
    }
  }

  /**
   * Cache the values of a partition, keeping track of any updates in the storage statuses of
   * other blocks along the way.
   * 缓存分区的值,保持跟踪在其他分区块存储状态更新。
   * The effective storage level refers to the level that actually specifies BlockManager put
   * behavior, not the level originally specified by the user. This is mainly for forcing a
   * MEMORY_AND_DISK partition to disk if there is not enough room to unroll the partition,
   * while preserving the the original semantics of the RDD as specified by the application.
   * 有效的存储级别是指实际指定BlockManager放置行为的级别,而不是用户最初指定的级别。
    * 如果没有足够的空间展开分区,同时保留应用程序指定的RDD的原始语义,则主要用于将MEMORY_AND_DISK分区强制到磁盘。
   */
  private def putInBlockManager[T](
      key: BlockId,
      values: Iterator[T],
      level: StorageLevel,
      updatedBlocks: ArrayBuffer[(BlockId, BlockStatus)],
      effectiveStorageLevel: Option[StorageLevel] = None): Iterator[T] = {
    //获取实际的存储级别
    val putLevel = effectiveStorageLevel.getOrElse(level)
    //存储级别,不充许使用内存
    if (!putLevel.useMemory) {
      /*
       * This RDD is not to be cached in memory, so we can just pass the computed values as an
       * iterator directly to the BlockManager rather than first fully unrolling it in memory.
       * 这盘是不被缓存在内存中,所以我们可以通过计算值为迭代器而不是直接向blockmanager第一完全展开它的记忆。
       */
      //数据直接写入磁盘
      updatedBlocks ++=
        blockManager.putIterator(key, values, level, tellMaster = true, effectiveStorageLevel)
      blockManager.get(key) match {
        case Some(v) => v.data.asInstanceOf[Iterator[T]]
        case None =>
          logInfo(s"Failure to store $key")
          throw new BlockException(key, s"Block manager failed to return cached value for $key!")
      }
    } else {
      /*
       * This RDD is to be cached in memory. In this case we cannot pass the computed values
       * to the BlockManager as an iterator and expect to read it back later. This is because
       * we may end up dropping a partition from memory store before getting it back.
       *这盘是被缓存在内存中,在这种情况下,我们无法通过计算值的blockmanager为迭代器和期待读回来以后。
       * 这是因为在返回内存之前,我们可能会从内存存储区中删除一个分区。
       * In addition, we must be careful to not unroll the entire partition in memory at once.
       * Otherwise, we may cause an OOM exception if the JVM does not have enough space for this
       * single partition. Instead, we unroll the values cautiously, potentially aborting and
       * dropping the partition to disk if applicable.
       * 此外,我们必须小心不要把整个分区内存在一次,否则,我们可能如果JVM没有该单分区有足够的空间导致OOM异常。
       * 相反，我们把价值观谨慎，可能中止和下降分区磁盘如果适用。
       */
      //如果存储级别充许使用内存,那么首先尝试展开
      blockManager.memoryStore.unrollSafely(key, values, updatedBlocks) match {
        case Left(arr) =>
          //有足够内存可以存储数据
          // We have successfully unrolled the entire partition, so cache it in memory
          //我们已经成功地把整个分区，所以缓存在内存中
          updatedBlocks ++=
            blockManager.putArray(key, arr, level, tellMaster = true, effectiveStorageLevel)
          arr.iterator.asInstanceOf[Iterator[T]]
        case Right(it) =>
          //如果展开数据失败,则将数据存入磁盘
          // There is not enough space to cache this partition in memory
          //内存不足,无法将此分区缓存在内存中。
          val returnValues = it.asInstanceOf[Iterator[T]]
          if (putLevel.useDisk) {
            logWarning(s"Persisting partition $key to disk instead.")
            val diskOnlyLevel = StorageLevel(useDisk = true, useMemory = false,
              useOffHeap = false, deserialized = false, putLevel.replication)
            putInBlockManager[T](key, returnValues, level, updatedBlocks, Some(diskOnlyLevel))
          } else {
            returnValues
          }
      }
    }
  }

}
