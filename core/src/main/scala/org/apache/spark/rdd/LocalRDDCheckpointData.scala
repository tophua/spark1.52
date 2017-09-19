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

package org.apache.spark.rdd

import scala.reflect.ClassTag

import org.apache.spark.{Logging, SparkEnv, SparkException, TaskContext}
import org.apache.spark.storage.{RDDBlockId, StorageLevel}
import org.apache.spark.util.Utils

/**
 * An implementation of checkpointing implemented on top of Spark's caching layer.
 *在Spark的缓存层之上实现的检查点实现
  *
 * Local checkpointing trades off fault tolerance for performance by skipping the expensive
 * step of saving the RDD data to a reliable and fault-tolerant storage. Instead, the data
 * is written to the local, ephemeral block storage that lives in each executor. This is useful
 * for use cases where RDDs build up long lineages that need to be truncated often (e.g. GraphX).
  * 本地检查点通过跳过将RDD数据保存到可靠且容错的存储器的昂贵步骤来消除性能容错,
  * 而是将数据写入驻留在每个执行器中的本地临时块存储,这对于RDD构建需要经常截断的长谱系（例如GraphX）的用例很有用。
 */
private[spark] class LocalRDDCheckpointData[T: ClassTag](@transient rdd: RDD[T])
  extends RDDCheckpointData[T](rdd) with Logging {

  /**
   * Ensure the RDD is fully cached so the partitions can be recovered later.
   * 确保RDD是缓存的分区恢复
   */
  protected override def doCheckpoint(): CheckpointRDD[T] = {
    val level = rdd.getStorageLevel

    // Assume storage level uses disk; otherwise memory eviction may cause data loss
    //假设存储级别使用磁盘,否则记忆驱逐可能会导致数据丢失
    assume(level.useDisk, s"Storage level $level is not appropriate for local checkpointing")

    // Not all actions compute all partitions of the RDD (e.g. take). For correctness, we
    //并非所有操作都会计算RDD的所有分区(例如,采取),为了正确,我们
    // must cache any missing partitions. TODO: avoid running another job here (SPARK-8582).
    //缓存丢失分区
    val action = (tc: TaskContext, iterator: Iterator[T]) => Utils.getIteratorSize(iterator)
    val missingPartitionIndices = rdd.partitions.map(_.index).filter { i =>
      !SparkEnv.get.blockManager.master.contains(RDDBlockId(rdd.id, i))
    }
    if (missingPartitionIndices.nonEmpty) {
      rdd.sparkContext.runJob(rdd, action, missingPartitionIndices)
    }

    new LocalCheckpointRDD[T](rdd)
  }

}

private[spark] object LocalRDDCheckpointData {

  val DEFAULT_STORAGE_LEVEL = StorageLevel.MEMORY_AND_DISK

  /**
   * Transform the specified storage level to one that uses disk.
   * 将指定的存储级别转换为磁盘,这保证了RDD可以进行多次正确重新计算,只要执行者不失败
   * This guarantees that the RDD can be recomputed multiple times correctly as long as
   * executors do not fail. Otherwise, if the RDD is cached in memory only, for instance,
   * the checkpoint data will be lost if the relevant block is evicted from memory.
   *
    * 只要执行器不失败，就可以正确地重新计算RDD。否则,如果RDD仅缓存在内存中,
    * 例如，如果相关块从内存中逐出，则检查点数据将丢失。
   * This method is idempotent.这种方法是幂等的
   */
  def transformStorageLevel(level: StorageLevel): StorageLevel = {
    // If this RDD is to be cached off-heap, fail fast since we cannot provide any
    // correctness guarantees about subsequent computations after the first one
    //如果这个RDD要被堆栈缓存,那么快速失败,因为我们不能在第一个之后提供关于后续计算的任何正确性保证
    if (level.useOffHeap) {
      throw new SparkException("Local checkpointing is not compatible with off-heap caching.")
    }

    StorageLevel(useDisk = true, level.useMemory, level.deserialized, level.replication)
  }
}
