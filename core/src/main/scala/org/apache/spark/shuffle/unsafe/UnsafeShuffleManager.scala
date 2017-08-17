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

package org.apache.spark.shuffle.unsafe

import java.util.Collections
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark._
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.sort.SortShuffleManager

/**
 * Subclass of [[BaseShuffleHandle]], used to identify when we've chosen to use the new shuffle.
  * [[BaseShuffleHandle]]的子类，用于识别何时选择使用新的shuffle。
 */
private[spark] class UnsafeShuffleHandle[K, V](
    shuffleId: Int,
    numMaps: Int,
    dependency: ShuffleDependency[K, V, V])
  extends BaseShuffleHandle(shuffleId, numMaps, dependency) {
}

private[spark] object UnsafeShuffleManager extends Logging {

  /**
   * The maximum number of shuffle output partitions that UnsafeShuffleManager supports.
    * nsafeShuffleManager支持的shuffle输出分区的最大数量。
   */
  val MAX_SHUFFLE_OUTPUT_PARTITIONS = PackedRecordPointer.MAXIMUM_PARTITION_ID + 1

  /**
   * Helper method for determining whether a shuffle should use the optimized unsafe shuffle
   * path or whether it should fall back to the original sort-based shuffle.
    * 用于确定shuffle应使用优化的不安全shuffle路径的辅助方法,或者是否应该回溯到原始的基于排序的shuffle。
   */
  def canUseUnsafeShuffle[K, V, C](dependency: ShuffleDependency[K, V, C]): Boolean = {
    val shufId = dependency.shuffleId
    val serializer = Serializer.getSerializer(dependency.serializer)
    if (!serializer.supportsRelocationOfSerializedObjects) {
      log.debug(s"Can't use UnsafeShuffle for shuffle $shufId because the serializer, " +
        s"${serializer.getClass.getName}, does not support object relocation")
      false
    } else if (dependency.aggregator.isDefined) {
      log.debug(s"Can't use UnsafeShuffle for shuffle $shufId because an aggregator is defined")
      false
    } else if (dependency.partitioner.numPartitions > MAX_SHUFFLE_OUTPUT_PARTITIONS) {
      log.debug(s"Can't use UnsafeShuffle for shuffle $shufId because it has more than " +
        s"$MAX_SHUFFLE_OUTPUT_PARTITIONS partitions")
      false
    } else {
      log.debug(s"Can use UnsafeShuffle for shuffle $shufId")
      true
    }
  }
}

/**
 * A shuffle implementation that uses directly-managed memory to implement several performance
 * optimizations for certain types of shuffles. In cases where the new performance optimizations
 * cannot be applied, this shuffle manager delegates to [[SortShuffleManager]] to handle those
 * shuffles.
 *
 * UnsafeShuffleManager's optimizations will apply when _all_ of the following conditions hold:
 *
 *  - The shuffle dependency specifies no aggregation or output ordering.
 *  - The shuffle serializer supports relocation of serialized values (this is currently supported
 *    by KryoSerializer and Spark SQL's custom serializers).
 *  - The shuffle produces fewer than 16777216 output partitions.
 *  - No individual record is larger than 128 MB when serialized.
 *
 * In addition, extra spill-merging optimizations are automatically applied when the shuffle
 * compression codec supports concatenation of serialized streams. This is currently supported by
 * Spark's LZF serializer.
 *
 * At a high-level, UnsafeShuffleManager's design is similar to Spark's existing SortShuffleManager.
 * In sort-based shuffle, incoming records are sorted according to their target partition ids, then
 * written to a single map output file. Reducers fetch contiguous regions of this file in order to
 * read their portion of the map output. In cases where the map output data is too large to fit in
 * memory, sorted subsets of the output can are spilled to disk and those on-disk files are merged
 * to produce the final output file.
 *
 * UnsafeShuffleManager optimizes this process in several ways:
 *
 *  - Its sort operates on serialized binary data rather than Java objects, which reduces memory
 *    consumption and GC overheads. This optimization requires the record serializer to have certain
 *    properties to allow serialized records to be re-ordered without requiring deserialization.
 *    See SPARK-4550, where this optimization was first proposed and implemented, for more details.
 *
 *  - It uses a specialized cache-efficient sorter ([[UnsafeShuffleExternalSorter]]) that sorts
 *    arrays of compressed record pointers and partition ids. By using only 8 bytes of space per
 *    record in the sorting array, this fits more of the array into cache.
 *
 *  - The spill merging procedure operates on blocks of serialized records that belong to the same
 *    partition and does not need to deserialize records during the merge.
 *
 *  - When the spill compression codec supports concatenation of compressed data, the spill merge
 *    simply concatenates the serialized and compressed spill partitions to produce the final output
 *    partition.  This allows efficient data copying methods, like NIO's `transferTo`, to be used
 *    and avoids the need to allocate decompression or copying buffers during the merge.
 *
 * For more details on UnsafeShuffleManager's design, see SPARK-7081.
 */
private[spark] class UnsafeShuffleManager(conf: SparkConf) extends ShuffleManager with Logging {
  //spark.shuffle.spill用于指定Shuffle过程中如果内存中的数据超过阈值(参考spark.shuffle.memoryFraction的设置),
  //那么是否需要将部分数据临时写入外部存储。如果设置为false，那么这个过程就会一直使用内,最后再合并到最终的Shuffle输出文件中去
  if (!conf.getBoolean("spark.shuffle.spill", true)) {
    logWarning(
      "spark.shuffle.spill was set to false, but this is ignored by the tungsten-sort shuffle " +
      "manager; its optimized shuffles will continue to spill to disk when necessary.")
  }

  private[this] val sortShuffleManager: SortShuffleManager = new SortShuffleManager(conf)
  private[this] val shufflesThatFellBackToSortShuffle =
    Collections.newSetFromMap(new ConcurrentHashMap[Int, java.lang.Boolean]())
  private[this] val numMapsForShufflesThatUsedNewPath = new ConcurrentHashMap[Int, Int]()

  /**
   * Register a shuffle with the manager and obtain a handle for it to pass to tasks.
    * 与manager注册一个洗牌，并获得一个句柄来传递给任务。
   */
  override def registerShuffle[K, V, C](
      shuffleId: Int,
      numMaps: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    if (UnsafeShuffleManager.canUseUnsafeShuffle(dependency)) {
      new UnsafeShuffleHandle[K, V](
        shuffleId, numMaps, dependency.asInstanceOf[ShuffleDependency[K, V, V]])
    } else {
      new BaseShuffleHandle(shuffleId, numMaps, dependency)
    }
  }

  /**
   * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive).
   * Called on executors by reduce tasks.
    * 获取一系列减少分区的读者（startPartition到endPartition-1，包括）。通过减少任务指定执行程序。
   */
  override def getReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext): ShuffleReader[K, C] = {
    sortShuffleManager.getReader(handle, startPartition, endPartition, context)
  }

  /** Get a writer for a given partition. Called on executors by map tasks.
    * 获取给定分区的writer。 通过Map任务调用执行器。 */
  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Int,//mapId对应RDD的partionsID
      context: TaskContext): ShuffleWriter[K, V] = {
    handle match {
      case unsafeShuffleHandle: UnsafeShuffleHandle[K, V] =>
        numMapsForShufflesThatUsedNewPath.putIfAbsent(handle.shuffleId, unsafeShuffleHandle.numMaps)
        val env = SparkEnv.get
        new UnsafeShuffleWriter(
          env.blockManager,
          shuffleBlockResolver.asInstanceOf[IndexShuffleBlockResolver],
          context.taskMemoryManager(),
          env.shuffleMemoryManager,
          unsafeShuffleHandle,
          mapId,//mapId对应RDD的partionsID
          context,
          env.conf)
      case other =>
        shufflesThatFellBackToSortShuffle.add(handle.shuffleId)
        //mapId对应RDD的partionsID
        sortShuffleManager.getWriter(handle, mapId, context)
    }
  }

  /** Remove a shuffle's metadata from the ShuffleManager.
    * 从ShuffleManager中删除shuffle的元数据*/
  override def unregisterShuffle(shuffleId: Int): Boolean = {
    if (shufflesThatFellBackToSortShuffle.remove(shuffleId)) {
      sortShuffleManager.unregisterShuffle(shuffleId)
    } else {
      Option(numMapsForShufflesThatUsedNewPath.remove(shuffleId)).foreach { numMaps =>
        (0 until numMaps).foreach { mapId => //mapId对应RDD的partionsID
          shuffleBlockResolver.removeDataByMap(shuffleId, mapId)
        }
      }
      true
    }
  }

  override val shuffleBlockResolver: IndexShuffleBlockResolver = {
    sortShuffleManager.shuffleBlockResolver
  }

  /** Shut down this ShuffleManager.
    * 关闭这个ShuffleManager */
  override def stop(): Unit = {
    sortShuffleManager.stop()
  }
}
