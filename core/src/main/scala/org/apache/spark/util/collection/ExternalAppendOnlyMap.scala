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

package org.apache.spark.util.collection

import java.io._
import java.util.Comparator

import scala.collection.BufferedIterator
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.google.common.io.ByteStreams

import org.apache.spark.{Logging, SparkEnv, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.serializer.{DeserializationStream, Serializer}
import org.apache.spark.storage.{BlockId, BlockManager}
import org.apache.spark.util.collection.ExternalAppendOnlyMap.HashComparator
import org.apache.spark.executor.ShuffleWriteMetrics

/**
 * :: DeveloperApi ::
 * An append-only map that spills sorted content to disk when there is insufficient space for it
  * to grow. *
  * 一个附加的Map,如果没有足够的空间来将分拣内容溢出到磁盘。
 * 主要负责做聚合
 * This map takes two passes over the data:
  * 该Map需要两次传递数据：
 *
 *   (1) Values are merged into combiners, which are sorted and spilled to disk as necessary
  *       值被合并到组合器中,根据需要将它们排序并分散到磁盘中
 *   (2) Combiners are read from disk and merged together,组合器从磁盘读取并合并在一起
 *
 * The setting of the spill threshold faces the following trade-off: If the spill threshold is
 * too high, the in-memory map may occupy more memory than is available, resulting in OOM.
 * However, if the spill threshold is too low, we spill frequently and incur unnecessary disk
 * writes. This may lead to a performance regression compared to the normal case of using the
 * non-spilling AppendOnlyMap.
 *  泄漏阈值的设置面临以下折衷：如果溢出阈值过高,则内存中映射可能会占用比可用内存更多的内存,导致OOM。
  * 但是,如果溢出阈值太低,我们会溢出经常发生不必要的磁盘写入。
  * 与使用不溢出的AppendOnlyMap的正常情况相比,这可能会导致性能回归。
 * Two parameters control the memory threshold:
 *  两个参数控制内存阈值：
 *   `spark.shuffle.memoryFraction` specifies the collective amount of memory used for storing
 *   these maps as a fraction of the executor's total memory. Since each concurrently running
 *   task maintains one map, the actual threshold for each map is this quantity divided by the
 *   number of running tasks.
 *  `spark.shuffle.memoryFraction`指定用于将这些映射存储为执行程序总内存的一部分的集合内存量。
  *  由于每个同时运行的任务维护一个map，每个map的实际阈值是该数量除以运行任务的数量。
  *
 *   `spark.shuffle.safetyFraction` specifies an additional margin of safety as a fraction of
 *   this threshold, in case map size estimation is not sufficiently accurate.
  *   `spark.shuffle.safetyFraction`指定额外的安全边际,作为该阈值的一小部分,以防map尺寸估计不够准确。
  *
  *   内存＋磁盘使用的是ExternalAppendOnlyMap,如果内存空间不足时,ExternalAppendOnlyMap可以将 \ records 进行 sort 后 spill 到磁盘上,
  *   等到需要它们的时候再进行归并
 */
@DeveloperApi
class ExternalAppendOnlyMap[K, V, C](
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C,
    serializer: Serializer = SparkEnv.get.serializer,
    blockManager: BlockManager = SparkEnv.get.blockManager)
  extends Iterable[(K, C)]
  with Serializable
  with Logging
  with Spillable[SizeTracker] {

  private var currentMap = new SizeTrackingAppendOnlyMap[K, C]
  private val spilledMaps = new ArrayBuffer[DiskMapIterator]
  private val sparkConf = SparkEnv.get.conf
  private val diskBlockManager = blockManager.diskBlockManager

  /**
   * Size of object batches when reading/writing from serializers.
   * 从串行器读取/写入时对象批次的大小
   * Objects are written in batches, with each batch using its own serialization stream. This
   * cuts down on the size of reference-tracking maps constructed when deserializing a stream.
   *  对象是批量编写的,每个批次使用自己的序列化流, 这减少了反序列化流时构建的引用跟踪映射的大小
   * NOTE: Setting this too low can cause excessive copying when serializing, since some serializers
   * grow internal data structures by growing + copying every time the number of objects doubles.
    * 注意：设置太低可能会在序列化时导致过多的复制，因为一些序列化程序通过在每次对象数量加倍时增加+复制来增长内部数据结构。
   */
  private val serializerBatchSize = sparkConf.getLong("spark.shuffle.spill.batchSize", 10000)

  // Number of bytes spilled in total
  //总共溢出的字节数
  private var _diskBytesSpilled = 0L
  def diskBytesSpilled: Long = _diskBytesSpilled

  // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
  //如果没有提供单位,请使用getSizeAsKb（而不是字节）来保持向后兼容性
  // 在ShuffleMapTask端通常也会增大Map任务的写磁盘的缓存,默认情况下是32K
  private val fileBufferSize =
    sparkConf.getSizeAsKb("spark.shuffle.file.buffer", "32k").toInt * 1024

  // Write metrics for current spill
  //编写当前溢出的指标
  private var curWriteMetrics: ShuffleWriteMetrics = _

  // Peak size of the in-memory map observed so far, in bytes
  //到目前为止观察到的内存中映射的峰值大小（以字节为单位）
  private var _peakMemoryUsedBytes: Long = 0L
  def peakMemoryUsedBytes: Long = _peakMemoryUsedBytes

  private val keyComparator = new HashComparator[K]
  private val ser = serializer.newInstance()

  /**
   * Insert the given key and value into the map.
    * 将给定的键和值插入到Map
   */
  def insert(key: K, value: V): Unit = {
    insertAll(Iterator((key, value)))
  }

  /**
   * Insert the given iterator of keys and values into the map.
    * 将键和值的给定迭代器插入到MAP
   *
   * When the underlying map needs to grow, check if the global pool of shuffle memory has
   * enough room for this to happen. If so, allocate the memory required to grow the map;
   * otherwise, spill the in-memory map to disk.
   *当底层地图需要增长时，请检查全局洗牌记忆库是否有足够的空间来实现。
    * 如果是这样，分配增长地图所需的内存; 否则，将内存中的地图溢出到磁盘。
   * The shuffle memory usage of the first trackMemoryThreshold entries is not tracked.
    * 未跟踪第一个trackMemoryThreshold条目的随机内存使用情况
   */
  def insertAll(entries: Iterator[Product2[K, V]]): Unit = {
    // An update function for the map that we reuse across entries to avoid allocating
    // a new closure each time
    //我们在条目之间重用的Map的更新功能,以避免每次分配一个新的关闭
    var curEntry: Product2[K, V] = null
    val update: (Boolean, C) => C = (hadVal, oldVal) => {
      //聚合运算
      if (hadVal) mergeValue(oldVal, curEntry._2) else createCombiner(curEntry._2)
    }

    while (entries.hasNext) {
      curEntry = entries.next()
      val estimatedSize = currentMap.estimateSize()
      if (estimatedSize > _peakMemoryUsedBytes) {
        _peakMemoryUsedBytes = estimatedSize
      }
      if (maybeSpill(currentMap, estimatedSize)) {
        currentMap = new SizeTrackingAppendOnlyMap[K, C]
      }
      currentMap.changeValue(curEntry._1, update)
      addElementsRead()
    }
  }

  /**
   * Insert the given iterable of keys and values into the map.
   * 将给定的键和值迭代插入到Map中。
   * When the underlying map needs to grow, check if the global pool of shuffle memory has
   * enough room for this to happen. If so, allocate the memory required to grow the map;
   * otherwise, spill the in-memory map to disk.
   *当底层Map需要增长时，请检查全局洗牌记忆库是否有足够的空间来实现。
    *  如果是这样，分配增长Map所需的内存;否则，将内存映射溢出到磁盘。
   * The shuffle memory usage of the first trackMemoryThreshold entries is not tracked.
   */
  def insertAll(entries: Iterable[Product2[K, V]]): Unit = {
    insertAll(entries.iterator)
  }

  /**
   * Sort the existing contents of the in-memory map and spill them to a temporary file on disk.
   * 对内存中映像的现有内容进行排序，并将其溢出到磁盘上的临时文件。,内存溢出操作
   */
  override protected[this] def spill(collection: SizeTracker): Unit = {
    val (blockId, file) = diskBlockManager.createTempLocalBlock()
    curWriteMetrics = new ShuffleWriteMetrics()
    var writer = blockManager.getDiskWriter(blockId, file, ser, fileBufferSize, curWriteMetrics)
    var objectsWritten = 0

    // List of batch sizes (bytes) in the order they are written to disk
    //按照写入磁盘的顺序列出批量大小（字节）
    val batchSizes = new ArrayBuffer[Long]

    // Flush the disk writer's contents to disk, and update relevant variables
    //将磁盘写入程序的内容刷新到磁盘，并更新相关变量
    def flush(): Unit = {
      val w = writer
      writer = null
      w.commitAndClose()
      _diskBytesSpilled += curWriteMetrics.shuffleBytesWritten
      batchSizes.append(curWriteMetrics.shuffleBytesWritten)
      objectsWritten = 0
    }

    var success = false
    try {
      val it = currentMap.destructiveSortedIterator(keyComparator)
      while (it.hasNext) {
        val kv = it.next()
        writer.write(kv._1, kv._2)
        objectsWritten += 1

        if (objectsWritten == serializerBatchSize) {
          flush()
          curWriteMetrics = new ShuffleWriteMetrics()
          /**
            * BlockManager会运行在Driver和每个Executor上,
            * 而运行在Driver上的BlockManger负责整个Job的Block的管理工作；
            * 运行在Executor上的BlockManger负责管理该Executor上的Block,并且向Driver的BlockManager汇报Block的信息和接收来自它的命令
            */
          writer = blockManager.getDiskWriter(blockId, file, ser, fileBufferSize, curWriteMetrics)
        }
      }
      if (objectsWritten > 0) {
        flush()
      } else if (writer != null) {
        val w = writer
        writer = null
        w.revertPartialWritesAndClose()
      }
      success = true
    } finally {
      if (!success) {
        // This code path only happens if an exception was thrown above before we set success;
        // close our stuff and let the exception be thrown further
        //只有在设置成功之前抛出异常时,才会发生此代码路径;关闭我们的东西并让异常进一步抛出
        if (writer != null) {
          writer.revertPartialWritesAndClose()
        }
        if (file.exists()) {
          file.delete()
        }
      }
    }

    spilledMaps.append(new DiskMapIterator(file, blockId, batchSizes))
  }

  /**
   * Return an iterator that merges the in-memory map with the spilled maps.
   * If no spill has occurred, simply return the in-memory map's iterator.
    * 返回一个迭代器,将内存映射与溢出的映射合并。如果没有溢出,只需返回内存映射的迭代器。
   */
  override def iterator: Iterator[(K, C)] = {
    if (spilledMaps.isEmpty) {
      currentMap.iterator
    } else {
      new ExternalIterator()
    }
  }

  /**
   * An iterator that sort-merges (K, C) pairs from the in-memory map and the spilled maps
    * 从存储器内映射和溢出映射中对（K，C）对进行排序合并的迭代器
   */
  private class ExternalIterator extends Iterator[(K, C)] {

    // A queue that maintains a buffer for each stream we are currently merging
    // This queue maintains the invariant that it only contains non-empty buffers
    //为我们当前合并的每个流维护一个缓冲区的队列此队列维护不变量,它只包含非空缓冲区
    private val mergeHeap = new mutable.PriorityQueue[StreamBuffer]

    // Input streams are derived both from the in-memory map and spilled maps on disk
    // The in-memory map is sorted in place, while the spilled maps are already in sorted order
    //输入流是从存储器内映射和磁盘上的溢出映射导出的,内存中映射是按原样进行排序的,而溢出的映射已经按排序顺序排列
    private val sortedMap = currentMap.destructiveSortedIterator(keyComparator)
    private val inputStreams = (Seq(sortedMap) ++ spilledMaps).map(it => it.buffered)

    inputStreams.foreach { it =>
      val kcPairs = new ArrayBuffer[(K, C)]
      readNextHashCode(it, kcPairs)
      if (kcPairs.length > 0) {
        mergeHeap.enqueue(new StreamBuffer(it, kcPairs))
      }
    }

    /**
     * Fill a buffer with the next set of keys with the same hash code from a given iterator. We
     * read streams one hash code at a time to ensure we don't miss elements when they are merged.
     * 使用来自给定迭代器的相同哈希码的下一组密钥填充缓冲区,我们一次读取流一个哈希码,以确保我们不会在元素合并时遗漏元素。
     * Assumes the given iterator is in sorted order of hash code.
     * 假设给定的迭代器按照哈希码的排序顺序。
     * @param it iterator to read from
     * @param buf buffer to write the results into
     */
    private def readNextHashCode(it: BufferedIterator[(K, C)], buf: ArrayBuffer[(K, C)]): Unit = {
      if (it.hasNext) {
        var kc = it.next()
        buf += kc
        val minHash = hashKey(kc)
        while (it.hasNext && it.head._1.hashCode() == minHash) {
          kc = it.next()
          buf += kc
        }
      }
    }

    /**
     * If the given buffer contains a value for the given key, merge that value into
     * baseCombiner and remove the corresponding (K, C) pair from the buffer.
      * 如果给定的缓冲区包含给定键的值，则将该值合并到baseCombiner中，并从缓冲区中删除相应的（K，C）对
     */
    private def mergeIfKeyExists(key: K, baseCombiner: C, buffer: StreamBuffer): C = {
      var i = 0
      while (i < buffer.pairs.length) {
        val pair = buffer.pairs(i)
        if (pair._1 == key) {
          // Note that there's at most one pair in the buffer with a given key, since we always
          // merge stuff in a map before spilling, so it's safe to return after the first we find
          //请注意，缓冲区中最多有一对给定的键，因为我们总是在溢出之前将图形合并在一个Map中，所以在我们首先找到之后可以安全地返回
          removeFromBuffer(buffer.pairs, i)
          return mergeCombiners(baseCombiner, pair._2)
        }
        i += 1
      }
      baseCombiner
    }

    /**
     * Remove the index'th element from an ArrayBuffer in constant time, swapping another element
     * into its place. This is more efficient than the ArrayBuffer.remove method because it does
     * not have to shift all the elements in the array over. It works for our array buffers because
     * we don't care about the order of elements inside, we just want to search them for a key.
      * 在一个恒定的时间内从ArrayBuffer中删除索引的元素，将另一个元素交换到它的位置。
      * 这比ArrayBuffer.remove方法更有效，因为它不必将数组中的所有元素移动。
      * 它适用于我们的数组缓冲区，因为我们不关心元素的顺序，我们只想搜索一个键。
     */
    private def removeFromBuffer[T](buffer: ArrayBuffer[T], index: Int): T = {
      val elem = buffer(index)
      //如果index == buffer.size - 1，这也可以
      buffer(index) = buffer(buffer.size - 1)  // This also works if index == buffer.size - 1
      buffer.reduceToSize(buffer.size - 1)
      elem
    }

    /**
     * Return true if there exists an input stream that still has unvisited pairs.
      * 如果存在仍然有未访问对的输入流,返回true
     */
    override def hasNext: Boolean = mergeHeap.length > 0

    /**
     * Select a key with the minimum hash, then combine all values with the same key from all
     * input streams.
      * 选择具有最小散列值的键，然后将所有值与所有输入流中相同的键组合
     */
    override def next(): (K, C) = {
      if (mergeHeap.length == 0) {
        throw new NoSuchElementException
      }
      // Select a key from the StreamBuffer that holds the lowest key hash
      //从包含最低密钥散列的StreamBuffer中选择一个密钥
      val minBuffer = mergeHeap.dequeue()
      val minPairs = minBuffer.pairs
      val minHash = minBuffer.minKeyHash
      val minPair = removeFromBuffer(minPairs, 0)
      val minKey = minPair._1
      var minCombiner = minPair._2
      assert(hashKey(minPair) == minHash)

      // For all other streams that may have this key (i.e. have the same minimum key hash),
      // merge in the corresponding value (if any) from that stream
      //对于可能具有该密钥（即具有相同的最小密钥哈希）的所有其他流，合并来自该流的相应值（如果有的话）
      val mergedBuffers = ArrayBuffer[StreamBuffer](minBuffer)
      while (mergeHeap.length > 0 && mergeHeap.head.minKeyHash == minHash) {
        val newBuffer = mergeHeap.dequeue()
        minCombiner = mergeIfKeyExists(minKey, minCombiner, newBuffer)
        mergedBuffers += newBuffer
      }

      // Repopulate each visited stream buffer and add it back to the queue if it is non-empty
      //重新填充每个访问流缓冲区，如果它不是空，则将其添加回队列
      mergedBuffers.foreach { buffer =>
        if (buffer.isEmpty) {
          readNextHashCode(buffer.iterator, buffer.pairs)
        }
        if (!buffer.isEmpty) {
          mergeHeap.enqueue(buffer)
        }
      }

      (minKey, minCombiner)
    }

    /**
     * A buffer for streaming from a map iterator (in-memory or on-disk) sorted by key hash.
     * Each buffer maintains all of the key-value pairs with what is currently the lowest hash
     * code among keys in the stream. There may be multiple keys if there are hash collisions.
     * Note that because when we spill data out, we only spill one value for each key, there is
     * at most one element for each key.
      * 用于通过键hash.Each缓冲区排序的映射迭代器（内存或磁盘）流的缓冲区。每个缓冲区将所有密钥值对与当前流中密钥中最低的哈希码保持一致。
      * 如果有哈希冲突，可能会有多个键, 请注意,因为当我们溢出数据时,我们只会为每个密钥溢出一个值,每个密钥最多只有一个元素。
     *
     * StreamBuffers are ordered by the minimum key hash currently available in their stream so
     * that we can put them into a heap and sort that.
      * StreamBuffers按照流中当前可用的最小密钥哈希排序，以便我们可以将它们放入堆并进行排序
     */
    private class StreamBuffer(
        val iterator: BufferedIterator[(K, C)],
        val pairs: ArrayBuffer[(K, C)])
      extends Comparable[StreamBuffer] {

      def isEmpty: Boolean = pairs.length == 0

      // Invalid if there are no more pairs in this stream
      //如果此流中没有更多对,则无效
      def minKeyHash: Int = {
        assert(pairs.length > 0)
        hashKey(pairs.head)
      }

      override def compareTo(other: StreamBuffer): Int = {
        //降序，因为mutable.PriorityQueue出列最大值，而不是最小值
        // descending order because mutable.PriorityQueue dequeues the max, not the min
        if (other.minKeyHash < minKeyHash) -1 else if (other.minKeyHash == minKeyHash) 0 else 1
      }
    }
  }

  /**
   * An iterator that returns (K, C) pairs in sorted order from an on-disk map
    * 一个从磁盘映射以排序顺序返回（K，C）对的迭代器
   */
  private class DiskMapIterator(file: File, blockId: BlockId, batchSizes: ArrayBuffer[Long])
    extends Iterator[(K, C)]
  {
    //大小将是batchSize.length + 1
    private val batchOffsets = batchSizes.scanLeft(0L)(_ + _)  // Size will be batchSize.length + 1
    assert(file.length() == batchOffsets.last,
      "File length is not equal to the last batch offset:\n" +
      s"    file length = ${file.length}\n" +
      s"    last batch offset = ${batchOffsets.last}\n" +
      s"    all batch offsets = ${batchOffsets.mkString(",")}"
    )

    private var batchIndex = 0  // Which batch we're in
    private var fileStream: FileInputStream = null

    // An intermediate stream that reads from exactly one batch
    // This guards against pre-fetching and other arbitrary behavior of higher level streams
    //从完全一个批次读取的中间流保护较高级别流的预取和其他任意行为
    private var deserializeStream = nextBatchStream()
    private var nextItem: (K, C) = null
    private var objectsRead = 0

    /**
     * Construct a stream that reads only from the next batch.
      * 构建只从下一批读取的流
     */
    private def nextBatchStream(): DeserializationStream = {
      // Note that batchOffsets.length = numBatches + 1 since we did a scan above; check whether
      // we're still in a valid batch.
      //注意，我们上面做了一个扫描，batchOffsets.length = numBatches + 1; 检查我们是否仍在有效的批次中
      if (batchIndex < batchOffsets.length - 1) {
        if (deserializeStream != null) {
          deserializeStream.close()
          fileStream.close()
          deserializeStream = null
          fileStream = null
        }

        val start = batchOffsets(batchIndex)
        fileStream = new FileInputStream(file)
        fileStream.getChannel.position(start)
        batchIndex += 1

        val end = batchOffsets(batchIndex)

        assert(end >= start, "start = " + start + ", end = " + end +
          ", batchOffsets = " + batchOffsets.mkString("[", ", ", "]"))

        val bufferedStream = new BufferedInputStream(ByteStreams.limit(fileStream, end - start))
        val compressedStream = blockManager.wrapForCompression(blockId, bufferedStream)
        ser.deserializeStream(compressedStream)
      } else {
        // No more batches left
        cleanup()
        null
      }
    }

    /**
     * Return the next (K, C) pair from the deserialization stream.
     *从反序列化流中返回下一个（K，C）对
     * If the current batch is drained, construct a stream for the next batch and read from it.
     * If no more pairs are left, return null.
      * 如果当前批处理已被排除,则构建下一批的流并从中读取,如果没有更多的对,返回null
     */
    private def readNextItem(): (K, C) = {
      try {
        val k = deserializeStream.readKey().asInstanceOf[K]
        val c = deserializeStream.readValue().asInstanceOf[C]
        val item = (k, c)
        objectsRead += 1
        if (objectsRead == serializerBatchSize) {
          objectsRead = 0
          deserializeStream = nextBatchStream()
        }
        item
      } catch {
        case e: EOFException =>
          cleanup()
          null
      }
    }

    override def hasNext: Boolean = {
      if (nextItem == null) {
        if (deserializeStream == null) {
          return false
        }
        nextItem = readNextItem()
      }
      nextItem != null
    }

    override def next(): (K, C) = {
      val item = if (nextItem == null) readNextItem() else nextItem
      if (item == null) {
        throw new NoSuchElementException
      }
      nextItem = null
      item
    }

    private def cleanup() {
      //防止阅读任何其他批次
      batchIndex = batchOffsets.length  // Prevent reading any other batch
      val ds = deserializeStream
      if (ds != null) {
        ds.close()
        deserializeStream = null
      }
      if (fileStream != null) {
        fileStream.close()
        fileStream = null
      }
      if (file.exists()) {
        file.delete()
      }
    }

    val context = TaskContext.get()
    // context is null in some tests of ExternalAppendOnlyMapSuite because these tests don't run in
    // a TaskContext.
    //在ExternalAppendOnlyMapSuite的某些测试中，上下文为null，因为这些测试不会在TaskContext中运行。
    if (context != null) {
      context.addTaskCompletionListener(context => cleanup())
    }
  }

  /** Convenience function to hash the given (K, C) pair by the key.
    * 方便功能用键哈希给定（K，C）对*/
  private def hashKey(kc: (K, C)): Int = ExternalAppendOnlyMap.hash(kc._1)
}

private[spark] object ExternalAppendOnlyMap {

  /**
   * Return the hash code of the given object. If the object is null, return a special hash code.
    * 返回给定对象的哈希码。 如果对象为空，则返回一个特殊的哈希码
   */
  private def hash[T](obj: T): Int = {
    if (obj == null) 0 else obj.hashCode()
  }

  /**
   * A comparator which sorts arbitrary keys based on their hash codes.
    * 一个比较器，它根据它们的哈希码对任意的键进行排序
   */
  private class HashComparator[K] extends Comparator[K] {
    def compare(key1: K, key2: K): Int = {
      val hash1 = hash(key1)
      val hash2 = hash(key2)
      if (hash1 < hash2) -1 else if (hash1 == hash2) 0 else 1
    }
  }
}
