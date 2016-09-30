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

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable

import com.google.common.annotations.VisibleForTesting
import com.google.common.io.ByteStreams

import org.apache.spark._
import org.apache.spark.serializer._
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.shuffle.sort.{ SortShuffleFileWriter, SortShuffleWriter }
import org.apache.spark.storage.{ BlockId, DiskBlockObjectWriter }

/**
 * Sorts and potentially merges a number of key-value pairs of type (K, V) to produce key-combiner
 * pairs of type (K, C). Uses a Partitioner to first group the keys into partitions, and then
 * optionally sorts keys within each partition using a custom Comparator. Can output a single
 * partitioned file with a different byte range for each partition, suitable for shuffle fetches.
 * 这个类用于对一些(K,V)类型的key-value对进行排序,如果需要就进行merge,生的结果是一些(K,C)类型的key-combiner(合并)对.
 * combiner就是对同样key的value进行合并的结果。它首先使用一个Partitioner来把key分到不同的partition,
 * 然后,如果有必要的话,就把每个partition内部的key按照一个特定的Comparator(比较器)来进行排序。
 * 它可以输出只一个分区了的文件,其中不同的partition位于这个文件的不同区域,这样就适用于shuffle时对数据的抓取.
 * If combining is disabled, the type C must equal V -- we'll cast the objects at the end.
 * 如果combining没有启用,C和V的类型必须相同 -- 在最后我们会对对象进行强制类型转换
 * Note: Although ExternalSorter is a fairly generic sorter, some of its configuration is tied
 * to its use in sort-based shuffle (for example, its block compression is controlled by
 * `spark.shuffle.compress`).  We may need to revisit this if ExternalSorter is used in other
 * non-shuffle contexts where we might want to use different configuration settings.
 * 注意:仅管ExternalSorte是一个比较通用的sorter,但是它的一些配置是和它在基于sort(分类)的shuffle的用处紧密相连的
 *(比如，它的block压缩是通过‘spark.shuffle.compress‘控制的).如果在非shuffle情况下使用ExternalSorter时
 * 我们想要另外的配置,可能就需要重新审视一下它的实现。
 * 
 * 				
 * @param aggregator optional Aggregator with combine functions to use for merging data
 * 				aggregator 类型为Option[Aggregator],提供combine函数,用于merge
 * @param partitioner optional Partitioner; if given, sort by partition ID and then key
 * 				如果提供了partitioner,就先按partitionID排序,然后再按key排序
 * @param ordering optional Ordering to sort keys within each partition; should be a total ordering
 *  			 用来对每个partition内部的key进行排,total ordering(即所有key必须可以互相比较大小,与partitial ordering不同)			
 * @param serializer serializer to use when spilling to disk
 *				serializer类型 用于spill数据到磁盘.
 *
 * Note that if an Ordering is given, we'll always sort using it, so only provide it if you really
 * want the output keys to be sorted. In a map task without map-side combine for example, you
 * probably want to pass None as the ordering to avoid extra sorting. On the other hand, if you do
 * want to do combining, having an Ordering is more efficient than not having it.
 * 
 * 注意,如果提供了Ordering， 那么我们就总会使用它进行排序(是指对partition内部的key排序),
 * 因此,只有在真正需要输出的数据按照key排列时才提供ordering.例如，在一个没有map-side combine的map任务中,
 * 你应该会需要传递None作为ordering,这样会避免额外的排序.另一方面,如果你的确需要combining, 提供一个Ordering会更好。

 * Users interact with this class in the following way:
 * 用户应该这么和这个类型进行交互
 * 1. Instantiate an ExternalSorter.
 *		初始化一个ExternalSorter
 * 2. Call insertAll() with a set of records.
 *		调用insertAll, 提供要排序的数据
 * 3. Request an iterator() back to traverse sorted/aggregated records.
 *     - or -
 *    Invoke writePartitionedFile() to create a file containing sorted/aggregated outputs
 *    that can be used in Spark's sort shuffle.
 *		请求一个iterator()来遍历排序/聚合后的数据。或者，调用writePartitionedFiles来创建一个包含了排序/聚合后数据的文件，
 * 		这个文件可以用于Spark的sort shuffle。
 * At a high level, this class works internally as follows:
 * 这个类在内部是这么工作的:
 * - We repeatedly fill up buffers of in-memory data, using either a PartitionedAppendOnlyMap if
 *   we want to combine by key, or a PartitionedSerializedPairBuffer or PartitionedPairBuffer if we
 *   don't. Inside these buffers, we sort elements by partition ID and then possibly also by key.
 *   To avoid calling the partitioner multiple times with each key, we store the partition ID
 *   alongside each record.
 *  我们重复地将数据填满内存中的buffer，如果我们想要combine，就使用PartitionedAppendOnlyMap作为buffer, 
 *  如果不想要combine，就使用PartitionedSerializedPairBuffer或者PartitionedPariBuffer。
 *  在这里buffer内部，我们使用partition Id对元素排序，如果需要，就也按key排序(对同样partition Id的元素)。
 *  为了避免重复调用partitioner，我们会把record和partition ID存储在一起。
 * - When each buffer reaches our memory limit, we spill it to a file. This file is sorted first
 *   by partition ID and possibly second by key or by hash code of the key, if we want to do
 *   aggregation. For each file, we track how many objects were in each partition in memory, so we
 *   don't have to write out the partition ID for every element.
 *   当buffer达到了容量上限以后，我们把它spill到文件。这个文件首先按partition ID排序，然后如果需要进行聚合，
 *   就用key或者key的hashcode作为第二顺序。对于每个文件，我们会追踪在内存时，每个partition里包括多少个对象，
 *   所以我们在写文件 时候就不必要为每个元素记录partition ID了
 * - When the user requests an iterator or file output, the spilled files are merged, along with
 *   any remaining in-memory data, using the same sort order defined above (unless both sorting
 *   and aggregation are disabled). If we need to aggregate by key, we either use a total ordering
 *   from the ordering parameter, or read the keys with the same hash code and compare them with
 *   each other for equality to merge values.
 *	 当用户请求获取迭代器或者文件时，spill出来的文件就会和内存中的数据一起被merge，
 *  并且使用上边定义的排列顺序(除非排序和聚合都没有开启)。如果我们需要按照key聚合，
 *  我们要不使用Ordering参数进行全排序，要不就读取有相同hash code的key,并且对它们进行比较来确定相等性,以进行merge。
 * - Users are expected to call stop() at the end to delete all the intermediate files.
 * 	  用户最后应该使用stop()来删除中间文件。
 * 
 * 作为一种特殊情况,如果Ordering和Aggregator都没有提供,并且partition的数目少于spark.shuffle.sort.bypassMergeThreshold(200), 
 * 会绕过merge-sort,每次spill时会为每个partition单独写一个文件,就像HashShuffleWriter一样。
 * 然后把这些文件连接起来产生一个单独的排序后的文件,这时就没有必要为每个元素进行两次序列化和两次反序列化(在merge中就需要这么做)。
 * 这会加快groupBy,sort等没有部分聚合的操作的map端的效率。 
 */
private[spark] class ExternalSorter[K, V, C](
  aggregator: Option[Aggregator[K, V, C]] = None,
  partitioner: Option[Partitioner] = None,
  ordering: Option[Ordering[K]] = None,
  serializer: Option[Serializer] = None)
    extends Logging
    with Spillable[WritablePartitionedPairCollection[K, C]]
    with SortShuffleFileWriter[K, V] {

  private val conf = SparkEnv.get.conf
  //从分区器中获取分区的个数 
  private val numPartitions = partitioner.map(_.numPartitions).getOrElse(1)
  //Partition的个数大于1的时候,才做分区
  private val shouldPartition = numPartitions > 1
  //根据Key获得Parttion,是否分区大于1
  private def getPartition(key: K): Int = {
    if (shouldPartition) partitioner.get.getPartition(key) else 0
  }

  // Since SPARK-7855, bypassMergeSort optimization is no longer performed as part of this class.
  // As a sanity check, make sure that we're not handling a shuffle which should use that path.
  if (SortShuffleWriter.shouldBypassMergeSort(conf, numPartitions, aggregator, ordering)) {
    throw new IllegalArgumentException("ExternalSorter should not be used to handle "
      + " a sort that the BypassMergeSortShuffleWriter should handle")
  }
  //从evn中获取blockManager
  private val blockManager = SparkEnv.get.blockManager
  //从blockManager获取diskBlockManager
  private val diskBlockManager = blockManager.diskBlockManager
  
  private val ser = Serializer.getSerializer(serializer)
  private val serInstance = ser.newInstance()
  //shuffle期间通过溢出数据到磁盘来降低了内存使用总量,如果为true
  private val spillingEnabled = conf.getBoolean("spark.shuffle.spill", true)

  // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
  // 在ShuffleMapTask端通常也会增大Map任务的写磁盘的缓存，默认情况下是32K
  private val fileBufferSize = conf.getSizeAsKb("spark.shuffle.file.buffer", "32k").toInt * 1024

  // Size of object batches when reading/writing from serializers.
  // 每次序列化流最多写入的元素个数
  // Objects are written in batches, with each batch using its own serialization stream. This
  // cuts down on the size of reference-tracking maps constructed when deserializing a stream.
  //
  // NOTE: Setting this too low can cause excessive copying when serializing, since some serializers
  // grow internal data structures by growing + copying every time the number of objects doubles.
  private val serializerBatchSize = conf.getLong("spark.shuffle.spill.batchSize", 10000)

  private val useSerializedPairBuffer =
    ordering.isEmpty && //没有提供Ordering,即不需要对partition内部的kv再排序
      conf.getBoolean("spark.shuffle.sort.serializeMapOutputs", true) &&//它默认即为true
      //即在序列化输出流写了两个对象以后，把这两个对象对应的字节块交换位置，序列化输出流仍然能读出这两个对象
      ser.supportsRelocationOfSerializedObjects//支持relocate序列化以后的对象
  private val kvChunkSize = conf.getInt("spark.shuffle.sort.kvChunkSize", 1 << 22) // 4 MB
  /**
   * buffer充分利用内存,直接对内存中的对象进行操作可以提高效率,减少序列化、反序列化和IO的开销,
   * 比如在内存中先对部分value进行聚合,会减少要序列化和写磁盘的数据量;在内存中对kv先按照partition组合在一起,
   * 也有利于以后的merge,而且越大的buffer写到磁盘中的文件越大,这意味着要合并的文件就越少.
   */
  private def newBuffer(): WritablePartitionedPairCollection[K, C] with SizeTracker = {
    if (useSerializedPairBuffer) {
      //没有提供Ordering,即不需要对partition内部的kv再排序
      new PartitionedSerializedPairBuffer(metaInitialRecords = 256, kvChunkSize, serInstance)
    } else {
      new PartitionedPairBuffer[K, C]
    }
  }
  // Data structures to store in-memory objects before we spill. Depending on whether we have an
  // Aggregator set, we either put objects into an AppendOnlyMap where we combine them, or we
  // store them in an array buffer.
  //在溢出之前,数据结构存储在内存对象中,这取决于是否有一个聚合集,或者存储在数组缓冲区中
  //数据都是先写内存,内存不够了,才写磁盘
  private var map = new PartitionedAppendOnlyMap[K, C]
  //PartitionedAppendOnlyMap放不下,buffer一次性写入磁盘文件
  private var buffer = newBuffer()

  // Total spilling statistics
  //总溢出统计
  private var _diskBytesSpilled = 0L
  def diskBytesSpilled: Long = _diskBytesSpilled

  // Peak size of the in-memory data structure observed so far, in bytes
  //到目前为止观察到的内存数据结构的峰值大小,以字节为单位
  private var _peakMemoryUsedBytes: Long = 0L
  def peakMemoryUsedBytes: Long = _peakMemoryUsedBytes

  // A comparator for keys K that orders them within a partition to allow aggregation or sorting.
  // Can be a partial ordering by hash code if a total ordering is not provided through by the
  // user. (A partial ordering means that equal keys have comparator.compare(k, k) = 0, but some
  // non-equal keys also have this, so we need to do a later pass to find truly equal keys).
  // Note that we ignore this if no aggregator and no ordering are given.
  //按指定的Key进行
  private val keyComparator: Comparator[K] = ordering.getOrElse(new Comparator[K] {
    override def compare(a: K, b: K): Int = {
      val h1 = if (a == null) 0 else a.hashCode()
      val h2 = if (b == null) 0 else b.hashCode()
      if (h1 < h2) -1 else if (h1 == h2) 0 else 1
    }
  })
  /**
   * 排序或指定聚合函数按照指定的Key进行排序.
   */
  private def comparator: Option[Comparator[K]] = {
    if (ordering.isDefined || aggregator.isDefined) {
      Some(keyComparator)
    } else {
      None
    }
  }

  // Information about a spilled file. Includes sizes in bytes of "batches" written by the
  // serializer as we periodically reset its stream, as well as number of elements in each
  // partition, used to efficiently keep track of partitions when merging.
  //溢出文件的信息,包括写的序列化“批次”文件大小,定期重置流,每个分区中的元素的数量
  private[this] case class SpilledFile(
    file: File,
    blockId: BlockId,
    serializerBatchSizes: Array[Long],
    elementsPerPartition: Array[Long])

  private val spills = new ArrayBuffer[SpilledFile]

  /**
   * Number of files this sorter has spilled so far.
   * 存储溢出的文件数
   * Exposed for testing.
   */
  private[spark] def numSpills: Int = spills.size

  override def insertAll(records: Iterator[Product2[K, V]]): Unit = {
    // TODO: stop combining if we find that the reduction factor isn't high
    //聚合函数对计算结果按照partitionID和Key聚合后排序
    val shouldCombine = aggregator.isDefined
    //map任务端计算结果在缓存中执行聚合和排序
    if (shouldCombine) { //是否定义聚合函数
      // Combine values in-memory first using our AppendOnlyMap    
      val mergeValue = aggregator.get.mergeValue //聚合函数获取例如reduceByKey(_+_) 
      //创建组合
      val createCombiner = aggregator.get.createCombiner //通过一个Value元素生成组合元素，会被多次调用
      //kv就是records每次遍历得到的中的(K V)值
      var kv: Product2[K, V] = null
      //定义update函数,它接受两个参数，1.是否在Map中包含了值hasValue2.旧值是多少，如果还不存在，那是null，
      //在scala中，null也是一个对象     
      val update = (hadValue: Boolean, oldValue: C) => {
         //如果已经存在,则进行merge,根据Key进行merge(所谓的merge,就是调用mergeValue方法),否则调用createCombiner获取值
         //createCombiner方法是(v:V)=>v就是原样输出值
        if (hadValue) mergeValue(oldValue, kv._2) else createCombiner(kv._2)
      }
      //迭代之前创建Iterator,每读取一条Product2(真正执行代码FlatMapFunction,PairFunction),     
      while (records.hasNext) {
        addElementsRead()//遍历计数,每遍历一次增1
        //每读取一条Product2,将每行字符串按照空格切分,并且给每个文本设置1,例如(Apache,1),(Spark,1)
        kv = records.next()//读取当前record
        //getPartition是根据Key获得Parttion,SizeTrackingAppendOnlyMap.changeValue与update函数配合,按照Key叠加Vaule
        map.changeValue((getPartition(kv._1), kv._1), update)
        //当SizeTrackingAppendOnlyMap的大小超过myMemoryThreshold时,将集合中的数据写入到磁盘并新建
        //SizeTrackingAppendOnlyMap,为了防止内存溢出
        maybeSpillCollection(usingMap = true)//是否要spill到磁盘
      }
    } else {
      //如果没有定义aggregator,将值插入到缓冲区中
      // Stick values into our buffer
      while (records.hasNext) {
        addElementsRead()
        val kv = records.next()
        //把计算结果简单地缓存到数组
        buffer.insert(getPartition(kv._1), kv._1, kv._2.asInstanceOf[C])
        maybeSpillCollection(usingMap = false)//是否要spill到磁盘
      }
    }
  }

  /**
   * Spill the current in-memory collection to disk if needed.
   * 将集合中的数据保存到磁盘并,判定集合是否溢出
   * 
   * @param usingMap whether we're using a map or buffer as our current in-memory collection
   */
  private def maybeSpillCollection(usingMap: Boolean): Unit = {
    if (!spillingEnabled) { //默认启用,如果不启用,则有OOM风险
      return
    }
    
    var estimatedSize = 0L//元素大小
    if (usingMap) {//如果使用Map,则是使用PartitionedAppendOnlyMap
      estimatedSize = map.estimateSize() //估计当前集合中对象占用内存的大小
      if (maybeSpill(map, estimatedSize)) { //maybeSpill判定集合是否溢出
        map = new PartitionedAppendOnlyMap[K, C]
      }
    } else {//否则使用buffer则是使用SizeTrackingPairBuffe
      estimatedSize = buffer.estimateSize()//估计当前集合中对象占用内存的大小
      if (maybeSpill(buffer, estimatedSize)) {
        buffer = newBuffer()
      }
    }

    if (estimatedSize > _peakMemoryUsedBytes) {
      _peakMemoryUsedBytes = estimatedSize
    }
  }

  /**
   * Spill our in-memory collection to a sorted file that we can merge later.
   * We add this file into `spilledFiles` to find it later.
   * 将内存集合溢出到稍后合并的已排序的文件中,我们添加文件spilledFiles,日后找到它
   * @param collection whichever collection we're using (map or buffer)
   */
  override protected[this] def spill(collection: WritablePartitionedPairCollection[K, C]): Unit = {
    // Because these files may be read during shuffle, their compression must be controlled by
    // spark.shuffle.compress instead of spark.shuffle.spill.compress, so we need to use
    // createTempShuffleBlock here; see SPARK-3426 for more context.
    //创建临时文件
    val (blockId, file) = diskBlockManager.createTempShuffleBlock()
    // These variables are reset after each flush
    //这些变量被重置后刷新
    var objectsWritten: Long = 0
    var spillMetrics: ShuffleWriteMetrics = null
    var writer: DiskBlockObjectWriter = null
    def openWriter(): Unit = {
      assert(writer == null && spillMetrics == null)
      //新创ShuffleWriteMetrics用于测量
      spillMetrics = new ShuffleWriteMetrics
      //getDiskWriterw创建DiskBlockObjectWriter
      writer = blockManager.getDiskWriter(blockId, file, serInstance, fileBufferSize, spillMetrics)
    }
    openWriter()

    // List of batch sizes (bytes) in the order they are written to disk
    //将列表中的批量大小(字节)的顺序写到磁盘上
    val batchSizes = new ArrayBuffer[Long]

    // How many elements we have in each partition
    //每一个分区中有多少个元素
    val elementsPerPartition = new Array[Long](numPartitions)

    // Flush the disk writer's contents to disk, and update relevant variables.
    // The writer is closed at the end of this process, and cannot be reused.
    //将内容保存到磁盘,并更新相关变量,写的操作结果时关闭进程,不能重复使用
    def flush(): Unit = {
      val w = writer
      writer = null
      w.commitAndClose()
      _diskBytesSpilled += spillMetrics.shuffleBytesWritten
      batchSizes.append(spillMetrics.shuffleBytesWritten)
      spillMetrics = null
      objectsWritten = 0
    }

    var success = false
    try {
      //对集合元素排序
      val it = collection.destructiveSortedWritablePartitionedIterator(comparator)
      while (it.hasNext) {
        //获得分区ID
        val partitionId = it.nextPartition()
        it.writeNext(writer) //将集合内容写入临时文件

        elementsPerPartition(partitionId) += 1
        objectsWritten += 1
        //遍历过程中,每当写入DiskBlockObjectWriter的元素个数达到批量序列化大小时,执行flush,
        //然后重新创建DiskBlokObjectWriter
        if (objectsWritten == serializerBatchSize) {
          flush()
          openWriter()
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
        //如果一个异常被抛出,此代码路径只发生在设置成功之前
        if (writer != null) {
          writer.revertPartialWritesAndClose()
        }
        if (file.exists()) {
          file.delete()
        }
      }
    }

    spills.append(SpilledFile(file, blockId, batchSizes.toArray, elementsPerPartition))
  }

  /**
   * Merge a sequence of sorted files, giving an iterator over partitions and then over elements
   * inside each partition. This can be used to either write out a new file or return data to
   * the user.
   * 合并一个排序的序列文件,给一个迭代器在每个分区内的元素,这可以用来写入新的文件或将数据返回给用户
   * Returns an iterator over all the data written to this object, grouped by partition. For each
   * partition we then have an iterator over its contents, and these are expected to be accessed
   * in order (you can't "skip ahead" to one partition without reading the previous one).
   * Guaranteed to return a key-value pair for each partition, in order of partition ID.
   * 返回写入此对象的所有数据的迭代器,按区分组,对于每一个分区,我们将内容上有一个迭代器,都将被访问的顺序
   * 以分区标识的顺序,每个分区返回一个键值对.
   */
  private def merge(spills: Seq[SpilledFile], inMemory: Iterator[((Int, K), C)]): Iterator[(Int, Iterator[Product2[K, C]])] = {
    val readers = spills.map(new SpillReader(_))//为每个spill出来的文件生成一个reader
    val inMemBuffered = inMemory.buffered//内存中的迭代器进行buffered,以方便查看其head的信息
    (0 until numPartitions).iterator.map { p =>//对每一个partition
      //对内存中的数据获取这个partition对应的iterator
      val inMemIterator = new IteratorForPartition(p, inMemBuffered)
      //把文件数据的迭代器和内存数据的迭代器都放在一个seq里
      val iterators = readers.map(_.readNextPartition()) ++ Seq(inMemIterator)
      if (aggregator.isDefined) {//如果需要聚合的话
        // Perform partial aggregation across partitions
        //对这个partition对应的那些iterator进行merge,并且聚合数据
        (p, mergeWithAggregation(
          iterators, aggregator.get.mergeCombiners, keyComparator, ordering.isDefined))
      } else if (ordering.isDefined) {
        // No aggregator given, but we have an ordering (e.g. used by reduce tasks in sortByKey);
        //没有给定聚合,
        // sort the elements without trying to merge them
        //排序的元素,而不试图合并它们
        (p, mergeSort(iterators, ordering.get))
      } else {
        (p, iterators.iterator.flatten)
      }
    }
  }

  /**
   * Merge-sort a sequence of (K, C) iterators using a given a comparator for the keys.
   * 使用一个给定比较器迭代器,合并序列的排序(k，c)
   */
  private def mergeSort(iterators: Seq[Iterator[Product2[K, C]]], comparator: Comparator[K]): Iterator[Product2[K, C]] =
    {
      val bufferedIters = iterators.filter(_.hasNext).map(_.buffered)
      type Iter = BufferedIterator[Product2[K, C]]
      val heap = new mutable.PriorityQueue[Iter]()(new Ordering[Iter] {
        // Use the reverse of comparator.compare because PriorityQueue dequeues the max
        override def compare(x: Iter, y: Iter): Int = -comparator.compare(x.head._1, y.head._1)
      })
      //将仅包含迭代器的hasNext = true
      heap.enqueue(bufferedIters: _*) // Will contain only the iterators with hasNext = true
      new Iterator[Product2[K, C]] {
        override def hasNext: Boolean = !heap.isEmpty

        override def next(): Product2[K, C] = {
          if (!hasNext) {
            throw new NoSuchElementException
          }
          val firstBuf = heap.dequeue()
          val firstPair = firstBuf.next()
          if (firstBuf.hasNext) {
            heap.enqueue(firstBuf)
          }
          firstPair
        }
      }
    }

  /**
   * Merge a sequence of (K, C) iterators by aggregating values for each key, assuming that each
   * iterator is sorted by key with a given comparator. If the comparator is not a total ordering
   * (e.g. when we sort objects by hash code and different keys may compare as equal although
   * they're not), we still merge them by doing equality tests for all keys that compare as equal.
   * 合并一个序列(k，c)通过聚合值对于每个关键的迭代器,假设每个迭代器都是由一个给定比较器的键进行排序,
   * 如果比较器不是一个总排序,通过做所有键的相等性测试来合并它们,作为相等的比较
   */
  private def mergeWithAggregation(
    iterators: Seq[Iterator[Product2[K, C]]],
    mergeCombiners: (C, C) => C,
    comparator: Comparator[K],
    totalOrder: Boolean): Iterator[Product2[K, C]] =
    {
      if (!totalOrder) {
        //非totalOrder,说明了这些迭代器中一个partition内部的元素实际是按照hash code排序的
        // We only have a partial ordering, e.g. comparing the keys by hash code, which means that
        // multiple distinct keys might be treated as equal by the ordering. To deal with this, we
        // need to read all keys considered equal by the ordering at once and compare them.
        //我们只有一个部分排序,通过哈希代码比较,这意味着多个不同的键可能被视为相等的顺序,
        //需要读取所有的键被认为是相等的顺序比较
        new Iterator[Iterator[Product2[K, C]]] {
          //先按comparator进行merge sort，不aggregate
          val sorted = mergeSort(iterators, comparator).buffered

          // Buffers reused across elements to decrease memory allocation
          //重用元素的缓冲区,以减少内存分配
          val keys = new ArrayBuffer[K]
          //存放keys中对应位置的key对应的所有combiner聚合后的结果
          val combiners = new ArrayBuffer[C]

          override def hasNext: Boolean = sorted.hasNext

          override def next(): Iterator[Product2[K, C]] = {
            if (!hasNext) {
              throw new NoSuchElementException
            }
            keys.clear()
            combiners.clear()
            val firstPair = sorted.next()//获取排序后iterator的第一个pair
            keys += firstPair._1//第一个pair的key放在keys里
            combiners += firstPair._2//第一个pair的combiner放在combiners里
            val key = firstPair._1//第一个pair的key
            while (sorted.hasNext && comparator.compare(sorted.head._1, key) == 0) {
               //获取sorted中跟前key compare以后为0的下一个kv。注意，compare为0不一定 ==号成立
              val pair = sorted.next()
              var i = 0
              var foundKey = false
              while (i < keys.size && !foundKey) {
               //用当前取出的这个kc的key与keys中key依次比较，找到一个==的，
                //就对combiner进行aggregate，然后结果放在combiners里，并且结束循环             
                if (keys(i) == pair._1) {
                  combiners(i) = mergeCombiners(combiners(i), pair._2)
                  foundKey = true
                }
                i += 1
              }
             //如果这个kc里的key与keys里所有key都不==，意味着它与它当前缓存的所有keycompare为0但不==，
             //所以它是一个新的key，就放在keys里，它的combiner放在combiners里
              if (!foundKey) {
                keys += pair._1
                combiners += pair._2
              }
            }

            // Note that we return an iterator of elements since we could've had many keys marked
            // equal by the partial order; we flatten this below to get a flat iterator of (K, C).
            //把keys和combiners 进行zip,得到iterator of (K, C)
            keys.iterator.zip(combiners.iterator)
          }
        }.flatMap(i => i)//flatMap之前是Iteator[compare为0的所有kc聚合而成的Iteator[K, C]],所以直接flatMap(i => i)就成了
      } else {
        // We have a total ordering, so the objects with the same key are sequential.
        //因为是total ordering的,意味着用Ordering排序，所以==的key是挨在一起的
        new Iterator[Product2[K, C]] {
          val sorted = mergeSort(iterators, comparator).buffered

          override def hasNext: Boolean = sorted.hasNext

          override def next(): Product2[K, C] = {
            if (!hasNext) {
              throw new NoSuchElementException
            }
            //获取排序后iterator的第一个pair
            val elem = sorted.next()
            //第一个pair的key放在keys里
            val k = elem._1//第一个pair的key
            var c = elem._2
            while (sorted.hasNext && sorted.head._1 == k) {//取出所有==的kc，进行merge
              val pair = sorted.next()
              c = mergeCombiners(c, pair._2)
            }
            (k, c)
          }
        }
      }
    }

  /**
   * An internal class for reading a spilled file partition by partition. Expects all the
   * partitions to be requested in order.
   * 一个用于读取分区的溢出文件分区的内部类,预计所有分区要按顺序要求
   */
  private[this] class SpillReader(spill: SpilledFile) {
    // Serializer batch offsets; size will be batchSize.length + 1
    //批量序列化位置
    val batchOffsets = spill.serializerBatchSizes.scanLeft(0L)(_ + _)

    // Track which partition and which batch stream we're in. These will be the indices of
    // the next element we will read. We'll also store the last partition read so that
    // readNextPartition() can figure out what partition that was from.
    //跟踪分区和批流中,将读到下一个元素的索引
    var partitionId = 0
    var indexInPartition = 0L
    var batchId = 0
    var indexInBatch = 0
    var lastPartitionId = 0

    skipToNextPartition()

    // Intermediate file and deserializer streams that read from exactly one batch
    // This guards against pre-fetching and other arbitrary behavior of higher level streams
    //读取一批中间文件反序列化流
    var fileStream: FileInputStream = null
    var deserializeStream = nextBatchStream() // Also sets fileStream

    var nextItem: (K, C) = null
    var finished = false

    /** Construct a stream that only reads from the next batch */
    //构造一个只从下一批读取的流
    def nextBatchStream(): DeserializationStream = {
      // Note that batchOffsets.length = numBatches + 1 since we did a scan above; check whether
      // we're still in a valid batch.
      
      if (batchId < batchOffsets.length - 1) {
        if (deserializeStream != null) {
          deserializeStream.close()
          fileStream.close()
          deserializeStream = null
          fileStream = null
        }

        val start = batchOffsets(batchId)
        fileStream = new FileInputStream(spill.file)
        fileStream.getChannel.position(start)
        batchId += 1

        val end = batchOffsets(batchId)

        assert(end >= start, "start = " + start + ", end = " + end +
          ", batchOffsets = " + batchOffsets.mkString("[", ", ", "]"))

        val bufferedStream = new BufferedInputStream(ByteStreams.limit(fileStream, end - start))
        val compressedStream = blockManager.wrapForCompression(spill.blockId, bufferedStream)
        serInstance.deserializeStream(compressedStream)
      } else {
        // No more batches left
        cleanup()
        null
      }
    }

    /**
     * Update partitionId if we have reached the end of our current partition, possibly skipping
     * empty partitions on the way.
     * 如果已经达到当前分区的结束则更新partitionid,可能跳过空的分区
     */
    private def skipToNextPartition() {
      while (partitionId < numPartitions &&
        indexInPartition == spill.elementsPerPartition(partitionId)) {
        partitionId += 1
        indexInPartition = 0L
      }
    }

    /**
     * Return the next (K, C) pair from the deserialization stream and update partitionId,
     * indexInPartition, indexInBatch and such to match its location.
     * 返回下一个(k，C)对,反序列化流和更新partitionid,相匹配的位置
     * If the current batch is drained, construct a stream for the next batch and read from it.
     * If no more pairs are left, return null.
     * 如果当前批处理被耗尽,读取构建下一个批次流,如果没有更多的对返回
     * 
     */
    private def readNextItem(): (K, C) = {
      if (finished || deserializeStream == null) {
        return null
      }
      val k = deserializeStream.readKey().asInstanceOf[K]
      val c = deserializeStream.readValue().asInstanceOf[C]
      lastPartitionId = partitionId
      // Start reading the next batch if we're done with this one
      //开始读取下一批
      indexInBatch += 1
      if (indexInBatch == serializerBatchSize) {
        indexInBatch = 0
        deserializeStream = nextBatchStream()
      }
      // Update the partition location of the element we're reading
      //更新我们正在读取的元素的分区位置
      indexInPartition += 1
      skipToNextPartition()
      // If we've finished reading the last partition, remember that we're done
      //如果我们已经读完了最后一个分区,标记已经完成
      if (partitionId == numPartitions) {
        finished = true
        if (deserializeStream != null) {
          deserializeStream.close()
        }
      }
      (k, c)
    }

    var nextPartitionToRead = 0
    //返回的迭代器可以迭代这个partition内的所有元素
    def readNextPartition(): Iterator[Product2[K, C]] = new Iterator[Product2[K, C]] {
      val myPartition = nextPartitionToRead
      nextPartitionToRead += 1

      override def hasNext: Boolean = {
        if (nextItem == null) {
          nextItem = readNextItem()
          if (nextItem == null) {
            return false
          }
        }
        assert(lastPartitionId >= myPartition)
        // Check that we're still in the right partition; note that readNextItem will have returned
        // null at EOF above so we would've returned false there
        lastPartitionId == myPartition
      }

      override def next(): Product2[K, C] = {
        if (!hasNext) {
          throw new NoSuchElementException
        }
        val item = nextItem
        nextItem = null
        item
      }
    }

    // Clean up our open streams and put us in a state where we can't read any more data
    //清理打开的流,并不能读取更多数据的状态
    def cleanup() {
      batchId = batchOffsets.length // Prevent reading any other batch,防止读取任何批次
      val ds = deserializeStream
      deserializeStream = null
      fileStream = null
      ds.close()
      // NOTE: We don't do file.delete() here because that is done in ExternalSorter.stop().
      // This should also be fixed in ExternalAppendOnlyMap.
    }
  }

  /**
   * Return an iterator over all the data written to this object, grouped by partition and
   * aggregated by the requested aggregator. For each partition we then have an iterator over its
   * contents, and these are expected to be accessed in order (you can't "skip ahead" to one
   * partition without reading the previous one). Guaranteed to return a key-value pair for each
   * partition, in order of partition ID.
   * 排序与分区分组,通过对集合按照指定的比较器进行排序,并且按照partition Id分组,生成迭代器
   * For now, we just merge all the spilled files in once pass, but this can be modified to
   * support hierarchical merging.
   */
  @VisibleForTesting
  def partitionedIterator: Iterator[(Int, Iterator[Product2[K, C]])] = {
    val usingMap = aggregator.isDefined
    val collection: WritablePartitionedPairCollection[K, C] = if (usingMap) map else buffer
    //没有发生spill
    if (spills.isEmpty) {
      // Special case: if we have only in-memory data, we don't need to merge streams, and perhaps
      // we don't even need to sort by anything other than partition ID
      //只有内存中的数据,就按是否有ordering采用不同的排序方式得到迭代器,然后按partition对迭代器中的数据进行组合
      if (!ordering.isDefined) {
        // The user hasn't requested sorted keys, so only sort by partition ID, not key
        //按照partition Id进行比较排序,生成迭代器,
        groupByPartition(collection.partitionedDestructiveSortedIterator(None))
      } else {
        // We do need to sort by both partition ID and key
        //keyComparator 比较器按着指定的Key进行排序
        groupByPartition(collection.partitionedDestructiveSortedIterator(Some(keyComparator)))
      }
    } else {
      // Merge spilled and in-memory data
      //comparator如果没有指定排序或聚合函数,合并
      merge(spills, collection.partitionedDestructiveSortedIterator(comparator))
    }
  }

  /**
   * Return an iterator over all the data written to this object, aggregated by our aggregator.
   * 返回写入此对象的所有数据的迭代器,我们的聚合聚集
   */
  def iterator: Iterator[Product2[K, C]] = partitionedIterator.flatMap(pair => pair._2)

  /**
   * Write all the data added into this ExternalSorter into a file in the disk store. This is
   * called by the SortShuffleWriter.
   * 把externalsorter的中所有数据存储到磁盘文件中,调用SortShuffleWriter
   * @param blockId block ID to write to. The index file will be blockId.name + ".index".
   * @param context a TaskContext for a running Spark task, for us to update shuffle metrics.
   * @return array of lengths, in bytes, of each partition of the file (used by map output tracker)
   */
  override def writePartitionedFile(
    blockId: BlockId,
    context: TaskContext,
    outputFile: File): Array[Long] = {

    // Track location of each range in the output file
    //跟踪输出文件中每个区域的位置
    val lengths = new Array[Long](numPartitions)
    //溢出到分区文件后合并 
    if (spills.isEmpty) {
      // Case where we only have in-memory data
      //说明只有内存中的数据，并没有发生spill
      val collection = if (aggregator.isDefined) map else buffer
      //获取
      val it = collection.destructiveSortedWritablePartitionedIterator(comparator)
      while (it.hasNext) {
        //获得DiskBlockObjectWriter对象
        val writer = blockManager.getDiskWriter(blockId, outputFile, serInstance, fileBufferSize,
          context.taskMetrics.shuffleWriteMetrics.get)
        val partitionId = it.nextPartition()//获取此次while循环开始时的partition id
        while (it.hasNext && it.nextPartition() == partitionId) {
          it.writeNext(writer)//把与这个partition id相同的数据全写入
          }
       //这个writer只用于写入这个partition的数据,因此当此partition数据写完后,需要commitAndClose，以使得reader可读这个文件段。 
        writer.commitAndClose()
        val segment = writer.fileSegment()
        //把这个partition对应的文件里数据的长度添加到lengths里
        lengths(partitionId) = segment.length
      }
    } else {
      // We must perform merge-sort; get an iterator by partition and write everything directly.
      //必须执行合并排序,通过一个迭代器直接写东西
      //内存中排序合并:将缓存的中间计算结果按照partition分组写入Block输出文件
      for ((id, elements) <- this.partitionedIterator) {
        if (elements.hasNext) {
          //获得DiskBlockObjectWriter
          val writer = blockManager.getDiskWriter(blockId, outputFile, serInstance, fileBufferSize,
            context.taskMetrics.shuffleWriteMetrics.get)
          for (elem <- elements) {
            //对于这个partition的所有数据,用一个writer写入
            writer.write(elem._1, elem._2)
          }
          writer.commitAndClose()
          val segment = writer.fileSegment()
          lengths(id) = segment.length
        }
      }
    }
    //此种方式还需要更新此任务与内存,磁盘有关的测量数据
    context.taskMetrics().incMemoryBytesSpilled(memoryBytesSpilled)
    context.taskMetrics().incDiskBytesSpilled(diskBytesSpilled)
    context.internalMetricsToAccumulators(
      InternalAccumulator.PEAK_EXECUTION_MEMORY).add(peakMemoryUsedBytes)

    lengths
  }

  def stop(): Unit = {
    spills.foreach(s => s.file.delete())
    spills.clear()
  }

  /**
   * Given a stream of ((partition, key), combiner) pairs *assumed to be sorted by partition ID*,
   * group together the pairs for each partition into a sub-iterator.
   * 主要用于destructiveSortedIterator生成的迭代器按照partition ID分组
   * @param data an iterator of elements, assumed to already be sorted by partition ID
   */
  private def groupByPartition(data: Iterator[((Int, K), C)]): Iterator[(Int, Iterator[Product2[K, C]])] =
    {
      val buffered = data.buffered
      (0 until numPartitions).iterator.map(p => (p, new IteratorForPartition(p, buffered)))
    }

  /**
   * An iterator that reads only the elements for a given partition ID from an underlying buffered
   * stream, assuming this partition is the next one to be read. Used to make it easier to return
   * partitioned iterators from our in-memory collection.
   */
  private[this] class IteratorForPartition(partitionId: Int, data: BufferedIterator[((Int, K), C)])
      extends Iterator[Product2[K, C]] {
    //如何区分partitionId呢?可见其hasNext会判断数据的partitionId
    override def hasNext: Boolean = data.hasNext && data.head._1._1 == partitionId

    override def next(): Product2[K, C] = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      val elem = data.next()
      (elem._1._2, elem._2)
    }
  }
}
