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

package org.apache.spark.storage

import java.nio.ByteBuffer
import java.util.LinkedHashMap

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.TaskContext
import org.apache.spark.util.{SizeEstimator, Utils}
import org.apache.spark.util.collection.SizeTrackingVector
/**
 * 内存实体:value,size内存大小,deserialized是否反序列化
 */
private case class MemoryEntry(value: Any, size: Long, deserialized: Boolean)

/**
 * Stores blocks in memory, either as Arrays of deserialized Java objects or as
 * serialized ByteBuffers.
 * 负责将没有序列化的Java对象数组或者序列化的ByteBuffer存储到内存中
 * maxMemory 最大可用的内存大小
 * 
 */
private[spark] class MemoryStore(blockManager: BlockManager, maxMemory: Long)
  extends BlockStore(blockManager) {

  private val conf = blockManager.conf
  //存储Block数据的Map,以BlockId为key,MemoryEntry为值,并能根据存储的先后顺序访问
  private val entries = new LinkedHashMap[BlockId, MemoryEntry](32, 0.75f, true)
 //当前内存使用情况
  @volatile private var currentMemory = 0L
 
  // Ensure only one thread is putting, and if necessary, dropping blocks at any given time
  //确保只有一个线程正在放置,如有必要,可以在任何给定的时间放置块
  //同步锁,保证只有一个线程在写和删除Block
  private val accountingLock = new Object

  // A mapping from taskAttemptId to amount of memory used for unrolling a block (in bytes)
  // All accesses of this map are assumed to have manually synchronized on `accountingLock`
  //当前Dirver或者Executor中所有线程展开的Block都存入此Map中,key为Task的Id,value为线程展开的所有块的内存大小总和
  private val unrollMemoryMap = mutable.HashMap[Long, Long]()
  // Same as `unrollMemoryMap`, but for pending unroll memory as defined below.
  //与“unrollMemoryMap”相同,但是如下所述等待展开内存,
  // Pending unroll memory refers to the intermediate memory occupied by a task
  // after the unroll but before the actual putting of the block in the cache.
  //待处理的展开存储器是指在展开后但在实际将块放入高速缓存之前由任务占用的中间存储器。
  // This chunk of memory is expected to be released *as soon as* we finish
  // caching the corresponding block as opposed to until after the task finishes.
  // This is only used if a block is successfully unrolled in its entirety in
  // memory (SPARK-4777).
  //一旦*完成缓存相应的块,直到任务完成后,这个内存块才会被释放*。 这仅在块在内存中成功展开（SPARK-4777）时才会使用。
  private val pendingUnrollMemoryMap = mutable.HashMap[Long, Long]()

  /**
   * 展开Iterator时需要保证的内存大小,值为maxMemory*conf.getDouble(“spark.storage.unrollFraction”, 0.2)
   * 若展开时没有足够的内存, 并且展开Iterator使用的内存没有达到maxUnrollMemory,
   * 需要将存储在内存中的可以存储到磁盘中的Block存储到磁盘,以释放内存
   * The amount of space ensured for unrolling values in memory, shared across all cores.
   * This space is not reserved in advance, but allocated dynamically by dropping existing blocks.
   */
  private val maxUnrollMemory: Long = {
    //Unroll内存：spark允许数据以序列化或非序列化的形式存储,序列化的数据不能拿过来直接使用,所以就需要先反序列化,即unroll
    val unrollFraction = conf.getDouble("spark.storage.unrollFraction", 0.2)
    (maxMemory * unrollFraction).toLong
  }

  // Initial memory to request before unrolling any block
  //在展开任何块之前请求的初始内存
  /**
   *初始化展开前block内存,默认1G  
   */
  private val unrollMemoryThreshold: Long =
    conf.getLong("spark.storage.unrollMemoryThreshold", 1024 * 1024)
  if (maxMemory < unrollMemoryThreshold) {
    logWarning(s"Max memory ${Utils.bytesToString(maxMemory)} is less than the initial memory " +
      s"threshold ${Utils.bytesToString(unrollMemoryThreshold)} needed to store a block in " +
      s"memory. Please configure Spark with more memory.")
  }

  logInfo("MemoryStore started with capacity %s".format(Utils.bytesToString(maxMemory)))

  /** Free memory not occupied by existing blocks. Note that this does not include unroll memory. */
  //当前Driver或者Executor未使用的内存
  def freeMemory: Long = maxMemory - currentMemory

  override def getSize(blockId: BlockId): Long = {
    entries.synchronized {
      entries.get(blockId).size//内存大小
    }
  }
/**
 * 如果Block可以被反序列化(即存储级别StorageLevel.deserialized)那么先对Block序列化,然后调putIterator
 */
  override def putBytes(blockId: BlockId, _bytes: ByteBuffer, level: StorageLevel): PutResult = {
    // Work on a duplicate - since the original input might be used elsewhere.
    // 在一个重复的工作 - 由于原始的输入可能在其他地方使用,
    //duplicate()返回一个新的字节的缓冲区共享老缓冲区的内容
    val bytes = _bytes.duplicate()//创建共享此缓冲区的新的字节缓存区
    bytes.rewind()//重新分配缓存
    if (level.deserialized) {
      val values = blockManager.dataDeserialize(blockId, bytes)
      putIterator(blockId, values, level, returnValues = true)
    } else {
      val putAttempt = tryToPut(blockId, bytes, bytes.limit, deserialized = false)
      //duplicate()返回一个新的字节的缓冲区共享老缓冲区的内容
      PutResult(bytes.limit(), Right(bytes.duplicate()), putAttempt.droppedBlocks)
    }
  }

  /**
   * Use `size` to test if there is enough space in MemoryStore. If so, create the ByteBuffer and
   * put it into MemoryStore. Otherwise, the ByteBuffer won't be created.
   * 将字节缓存形式的Block存储到内存中,若level.deserialized为真,则需要将字节缓存反序列化,
   * 以数组的形式（Array[Any]）存储；若为假则以字节缓存形式（ByteBuffer）存储
   * The caller should guarantee that `size` is correct.
   */
  def putBytes(blockId: BlockId, size: Long, _bytes: () => ByteBuffer): PutResult = {
    // Work on a duplicate - since the original input might be used elsewhere.
    //在一个重复的工作 - 由于原始的输入可能在其他地方使用
    //duplicate()返回一个新的字节的缓冲区共享老缓冲区的内容
    lazy val bytes = _bytes().duplicate().rewind().asInstanceOf[ByteBuffer]
    val putAttempt = tryToPut(blockId, () => bytes, size, deserialized = false)
    val data =
      if (putAttempt.success) {
        assert(bytes.limit == size)
        //duplicate()返回一个新的字节的缓冲区共享老缓冲区的内容
        Right(bytes.duplicate())
      } else {
        null
      }
    PutResult(size, data, putAttempt.droppedBlocks)
  }
  /**
   * 内存写入
   */
  override def putArray(
      blockId: BlockId,
      values: Array[Any],
      level: StorageLevel,
      returnValues: Boolean): PutResult = {
    if (level.deserialized) {
      //首先对对象大小进行估算
      val sizeEstimate = SizeEstimator.estimate(values.asInstanceOf[AnyRef])
      //尝试写入内存
      val putAttempt = tryToPut(blockId, values, sizeEstimate, deserialized = true)
      //如果unrollsafely返回的数据匹配Left,整个block是可以一次性放入内存的
      PutResult(sizeEstimate, Left(values.iterator), putAttempt.droppedBlocks)
    } else {
      //不支持序列化
      val bytes = blockManager.dataSerialize(blockId, values.iterator)
      //尝试写入内存,如果unrollsafely返回的数据匹配Left,整个block是可以一次性放入内存的
      val putAttempt = tryToPut(blockId, bytes, bytes.limit, deserialized = false)
      //duplicate返回一个新的字节的缓冲区共享老缓冲区的内
      PutResult(bytes.limit(), Right(bytes.duplicate()), putAttempt.droppedBlocks)
    }
  }

  override def putIterator(
      blockId: BlockId,
      values: Iterator[Any],
      level: StorageLevel,
      returnValues: Boolean): PutResult = {
    putIterator(blockId, values, level, returnValues, allowPersistToDisk = true)
  }

  /**
   * Attempt to put the given block in memory store.
   * 尝试将给定的块放在内存中
   * There may not be enough space to fully unroll the iterator in memory, in which case we
   * optionally drop the values to disk if
    * 可能没有足够的空间来完全展开内存中的迭代器,在这种情况下,我们可以将值放在磁盘上
   *   (1) the block's storage level specifies useDisk, and 该块的存储级别指定useDisk，和
   *   (2) `allowPersistToDisk` is true.   `allowPersistToDisk`是真的。
   *
   * One scenario in which `allowPersistToDisk` is false is when the BlockManager reads a block
   * back from disk and attempts to cache it in memory. In this case, we should not persist the
   * block back on disk again, as it is already in disk store.
    * `allowPersistToDisk`为false的一种情况是当BlockManager从磁盘读取一个块并尝试将其缓存在内存中时。
    * 在这种情况下，我们不应该再次将磁盘块重新保留在磁盘存储中。
   */
  private[storage] def putIterator(
      blockId: BlockId,
      values: Iterator[Any],
      level: StorageLevel,
      returnValues: Boolean,
      allowPersistToDisk: Boolean): PutResult = {
    val droppedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]
    //unrollSafely将块在内存中安全展开,如果返回数据的类型匹配Left则说明内存足够,调用putArray方法写入内存
    //如果返回数据类型Right,则说明内存不足并写入硬盘或者放弃
    val unrolledValues = unrollSafely(blockId, values, droppedBlocks)
    unrolledValues match {
      case Left(arrayValues) =>
        //如果返回数据的类型匹配Left则说明内存足够,调用putArray方法写入内存
        // Values are fully unrolled in memory, so store them as an array
        //值在内存中完全展开，因此将它们存储为数组
        val res = putArray(blockId, arrayValues, level, returnValues)
        droppedBlocks ++= res.droppedBlocks
        PutResult(res.size, res.data, droppedBlocks)
      case Right(iteratorValues) =>
        // Not enough space to unroll this block; drop to disk if applicable
        //没有足够的空间来展开这个块; 如果内存不足并写入硬盘
        if (level.useDisk && allowPersistToDisk) {
          logWarning(s"Persisting block $blockId to disk instead.")
          val res = blockManager.diskStore.putIterator(blockId, iteratorValues, level, returnValues)
          PutResult(res.size, res.data, droppedBlocks)
        } else {
          //放弃
          PutResult(0, Left(iteratorValues), droppedBlocks)
        }
    }
  }
/**
 * 获取内存中的数据
 */
  override def getBytes(blockId: BlockId): Option[ByteBuffer] = {
    //从entries(LinkedHashMap)中获取MemoryEntry,
    val entry = entries.synchronized {
      entries.get(blockId)//MemoryEntry
    }
    if (entry == null) {
      None
    } else if (entry.deserialized) {
     //如果MemoryEntry支持反序列化,则将MemoryEntry的value反序列化后返回
      Some(blockManager.dataSerialize(blockId, entry.value.asInstanceOf[Array[Any]].iterator))
    } else {
      //不支持序列化,对MemoryEntny的value复制ByteBuffer后返回
      //实际上并不复制数据
      //duplicate()返回一个新的字节的缓冲区共享老缓冲区的内容
      Some(entry.value.asInstanceOf[ByteBuffer].duplicate()) // Doesn't actually copy the data
    }
  }
/**
 * 用于从内存中获取数据
 */
  override def getValues(blockId: BlockId): Option[Iterator[Any]] = {
    //从entries(LinkedHashMap)中获取MemoryEntry,
    val entry = entries.synchronized {
      entries.get(blockId)
    }
    if (entry == null) {
      None
    } else if (entry.deserialized) {
       //如果MemoryEntry支持反序列化,则将MemoryEntry的value反序列化后返回
      Some(entry.value.asInstanceOf[Array[Any]].iterator)
    } else {
      //不支持序列化,对MemoryEntny的value复制ByteBuffer后返回     
      val buffer = entry.value.asInstanceOf[ByteBuffer].duplicate() // Doesn't actually copy data
      Some(blockManager.dataDeserialize(blockId, buffer))
    }
  }
/**
 * 用于从内存中删除Block数据,并更新当前内存
 */
  override def remove(blockId: BlockId): Boolean = {
    entries.synchronized {
       //从entries(LinkedHashMap)中获取MemoryEntry,
      val entry = entries.remove(blockId)//返回移除块实体MemoryEntry
      if (entry != null) {
        currentMemory -= entry.size//更新当前可用内存
        logDebug(s"Block $blockId of size ${entry.size} dropped from memory (free $freeMemory)")
        true
      } else {
        false
      }
    }
  }
//清空entries,并令currentMemory为0
  override def clear() {
    entries.synchronized {
      entries.clear()
      currentMemory = 0
    }
    logInfo("MemoryStore cleared")
  }

  /**
   * Unroll the given block in memory safely.
   * 安全展开,为了防止写入内存的数据过大,导致内存溢出
   * Spark采用了一种优化方案:在正式写入内存之前,先用逻辑方式申请内存,如果申请成功,再写入内存
   * 这个过程称为安全展开
   * The safety of this operation refers to avoiding potential OOM exceptions caused by
   * unrolling the entirety of the block in memory at once. This is achieved by periodically
   * checking whether the memory restrictions for unrolling blocks are still satisfied,
   * stopping immediately if not. This check is a safeguard against the scenario in which
   * there is not enough free memory to accommodate the entirety of a single block.
   *
    *此操作的安全性是指避免由于一次性在内存中展开整个块而导致的潜在OOM异常。这是通过定期实现的
    *检查展开块的内存限制是否仍然满足,如果不能立即停止,此检查是针对不足够的可用内存以容纳单个块的整体的情况的保护。

   * This method returns either an array with the contents of the entire block or an iterator
   * containing the values of the block (if the array would have exceeded available memory).
    * 此方法返回具有整个块的内容的数组或包含块的值的迭代器(如果数组将超过可用内存)
    * Either 一个函数(或方法)在传入不同参数时会返回不同的值,返回值是两个不相关的类型
    * 分别为：Left和Right,惯例中我们一般认为Left包含错误或无效值,Right包含正确或有效值
   */
  def unrollSafely(
      blockId: BlockId,
      values: Iterator[Any],
      droppedBlocks: ArrayBuffer[(BlockId, BlockStatus)])
    : Either[Array[Any], Iterator[Any]] = {

    // Number of elements unrolled so far
    //展开的元素数量
    var elementsUnrolled = 0
    // Whether there is still enough memory for us to continue unrolling this block
    //标记是否有足够的内存可以继续展开Block
    var keepUnrolling = true
    // Initial per-task memory to request for unrolling blocks (bytes). Exposed for testing.
    //每个线程用来展开Block的初始内存阀值
    val initialMemoryThreshold = unrollMemoryThreshold
    // How often to check whether we need to request more memory
    //经过多少次展开内存后,判断是否需要申请更多内存
    val memoryCheckPeriod = 16
    // Memory currently reserved by this task for this particular unrolling operation
    //当前线程保留的用于特殊展开操作的内存值
    var memoryThreshold = initialMemoryThreshold
    // Memory to request as a multiple of current vector size
    //存储器请求作为当前矢量大小的倍数
    //内存请求因子
    val memoryGrowthFactor = 1.5
    // Previous unroll memory held by this task, for releasing later (only at the very end)
    //之前当前线程已经展开的驻留的内存大小,当前线程增加的展开内存,最后会释放
    val previousMemoryReserved = currentUnrollMemoryForThisTask
    // Underlying vector for unrolling the block
    //跟踪展开的内存
    var vector = new SizeTrackingVector[Any]

    // Request enough memory to begin unrolling
    //标记是否有足够的内存可以继续展开Block
    keepUnrolling = reserveUnrollMemoryForThisTask(initialMemoryThreshold)

    if (!keepUnrolling) {
      logWarning(s"Failed to reserve initial memory threshold of " +
        s"${Utils.bytesToString(initialMemoryThreshold)} for computing block $blockId in memory.")
    }

    // Unroll this block safely, checking whether we have exceeded our threshold periodically
    //安全地展开此块,检查我们是否定期超出阈值
    try {
      while (values.hasNext && keepUnrolling) {//values有元素并keepUnrolling足够的内存可用
        vector += values.next()//则vector添加values对象,
        if (elementsUnrolled % memoryCheckPeriod == 0) {//
          // If our vector's size has exceeded the threshold, request more memory
          //则开始检查currentSize是否已经比memoryThreshold大?
          val currentSize = vector.estimateSize()//评估集合中的对象内存大小
          if (currentSize >= memoryThreshold) {
            //如果对象内存大小currentSize超过memoryThreshold初始值1G,则需要申请内存
            //申请内个大小
            val amountToRequest = (currentSize * memoryGrowthFactor - memoryThreshold).toLong
            // Hold the accounting lock, in case another thread concurrently puts a block that
            // takes up the unrolling space we just ensured here
            //持有访问锁,如果另一个线程并发定入块的所占用的空间,只在这里展开了
            accountingLock.synchronized {
              if (!reserveUnrollMemoryForThisTask(amountToRequest)) {//如果失败
                // If the first request is not granted, try again after ensuring free space
                // If there is still not enough space, give up and drop the partition
                //如果maxUnrollMemory(当前Driver或者Exector展开Block最大占用内存)
                //>currentUnrollMemory(所有线程展开的Block的内存之和)
                val spaceToEnsure = maxUnrollMemory - currentUnrollMemory
                if (spaceToEnsure > 0) {
                  //则要求释放当前blockId内存,
                  val result = ensureFreeSpace(blockId, spaceToEnsure)
                  droppedBlocks ++= result.droppedBlocks
                }
                //申请到的内存大小会先保存到unrollMemoryMap集合中Key->taskid,value->memory
                keepUnrolling = reserveUnrollMemoryForThisTask(amountToRequest)
              }
            }
            // New threshold is currentSize * memoryGrowthFactor
            //新阈值为currentSize * memoryGrowthFactor
            //elementsUnrolled自增1
            memoryThreshold += amountToRequest
          }
        }
        elementsUnrolled += 1
      }
      //根据是否将Block完整地放入内存,以数组或者迭代器形式返回 Vector的数据
      if (keepUnrolling) {
        // We successfully unrolled the entirety of this block
        //我们成功地展开了这个块的整体
        Left(vector.toArray)
      } else {
        // We ran out of space while unrolling the values for this block
        //在展开该块的值时，我们用完了空间
        logUnrollFailureMessage(blockId, vector.estimateSize())
        Right(vector.iterator ++ values)
      }

    } finally {
      // If we return an array, the values returned will later be cached in `tryToPut`.
      // In this case, we should release the memory after we cache the block there.
      // Otherwise, if we return an iterator, we release the memory reserved here
      // later when the task finishes.
      //如果我们返回一个数组，返回的值将被稍后缓存在`tryToPut`中,在这种情况下,我们应该在缓存块后释放内存。
      // 否则,如果我们返回一个迭代器,我们会在任务完成后释放保留的内存。
      if (keepUnrolling) {
        //计算本次展开块实际占用的空间amountToRelease,并更新unrollMemoryMap中当前任务线程占用的内存大小
        accountingLock.synchronized {
          val amountToRelease = currentUnrollMemoryForThisTask - previousMemoryReserved
          releaseUnrollMemoryForThisTask(amountToRelease)
          reservePendingUnrollMemoryForThisTask(amountToRelease)
        }
      }
    }
  }

  /**
   * Return the RDD ID that a given block ID is from, or None if it is not an RDD block.
   * 返回给定块ID的RDD ID,如果不是RDD块,则返回None。
   */
  private def getRddId(blockId: BlockId): Option[Int] = {
    blockId.asRDDId.map(_.rddId)
  }

  private def tryToPut(
      blockId: BlockId,
      value: Any,
      size: Long,
      deserialized: Boolean): ResultWithDroppedBlocks = {
    tryToPut(blockId, () => value, size, deserialized)
  }

  /**
   * Try to put in a set of values, if we can free up enough space. The value should either be
   * an Array if deserialized is true or a ByteBuffer otherwise. Its (possibly estimated) size
   * must also be passed by the caller.
    *
   * 尝试放置一套值，如果我们可以释放足够的空间,如果反序列化为true,则该值应为数组,否则为ByteBuffer,它的(可能估计的)大小也必须由调用者传递。
    *
   * `value` will be lazily created. If it cannot be put into MemoryStore or disk, `value` won't be
   * created to avoid OOM since it may be a big ByteBuffer.
   *`value`将被懒惰创建。 如果它不能放入MemoryStore或磁盘，`value`将不会创建为避免OOM，因为它可能是一个大的ByteBuffer。
    *
   * Synchronize on `accountingLock` to ensure that all the put requests and its associated block
   * dropping is done by only on thread at a time. Otherwise while one thread is dropping
   * blocks to free memory for one block, another thread may use up the freed space for
   * another block.
   * 在“accountingLock”上进行同步，以确保所有的put请求及其关联的块丢弃都只能在线程上完成,
    * 否则，当一个线程正在丢弃块以释放一个块的内存时，另一个线程可能会将释放的空间用于另一个块。
    *
   * Return whether put was successful, along with the blocks dropped in the process.
   * 返回是否成功,以及在进程中删除的块。
   */
  private def tryToPut(
      blockId: BlockId,
      value: () => Any,
      size: Long,
      deserialized: Boolean): ResultWithDroppedBlocks = {

    /* TODO: Its possible to optimize the locking by locking entries only when selecting blocks
     * to be dropped. Once the to-be-dropped blocks have been selected, and lock on entries has
     * been released, it must be ensured that those to-be-dropped blocks are not double counted
     * for freeing up more space for another block that needs to be put. Only then the actually
     * dropping of blocks (and writing to disk if necessary) can proceed in parallel.
      * 被丢弃一旦选择了要删除的块,并且已经释放了条目锁定,则必须确保那些被丢弃的块不会被重新计算,
      * 以释放需要放置的另一个块的更多空间,只有这样,实际上丢弃块（如果需要,写入磁盘）可以并行进行。
      * */
   //写入数据是否成功
    var putSuccess = false
    //移除块列表 Key->BlockId value->BlockStatus
    val droppedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]

    accountingLock.synchronized {
      //因为在展开阶段,即便内存充足,当真正写入数据时依然可能内存不足,所以需要再次确认空闲内存是否充足
      val freeSpaceResult = ensureFreeSpace(blockId, size)
      //确认是否有足够内存
      val enoughFreeSpace = freeSpaceResult.success
      droppedBlocks ++= freeSpaceResult.droppedBlocks
     
      if (enoughFreeSpace) { //判断内存充足
        //则创建MemoryEntry对象
        val entry = new MemoryEntry(value(), size, deserialized)
        entries.synchronized {
          //将blockId与MemoryEntry对象放入entries(LinkedHashMap)中
          entries.put(blockId, entry)
          //currentMemory增加估算对象内存大小size
          currentMemory += size
        }
        val valuesOrBytes = if (deserialized) "values" else "bytes"
        logInfo("Block %s stored as %s in memory (estimated size %s, free %s)".format(
          blockId, valuesOrBytes, Utils.bytesToString(size), Utils.bytesToString(freeMemory)))
       //写入内存成功
        putSuccess = true
      } else {//如果内存不充足
        // Tell the block manager that we couldn't put it in memory so that it can drop it to
        // disk if the block allows disk storage.
        //告诉块管理器,我们无法将其放在内存中,以便如果块允许磁盘存储,它可以将其放到磁盘上。
        //如果此时内存不足,还要把blockId对应MemoryEntry对象迁移到磁盘或清除
        lazy val data = if (deserialized) {
          Left(value().asInstanceOf[Array[Any]])
        } else {
          Right(value().asInstanceOf[ByteBuffer].duplicate())
        }
        //将blockId从内存中移出,可能把它放在磁盘上,返回移除块的信息
        val droppedBlockStatus = blockManager.dropFromMemory(blockId, () => data)
        droppedBlockStatus.foreach { status => droppedBlocks += ((blockId, status)) }
      }
      // Release the unroll memory used because we no longer need the underlying Array
      //释放使用的展开内存,因为我们不再需要底层数组
      //释放展开的内存
      releasePendingUnrollMemoryForThisTask()
    }
    ResultWithDroppedBlocks(putSuccess, droppedBlocks)
  }

  /**
   * Try to free up a given amount of space to store a particular block, but can fail if
   * either the block is bigger than our memory or it would require replacing another block
   * from the same RDD (which leads to a wasteful cyclic replacement pattern for RDDs that
   * don't fit into memory that we want to avoid).
    *
    * 尝试释放一定量的空间来存储特定的块,但是如果块大于我们的内存,则可能会失败,
    * 否则将需要从相同的RDD替换另一个块(这导致RDD的浪费循环替换模式,不适合我们想避免的记忆)。
   *
   * Assume that `accountingLock` is held by the caller to ensure only one thread is dropping
   * blocks. Otherwise, the freed space may fill up before the caller puts in their new value.
    * 假设`accountingLock`由调用者保存,以确保只有一个线程正在丢弃块,否则,释放的空间可能会在调用者提供新值之前填满。
   *
   * Return whether there is enough free space, along with the blocks dropped in the process.
    * 返回是否有足够的可用空间,以及在进程中丢弃的块。
   * 确认是否有足够内存,如果不足,会释放MemoryEntry占用的内存
   */
  private def ensureFreeSpace(
      blockIdToAdd: BlockId,
      space: Long): ResultWithDroppedBlocks = {
    logInfo(s"ensureFreeSpace($space) called with curMem=$currentMemory, maxMem=$maxMemory")
    //移除块的列表Key->BlockId value->BlockStatus
    val droppedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]
    //space 需要腾出的内存大小
    if (space > maxMemory) {
      //Space不能超过maxMemory的限制
      logInfo(s"Will not store $blockIdToAdd as it is larger than our memory limit")
      return ResultWithDroppedBlocks(success = false, droppedBlocks)
    }

    // Take into account the amount of memory currently occupied by unrolling blocks
    // and minus the pending unroll memory for that block on current thread.
    //考虑到展开块占用的内存量,并减去当前线程上该块的待处理展开内存。
   //获得当前任务Id
    val taskAttemptId = currentTaskAttemptId()
    //实际空闲的内存,
    val actualFreeMemory = freeMemory - currentUnrollMemory +
      pendingUnrollMemoryMap.getOrElse(taskAttemptId, 0L)//线程任务内存
     
     //如果actualFreeMemory小于space,则说明空闲空间不足,需要释放一部已经占用的内存
    if (actualFreeMemory < space) {
      //blockIdToAdd 将要添加的Block对应的BlockId
      val rddToAdd = getRddId(blockIdToAdd)
      //已经选择要从内存中腾出的Block对应的BlockId的数组
      val selectedBlocks = new ArrayBuffer[BlockId]
      //selectedMemory中的所有块的总大小
      var selectedMemory = 0L

      // This is synchronized to ensure that the set of entries is not changed
      // (because of getValue or getBytes) while traversing the iterator, as that
      // can lead to exceptions.
      //这是同步的,以确保在遍历迭代器时不会更改条目集(因为getValue或getBytes),因为这可能会导致异常。
      //用于存放Block,类型LinkedHashMap
      entries.synchronized {
        val iterator = entries.entrySet().iterator()
        //实际空闲的内存,space需要腾出的内存,迭代
        while (actualFreeMemory + selectedMemory < space && iterator.hasNext) {
          val pair = iterator.next()//Entry[BlockId, MemoryEntry]
          val blockId = pair.getKey//BlockId
          //如果blockIdToAdd的RddId是空或者blockIdToAdd的rddId不等于MemoryEntry对应block的rddId
          //排除当前RDD自身在MemoryStore中存储的Block
          if (rddToAdd.isEmpty || rddToAdd != getRddId(blockId)) {
            selectedBlocks += blockId//则将blockId加入selectedBlocks
            selectedMemory += pair.getValue.size//将selectedMemory增加MemoryEntry的大小
          }
        }
      }
      //实际空闲的内存actualFreeMemory+selectedMemory大于等于需要腾出的内存
      if (actualFreeMemory + selectedMemory >= space) {//则说明可以腾出足够的内存空间
        logInfo(s"${selectedBlocks.size} blocks selected for dropping")
        for (blockId <- selectedBlocks) {
          //迭代selectedBlocks,将selectedBlocks中所有的blockId对应的entries里的MemoryEntry取出          
          val entry = entries.synchronized { entries.get(blockId) }
          // This should never be null as only one task should be dropping
          // blocks and removing entries. However the check is still here for
          // future safety.
          //这不应该是空的,因为只有一个任务应该是删除块和删除条目,但是,这项支票仍然在未来的安全
          if (entry != null) {
            //判断是否可以反序列化,
            val data = if (entry.deserialized) {
              //转换Array[Any]
              Left(entry.value.asInstanceOf[Array[Any]])
            } else {//转换 ByteBuffer
              Right(entry.value.asInstanceOf[ByteBuffer].duplicate())
            }
             //调用dropFromMemory方法,从内存中移除blockId及MemoryEntry,最终返回移除block的状态
            val droppedBlockStatus = blockManager.dropFromMemory(blockId, data)
            droppedBlockStatus.foreach { status => droppedBlocks += ((blockId, status)) }
          }
        }
        return ResultWithDroppedBlocks(success = true, droppedBlocks)
      } else {
        logInfo(s"Will not store $blockIdToAdd as it would require dropping another block " +
          "from the same RDD")
        return ResultWithDroppedBlocks(success = false, droppedBlocks)
      }
    }
    //如果actualFreeMemory大于等于space,说明此时已经有充足的内存,不需要释放内存空间,直接返回
    ResultWithDroppedBlocks(success = true, droppedBlocks)
  }

  override def contains(blockId: BlockId): Boolean = {
    entries.synchronized { entries.containsKey(blockId) }
  }
/**
 * 获得当前Task ID
 */
  private def currentTaskAttemptId(): Long = {
    // In case this is called on the driver, return an invalid task attempt id.
    //如果Driver上调用,则返回一个无效的任务尝试标识
    Option(TaskContext.get()).map(_.taskAttemptId()).getOrElse(-1L)
  }

  /**
   * Reserve additional memory for unrolling blocks used by this task.
   * Return whether the request is granted.
   * 申请到的内存大小会先保存到unrollMemoryMap集合中Key->taskid,value->memory
   */
  def reserveUnrollMemoryForThisTask(memory: Long): Boolean = {
    accountingLock.synchronized {
     //确认当前可用内存大于 当前所有展开内存之和 + 对象申请内存
      val granted = freeMemory > currentUnrollMemory + memory
      if (granted) {
        //获取当前任务ID
        val taskAttemptId = currentTaskAttemptId()
        //key为Task的Id,value为线程展开的所有块的内存大小总和
        unrollMemoryMap(taskAttemptId) = unrollMemoryMap.getOrElse(taskAttemptId, 0L) + memory
      }
      granted
    }
  }

  /**
   * Release memory used by this task for unrolling blocks.
   * If the amount is not specified, remove the current task's allocation altogether.
   * 释放当前任务中展开的内存,如果未指定内存,则全部删除当前任务的分配的内存  
   */
  def releaseUnrollMemoryForThisTask(memory: Long = -1L): Unit = {
    val taskAttemptId = currentTaskAttemptId()
    accountingLock.synchronized {
      if (memory < 0) { 
        unrollMemoryMap.remove(taskAttemptId)
      } else {        
        unrollMemoryMap(taskAttemptId) = unrollMemoryMap.getOrElse(taskAttemptId, memory) - memory
        // If this task claims no more unroll memory, release it completely
        // 如果这项任务要求不再打开记忆,完全释放它
        if (unrollMemoryMap(taskAttemptId) <= 0) {
          unrollMemoryMap.remove(taskAttemptId)
        }
      }
    }
  }

  /**
   * Reserve the unroll memory of current unroll successful block used by this task
   * until actually put the block into memory entry.
   * 清理当前task已经展开的block对应的预展开的内存,释放更多的空间
   */
  def reservePendingUnrollMemoryForThisTask(memory: Long): Unit = {
    val taskAttemptId = currentTaskAttemptId()
    accountingLock.synchronized {     
       pendingUnrollMemoryMap(taskAttemptId) =
         pendingUnrollMemoryMap.getOrElse(taskAttemptId, 0L) + memory
    }
  }

  /**
   * Release pending unroll memory of current unroll successful block used by this task
   * 清理当前task已经展开的block对应的预展开的内存,释放更多的空间
   */
  def releasePendingUnrollMemoryForThisTask(): Unit = {
    val taskAttemptId = currentTaskAttemptId()
    accountingLock.synchronized {
      pendingUnrollMemoryMap.remove(taskAttemptId)
    }
  }

  /**
   * Return the amount of memory currently occupied for unrolling blocks across all tasks.
   * 所有展开和展开期间的Block的内存之和,即当前Driver或者Executor中所有线程展开的Block的内存之和
   */
  def currentUnrollMemory: Long = accountingLock.synchronized {
    unrollMemoryMap.values.sum + pendingUnrollMemoryMap.values.sum
  }

  /**
   * Return the amount of memory currently occupied for unrolling blocks by this task.
   * 返回当前占用展开任务占用的内存大小
   */
  def currentUnrollMemoryForThisTask: Long = accountingLock.synchronized {
    unrollMemoryMap.getOrElse(currentTaskAttemptId(), 0L)
  }

  /**
   * Return the number of tasks currently unrolling blocks.
   * 返回任务目前展开的块的数量
   */
  def numTasksUnrolling: Int = accountingLock.synchronized { unrollMemoryMap.keys.size }

  /**
   * Log information about current memory usage.
   * 关于当前内存使用的日志信息。
   */
  def logMemoryUsage(): Unit = {
    val blocksMemory = currentMemory
    val unrollMemory = currentUnrollMemory
    val totalMemory = blocksMemory + unrollMemory
    logInfo(
      s"Memory use = ${Utils.bytesToString(blocksMemory)} (blocks) + " +
      s"${Utils.bytesToString(unrollMemory)} (scratch space shared across " +
      s"$numTasksUnrolling tasks(s)) = ${Utils.bytesToString(totalMemory)}. " +
      s"Storage limit = ${Utils.bytesToString(maxMemory)}."
    )
  }

  /**
   * Log a warning for failing to unroll a block.
   * 日志警告展开块失败
   * @param blockId ID of the block we are trying to unroll.
   * @param finalVectorSize Final size of the vector before unrolling failed.
   */
  def logUnrollFailureMessage(blockId: BlockId, finalVectorSize: Long): Unit = {
    logWarning(
      s"Not enough space to cache $blockId in memory! " +
      s"(computed ${Utils.bytesToString(finalVectorSize)} so far)"
    )
    logMemoryUsage()
  }
}
/**
 * 移除块类
 */
private[spark] case class ResultWithDroppedBlocks(
    success: Boolean,//是否成功
    droppedBlocks: Seq[(BlockId, BlockStatus)])//移除块列表
