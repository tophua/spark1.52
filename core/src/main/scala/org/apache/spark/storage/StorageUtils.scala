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

import scala.collection.Map
import scala.collection.mutable

import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 * Storage information for each BlockManager.
 * 每个BlockManager存储的状态信息,
  *
 * This class assumes BlockId and BlockStatus are immutable, such that the consumers of this
 * class cannot mutate the source of the information. Accesses are not thread-safe.
  * 这个类假定BlockId和BlockStatus是不可变的,使得这个类的消费者不能突变信息的来源,访问不是线程安全的
 */
@DeveloperApi
class StorageStatus(val blockManagerId: BlockManagerId, val maxMem: Long) {

  /**
   * Internal representation of the blocks stored in this block manager.
   * 内部表示存储在块管理器中的块,存储RDD块充许快速检索RDD块
   * We store RDD blocks and non-RDD blocks separately to allow quick retrievals of RDD blocks.
   * These collections should only be mutated through the add/update/removeBlock methods.
    * 我们分别存储RDD块和非RDD块,以便快速检索RDD块,这些集合只能通过add/update/removeBlock方法进行突变
   */
  private val _rddBlocks = new mutable.HashMap[Int, mutable.Map[BlockId, BlockStatus]]
  private val _nonRddBlocks = new mutable.HashMap[BlockId, BlockStatus]

  /**
   * Storage information of the blocks that entails memory, disk, and off-heap memory usage.
   * 存储的块的存储信息,包括内存、磁盘和内存堆内存使用情况,
   * As with the block maps, we store the storage information separately for RDD blocks and
   * non-RDD blocks for the same reason. In particular, RDD storage information is stored
   * in a map indexed by the RDD ID to the following 4-tuple:
   *  与块映射一样,由于相同的原因,我们将RDD块和非RDD块分别存储存储信息
    *  特别地,RDD存储信息存储在由RDD ID索引的映射到以下4元组中：
   *   (memory size, disk size, off-heap size, storage level)
   *   （内存大小,磁盘大小,堆内大小,存储级别）
   * We assume that all the blocks that belong to the same RDD have the same storage level.
   * This field is not relevant to non-RDD blocks, however, so the storage information for
   * non-RDD blocks contains only the first 3 fields (in the same order).
    * 我们假设属于同一个RDD的所有块具有相同的存储级别。但是，该字段与非RDD块无关，所以相关但非RDD块存储信息只包含前3个字段。
   */
  private val _rddStorageInfo = new mutable.HashMap[Int, (Long, Long, Long, StorageLevel)]
  private var _nonRddStorageInfo: (Long, Long, Long) = (0L, 0L, 0L)

  /** Create a storage status with an initial set of blocks, leaving the source unmodified.
    * 创建具有初始块块的存储状态,使源不被修改 */
  def this(bmid: BlockManagerId, maxMem: Long, initialBlocks: Map[BlockId, BlockStatus]) {
    this(bmid, maxMem)
    initialBlocks.foreach { case (bid, bstatus) => addBlock(bid, bstatus) }
  }

  /**
   * Return the blocks stored in this block manager.
   * 返回存储在块管理器中的块
   * Note that this is somewhat expensive, as it involves cloning the underlying maps and then
   * concatenating them together. Much faster alternatives exist for common operations such as
   * contains, get, and size.
    * 请注意,这有点贵,因为它涉及克隆底层Map,然后将它们连接在一起,对于常用操作(如contains，get和size),存在更快的替代方案
   */
  def blocks: Map[BlockId, BlockStatus] = _nonRddBlocks ++ rddBlocks

  /**
   * Return the RDD blocks stored in this block manager.
    * 返回存储在此块管理器中的RDD块
   *
   * Note that this is somewhat expensive, as it involves cloning the underlying maps and then
   * concatenating them together. Much faster alternatives exist for common operations such as
   * getting the memory, disk, and off-heap memory sizes occupied by this RDD.
    * 请注意,这有点贵,因为它涉及克隆底层Map,然后将它们连接在一起,
    * 通常的操作可能存在更快的备选方案,例如获取此RDD占用的内存,磁盘和非堆内存大小。
   */
  def rddBlocks: Map[BlockId, BlockStatus] = _rddBlocks.flatMap { case (_, blocks) => blocks }

  /** Return the blocks that belong to the given RDD stored in this block manager.
    * 返回属于此块管理器中存储的给定RDD的块。*/
  def rddBlocksById(rddId: Int): Map[BlockId, BlockStatus] = {
    _rddBlocks.get(rddId).getOrElse(Map.empty)
  }

  /** Add the given block to this storage status. If it already exists, overwrite it.
    * 将给定的块添加到此存储状态。 如果它已经存在，覆盖它 */
  private[spark] def addBlock(blockId: BlockId, blockStatus: BlockStatus): Unit = {
    updateStorageInfo(blockId, blockStatus)
    blockId match {
      case RDDBlockId(rddId, _) =>
        _rddBlocks.getOrElseUpdate(rddId, new mutable.HashMap)(blockId) = blockStatus
      case _ =>
        _nonRddBlocks(blockId) = blockStatus
    }
  }

  /** Update the given block in this storage status. If it doesn't already exist, add it.
    * 更新此存储状态中的给定块。 如果它不存在，请添加它。*/
  private[spark] def updateBlock(blockId: BlockId, blockStatus: BlockStatus): Unit = {
    addBlock(blockId, blockStatus)
  }

  /** Remove the given block from this storage status.
    * 从给定的块中删除此存储状态。 */
  private[spark] def removeBlock(blockId: BlockId): Option[BlockStatus] = {
    updateStorageInfo(blockId, BlockStatus.empty)
    blockId match {
      case RDDBlockId(rddId, _) =>
        // Actually remove the block, if it exists
        //实际删除块，如果存在
        if (_rddBlocks.contains(rddId)) {
          val removed = _rddBlocks(rddId).remove(blockId)
          // If the given RDD has no more blocks left, remove the RDD
          //如果给定的RDD没有更多的块，则删除RDD
          if (_rddBlocks(rddId).isEmpty) {
            _rddBlocks.remove(rddId)
          }
          removed
        } else {
          None
        }
      case _ =>
        _nonRddBlocks.remove(blockId)
    }
  }

  /**
   * Return whether the given block is stored in this block manager in O(1) time.
    * 在O（1）时间内返回给定的块是否存储在该块管理器中。
   * Note that this is much faster than `this.blocks.contains`, which is O(blocks) time.
    * 请注意，这比“this.blocks.contains”快得多,这是O(块)时间。
   */
  def containsBlock(blockId: BlockId): Boolean = {
    blockId match {
      case RDDBlockId(rddId, _) =>
        _rddBlocks.get(rddId).exists(_.contains(blockId))
      case _ =>
        _nonRddBlocks.contains(blockId)
    }
  }

  /**
   * Return the given block stored in this block manager in O(1) time.
    * 在O（1）时间内返回存储在此块管理器中的给定块
   * Note that this is much faster than `this.blocks.get`, which is O(blocks) time.
    * 请注意，这比“this.blocks.get”快得多,这是O(块)时间。
   */
  def getBlock(blockId: BlockId): Option[BlockStatus] = {
    blockId match {
      case RDDBlockId(rddId, _) =>
        _rddBlocks.get(rddId).map(_.get(blockId)).flatten
      case _ =>
        _nonRddBlocks.get(blockId)
    }
  }

  /**
   * Return the number of blocks stored in this block manager in O(RDDs) time.
    * 在O（RDD）时间内返回存储在此块管理器中的块数
   * Note that this is much faster than `this.blocks.size`, which is O(blocks) time.
    * 请注意，这比“this.blocks.size”快得多，这是O（块）时间
   */
  def numBlocks: Int = _nonRddBlocks.size + numRddBlocks

  /**
   * Return the number of RDD blocks stored in this block manager in O(RDDs) time.
    * 在O（RDD）时间内返回存储在该块管理器中的RDD块的数量。
   * Note that this is much faster than `this.rddBlocks.size`, which is O(RDD blocks) time.
    * 请注意，这比“this.rddBlocks.size”快得多，这是O（RDD块）时间。
   */
  def numRddBlocks: Int = _rddBlocks.values.map(_.size).sum

  /**
   * Return the number of blocks that belong to the given RDD in O(1) time.
   * Note that this is much faster than `this.rddBlocksById(rddId).size`, which is
   * O(blocks in this RDD) time.
    * 返回O（1）时间内属于给定RDD的块数。请注意，这比“this.rddBlocksById(rddId).size”快得多，这是O（在此RDD中阻塞）时间。
   */
  def numRddBlocksById(rddId: Int): Int = _rddBlocks.get(rddId).map(_.size).getOrElse(0)

  /** 
   *  Return the memory remaining in this block manager.
   *  返回块管理器中剩余的内存
   *  */
  def memRemaining: Long = maxMem - memUsed

  /** 
   *  Return the memory used by this block manager.
   *  返回已使用内存 
   *  */
  def memUsed: Long = _nonRddStorageInfo._1 + _rddBlocks.keys.toSeq.map(memUsedByRdd).sum

  /** 
   *  Return the disk space used by this block manager. 
   *  返回硬盘空间使用大小
   *  */
  def diskUsed: Long = _nonRddStorageInfo._2 + _rddBlocks.keys.toSeq.map(diskUsedByRdd).sum

  /** 
   *  Return the off-heap space used by this block manager. 
   *  返回此块管理器使用的堆内存空间
   *  */
  def offHeapUsed: Long = _nonRddStorageInfo._3 + _rddBlocks.keys.toSeq.map(offHeapUsedByRdd).sum

  /**
   *  Return the memory used by the given RDD in this block manager in O(1) time.
    *  在O（1）时间内返回此块管理器中给定RDD使用的内存。
   *  返回给定的RDD块管理使用的内存
   *   */
  def memUsedByRdd(rddId: Int): Long = _rddStorageInfo.get(rddId).map(_._1).getOrElse(0L)

  /**
    * Return the disk space used by the given RDD in this block manager in O(1) time.
    * 在O（1）时间内返回给定RDD在该块管理器中使用的磁盘空间。
    *  */
  def diskUsedByRdd(rddId: Int): Long = _rddStorageInfo.get(rddId).map(_._2).getOrElse(0L)

  /**
    * Return the off-heap space used by the given RDD in this block manager in O(1) time.
    * 在O（1）时间内返回给定RDD在该块管理器中使用的堆栈外空间
    * */
  def offHeapUsedByRdd(rddId: Int): Long = _rddStorageInfo.get(rddId).map(_._3).getOrElse(0L)

  /**
    * Return the storage level, if any, used by the given RDD in this block manager.
    * 返回此块管理器中给定RDD使用的存储级别(如果有)
    *  */
  def rddStorageLevel(rddId: Int): Option[StorageLevel] = _rddStorageInfo.get(rddId).map(_._4)

  /**
   * Update the relevant storage info, taking into account any existing status for this block.
    * 考虑到该块的任何现有状态,更新相关的存储信息
   */
  private def updateStorageInfo(blockId: BlockId, newBlockStatus: BlockStatus): Unit = {
    val oldBlockStatus = getBlock(blockId).getOrElse(BlockStatus.empty)
    val changeInMem = newBlockStatus.memSize - oldBlockStatus.memSize
    val changeInDisk = newBlockStatus.diskSize - oldBlockStatus.diskSize
    val changeInExternalBlockStore =
      newBlockStatus.externalBlockStoreSize - oldBlockStatus.externalBlockStoreSize
    val level = newBlockStatus.storageLevel

    // Compute new info from old info
    //从旧信息计算新信息
    val (oldMem, oldDisk, oldExternalBlockStore) = blockId match {
      case RDDBlockId(rddId, _) =>
        _rddStorageInfo.get(rddId)
          .map { case (mem, disk, externalBlockStore, _) => (mem, disk, externalBlockStore) }
          .getOrElse((0L, 0L, 0L))
      case _ =>
        _nonRddStorageInfo
    }
    val newMem = math.max(oldMem + changeInMem, 0L)
    val newDisk = math.max(oldDisk + changeInDisk, 0L)
    val newExternalBlockStore = math.max(oldExternalBlockStore + changeInExternalBlockStore, 0L)

    // Set the correct info
    //设置正确的信息
    blockId match {
      case RDDBlockId(rddId, _) =>
        // If this RDD is no longer persisted, remove it
        //如果此RDD不再存在,请将其删除
        if (newMem + newDisk + newExternalBlockStore == 0) {
          _rddStorageInfo.remove(rddId)
        } else {
          _rddStorageInfo(rddId) = (newMem, newDisk, newExternalBlockStore, level)
        }
      case _ =>
        _nonRddStorageInfo = (newMem, newDisk, newExternalBlockStore)
    }
  }

}

/** Helper methods for storage-related objects.
  * 存储相关对象的辅助方法*/
private[spark] object StorageUtils {

  /**
   * Update the given list of RDDInfo with the given list of storage statuses.
   * 存储状态给定的列表更新RDDInfo给出的列表
   * This method overwrites the old values stored in the RDDInfo's.
    * 此方法将覆盖存储在RDDInfo中的旧值
   */
  def updateRddInfo(rddInfos: Seq[RDDInfo], statuses: Seq[StorageStatus]): Unit = {
    rddInfos.foreach { rddInfo =>
      val rddId = rddInfo.id
      // Assume all blocks belonging to the same RDD have the same storage level
      //假设属于同一RDD的所有块具有相同的存储级别
      val storageLevel = statuses
        .flatMap(_.rddStorageLevel(rddId)).headOption.getOrElse(StorageLevel.NONE)
      val numCachedPartitions = statuses.map(_.numRddBlocksById(rddId)).sum
      val memSize = statuses.map(_.memUsedByRdd(rddId)).sum
      val diskSize = statuses.map(_.diskUsedByRdd(rddId)).sum
      val externalBlockStoreSize = statuses.map(_.offHeapUsedByRdd(rddId)).sum

      rddInfo.storageLevel = storageLevel
      rddInfo.numCachedPartitions = numCachedPartitions
      rddInfo.memSize = memSize
      rddInfo.diskSize = diskSize
      rddInfo.externalBlockStoreSize = externalBlockStoreSize
    }
  }

  /**
   * Return a mapping from block ID to its locations for each block that belongs to the given RDD.
    * 从块ID返回到属于给定RDD的每个块的位置的映射
   */
  def getRddBlockLocations(rddId: Int, statuses: Seq[StorageStatus]): Map[BlockId, Seq[String]] = {
    val blockLocations = new mutable.HashMap[BlockId, mutable.ListBuffer[String]]
    statuses.foreach { status =>
      status.rddBlocksById(rddId).foreach { case (bid, _) =>
        val location = status.blockManagerId.hostPort
        blockLocations.getOrElseUpdate(bid, mutable.ListBuffer.empty) += location
      }
    }
    blockLocations
  }

}
