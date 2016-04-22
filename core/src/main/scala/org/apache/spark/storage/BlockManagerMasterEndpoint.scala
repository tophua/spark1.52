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

import java.util.{HashMap => JHashMap}

import scala.collection.immutable.HashSet
import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.concurrent.{ExecutionContext, Future}

import org.apache.spark.rpc.{RpcEndpointRef, RpcEnv, RpcCallContext, ThreadSafeRpcEndpoint}
import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.scheduler._
import org.apache.spark.storage.BlockManagerMessages._
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * 在Master跟踪所有Slave节点的Block的信息
 * BlockManagerMasterEndpoint is an [[ThreadSafeRpcEndpoint]] on the master node to track statuses
 * of all slaves' block managers.
 */
private[spark]
class BlockManagerMasterEndpoint(
    override val rpcEnv: RpcEnv,
    val isLocal: Boolean,
    conf: SparkConf,
    listenerBus: LiveListenerBus)
  extends ThreadSafeRpcEndpoint with Logging {
  //保存BlockManagerId到BlockManagerInfo的映射,BlockManagerInfo保存了slave节点的内存使用情况
  //slave上的Block状态,Master通过个BlockMasterSlaveActor的Reference可以向Slave发送命令查询请求
  // Mapping from block manager id to the block manager's information.
  private val blockManagerInfo = new mutable.HashMap[BlockManagerId, BlockManagerInfo]
 //保存executor ID到BlockManagerInfo的映射,这样Master就可以通过executor ID查找到BlockManagerId
  // Mapping from executor ID to block manager ID.
  private val blockManagerIdByExecutor = new mutable.HashMap[String, BlockManagerId]
  //保存Block是在那些BlockManager上的HashMap,由于Block可能在多个Slave上都有备份,因此注意Value是一个
  //不可变HashSet,通过查询blockLocations就可以查询到某个Block所在的物理位置
  // Mapping from block id to the set of block managers that have the block.
  private val blockLocations = new JHashMap[BlockId, mutable.HashSet[BlockManagerId]]

  private val askThreadPool = ThreadUtils.newDaemonCachedThreadPool("block-manager-ask-thread-pool")
  private implicit val askExecutionContext = ExecutionContext.fromExecutorService(askThreadPool)

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    //当BlockManager在创建后,向BlockManagerActor发送消息RegisterBlockManager进行注册
    //Master Actor保存该BlockManage所包含的Block等信息
    case RegisterBlockManager(blockManagerId, maxMemSize, slaveEndpoint) =>
      register(blockManagerId, maxMemSize, slaveEndpoint)
      context.reply(true)
   //向Master汇报Block的信息,Master会记录这些信息并且提供Slave查询
    case _updateBlockInfo @ UpdateBlockInfo(
      blockManagerId, blockId, storageLevel, deserializedSize, size, externalBlockStoreSize) =>
      context.reply(updateBlockInfo(
        blockManagerId, blockId, storageLevel, deserializedSize, size, externalBlockStoreSize))
      listenerBus.post(SparkListenerBlockUpdated(BlockUpdatedInfo(_updateBlockInfo)))
    //获得某个Block所在的位置信息,返回BlockManagerId组成的列表,Block可能在多个节点上都有备份
    case GetLocations(blockId) =>
      context.reply(getLocations(blockId))
    //获得某个Block所在的位置信息,返回BlockManagerId组成的列表,Block可能在多个节点上都有备份
    //一次获取多个Block的位置信息
    case GetLocationsMultipleBlockIds(blockIds) =>
      context.reply(getLocationsMultipleBlockIds(blockIds))
   //获得其他BlockManager的id
    case GetPeers(blockManagerId) =>
      context.reply(getPeers(blockManagerId))
    //根据executorId获取Executor的Thread Dump,获得了Executor的hostname和port后,会通过AkkA向Executor发送请求信息
    case GetRpcHostPortForExecutor(executorId) =>
      context.reply(getRpcHostPortForExecutor(executorId))
    //获取所有Executor的内存使用的状态,包括使用的最大的内存大小,剩余的内存大小
    case GetMemoryStatus =>
      context.reply(memoryStatus)
     //返回每个Executor的Storage的状态,包括每个Executor最大可用的内存数据和Block的信息
    case GetStorageStatus =>
      context.reply(storageStatus)
    //根据blockId和askSlaves向Master返回该Block的blockStatus
    case GetBlockStatus(blockId, askSlaves) =>
      context.reply(blockStatus(blockId, askSlaves))
    //根据BlockId获取Block的Status,如果askSlave为true,那么需要到所有的Slave上查询结果
    case GetMatchingBlockIds(filter, askSlaves) =>
      context.reply(getMatchingBlockIds(filter, askSlaves))
     //根据RddId删除该Excutor上RDD所关联的所有Block
    case RemoveRdd(rddId) =>
      context.reply(removeRdd(rddId))
    //根据shuffleId删除该Executor上所有和该Shuffle相关的Block
    case RemoveShuffle(shuffleId) =>
      context.reply(removeShuffle(shuffleId))
    //根据broadcastId删除该Executor上和该广播变量相关的所有Block
    case RemoveBroadcast(broadcastId, removeFromDriver) =>
      context.reply(removeBroadcast(broadcastId, removeFromDriver))
    //根据BlockId删除该Executor上所有和该Shuffle相关的Block
    case RemoveBlock(blockId) =>
      removeBlockFromWorkers(blockId)
      context.reply(true)
  //删除Master上保存的execId对应的Executor上的BlockManager的信息
    case RemoveExecutor(execId) =>
      removeExecutor(execId)
      context.reply(true)

    case StopBlockManagerMaster =>
      context.reply(true)
      stop()
    //接收DAGScheduler.executorHeartbeatReceived消息    
    case BlockManagerHeartbeat(blockManagerId) =>
      context.reply(heartbeatReceived(blockManagerId))

    case HasCachedBlocks(executorId) =>
      blockManagerIdByExecutor.get(executorId) match {
        case Some(bm) =>
          if (blockManagerInfo.contains(bm)) {
            val bmInfo = blockManagerInfo(bm)
            context.reply(bmInfo.cachedBlocks.nonEmpty)
          } else {
            context.reply(false)
          }
        case None => context.reply(false)
      }
  }

  private def removeRdd(rddId: Int): Future[Seq[Int]] = {
    // First remove the metadata for the given RDD, and then asynchronously remove the blocks
    // from the slaves.

    // Find all blocks for the given RDD, remove the block from both blockLocations and
    // the blockManagerInfo that is tracking the blocks.
    val blocks = blockLocations.keys.flatMap(_.asRDDId).filter(_.rddId == rddId)
    blocks.foreach { blockId =>
      val bms: mutable.HashSet[BlockManagerId] = blockLocations.get(blockId)
      bms.foreach(bm => blockManagerInfo.get(bm).foreach(_.removeBlock(blockId)))
      blockLocations.remove(blockId)
    }

    // Ask the slaves to remove the RDD, and put the result in a sequence of Futures.
    // The dispatcher is used as an implicit argument into the Future sequence construction.
    val removeMsg = RemoveRdd(rddId)
    Future.sequence(
      blockManagerInfo.values.map { bm =>
        bm.slaveEndpoint.ask[Int](removeMsg)
      }.toSeq
    )
  }

  private def removeShuffle(shuffleId: Int): Future[Seq[Boolean]] = {
    // Nothing to do in the BlockManagerMasterEndpoint data structures
    val removeMsg = RemoveShuffle(shuffleId)
    Future.sequence(
      blockManagerInfo.values.map { bm =>
        bm.slaveEndpoint.ask[Boolean](removeMsg)
      }.toSeq
    )
  }

  /**
   * Delegate RemoveBroadcast messages to each BlockManager because the master may not notified
   * of all broadcast blocks. If removeFromDriver is false, broadcast blocks are only removed
   * from the executors, but not from the driver.
   */
  private def removeBroadcast(broadcastId: Long, removeFromDriver: Boolean): Future[Seq[Int]] = {
    val removeMsg = RemoveBroadcast(broadcastId, removeFromDriver)
    val requiredBlockManagers = blockManagerInfo.values.filter { info =>
      removeFromDriver || !info.blockManagerId.isDriver
    }
    Future.sequence(
      requiredBlockManagers.map { bm =>
        bm.slaveEndpoint.ask[Int](removeMsg)
      }.toSeq
    )
  }

  private def removeBlockManager(blockManagerId: BlockManagerId) {
    val info = blockManagerInfo(blockManagerId)

    // Remove the block manager from blockManagerIdByExecutor.
    blockManagerIdByExecutor -= blockManagerId.executorId

    // Remove it from blockManagerInfo and remove all the blocks.
    blockManagerInfo.remove(blockManagerId)
    val iterator = info.blocks.keySet.iterator
    while (iterator.hasNext) {
      val blockId = iterator.next
      val locations = blockLocations.get(blockId)
      locations -= blockManagerId
      if (locations.size == 0) {
        blockLocations.remove(blockId)
      }
    }
    listenerBus.post(SparkListenerBlockManagerRemoved(System.currentTimeMillis(), blockManagerId))
    logInfo(s"Removing block manager $blockManagerId")
  }

  private def removeExecutor(execId: String) {
    logInfo("Trying to remove executor " + execId + " from BlockManagerMaster.")
    blockManagerIdByExecutor.get(execId).foreach(removeBlockManager)
  }

  /**
   * Return true if the driver knows about the given block manager. Otherwise, return false,
   * indicating that the block manager should re-register.
   * 最终更新BlockManagerMaster对BlockManager的最后可见时间(即更新Block-ManagerId对应的BlockManagerInfo的_lastSeenMS)
   */
  private def heartbeatReceived(blockManagerId: BlockManagerId): Boolean = {
    if (!blockManagerInfo.contains(blockManagerId)) {
      blockManagerId.isDriver && !isLocal
    } else {
      blockManagerInfo(blockManagerId).updateLastSeenMs()
      true
    }
  }

  // Remove a block from the slaves that have it. This can only be used to remove
  // blocks that the master knows about.
  private def removeBlockFromWorkers(blockId: BlockId) {
    val locations = blockLocations.get(blockId)
    if (locations != null) {
      locations.foreach { blockManagerId: BlockManagerId =>
        val blockManager = blockManagerInfo.get(blockManagerId)
        if (blockManager.isDefined) {
          // Remove the block from the slave's BlockManager.
          // Doesn't actually wait for a confirmation and the message might get lost.
          // If message loss becomes frequent, we should add retry logic here.
          blockManager.get.slaveEndpoint.ask[Boolean](RemoveBlock(blockId))
        }
      }
    }
  }

  // Return a map from the block manager id to max memory and remaining memory.
  private def memoryStatus: Map[BlockManagerId, (Long, Long)] = {
    blockManagerInfo.map { case(blockManagerId, info) =>
      (blockManagerId, (info.maxMem, info.remainingMem))
    }.toMap
  }

  private def storageStatus: Array[StorageStatus] = {
    blockManagerInfo.map { case (blockManagerId, info) =>
      new StorageStatus(blockManagerId, info.maxMem, info.blocks)
    }.toArray
  }

  /**
   * Return the block's status for all block managers, if any. NOTE: This is a
   * potentially expensive operation and should only be used for testing.
   *
   * If askSlaves is true, the master queries each block manager for the most updated block
   * statuses. This is useful when the master is not informed of the given block by all block
   * managers.
   */
  private def blockStatus(
      blockId: BlockId,
      askSlaves: Boolean): Map[BlockManagerId, Future[Option[BlockStatus]]] = {
    val getBlockStatus = GetBlockStatus(blockId)
    /*
     * Rather than blocking on the block status query, master endpoint should simply return
     * Futures to avoid potential deadlocks. This can arise if there exists a block manager
     * that is also waiting for this master endpoint's response to a previous message.
     */
    blockManagerInfo.values.map { info =>
      val blockStatusFuture =
        if (askSlaves) {
          info.slaveEndpoint.ask[Option[BlockStatus]](getBlockStatus)
        } else {
          Future { info.getStatus(blockId) }
        }
      (info.blockManagerId, blockStatusFuture)
    }.toMap
  }

  /**
   * Return the ids of blocks present in all the block managers that match the given filter.
   * NOTE: This is a potentially expensive operation and should only be used for testing.
   *
   * If askSlaves is true, the master queries each block manager for the most updated block
   * statuses. This is useful when the master is not informed of the given block by all block
   * managers.
   */
  private def getMatchingBlockIds(
      filter: BlockId => Boolean,
      askSlaves: Boolean): Future[Seq[BlockId]] = {
    val getMatchingBlockIds = GetMatchingBlockIds(filter)
    Future.sequence(
      blockManagerInfo.values.map { info =>
        val future =
          if (askSlaves) {
            info.slaveEndpoint.ask[Seq[BlockId]](getMatchingBlockIds)
          } else {
            Future { info.blocks.keys.filter(filter).toSeq }
          }
        future
      }
    ).map(_.flatten.toSeq)
  }

  private def register(id: BlockManagerId, maxMemSize: Long, slaveEndpoint: RpcEndpointRef) {
    val time = System.currentTimeMillis()
    if (!blockManagerInfo.contains(id)) {
      blockManagerIdByExecutor.get(id.executorId) match {
        case Some(oldId) =>
          // A block manager of the same executor already exists, so remove it (assumed dead)
          logError("Got two different block manager registrations on same executor - "
              + s" will replace old one $oldId with new one $id")
          removeExecutor(id.executorId)
        case None =>
      }
      logInfo("Registering block manager %s with %s RAM, %s".format(
        id.hostPort, Utils.bytesToString(maxMemSize), id))

      blockManagerIdByExecutor(id.executorId) = id

      blockManagerInfo(id) = new BlockManagerInfo(
        id, System.currentTimeMillis(), maxMemSize, slaveEndpoint)
    }
    listenerBus.post(SparkListenerBlockManagerAdded(time, id, maxMemSize))
  }

  private def updateBlockInfo(
      blockManagerId: BlockManagerId,
      blockId: BlockId,
      storageLevel: StorageLevel,
      memSize: Long,
      diskSize: Long,
      externalBlockStoreSize: Long): Boolean = {

    if (!blockManagerInfo.contains(blockManagerId)) {
      if (blockManagerId.isDriver && !isLocal) {
        // We intentionally do not register the master (except in local mode),
        // so we should not indicate failure.
        return true
      } else {
        return false
      }
    }

    if (blockId == null) {
      blockManagerInfo(blockManagerId).updateLastSeenMs()
      return true
    }

    blockManagerInfo(blockManagerId).updateBlockInfo(
      blockId, storageLevel, memSize, diskSize, externalBlockStoreSize)

    var locations: mutable.HashSet[BlockManagerId] = null
    if (blockLocations.containsKey(blockId)) {
      locations = blockLocations.get(blockId)
    } else {
      locations = new mutable.HashSet[BlockManagerId]
      blockLocations.put(blockId, locations)
    }

    if (storageLevel.isValid) {
      locations.add(blockManagerId)
    } else {
      locations.remove(blockManagerId)
    }

    // Remove the block from master tracking if it has been removed on all slaves.
    if (locations.size == 0) {
      blockLocations.remove(blockId)
    }
    true
  }

  private def getLocations(blockId: BlockId): Seq[BlockManagerId] = {
    if (blockLocations.containsKey(blockId)) blockLocations.get(blockId).toSeq else Seq.empty
  }

  private def getLocationsMultipleBlockIds(
      blockIds: Array[BlockId]): IndexedSeq[Seq[BlockManagerId]] = {
    blockIds.map(blockId => getLocations(blockId))
  }

  /** Get the list of the peers of the given block manager */
  //getPeers 从BlockManagerInfo中过滤掉Driver的BlockManager和当前的Executor的BlockManager,
  //将其余的BlockManagerId都返回
  private def getPeers(blockManagerId: BlockManagerId): Seq[BlockManagerId] = {
    val blockManagerIds = blockManagerInfo.keySet
    if (blockManagerIds.contains(blockManagerId)) {
      blockManagerIds.filterNot { _.isDriver }.filterNot { _ == blockManagerId }.toSeq
    } else {
      Seq.empty
    }
  }

  /**
   * Returns the hostname and port of an executor, based on the [[RpcEnv]] address of its
   * [[BlockManagerSlaveEndpoint]].
   */
  private def getRpcHostPortForExecutor(executorId: String): Option[(String, Int)] = {
    for (
      blockManagerId <- blockManagerIdByExecutor.get(executorId);
      info <- blockManagerInfo.get(blockManagerId)
    ) yield {
      (info.slaveEndpoint.address.host, info.slaveEndpoint.address.port)
    }
  }

  override def onStop(): Unit = {
    askThreadPool.shutdownNow()
  }
}

@DeveloperApi
case class BlockStatus(
    storageLevel: StorageLevel,
    memSize: Long,
    diskSize: Long,
    externalBlockStoreSize: Long) {
  def isCached: Boolean = memSize + diskSize + externalBlockStoreSize > 0
}

@DeveloperApi
object BlockStatus {
  def empty: BlockStatus = BlockStatus(StorageLevel.NONE, 0L, 0L, 0L)
}

private[spark] class BlockManagerInfo(
    val blockManagerId: BlockManagerId,
    timeMs: Long,
    val maxMem: Long,
    val slaveEndpoint: RpcEndpointRef)
  extends Logging {

  private var _lastSeenMs: Long = timeMs
  private var _remainingMem: Long = maxMem

  // Mapping from block id to its status.
  private val _blocks = new JHashMap[BlockId, BlockStatus]

  // Cached blocks held by this BlockManager. This does not include broadcast blocks.
  private val _cachedBlocks = new mutable.HashSet[BlockId]

  def getStatus(blockId: BlockId): Option[BlockStatus] = Option(_blocks.get(blockId))

  def updateLastSeenMs() {
    _lastSeenMs = System.currentTimeMillis()
  }

  def updateBlockInfo(
      blockId: BlockId,
      storageLevel: StorageLevel,
      memSize: Long,
      diskSize: Long,
      externalBlockStoreSize: Long) {

    updateLastSeenMs()

    if (_blocks.containsKey(blockId)) {
      // The block exists on the slave already.
      val blockStatus: BlockStatus = _blocks.get(blockId)
      val originalLevel: StorageLevel = blockStatus.storageLevel
      val originalMemSize: Long = blockStatus.memSize

      if (originalLevel.useMemory) {
        _remainingMem += originalMemSize
      }
    }

    if (storageLevel.isValid) {
      /* isValid means it is either stored in-memory, on-disk or on-externalBlockStore.
       * The memSize here indicates the data size in or dropped from memory,
       * externalBlockStoreSize here indicates the data size in or dropped from externalBlockStore,
       * and the diskSize here indicates the data size in or dropped to disk.
       * They can be both larger than 0, when a block is dropped from memory to disk.
       * Therefore, a safe way to set BlockStatus is to set its info in accurate modes. */
      var blockStatus: BlockStatus = null
      if (storageLevel.useMemory) {
        blockStatus = BlockStatus(storageLevel, memSize, 0, 0)
        _blocks.put(blockId, blockStatus)
        _remainingMem -= memSize
        logInfo("Added %s in memory on %s (size: %s, free: %s)".format(
          blockId, blockManagerId.hostPort, Utils.bytesToString(memSize),
          Utils.bytesToString(_remainingMem)))
      }
      if (storageLevel.useDisk) {
        blockStatus = BlockStatus(storageLevel, 0, diskSize, 0)
        _blocks.put(blockId, blockStatus)
        logInfo("Added %s on disk on %s (size: %s)".format(
          blockId, blockManagerId.hostPort, Utils.bytesToString(diskSize)))
      }
      if (storageLevel.useOffHeap) {
        blockStatus = BlockStatus(storageLevel, 0, 0, externalBlockStoreSize)
        _blocks.put(blockId, blockStatus)
        logInfo("Added %s on ExternalBlockStore on %s (size: %s)".format(
          blockId, blockManagerId.hostPort, Utils.bytesToString(externalBlockStoreSize)))
      }
      if (!blockId.isBroadcast && blockStatus.isCached) {
        _cachedBlocks += blockId
      }
    } else if (_blocks.containsKey(blockId)) {
      // If isValid is not true, drop the block.
      val blockStatus: BlockStatus = _blocks.get(blockId)
      _blocks.remove(blockId)
      _cachedBlocks -= blockId
      if (blockStatus.storageLevel.useMemory) {
        logInfo("Removed %s on %s in memory (size: %s, free: %s)".format(
          blockId, blockManagerId.hostPort, Utils.bytesToString(blockStatus.memSize),
          Utils.bytesToString(_remainingMem)))
      }
      if (blockStatus.storageLevel.useDisk) {
        logInfo("Removed %s on %s on disk (size: %s)".format(
          blockId, blockManagerId.hostPort, Utils.bytesToString(blockStatus.diskSize)))
      }
      if (blockStatus.storageLevel.useOffHeap) {
        logInfo("Removed %s on %s on externalBlockStore (size: %s)".format(
          blockId, blockManagerId.hostPort,
          Utils.bytesToString(blockStatus.externalBlockStoreSize)))
      }
    }
  }

  def removeBlock(blockId: BlockId) {
    if (_blocks.containsKey(blockId)) {
      _remainingMem += _blocks.get(blockId).memSize
      _blocks.remove(blockId)
    }
    _cachedBlocks -= blockId
  }

  def remainingMem: Long = _remainingMem

  def lastSeenMs: Long = _lastSeenMs

  def blocks: JHashMap[BlockId, BlockStatus] = _blocks

  // This does not include broadcast blocks.
  def cachedBlocks: collection.Set[BlockId] = _cachedBlocks

  override def toString: String = "BlockManagerInfo " + timeMs + " " + _remainingMem

  def clear() {
    _blocks.clear()
  }
}
