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

import java.io._
import java.nio.{ ByteBuffer, MappedByteBuffer }

import scala.collection.mutable.{ ArrayBuffer, HashMap }
import scala.concurrent.{ ExecutionContext, Await, Future }
import scala.concurrent.duration._
import scala.util.Random

import sun.nio.ch.DirectBuffer

import org.apache.spark._
import org.apache.spark.executor.{ DataReadMethod, ShuffleWriteMetrics }
import org.apache.spark.io.CompressionCodec
import org.apache.spark.network._
import org.apache.spark.network.buffer.{ ManagedBuffer, NioManagedBuffer }
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.shuffle.ExternalShuffleClient
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.serializer.{ SerializerInstance, Serializer }
import org.apache.spark.shuffle.ShuffleManager
import org.apache.spark.shuffle.hash.HashShuffleManager
import org.apache.spark.util._

private[spark] sealed trait BlockValues
private[spark] case class ByteBufferValues(buffer: ByteBuffer) extends BlockValues
private[spark] case class IteratorValues(iterator: Iterator[Any]) extends BlockValues
private[spark] case class ArrayValues(buffer: Array[Any]) extends BlockValues

/* Class for returning a fetched(取得) block and associated metrics. */
//类用来表示返回的匹配的block
private[spark] class BlockResult(
  val data: Iterator[Any],
  val readMethod: DataReadMethod.Value,
  val bytes: Long)

/**
 * Manager running on every node (driver and executors) which provides interfaces for putting and
 * retrieving blocks both locally and remotely into various stores (memory, disk, and off-heap).
 * 提供Storage模块与其他其他模块的交互接口
 * BlockManager创建的时候会持有一个BlockManagerMaster,
 * master BlockManagerMaster会把请求转发给BlockManagerMasterEndpoint来完成元数据的管理和维护.
 * Note that #initialize() must be called before the BlockManager is usable.
 */
private[spark] class BlockManager(
  executorId: String,
  rpcEnv: RpcEnv,
  val master: BlockManagerMaster, //BlockManagerMasterEndpoint
  defaultSerializer: Serializer,
  maxMemory: Long,
  val conf: SparkConf,
  mapOutputTracker: MapOutputTracker,
  shuffleManager: ShuffleManager,
  blockTransferService: BlockTransferService,
  securityManager: SecurityManager,
  numUsableCores: Int)
    extends BlockDataManager with Logging {
  //磁盘块管理器
  val diskBlockManager = new DiskBlockManager(this, conf)
  //BlockManager缓存BlockId及对应的BlockInfo
  private val blockInfo = new TimeStampedHashMap[BlockId, BlockInfo]
  //Executor线程池
  private val futureExecutionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("block-manager-future", 128))

  // Actual storage of where blocks are kept
  //扩展存储块初始化
  private var externalBlockStoreInitialized = false
  //内存存储
  private[spark] val memoryStore = new MemoryStore(this, maxMemory)
  //磁盘存储
  private[spark] val diskStore = new DiskStore(this, diskBlockManager)
  //扩展块存储
  private[spark] lazy val externalBlockStore: ExternalBlockStore = {
    externalBlockStoreInitialized = true
    new ExternalBlockStore(this, executorId)
  }
  //是否有外部ShuffleService可用
  private[spark] val externalShuffleServiceEnabled = conf.getBoolean("spark.shuffle.service.enabled", false)

  // Port used by the external shuffle service. In Yarn mode, this may be already be
  // set through the Hadoop configuration as the server is launched in the Yarn NM.
  //使用扩展Shuffle service端口
  private val externalShuffleServicePort =
    Utils.getSparkOrYarnConfig(conf, "spark.shuffle.service.port", "7337").toInt

  // Check that we're not using external shuffle service with consolidated shuffle files.
  // 检查外部不使用合并文件的shuffle服务
  if (externalShuffleServiceEnabled
    //如果为true，在shuffle时就合并中间文件，对于有大量Reduce任务的shuffle来说，合并文件可 以提高文件系统性能，
    //如果使用的是ext4 或 xfs 文件系统，建议设置为true；对于ext3，由于文件系统的限制，设置为true反而会使内核>8的机器降低性能
    && conf.getBoolean("spark.shuffle.consolidateFiles", false)
    && shuffleManager.isInstanceOf[HashShuffleManager]) {
    throw new UnsupportedOperationException("Cannot use external shuffle service with consolidated"
      + " shuffle files in hash-based shuffle. Please disable spark.shuffle.consolidateFiles or "
      + " switch to sort-based shuffle.")
  }

  var blockManagerId: BlockManagerId = _

  // Address of the server that serves this executor's shuffle files. This is either an external
  // service, or just our own Executor's BlockManager.
  //执行shuffle服务器的地址
  private[spark] var shuffleServerId: BlockManagerId = _

  // Client to read other executors' shuffle files. This is either an external service, or just the
  // standard BlockTransferService to directly connect to other Executors.
  //shuffleClient客户端,是否有外ShuffleService可用
  /**
   * 为什么网络服务组织存储体系里面?
   * Spark是分布式部署,每个Task最终都运行在不同的机器节点上,map任务的输出结果直接存储到map任务所在机器的存储体系中
   * reduce任务极有可能不在同一机器上运行,需要远程下载map任务的中间输出结果
   */
  private[spark] val shuffleClient = if (externalShuffleServiceEnabled) {
    //创建新的ExternalShuffleClient
    val transConf = SparkTransportConf.fromSparkConf(conf, numUsableCores)
    new ExternalShuffleClient(transConf, securityManager, securityManager.isAuthenticationEnabled(),
      securityManager.isSaslEncryptionEnabled())
  } else {
    //使用默认BlockTransferService
    blockTransferService
  }

  // Whether to compress broadcast variables that are stored
  //压缩算法
  private val compressBroadcast = conf.getBoolean("spark.broadcast.compress", true)
  // Whether to compress shuffle output that are stored
  //是否压缩map输出文件，压缩将使用spark.io.compression.codec
  private val compressShuffle = conf.getBoolean("spark.shuffle.compress", true)
  // Whether to compress RDD partitions that are stored serialized
  //是否压缩RDD分区
  private val compressRdds = conf.getBoolean("spark.rdd.compress", false)
  // Whether to compress shuffle output temporarily spilled to disk
  //是否压缩在shuffle期间溢出的数据，如果压缩将使用spark.io.compression.codec。
  private val compressShuffleSpill = conf.getBoolean("spark.shuffle.spill.compress", true)
  //向查找或注册BlockManagerSlaveEndpoint
  private val slaveEndpoint = rpcEnv.setupEndpoint(
    "BlockManagerEndpoint" + BlockManager.ID_GENERATOR.next,
    new BlockManagerSlaveEndpoint(rpcEnv, this, mapOutputTracker))

  // Pending re-registration action being executed asynchronously or null if none is pending.
  // Accesses should synchronize on asyncReregisterLock.
  //异步执行申请重新注册的动作,如果没有等待
  private var asyncReregisterTask: Future[Unit] = null
  private val asyncReregisterLock = new Object
  //非广播清理器
  private val metadataCleaner = new MetadataCleaner(
    MetadataCleanerType.BLOCK_MANAGER, this.dropOldNonBroadcastBlocks, conf)
  //广播清理器
  private val broadcastCleaner = new MetadataCleaner(
    MetadataCleanerType.BROADCAST_VARS, this.dropOldBroadcastBlocks, conf)

  // Field related to peer block managers that are necessary for block replication  
  @volatile private var cachedPeers: Seq[BlockManagerId] = _
  private val peerFetchLock = new Object
  private var lastPeerFetchTime = 0L

  /* The compression codec to use. Note that the "lazy" val is necessary because we want to delay
   * the initialization of the compression codec until it is first used. The reason is that a Spark
   * program could be using a user-defined codec in a third party jar, which is loaded in
   * Executor.updateDependencies. When the BlockManager is initialized, user level jars hasn't been
   * loaded yet. 
   * 压缩包解码器,lazy表示在调用它时才实例化一个解码器,主要是针对用户自定义的jar包
   * */
  private lazy val compressionCodec: CompressionCodec = CompressionCodec.createCodec(conf)

  /**
   * Construct a BlockManager with a memory limit set based on system properties.
   * 构建blockmanager设置内存限制系统属性
   */
  def this(
    execId: String,
    rpcEnv: RpcEnv,
    master: BlockManagerMaster,
    serializer: Serializer,
    conf: SparkConf,
    mapOutputTracker: MapOutputTracker,
    shuffleManager: ShuffleManager,
    blockTransferService: BlockTransferService,
    securityManager: SecurityManager,
    numUsableCores: Int) = {
    this(execId, rpcEnv, master, serializer, BlockManager.getMaxMemory(conf),
      conf, mapOutputTracker, shuffleManager, blockTransferService, securityManager, numUsableCores)
  }

  /**
   * Initializes the BlockManager with the given appId. This is not performed in the constructor as
   * the appId may not be known at BlockManager instantiation time (in particular for the driver,
   * where it is only learned after registration with the TaskScheduler).
   * 给定appId初始化BlockManager,
   * This method initializes the BlockTransferService and ShuffleClient, registers with the
   * BlockManagerMaster, starts the BlockManagerWorker endpoint, and registers with a local shuffle
   * service if configured.
   */
  def initialize(appId: String): Unit = {
    //blockTransferService 初始化
    blockTransferService.init(this)
    //shuffleClient初始化
    shuffleClient.init(appId)
    //blockManagerId创建
    blockManagerId = BlockManagerId(
      //包括标识Slave的ExecutorId,HostName和Port
      executorId, blockTransferService.hostName, blockTransferService.port)
    //shuffleServerId创建,当有外部externalShuffleServiceEnabled则初始化
    shuffleServerId = if (externalShuffleServiceEnabled) {
      BlockManagerId(executorId, blockTransferService.hostName, externalShuffleServicePort)
    } else {
      blockManagerId
    }
    /**
     * 表示Executor的BlockManger与Driver的BlockManager进行消息通信,例 如:注册BlockManager,更新Block信息,
     * 获取Block所在的BlockManager,删除Exceutor
     */
    //向blockManagerMaster注册blockManagerId,BlockManagerMaster对存在于所有Executor上的BlockManager统一管理
    master.registerBlockManager(blockManagerId, maxMemory, slaveEndpoint)

    // Register Executors' configuration with the local shuffle service, if one should exist.
    //当有外部shuffle service时,还需要向blockManagerMaster注册shuffleId
    if (externalShuffleServiceEnabled && !blockManagerId.isDriver) {
      registerWithExternalShuffleServer()
    }
  }
  //当有外部shuffle service时,还需要向blockManagerMaster注册shuffleId
  private def registerWithExternalShuffleServer() {
    logInfo("Registering executor with local external shuffle service.")
    val shuffleConfig = new ExecutorShuffleInfo(
      diskBlockManager.localDirs.map(_.toString),
      diskBlockManager.subDirsPerLocalDir,
      shuffleManager.getClass.getName)

    val MAX_ATTEMPTS = 3
    val SLEEP_TIME_SECS = 5

    for (i <- 1 to MAX_ATTEMPTS) {
      try {
        // Synchronous and will throw an exception if we cannot connect.
        shuffleClient.asInstanceOf[ExternalShuffleClient].registerWithShuffleServer(
          shuffleServerId.host, shuffleServerId.port, shuffleServerId.executorId, shuffleConfig)
        return
      } catch {
        case e: Exception if i < MAX_ATTEMPTS =>
          logError(s"Failed to connect to external shuffle server, will retry ${MAX_ATTEMPTS - i}"
            + s" more times after waiting $SLEEP_TIME_SECS seconds...", e)
          Thread.sleep(SLEEP_TIME_SECS * 1000)
      }
    }
  }

  /**
   * Report all blocks to the BlockManager again. This may be necessary if we are dropped
   * by the BlockManager and come back or if we become capable of recovering blocks on disk after
   * an executor crash.
   * 再次将所有的blocks汇报给BlockManager,这个方法强调所有的blocks必须都能在BlockManager的管理下，
   * 因为可能会出现各种因素，如slave需要重新注册、进程冲突导致block变化等,让blocks产生变化
   * This function deliberately(故意) fails silently if the master returns false (indicating that
   * the slave needs to re-register). The error condition will be detected again by the next
   * heart beat attempt or new block registration and another try to re-register all blocks
   * will be made then.
   * 错误状态将尝试再次检测到下一个心跳或重新块注册
   */
  private def reportAllBlocks(): Unit = {
    logInfo(s"Reporting ${blockInfo.size} blocks to the master.")
    for ((blockId, info) <- blockInfo) {
      val status = getCurrentBlockStatus(blockId, info)
      if (!tryToReportBlockStatus(blockId, info, status)) {
        logError(s"Failed to report $blockId to master; giving up.")
        return
      }
    }
  }

  /**
   * Re-register with the master and report all blocks to it. This will be called by the heart beat
   * thread if our heartbeat to the block manager indicates that we were not registered.
   * 重新注册BlockManager,这个方法主要是在心跳进程发现BlockManager没有注册时调用,调用无需在锁状态下执行
   * Note that this method must be called without any BlockInfo locks held.
   */
  def reregister(): Unit = {
    // TODO: We might need to rate limit re-registering.
    logInfo("BlockManager re-registering with master")
    master.registerBlockManager(blockManagerId, maxMemory, slaveEndpoint)
    reportAllBlocks()
  }

  /**
   * Re-register with the master sometime soon.
   * 异步地重新向master注册BlockManager
   */
  private def asyncReregister(): Unit = {
    asyncReregisterLock.synchronized {
      if (asyncReregisterTask == null) {
        asyncReregisterTask = Future[Unit] {
          // This is a blocking action and should run in futureExecutionContext which is a cached
          // thread pool
          reregister()
          asyncReregisterLock.synchronized {
            asyncReregisterTask = null
          }
        }(futureExecutionContext)
      }
    }
  }

  /**
   * For testing. Wait for any pending asynchronous re-registration; otherwise, do nothing.
   * 如果有其他的异步重注册进程,则等待
   */
  def waitForAsyncReregister(): Unit = {
    val task = asyncReregisterTask
    if (task != null) {
      Await.ready(task, Duration.Inf)
    }
  }

  /**
   * Interface to get local block data. Throws an exception if the block cannot be found or
   * cannot be read successfully.
   * 用于从本地获取Block数据
   *
   */
  override def getBlockData(blockId: BlockId): ManagedBuffer = {

    if (blockId.isShuffle) {
      //如果是ShuffleMapTask的输出,那么多个Partition的中间结果都写入同一个文件
      //怎么读取不同partition的中间结果?IndexShuffleBlockManager的getBlockData方法解决这个问题
      shuffleManager.shuffleBlockResolver.getBlockData(blockId.asInstanceOf[ShuffleBlockId])
    } else {
      //如果是ResultTask的输出,则使用doGetLocal来获取本地中间结果数据
      val blockBytesOpt = doGetLocal(blockId, asBlockResult = false)
        .asInstanceOf[Option[ByteBuffer]]
      if (blockBytesOpt.isDefined) {
        val buffer = blockBytesOpt.get
        new NioManagedBuffer(buffer)
      } else {
        throw new BlockNotFoundException(blockId.toString)
      }
    }
  }

  /**
   * Put the block locally, using the given storage level.
   */
  override def putBlockData(blockId: BlockId, data: ManagedBuffer, level: StorageLevel): Unit = {
    putBytes(blockId, data.nioByteBuffer(), level)
  }

  /**
   * Get the BlockStatus for the block identified by the given ID, if it exists.
   * 根据blockId获取block的信息
   * NOTE: This is mainly for testing, and it doesn't fetch information from external block store.
   */
  def getStatus(blockId: BlockId): Option[BlockStatus] = {
    blockInfo.get(blockId).map { info =>
      val memSize = if (memoryStore.contains(blockId)) memoryStore.getSize(blockId) else 0L
      val diskSize = if (diskStore.contains(blockId)) diskStore.getSize(blockId) else 0L
      // Assume that block is not in external block store
      BlockStatus(info.level, memSize, diskSize, 0L)
    }
  }

  /**
   * Get the ids of existing blocks that match the given filter. Note that this will
   * query the blocks stored in the disk block manager (that the block manager
   * may not know of).
   * 指定过滤器对所有的blocks进行过滤
   */
  def getMatchingBlockIds(filter: BlockId => Boolean): Seq[BlockId] = {
    (blockInfo.keys ++ diskBlockManager.getAllBlocks()).filter(filter).toSeq
  }

  /**
   * doput在完成数据的存储后,会利用reportBlockStatus告知BlockManagerMaster数据定入详细信息
   * Tell the master about the current storage status of a block. This will send a block update
   * message reflecting the current status, *not* the desired storage level in its block info.
   * For example, a block with MEMORY_AND_DISK set might have fallen out to be only on disk.
   *
   * droppedMemorySize exists to account for when the block is dropped from memory to disk (so
   * it is still valid). This ensures that update in master will compensate for the increase in
   * memory on slave.
   * 向master报告block所在存储位置的状况,这个信息不仅反映了block当前的状态,还用于更新block的信息。
   * 但是这个存储状态的信息,如磁盘、内存、cache存储等等,并不一定是block的Information中所期望的存储信息，
   * 例如MEMORY_AND_DISK等
   *
   * reportBlockStatus 用于向BlockManagerMasterActor报告block所在存储位置的状况
   * 什么时候Slave向Master汇报Block状态
   * 1)dropFromMemory将某个Block从内存中移出
   * 2)dropOldBlocks删除一个block时
   * 3)doput写一个新的Block时
   */
  private def reportBlockStatus(
    blockId: BlockId,
    info: BlockInfo,
    status: BlockStatus,
    droppedMemorySize: Long = 0L): Unit = {
    //tryToReportBlockStatus调用BlockManagerMaster的updateBlockInfo方法向BlockManagerMasterActor发送
    //updateBlockInfo消息更新Block占用的内存大小,磁盘大小,存储级别等信息
    val needReregister = !tryToReportBlockStatus(blockId, info, status, droppedMemorySize)
    //如果BlockManager还没有向BlockManagerMasterActor注册,则调用asyncReregister方法,
    if (needReregister) {
      logInfo(s"Got told to re-register updating block $blockId")
      // Re-registering will report our new block for free.
      asyncReregister() //Reregister,最后Reregister实际调用了BlockmanagerMaster的RegisterBlockManager方法
      //和ReportAllBlocks方法
    }
    logDebug(s"Told master about block $blockId")
  }

  /**
   * Actually send a UpdateBlockInfo message. Returns the master's response,
   * which will be true if the block was successfully recorded and false if
   * the slave needs to re-register.
   * tryToReportBlockStatus调用BlockManagerMaster的updateBlockInfo方法向BlockManagerMasterActor发送
   * updateBlockInfo消息更新Block占用的内存大小,磁盘大小,存储级别等信息
   */
  private def tryToReportBlockStatus(
    blockId: BlockId,
    info: BlockInfo,
    status: BlockStatus,
    droppedMemorySize: Long = 0L): Boolean = {
    if (info.tellMaster) {
      val storageLevel = status.storageLevel
      val inMemSize = Math.max(status.memSize, droppedMemorySize)
      val inExternalBlockStoreSize = status.externalBlockStoreSize
      val onDiskSize = status.diskSize
      master.updateBlockInfo(
        blockManagerId, blockId, storageLevel, inMemSize, onDiskSize, inExternalBlockStoreSize)
    } else {
      true
    }
  }

  /**
   * Return the updated storage status of the block with the given ID. More specifically, if
   * the block is dropped from memory and possibly added to disk, return the new storage level
   * and the updated in-memory and on-disk sizes.
   * 返回指定block所在存储块的最新信息。特别的,当block从内存移到磁盘时,更改其存储级别并更新内存和磁盘大小
   */
  private def getCurrentBlockStatus(blockId: BlockId, info: BlockInfo): BlockStatus = {
    info.synchronized {
      info.level match {
        case null =>
          BlockStatus(StorageLevel.NONE, 0L, 0L, 0L)
        case level =>
          val inMem = level.useMemory && memoryStore.contains(blockId)
          val inExternalBlockStore = level.useOffHeap && externalBlockStore.contains(blockId)
          val onDisk = level.useDisk && diskStore.contains(blockId)
          val deserialized = if (inMem) level.deserialized else false
          val replication = if (inMem || inExternalBlockStore || onDisk) level.replication else 1
          val storageLevel =
            StorageLevel(onDisk, inMem, inExternalBlockStore, deserialized, replication)
          val memSize = if (inMem) memoryStore.getSize(blockId) else 0L
          val externalBlockStoreSize =
            if (inExternalBlockStore) externalBlockStore.getSize(blockId) else 0L
          val diskSize = if (onDisk) diskStore.getSize(blockId) else 0L
          BlockStatus(storageLevel, memSize, diskSize, externalBlockStoreSize)
      }
    }
  }

  /**
   * Get locations of an array of blocks.
   * 获取一系列block的位置
   */
  private def getLocationBlockIds(blockIds: Array[BlockId]): Array[Seq[BlockManagerId]] = {
    val startTimeMs = System.currentTimeMillis
    val locations = master.getLocations(blockIds).toArray
    logDebug("Got multiple block location in %s".format(Utils.getUsedTimeMs(startTimeMs)))
    locations
  }

  /**
   * Get block from local block manager.
   * 从本地block manager获取block
   * 当reduce任务与map任务处于同一节点时,不需要远程拉取,只需调取doGetLocal
   */
  def getLocal(blockId: BlockId): Option[BlockResult] = {
    logDebug(s"Getting local block $blockId")
    doGetLocal(blockId, asBlockResult = true).asInstanceOf[Option[BlockResult]]
  }

  /**
   * Get block from the local block manager as serialized bytes.
   * 以序列化字节流的形式从本地block manager获取block
   */
  def getLocalBytes(blockId: BlockId): Option[ByteBuffer] = {
    logDebug(s"Getting local block $blockId as bytes")
    // As an optimization for map output fetches, if the block is for a shuffle, return it
    // without acquiring a lock; the disk store never deletes (recent) items so this should work
    if (blockId.isShuffle) {
      val shuffleBlockResolver = shuffleManager.shuffleBlockResolver
      // TODO: This should gracefully handle case where local block is not available. Currently
      // downstream code will throw an exception.
      Option(
        shuffleBlockResolver.getBlockData(blockId.asInstanceOf[ShuffleBlockId]).nioByteBuffer())
    } else {
      doGetLocal(blockId, asBlockResult = false).asInstanceOf[Option[ByteBuffer]]
    }
  }
  /**
   * 以序列化字节流的形式从本地block manager获取block,用来获取block并以序列化形式输出
   */
  private def doGetLocal(blockId: BlockId, asBlockResult: Boolean): Option[Any] = {
    val info = blockInfo.get(blockId).orNull //如果选项包含有值返回选项值，否则返回 null
    if (info != null) {
      info.synchronized { //BlockInfom线程同步
        // Double check to make sure the block is still there. There is a small chance(小的机会) that the
        // block has been removed by removeBlock (which also synchronizes on the blockInfo object).
        // Note that this only checks metadata tracking. If user intentionally deleted the block
        // on disk or from off heap storage without using removeBlock, this conditional check will
        // still pass but eventually we will get an exception because we can't find the block.
        if (blockInfo.get(blockId).isEmpty) { //再次检查
          logWarning(s"Block $blockId had been removed")
          return None
        }

        // If another thread is writing the block, wait for it to become ready.
        //如果另一个线程写入block,true返回一个可用的block,否则false
        if (!info.waitForReady()) {
          // If we get here, the block write failed.
          logWarning(s"Block $blockId was marked as failure.")
          return None
        }

        val level = info.level
        logDebug(s"Level for block $blockId is $level")

        // Look for the block in memory
        //如果Block充许使用内存,则调用memoryStore.getValues或getBytes获取
        if (level.useMemory) {
          logDebug(s"Getting block $blockId from memory")
          val result = if (asBlockResult) { //默认false
            memoryStore.getValues(blockId).map(new BlockResult(_, DataReadMethod.Memory, info.size))
          } else {
            //MemoryStore内存存储获取数据
            memoryStore.getBytes(blockId)
          }
          result match {
            case Some(values) =>
              return result
            case None =>
              logDebug(s"Block $blockId not found in memory")
          }
        }

        // Look for the block in external block store
        //如果Block充许使用扩展 block store
        if (level.useOffHeap) {
          logDebug(s"Getting block $blockId from ExternalBlockStore")
          if (externalBlockStore.contains(blockId)) {
            val result = if (asBlockResult) {
              externalBlockStore.getValues(blockId)
                .map(new BlockResult(_, DataReadMethod.Memory, info.size))
            } else {
              externalBlockStore.getBytes(blockId)
            }
            result match {
              case Some(values) =>
                return result
              case None =>
                logDebug(s"Block $blockId not found in ExternalBlockStore")
            }
          }
        }

        // Look for block on disk, potentially storing it back in memory if required
        //查找硬盘上块,如果需要的话，可能会在内存中存储它
        if (level.useDisk) {
          logDebug(s"Getting block $blockId from disk")
          val bytes: ByteBuffer = diskStore.getBytes(blockId) match {
            case Some(b) => b
            case None =>
              throw new BlockException(
                blockId, s"Block $blockId not found on disk, though it should be")
          }
          //position 表示当前进行读写操作时的位置
          assert(0 == bytes.position())

          if (!level.useMemory) {
            // If the block shouldn't be stored in memory, we can just return it
            //如果块不应该被存储在内存中，我们可以只返回它
            if (asBlockResult) {
              return Some(new BlockResult(dataDeserialize(blockId, bytes), DataReadMethod.Disk,
                info.size))
            } else {
              return Some(bytes)
            }
          } else {
            // Otherwise, we also have to store something in the memory store
            //否则,我们还必须在存储器存储中存储一些东西
            if (!level.deserialized || !asBlockResult) {
              /* We'll store the bytes in memory if the block's storage level includes
               * "memory serialized", or if it should be cached as objects in memory
               * but we only requested its serialized bytes. */
              memoryStore.putBytes(blockId, bytes.limit, () => {
                // https://issues.apache.org/jira/browse/SPARK-6076
                // If the file size is bigger than the free memory, OOM will happen. So if we cannot
                // put it into MemoryStore, copyForMemory should not be created. That's why this
                // action is put into a `() => ByteBuffer` and created lazily.
                val copyForMemory = ByteBuffer.allocate(bytes.limit) //Buffer对象首先要进行分配
                copyForMemory.put(bytes)
              })
              //rewind将position设回0，所以你可以重读Buffer中的所有数据,limit保持不变      
              bytes.rewind()
            }
            if (!asBlockResult) {
              return Some(bytes)
            } else {
              val values = dataDeserialize(blockId, bytes)
              if (level.deserialized) {
                // Cache the values before returning them
                val putResult = memoryStore.putIterator(
                  blockId, values, level, returnValues = true, allowPersistToDisk = false)
                // The put may or may not have succeeded, depending on whether there was enough
                // space to unroll the block. Either way, the put here should return an iterator.
                putResult.data match {
                  case Left(it) =>
                    return Some(new BlockResult(it, DataReadMethod.Disk, info.size))
                  case _ =>
                    // This only happens if we dropped the values back to disk (which is never)
                    throw new SparkException("Memory store did not return an iterator!")
                }
              } else {
                return Some(new BlockResult(values, DataReadMethod.Disk, info.size))
              }
            }
          }
        }
      }
    } else {
      logDebug(s"Block $blockId not registered locally")
    }
    None
  }

  /**
   * Get block from remote block managers.
   * 从远程block manager获取block
   */
  def getRemote(blockId: BlockId): Option[BlockResult] = {
    logDebug(s"Getting remote block $blockId")
    doGetRemote(blockId, asBlockResult = true).asInstanceOf[Option[BlockResult]]
  }

  /**
   * Get block from remote block managers as serialized bytes.
   * 同getLocalBytes()
   */
  def getRemoteBytes(blockId: BlockId): Option[ByteBuffer] = {
    logDebug(s"Getting remote block $blockId as bytes")
    doGetRemote(blockId, asBlockResult = false).asInstanceOf[Option[ByteBuffer]]
  }
  /**
   * 获取远程Block数据
   */
  private def doGetRemote(blockId: BlockId, asBlockResult: Boolean): Option[Any] = {
    require(blockId != null, "BlockId is null")
    //向master.getLocations(blockId)发送消息获取Block数据存储的BlockManagerId,
    //如果Block数据复制份数大小1个,则会返回多个BlockManagerId,
    //对这些BlockManager洗牌,避免总是从一个远程BlockManager获取Block数据
    val locations = Random.shuffle(master.getLocations(blockId))
    for (loc <- locations) {
      logDebug(s"Getting remote block $blockId from $loc")
      //根据返回的BlockManagerId信息,使用fetchBlockSync远程同步获取Block数据
      val data = blockTransferService.fetchBlockSync(
        loc.host, loc.port, loc.executorId, blockId.toString).nioByteBuffer()

      if (data != null) {
        if (asBlockResult) {
          return Some(new BlockResult(
            dataDeserialize(blockId, data),
            DataReadMethod.Network,
            data.limit()))
        } else {
          return Some(data)
        }
      }
      logDebug(s"The value of block $blockId is null")
    }
    logDebug(s"Block $blockId not found")
    None
  }

  /**
   * Get a block from the block manager (either local or remote).
   * 通过BlockId获取Block先尝试从本获取,如果没有所要获取的内容,则发远程获取
   */
  def get(blockId: BlockId): Option[BlockResult] = {
    val local = getLocal(blockId) //BlockId.name= "rdd_" + rddId + "_" + splitIndex
    if (local.isDefined) { //isDefined 方法来检查它是否有值
      logInfo(s"Found block $blockId locally")
      return local
    }
    val remote = getRemote(blockId) //
    if (remote.isDefined) {
      logInfo(s"Found block $blockId remotely")
      return remote
    }
    None
  }
  //在Iterator中加入block的信息
  def putIterator(
    blockId: BlockId,
    values: Iterator[Any],
    level: StorageLevel,
    tellMaster: Boolean = true,
    effectiveStorageLevel: Option[StorageLevel] = None): Seq[(BlockId, BlockStatus)] = {
    require(values != null, "Values is null")
    doPut(blockId, IteratorValues(values), level, tellMaster, effectiveStorageLevel)
  }

  /**
   * A short circuited method to get a block writer that can write data directly to disk.
   * The Block will be appended to the File specified by filename. Callers should handle error
   * cases.
   * 创建一个能够直接将数据写到磁盘的writer,block通过文件名来指定写入的文件,
   * 这个方法通常用来在shuffle之后写入shuffle的输出文件
   */
  def getDiskWriter(
    blockId: BlockId,
    file: File,
    serializerInstance: SerializerInstance,
    bufferSize: Int,
    writeMetrics: ShuffleWriteMetrics): DiskBlockObjectWriter = {
    val compressStream: OutputStream => OutputStream = wrapForCompression(blockId, _)
    //是否同步写入
    val syncWrites = conf.getBoolean("spark.shuffle.sync", false)
    new DiskBlockObjectWriter(blockId, file, serializerInstance, bufferSize, compressStream,
      syncWrites, writeMetrics)
  }

  /**
   * Put a new block of values to the block manager.
   * Return a list of blocks updated as a result of this put.
   * 在block manager中写入新的block,block的值是Array数组
   */
  def putArray(
    blockId: BlockId,
    values: Array[Any],
    level: StorageLevel,
    tellMaster: Boolean = true,
    effectiveStorageLevel: Option[StorageLevel] = None): Seq[(BlockId, BlockStatus)] = {
    require(values != null, "Values is null")
    doPut(blockId, ArrayValues(values), level, tellMaster, effectiveStorageLevel)
  }

  /**
   * Put a new block of serialized bytes to the block manager.
   * Return a list of blocks updated as a result of this put.
   * 用于将程序化字节组成的Block写入存储系统
   */
  def putBytes(
    blockId: BlockId,
    bytes: ByteBuffer,
    level: StorageLevel,
    tellMaster: Boolean = true,
    effectiveStorageLevel: Option[StorageLevel] = None): Seq[(BlockId, BlockStatus)] = {
    require(bytes != null, "Bytes is null")
    doPut(blockId, ByteBufferValues(bytes), level, tellMaster, effectiveStorageLevel)
  }

  /**
   * 数据写入过程中重要的一个函数就是put,在默认的情况一下,Spark只会将RDD的计算结果保存到内存中,这个时候
   * 是不会产生备份的,如果该RDD的计算结果丢失,则重新计算.
   * 如果要将RDD的计算结果存储到多个节点,那么调用persist函数时,将参数指定为MEMORY_ONLY_2
   * 当指定的备份数目大于1时,doput函数会调用replicate将数据备份到其他节点
   * Put the given block according to the given level in one of the block stores, replicating
   * the values if necessary.
   *
   * The effective storage level refers to the level according to which the block will actually be
   * handled. This allows the caller to specify an alternate behavior of doPut while preserving
   * the original level specified by the user.
   * 真正数据写入流程
   * 1)获取putBlockInfo,如果BlockInfo中已经缓存了BlockInfo,则使用缓存的BlockInfo,否则使用新建的BlockInfo
   * 2)获取块最终使用的存储级别PutLevel,根据putLevel判断块写入的BlockStore,优先使用MemoryStore,其他TechyonStone和DiskStore
   *   依据Data的实际包装类型,分别调用BlockStore不同方法
   * 3)写入完毕,将写入操作导致从内存drop掉的Block更新到updatedBlocks中,使用getCurrentBlockStatus获取写入Block的状态
   *   将putBlockInfo设置为充许其他线程读取,调用reportBlockStatus将当前Block的信息更新到upatedBlocks中的Block的状态
   *   由于都发生的变化,所以都需要向BlockManagerMasterActor发送updateBlockInfo消息
   * 4)如果putLevel.replication大于1,即为了容错考虑,数据的备份数量大于1的时候,需要将Block的数据备份到其他节点上.
   *
   */
  private def doPut(
    blockId: BlockId,
    data: BlockValues,
    level: StorageLevel,
    tellMaster: Boolean = true,
    effectiveStorageLevel: Option[StorageLevel] = None): Seq[(BlockId, BlockStatus)] = {

    require(blockId != null, "BlockId is null")
    require(level != null && level.isValid, "StorageLevel is null or invalid")
    effectiveStorageLevel.foreach { level =>
      require(level != null && level.isValid, "Effective StorageLevel is null or invalid")
    }

    // Return value
    val updatedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]

    /* Remember the block's storage level so that we can correctly drop it to disk if it needs
     * to be dropped right after it got put into memory. Note, however, that other threads will
     * not be able to get() this block until we call markReady on its BlockInfo. */
    /**
     * 1)获取putBlockInfo,如果BlockInfo中已经缓存了BlockInfo,则使用缓存的BlockInfo,否则使用新建的BlockInfo
     */
    val putBlockInfo = {
      val tinfo = new BlockInfo(level, tellMaster)
      // Do atomically !
      //putIfAbsent 如果不在存blockId对应的值,则将tinfo以Key加入Map,否则返回Key对应的旧值
      val oldBlockOpt = blockInfo.putIfAbsent(blockId, tinfo)
      if (oldBlockOpt.isDefined) {
        if (oldBlockOpt.get.waitForReady()) {
          logWarning(s"Block $blockId already exists on this machine; not re-adding it")
          return updatedBlocks
        }
        // TODO: So the block info exists - but previous attempt to load it (?) failed.
        // What do we do now ? Retry on it ?
        oldBlockOpt.get
      } else {
        tinfo
      }
    }

    val startTimeMs = System.currentTimeMillis

    /* If we're storing values and we need to replicate the data, we'll want access to the values,
     * but because our put will read the whole iterator, there will be no values left. For the
     * case where the put serializes data, we'll remember the bytes, above; but for the case where
     * it doesn't, such as deserialized storage, let's rely on the put returning an Iterator. */
    var valuesAfterPut: Iterator[Any] = null

    // Ditto for the bytes after the put
    var bytesAfterPut: ByteBuffer = null

    // Size of the block in bytes
    var size = 0L

    // The level we actually use to put the block

    /**
     * 2)获取块最终使用的存储级别PutLevel,根据putLevel判断块写入的BlockStore,
     * 优先使用MemoryStore,其他TechyonStone和DiskStore,依据Data的实际包装类型,分别调用BlockStore不同方法
     */
    val putLevel = effectiveStorageLevel.getOrElse(level)

    // If we're storing bytes, then initiate the replication before storing them locally.
    // This is faster as data is already serialized and ready to send.
    val replicationFuture = data match {
      case b: ByteBufferValues if putLevel.replication > 1 =>
        //如果putLevel.replication大于1,即为了容错考虑,数据的备份数量大于1的时候,需要将Block的数据备份到其他节点上
        // Duplicate doesn't copy the bytes, but just creates a wrapper
        val bufferView = b.buffer.duplicate()
        Future {
          // This is a blocking action and should run in futureExecutionContext which is a cached
          // thread pool
          replicate(blockId, bufferView, putLevel) //
        }(futureExecutionContext)
      case _ => null
    }

    putBlockInfo.synchronized {
      logTrace("Put for block %s took %s to get into synchronized block"
        .format(blockId, Utils.getUsedTimeMs(startTimeMs)))

      var marked = false
      try {
        // returnValues - Whether to return the values put
        // blockStore - The type of storage to put these values into
        val (returnValues, blockStore: BlockStore) = {
          if (putLevel.useMemory) { //优先使用memoryStore
            // Put it in memory first, even if it also has useDisk set to true;
            // We will drop it to disk later if the memory store can't hold it.
            (true, memoryStore)
          } else if (putLevel.useOffHeap) {
            // Use external block store 

            (false, externalBlockStore)
          } else if (putLevel.useDisk) {
            // Don't get back the bytes from put unless we replicate them
            //最后使用diskStore
            (putLevel.replication > 1, diskStore)
          } else {
            assert(putLevel == StorageLevel.NONE)
            throw new BlockException(
              blockId, s"Attempted to put block $blockId without specifying storage level!")
          }
        }

        // Actually put the values
        //根据result包装类型分别调用BlockStore不同的方法写入数据,如putIterator,putArray,putBytes
        val result = data match {
          case IteratorValues(iterator) =>
            blockStore.putIterator(blockId, iterator, putLevel, returnValues)
          case ArrayValues(array) =>
            blockStore.putArray(blockId, array, putLevel, returnValues)
          case ByteBufferValues(bytes) =>
            bytes.rewind()
            blockStore.putBytes(blockId, bytes, putLevel)
        }
        size = result.size
        result.data match {
          case Left(newIterator) if putLevel.useMemory => valuesAfterPut = newIterator
          case Right(newBytes)                         => bytesAfterPut = newBytes
          case _                                       =>
        }

        // Keep track of which blocks are dropped from memory
        //写入完毕,将写入操作导致从内存移除掉Block,同时更新到updatedBlocks
        if (putLevel.useMemory) {
          result.droppedBlocks.foreach { updatedBlocks += _ }
        }
        //getCurrentBlockStatus获取写入BlockStatus状态
        val putBlockStatus = getCurrentBlockStatus(blockId, putBlockInfo)

        if (putBlockStatus.storageLevel != StorageLevel.NONE) {
          // Now that the block is in either the memory, externalBlockStore, or disk store,
          // let other threads read it, and tell the master about it.
          marked = true
          //将putBlockStatus设置为允许其他线程读取
          putBlockInfo.markReady(size)
          if (tellMaster) {
            //是否上报Master,将当前Block的信息更新到BlockManagerMasterActor,            
            reportBlockStatus(blockId, putBlockInfo, putBlockStatus)
          }
          //将putBlockInfo添加到updatedBlocks中
          updatedBlocks += ((blockId, putBlockStatus))
        }
      } finally {
        // If we failed in putting the block to memory/disk, notify other possible readers
        // that it has failed, and then remove it from the block info map.
        if (!marked) {
          // Note that the remove must happen before markFailure otherwise another thread
          // could've inserted a new BlockInfo before we remove it.
          blockInfo.remove(blockId)
          putBlockInfo.markFailure()
          logWarning(s"Putting block $blockId failed")
        }
      }
    }
    logDebug("Put block %s locally took %s".format(blockId, Utils.getUsedTimeMs(startTimeMs)))

    // Either we're storing bytes and we asynchronously started replication, or we're storing
    // values and need to serialize and replicate them now:
    ///putLevel.replication大于1,当指定的备份数目大于1时,即为容错考虑,数据的备份数量大于1的时候
    //需要将Block的数据备份其他节点
    if (putLevel.replication > 1) {
      data match {
        case ByteBufferValues(bytes) =>
          if (replicationFuture != null) {
            Await.ready(replicationFuture, Duration.Inf)
          }
        case _ =>
          val remoteStartTime = System.currentTimeMillis
          // Serialize the block if not already done
          if (bytesAfterPut == null) {
            if (valuesAfterPut == null) {
              throw new SparkException(
                "Underlying put returned neither an Iterator nor bytes! This shouldn't happen.")
            }
            bytesAfterPut = dataSerialize(blockId, valuesAfterPut)
          }
          //需要将Block的数据备份其他节点
          replicate(blockId, bytesAfterPut, putLevel)
          logDebug("Put block %s remotely took %s"
            .format(blockId, Utils.getUsedTimeMs(remoteStartTime)))
      }
    }

    BlockManager.dispose(bytesAfterPut)

    if (putLevel.replication > 1) {
      logDebug("Putting block %s with replication took %s"
        .format(blockId, Utils.getUsedTimeMs(startTimeMs)))
    } else {
      logDebug("Putting block %s without replication took %s"
        .format(blockId, Utils.getUsedTimeMs(startTimeMs)))
    }

    updatedBlocks
  }

  /**
   * Get peer block managers in the system.
   *
   */
  private def getPeers(forceFetch: Boolean): Seq[BlockManagerId] = {
    peerFetchLock.synchronized {
      //cachedPeers缓存的超时间,默认60秒,可以修改cachedPeersTtl属性改变大小
      val cachedPeersTtl = conf.getInt("spark.storage.cachedPeersTtl", 60 * 1000) // milliseconds
      val timeout = System.currentTimeMillis - lastPeerFetchTime > cachedPeersTtl
      //cachedPeers 前当BlockManager缓存的BlockManagerId
      //forceFetch 标记是否强制从BlockManagerMasterActor获取最新BlockManagerId
      if (cachedPeers == null || forceFetch || timeout) {
        //当cachedPeers为空或者forceFetch为true或者当前时间超时
        //从BlockManagerMasterEndpoint获取最新BlockManagerID
        cachedPeers = master.getPeers(blockManagerId).sortBy(_.hashCode)
        lastPeerFetchTime = System.currentTimeMillis
        logDebug("Fetched peers from master: " + cachedPeers.mkString("[", ",", "]"))
      }
      cachedPeers
    }
  }

  /**
   * 复制block到另外的节点上
   * Replicate block to another node. Not that this is a blocking call that returns after
   * the block has been replicated.
   */
  private def replicate(blockId: BlockId, data: ByteBuffer, level: StorageLevel): Unit = {
    //最大复制失败数
    val maxReplicationFailures = conf.getInt("spark.storage.maxReplicationFailures", 1)
    //需要复制的备份数
    val numPeersToReplicateTo = level.replication - 1
    //可以作为备份的BlockManager的缓存
    val peersForReplication = new ArrayBuffer[BlockManagerId]
    //已经作为备份的BlockManager的缓存
    val peersReplicatedTo = new ArrayBuffer[BlockManagerId]
    //已经复制失败的BlockManager的缓存
    val peersFailedToReplicateTo = new ArrayBuffer[BlockManagerId]

    val tLevel = StorageLevel(
      level.useDisk, level.useMemory, level.useOffHeap, level.deserialized, 1)
    val startTime = System.currentTimeMillis

    //标记复制失败
    var replicationFailed = false
    //复制失败次数
    var failures = 0
    //标记复制是否完成
    var done = false

    // Get cached list of peers
    //为了容灾,peersForReplication中缓存的BlockManager不应当是当前的BlockManager
    //获取其他所有BlockManager的方法是getPeers
    peersForReplication ++= getPeers(forceFetch = false)

    // Get a random peer. Note that this selection of a peer is deterministic on the block id.
    // So assuming the list of peers does not change and no replication failures,
    // if there are multiple attempts in the same node to replicate the same block,
    // the same set of peers will be selected.
    //使用blockId的哈希值,这样保证在同一个节点上多次尝试复制同一个Block,保证它始终被复制到同一批节点上.
    val random = new Random(blockId.hashCode)
    //内部函数,用于随机获取BlockManagerId
    def getRandomPeer(): Option[BlockManagerId] = {
      // If replication had failed, then force update the cached list of peers and remove the peers
      // that have been already used
      if (replicationFailed) {
        //当复制失败并且再次尝试时,会强制从BlockManagerMasterEndpoint获取最新BlockManagerID
        peersForReplication.clear()
        peersForReplication ++= getPeers(forceFetch = true)
        //并且从peersForReplication中排除peersReplicatedTo和peersFailedToReplicateTo
        //即排除已经使用和已经复制失败的BlockManager的BlockManagerId
        peersForReplication --= peersReplicatedTo
        peersForReplication --= peersFailedToReplicateTo
      }
      if (!peersForReplication.isEmpty) {
        Some(peersForReplication(random.nextInt(peersForReplication.size)))
      } else {
        None
      }
    }

    // One by one choose a random peer and try uploading the block to it
    // If replication fails (e.g., target peer is down), force the list of cached peers
    // to be re-fetched from driver and then pick another random peer for replication. Also
    // temporarily black list the peer for which replication failed.
    //
    // This selection of a peer and replication is continued in a loop until one of the
    // following 3 conditions is fulfilled:
    // (i) specified number of peers have been replicated to
    // (ii) too many failures in replicating to peers
    // (iii) no peer left to replicate to

    //复制过程
    while (!done) {
      getRandomPeer() match { //随机获取BlockManager
        case Some(peer) =>
          try {
            val onePeerStartTime = System.currentTimeMillis
            data.rewind()
            logTrace(s"Trying to replicate $blockId of ${data.limit()} bytes to $peer")
            //上传Block到BlockManager,异步上传方法
            blockTransferService.uploadBlockSync(
              peer.host, peer.port, peer.executorId, blockId, new NioManagedBuffer(data), tLevel)
            logTrace(s"Replicated $blockId of ${data.limit()} bytes to $peer in %s ms"
              .format(System.currentTimeMillis - onePeerStartTime))
            //将peer,BlockManager添加到peersReplicatedTo,而从peersForReplication中移除
            peersReplicatedTo += peer
            peersForReplication -= peer

            replicationFailed = false

            if (peersReplicatedTo.size == numPeersToReplicateTo) {
              done = true // specified number of peers have been replicated to
            }
          } catch {
            case e: Exception =>
              //如果上传过程中出现异常则将此BlockManager添加replicationFailed,failures自增1
              logWarning(s"Failed to replicate $blockId to $peer, failure #$failures", e)
              failures += 1
              replicationFailed = true
              peersFailedToReplicateTo += peer
              //如果上传失败,以上过程会迭代多次,直到失败次数failures超过最大失败次数
              if (failures > maxReplicationFailures) { // too many failures in replcating to peers
                done = true
              }
          }
        case None => // no peer left to replicate to
          done = true
      }
    }
    val timeTakeMs = (System.currentTimeMillis - startTime)
    logDebug(s"Replicating $blockId of ${data.limit()} bytes to " +
      s"${peersReplicatedTo.size} peer(s) took $timeTakeMs ms")
    if (peersReplicatedTo.size < numPeersToReplicateTo) {
      logWarning(s"Block $blockId replicated to only " +
        s"${peersReplicatedTo.size} peer(s) instead of $numPeersToReplicateTo peers")
    }
  }

  /**
   * Read a block consisting of a single object.
   */
  def getSingle(blockId: BlockId): Option[Any] = {
    get(blockId).map(_.data.next())
  }

  /**
   * Write a block consisting of a single object.
   * 将一个对象构成的Block写入存储系统
   */
  def putSingle(
    blockId: BlockId,
    value: Any,
    level: StorageLevel,
    tellMaster: Boolean = true): Seq[(BlockId, BlockStatus)] = {
    putIterator(blockId, Iterator(value), level, tellMaster)
  }
  /**  
   * 将内存中的block溢出到磁盘中。往往在内存达到限制时调用
   */
  def dropFromMemory(
    blockId: BlockId,
    data: Either[Array[Any], ByteBuffer]): Option[BlockStatus] = {
    dropFromMemory(blockId, () => data)
  }

  /**
   * Drop a block from memory, possibly putting it on disk if applicable. Called when the memory
   * store reaches its limit and needs to free up space.
   *
   * If `data` is not put on disk, it won't be created.
   *
   * Return the block status if the given block has been updated, else None.
   * 将某个blockId从内存中移出
   * 当内存不足时,需要腾出部分内存空间
   */
  def dropFromMemory(
    blockId: BlockId,
    data: () => Either[Array[Any], ByteBuffer]): Option[BlockStatus] = {

    logInfo(s"Dropping block $blockId from memory")
    val info = blockInfo.get(blockId).orNull

    // If the block has not already been dropped
    //检查是否存在要迁移的BlockId,如果存在,从BlockInfo中获取Block的StorageLevel
    if (info != null) {
      info.synchronized {
        // required ? As of now, this will be invoked only for blocks which are ready
        // But in case this changes in future, adding for consistency sake.
        if (!info.waitForReady()) {
          // If we get here, the block write failed.
          logWarning(s"Block $blockId was marked as failure. Nothing to drop")
          return None
        } else if (blockInfo.get(blockId).isEmpty) {
          logWarning(s"Block $blockId was already dropped.")
          return None
        }
        var blockIsUpdated = false
        val level = info.level

        // Drop to disk, if storage level requires
        //如果level.useDisk存入硬盘,diskStore.contains(blockId)中不存在此文件
        if (level.useDisk && !diskStore.contains(blockId)) {
          logInfo(s"Writing block $blockId to disk")
          data() match {
            case Left(elements) =>
              //putArray存入硬盘
              diskStore.putArray(blockId, elements, level, returnValues = false)
            case Right(bytes) =>
              //putArray存入硬盘
              diskStore.putBytes(blockId, bytes, level)
          }
          blockIsUpdated = true
        }

        // Actually drop from memory store
        //从memoryStore中清除此 blockId对应的Block
        val droppedMemorySize =
          if (memoryStore.contains(blockId)) memoryStore.getSize(blockId) else 0L
        val blockIsRemoved = memoryStore.remove(blockId)
        if (blockIsRemoved) {
          blockIsUpdated = true
        } else {
          logWarning(s"Block $blockId could not be dropped from memory as it does not exist")
        }
        //获取Block的最新状态
        val status = getCurrentBlockStatus(blockId, info)
        if (info.tellMaster) {
          //reportBlockStatus给BlockManagerMasterActor报告状态
          reportBlockStatus(blockId, info, status, droppedMemorySize)
        }
        if (!level.useDisk) {
          //从blockInfo清除此blockId,返回block状态
          // The block is completely gone from this node; forget it so we can put() it again later.
          blockInfo.remove(blockId)
        }
        if (blockIsUpdated) {
          return Some(status)
        }
      }
    }
    None
  }

  /**
   * Remove all blocks belonging to the given RDD.
   * @return The number of blocks removed.
   * 将指定RDD的block全部移除，返回移除的block的数量。
   */
  def removeRdd(rddId: Int): Int = {
    // TODO: Avoid a linear scan by creating another mapping of RDD.id to blocks.
    logInfo(s"Removing RDD $rddId")
    val blocksToRemove = blockInfo.keys.flatMap(_.asRDDId).filter(_.rddId == rddId)
    blocksToRemove.foreach { blockId => removeBlock(blockId, tellMaster = false) }
    blocksToRemove.size
  }

  /**
   * Remove all blocks belonging to the given broadcast.
   * 将指定的broadcast的block全部移除,
   * 这个方法和removeRdd都是循环移除自身的所有block,移除的方法为removeBlock()
   */
  def removeBroadcast(broadcastId: Long, tellMaster: Boolean): Int = {
    logDebug(s"Removing broadcast $broadcastId")
    val blocksToRemove = blockInfo.keys.collect {
      case bid @ BroadcastBlockId(`broadcastId`, _) => bid
    }
    blocksToRemove.foreach { blockId => removeBlock(blockId, tellMaster) }
    blocksToRemove.size
  }

  /**
   * Remove a block from both memory and disk.
   * 移除内存和磁盘中的指定block,同时需要告知master并更新block信息reportBlockStatuses
   */
  def removeBlock(blockId: BlockId, tellMaster: Boolean = true): Unit = {
    logDebug(s"Removing block $blockId")
    val info = blockInfo.get(blockId).orNull
    if (info != null) {
      info.synchronized {
        // Removals are idempotent in disk store and memory store. At worst, we get a warning.
        val removedFromMemory = memoryStore.remove(blockId)
        val removedFromDisk = diskStore.remove(blockId)
        val removedFromExternalBlockStore =
          if (externalBlockStoreInitialized) externalBlockStore.remove(blockId) else false
        if (!removedFromMemory && !removedFromDisk && !removedFromExternalBlockStore) {
          logWarning(s"Block $blockId could not be removed as it was not found in either " +
            "the disk, memory, or external block store")
        }
        blockInfo.remove(blockId)
        if (tellMaster && info.tellMaster) {
          val status = getCurrentBlockStatus(blockId, info)
          reportBlockStatus(blockId, info, status)
        }
      }
    } else {
      // The block has already been removed; do nothing.
      logWarning(s"Asked to remove block $blockId, which does not exist")
    }
  }
  /**
   * 为了有效利用磁盘空间和内存,metadataCleaner和broadcastCleanner分别用于清除很久不用的非广播和广播信息
   * 移除旧的没有的/旧的broadcast block
   */
  private def dropOldNonBroadcastBlocks(cleanupTime: Long): Unit = {
    logInfo(s"Dropping non broadcast blocks older than $cleanupTime")
    dropOldBlocks(cleanupTime, !_.isBroadcast)
  }

  private def dropOldBroadcastBlocks(cleanupTime: Long): Unit = {
    logInfo(s"Dropping broadcast blocks older than $cleanupTime")
    dropOldBlocks(cleanupTime, _.isBroadcast)
  }
  /**
   * 删除很久不使用的Block从MemoryStore,DiskStore,TachyonStore清除
   * 删除一个Block,并向BlockManagerMaster发送消息,同时删除其他Slave节点
   */
  private def dropOldBlocks(cleanupTime: Long, shouldDrop: (BlockId => Boolean)): Unit = {
    val iterator = blockInfo.getEntrySet.iterator
    //遍历blockInfo
    while (iterator.hasNext) {
      val entry = iterator.next()
      val (id, info, time) = (entry.getKey, entry.getValue.value, entry.getValue.timestamp)
      if (time < cleanupTime && shouldDrop(id)) {
        info.synchronized {
          val level = info.level
          if (level.useMemory) { memoryStore.remove(id) }
          if (level.useDisk) { diskStore.remove(id) }
          if (level.useOffHeap) { externalBlockStore.remove(id) }
          iterator.remove()
          logInfo(s"Dropped block $id")
        }
        val status = getCurrentBlockStatus(id, info)
        reportBlockStatus(id, info, status)
      }
    }
  }
  /**
   * 判断是否经过压缩,共有四种压缩包——shuffle，broadcast，rdds，shuffleSpill
   */
  private def shouldCompress(blockId: BlockId): Boolean = {
    blockId match {
      case _: ShuffleBlockId     => compressShuffle
      case _: BroadcastBlockId   => compressBroadcast
      case _: RDDBlockId         => compressRdds
      case _: TempLocalBlockId   => compressShuffleSpill
      case _: TempShuffleBlockId => compressShuffle
      case _                     => false
    }
  }

  /**
   * Wrap an output stream for compression if block compression is enabled for its block type
   * 有两种加载方式，根据参数决定是压缩输入流还是压缩输出流
   */
  def wrapForCompression(blockId: BlockId, s: OutputStream): OutputStream = {
    if (shouldCompress(blockId)) compressionCodec.compressedOutputStream(s) else s
  }

  /**
   * Wrap an input stream for compression if block compression is enabled for its block type
   */
  def wrapForCompression(blockId: BlockId, s: InputStream): InputStream = {
    if (shouldCompress(blockId)) compressionCodec.compressedInputStream(s) else s
  }

  /** 
   *  Serializes into a stream.
   *  序列化为流 
   *  */
  //数据序列化,如果写入存储体系的数据本身是序列化,那么读取时应该对其反序列化
  def dataSerializeStream(
    blockId: BlockId,
    outputStream: OutputStream,
    values: Iterator[Any],
    serializer: Serializer = defaultSerializer): Unit = {
    val byteStream = new BufferedOutputStream(outputStream)
    val ser = serializer.newInstance()
    ser.serializeStream(wrapForCompression(blockId, byteStream)).writeAll(values).close()
  }

  /** 
   *  Serializes into a byte buffer.
   *  序列化为字符缓存
   *   */
  def dataSerialize(
    blockId: BlockId,
    values: Iterator[Any],
    serializer: Serializer = defaultSerializer): ByteBuffer = {
    val byteStream = new ByteArrayOutputStream(4096)
    dataSerializeStream(blockId, byteStream, values, serializer)
    ByteBuffer.wrap(byteStream.toByteArray)
  }

  /**
   * Deserializes a ByteBuffer into an iterator of values and disposes of it when the end of
   * the iterator is reached.
   * 数据序列化方法:
   * 如果写入存储体系的数据本身是序列化,那么读取时应该对其反序列化
   */
  def dataDeserialize(
    blockId: BlockId,
    bytes: ByteBuffer,
    serializer: Serializer = defaultSerializer): Iterator[Any] = {
    bytes.rewind()
    dataDeserializeStream(blockId, new ByteBufferInputStream(bytes, true), serializer)
  }

  /**
   * Deserializes a InputStream into an iterator of values and disposes of it when the end of
   * the iterator is reached.
   */
  def dataDeserializeStream(
    blockId: BlockId,
    inputStream: InputStream,
    serializer: Serializer = defaultSerializer): Iterator[Any] = {
    val stream = new BufferedInputStream(inputStream)
    serializer.newInstance().deserializeStream(wrapForCompression(blockId, stream)).asIterator
  }
  /**
   * 清除各种类的实例化对象
   */
  def stop(): Unit = {
    blockTransferService.close()
    if (shuffleClient ne blockTransferService) {
      // Closing should be idempotent, but maybe not for the NioBlockTransferService.
      shuffleClient.close()
    }
    diskBlockManager.stop()
    rpcEnv.stop(slaveEndpoint)
    blockInfo.clear()
    memoryStore.clear()
    diskStore.clear()
    if (externalBlockStoreInitialized) {
      externalBlockStore.clear()
    }
    metadataCleaner.cancel()
    broadcastCleaner.cancel()
    futureExecutionContext.shutdownNow()
    logInfo("BlockManager stopped")
  }
}

private[spark] object BlockManager extends Logging {
  private val ID_GENERATOR = new IdGenerator

  /** Return the total amount of storage memory available. */
  private def getMaxMemory(conf: SparkConf): Long = {
    //Spark用于缓存的内存大小所占用的Java堆的比率
    val memoryFraction = conf.getDouble("spark.storage.memoryFraction", 0.6)
    val safetyFraction = conf.getDouble("spark.storage.safetyFraction", 0.9)
    //execution内存最多仅占JVM heap的0.6*0.9=54%,对于无需cache数据的应用，大部分heap内存都被浪费了
    //而（shuffle等）中间数据却被频繁spill到磁盘并读取
    (Runtime.getRuntime.maxMemory * memoryFraction * safetyFraction).toLong
  }

  /**
   * Attempt to clean up a ByteBuffer if it is memory-mapped. This uses an *unsafe* Sun API that
   * might cause errors if one attempts to read from the unmapped buffer, but it's better than
   * waiting for the GC to find it because that could lead to huge numbers of open files. There's
   * unfortunately no standard API to do this.
   */
  def dispose(buffer: ByteBuffer): Unit = {
    if (buffer != null && buffer.isInstanceOf[MappedByteBuffer]) {
      logTrace(s"Unmapping $buffer")
      if (buffer.asInstanceOf[DirectBuffer].cleaner() != null) {
        buffer.asInstanceOf[DirectBuffer].cleaner().clean()
      }
    }
  }

  def blockIdsToHosts(
    blockIds: Array[BlockId],
    env: SparkEnv,
    blockManagerMaster: BlockManagerMaster = null): Map[BlockId, Seq[String]] = {

    // blockManagerMaster != null is used in tests
    assert(env != null || blockManagerMaster != null)
    val blockLocations: Seq[Seq[BlockManagerId]] = if (blockManagerMaster == null) {
      env.blockManager.getLocationBlockIds(blockIds)
    } else {
      blockManagerMaster.getLocations(blockIds)
    }

    val blockManagers = new HashMap[BlockId, Seq[String]]
    for (i <- 0 until blockIds.length) {
      blockManagers(blockIds(i)) = blockLocations(i).map(_.host)
    }
    blockManagers.toMap
  }
}
