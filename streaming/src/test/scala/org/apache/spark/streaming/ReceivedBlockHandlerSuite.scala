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

package org.apache.spark.streaming

import java.io.File
import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.language.postfixOps

import org.apache.hadoop.conf.Configuration
import org.scalatest.{BeforeAndAfter, Matchers}
import org.scalatest.concurrent.Eventually._

import org.apache.spark._
import org.apache.spark.network.nio.NioBlockTransferService
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.scheduler.LiveListenerBus
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.shuffle.hash.HashShuffleManager
import org.apache.spark.storage._
import org.apache.spark.streaming.receiver._
import org.apache.spark.streaming.util._
import org.apache.spark.util.{ManualClock, Utils}
import WriteAheadLogBasedBlockHandler._
import WriteAheadLogSuite._
/**
 * 接收块处理测试套件
 */
class ReceivedBlockHandlerSuite
  extends SparkFunSuite
  with BeforeAndAfter
  with Matchers
  with Logging {
  //日志滚动间隔秒
  val conf = new SparkConf().set("spark.streaming.receiver.writeAheadLog.rollingIntervalSecs", "1")
  val hadoopConf = new Configuration()
  val streamId = 1
  //创建安全管理
  val securityMgr = new SecurityManager(conf)
  //MapOutputTrackerMaster中维护的mapStatuses从本地或者其他远程节点读取文件
  val mapOutputTracker = new MapOutputTrackerMaster(conf)
  val shuffleManager = new HashShuffleManager(conf)
  //对象序列化
  val serializer = new KryoSerializer(conf)
  val manualClock = new ManualClock
  //块管理大小
  val blockManagerSize = 10000000
  val blockManagerBuffer = new ArrayBuffer[BlockManager]()

  var rpcEnv: RpcEnv = null
  var blockManagerMaster: BlockManagerMaster = null
  var blockManager: BlockManager = null
  var storageLevel: StorageLevel = null
  var tempDirectory: File = null

  before {
    rpcEnv = RpcEnv.create("test", "localhost", 0, conf, securityMgr)
    conf.set("spark.driver.port", rpcEnv.address.port.toString)

    blockManagerMaster = new BlockManagerMaster(rpcEnv.setupEndpoint("blockmanager",
      new BlockManagerMasterEndpoint(rpcEnv, true, conf, new LiveListenerBus)), conf, true)

    storageLevel = StorageLevel.MEMORY_ONLY_SER
    blockManager = createBlockManager(blockManagerSize, conf)
    //创建临时目录
    tempDirectory = Utils.createTempDir()
    manualClock.setTime(0)
  }

  after {
    for ( blockManager <- blockManagerBuffer ) {
      if (blockManager != null) {
        //块管理服务暂停
        blockManager.stop()
      }
    }
    blockManager = null
    //清空blockManager
    blockManagerBuffer.clear()
    if (blockManagerMaster != null) {
      blockManagerMaster.stop()
      blockManagerMaster = null
    }
    //远程调用关闭
    rpcEnv.shutdown()
    //等待终止服务
    rpcEnv.awaitTermination()
    rpcEnv = null
    //递归删除临时目录
    Utils.deleteRecursively(tempDirectory)
  }

  test("BlockManagerBasedBlockHandler - store blocks") {//存储块
    withBlockManagerBasedBlockHandler { handler =>
      testBlockStoring(handler) { case (data, blockIds, storeResults) =>
        // Verify the data in block manager is correct
        //验证块管理器中的数据是否正确
        val storedData = blockIds.flatMap { blockId =>
          blockManager.getLocal(blockId).map(_.data.map(_.toString).toList).getOrElse(List.empty)
        }.toList
        storedData shouldEqual data

        // Verify that the store results are instances of BlockManagerBasedStoreResult
        //验证存储结果的实例blockmanagerbasedstoreresult
        assert(
          storeResults.forall { _.isInstanceOf[BlockManagerBasedStoreResult] },
          "Unexpected store result type"
        )
      }
    }
  }

  test("BlockManagerBasedBlockHandler - handle errors in storing block") {//存储块中的处理错误
    withBlockManagerBasedBlockHandler { handler =>
      testErrorHandling(handler)
    }
  }

  test("WriteAheadLogBasedBlockHandler - store blocks") {//存储块
    withWriteAheadLogBasedBlockHandler { handler =>
      testBlockStoring(handler) { case (data, blockIds, storeResults) =>
        // Verify the data in block manager is correct
        //验证块管理器中的数据是否正确
        val storedData = blockIds.flatMap { blockId =>
          blockManager.getLocal(blockId).map(_.data.map(_.toString).toList).getOrElse(List.empty)
        }.toList
        storedData shouldEqual data

        // Verify that the store results are instances of WriteAheadLogBasedStoreResult
        //验证存储结果的实例WriteAheadLogBasedStoreResult
        assert(
          storeResults.forall { _.isInstanceOf[WriteAheadLogBasedStoreResult] },
          "Unexpected store result type"
        )
        // Verify the data in write ahead log files is correct
        //在预写式日志文件中验证数据是正确
        val walSegments = storeResults.map { result =>
          result.asInstanceOf[WriteAheadLogBasedStoreResult].walRecordHandle
        }
        val loggedData = walSegments.flatMap { walSegment =>
          val fileSegment = walSegment.asInstanceOf[FileBasedWriteAheadLogSegment]
          val reader = new FileBasedWriteAheadLogRandomReader(fileSegment.path, hadoopConf)
          val bytes = reader.read(fileSegment)
          reader.close()
          blockManager.dataDeserialize(generateBlockId(), bytes).toList
        }
        loggedData shouldEqual data
      }
    }
  }

  test("WriteAheadLogBasedBlockHandler - handle errors in storing block") {//存储块中的处理错误
    withWriteAheadLogBasedBlockHandler { handler =>
      testErrorHandling(handler)
    }
  }

  test("WriteAheadLogBasedBlockHandler - clean old blocks") {//清理旧的块
    withWriteAheadLogBasedBlockHandler { handler =>
      val blocks = Seq.tabulate(10) { i => IteratorBlock(Iterator(1 to i)) }
      storeBlocks(handler, blocks)

      val preCleanupLogFiles = getWriteAheadLogFiles()
      require(preCleanupLogFiles.size > 1)

      // this depends on the number of blocks inserted using generateAndStoreData()
      //这取决于块的数量插入使用
      manualClock.getTimeMillis() shouldEqual 5000L

      val cleanupThreshTime = 3000L
      handler.cleanupOldBlocks(cleanupThreshTime)
      eventually(timeout(10000 millis), interval(10 millis)) {
        getWriteAheadLogFiles().size should be < preCleanupLogFiles.size
      }
    }
  }

  test("Test Block - count messages") {//统计消息
    // Test count with BlockManagedBasedBlockHandler
    //测试计数BlockManagedBasedBlockHandler
    testCountWithBlockManagerBasedBlockHandler(true)
    // Test count with WriteAheadLogBasedBlockHandler
    testCountWithBlockManagerBasedBlockHandler(false)
  }

  test("Test Block - isFullyConsumed") {//完全消耗
    val sparkConf = new SparkConf()
    sparkConf.set("spark.storage.unrollMemoryThreshold", "512")
    // spark.storage.unrollFraction set to 0.4 for BlockManager
    //Unroll内存：spark允许数据以序列化或非序列化的形式存储,序列化的数据不能拿过来直接使用,所以就需要先反序列化,即unroll
    sparkConf.set("spark.storage.unrollFraction", "0.4")
    // Block Manager with 12000 * 0.4 = 4800 bytes of free space for unroll
    //块管理12000 * 0.4 = 4800字节的自由空间展开
    blockManager = createBlockManager(12000, sparkConf)

    // there is not enough space to store this block in MEMORY,
    //没有足够的空间来存储这个块在内存中,
    // But BlockManager will be able to sereliaze this block to WAL(预写式日志)
    // and hence count returns correct value.
    //但BlockManager将序列化这块到WAL,因此count返回正确的值
     testRecordcount(false, StorageLevel.MEMORY_ONLY,
      IteratorBlock((List.fill(70)(new Array[Byte](100))).iterator), blockManager, Some(70))

    // there is not enough space to store this block in MEMORY,
    //没有足够的空间来存储这个块在内存中
    // But BlockManager will be able to sereliaze this block to DISK
    // and hence count returns correct value.
    testRecordcount(true, StorageLevel.MEMORY_AND_DISK,
      IteratorBlock((List.fill(70)(new Array[Byte](100))).iterator), blockManager, Some(70))

    // there is not enough space to store this block With MEMORY_ONLY StorageLevel.
    //没有足够的空间来存储这一块MEMORY_ONLY存储级别
    // BlockManager will not be able to unroll this block
    // and hence it will not tryToPut this block, resulting the SparkException
    //blockmanager将无法打开展开块,因此它不会trytoput这块
    storageLevel = StorageLevel.MEMORY_ONLY
    withBlockManagerBasedBlockHandler { handler =>
      val thrown = intercept[SparkException] {
        storeSingleBlock(handler, IteratorBlock((List.fill(70)(new Array[Byte](100))).iterator))
      }
    }
  }

  private def testCountWithBlockManagerBasedBlockHandler(isBlockManagerBasedBlockHandler: Boolean) {
    // ByteBufferBlock-MEMORY_ONLY
    //字节缓冲区块--只在内存中,将RDD 作为反序列化的的对象存储JVM 中
    testRecordcount(isBlockManagerBasedBlockHandler, StorageLevel.MEMORY_ONLY,
      ByteBufferBlock(ByteBuffer.wrap(Array.tabulate(100)(i => i.toByte))), blockManager, None)
    // ByteBufferBlock-MEMORY_ONLY_SER  MEMORY_ONLY不同的是会将数据备份到集群中两个不同的节点
    testRecordcount(isBlockManagerBasedBlockHandler, StorageLevel.MEMORY_ONLY_SER,
      ByteBufferBlock(ByteBuffer.wrap(Array.tabulate(100)(i => i.toByte))), blockManager, None)
    // ArrayBufferBlock-MEMORY_ONLY
    testRecordcount(isBlockManagerBasedBlockHandler, StorageLevel.MEMORY_ONLY,
      ArrayBufferBlock(ArrayBuffer.fill(25)(0)), blockManager, Some(25))
    // ArrayBufferBlock-MEMORY_ONLY_SER MEMORY_ONLY不同的是会将数据备份到集群中两个不同的节点
    testRecordcount(isBlockManagerBasedBlockHandler, StorageLevel.MEMORY_ONLY_SER,
      ArrayBufferBlock(ArrayBuffer.fill(25)(0)), blockManager, Some(25))
    // ArrayBufferBlock-DISK_ONLY
    testRecordcount(isBlockManagerBasedBlockHandler, StorageLevel.DISK_ONLY,
      ArrayBufferBlock(ArrayBuffer.fill(50)(0)), blockManager, Some(50))
    // ArrayBufferBlock-MEMORY_AND_DISK 在内存空间不足的情况下,将序列化之后的数据存储于磁盘
    testRecordcount(isBlockManagerBasedBlockHandler, StorageLevel.MEMORY_AND_DISK,
      ArrayBufferBlock(ArrayBuffer.fill(75)(0)), blockManager, Some(75))
    // IteratorBlock-MEMORY_ONLY
    testRecordcount(isBlockManagerBasedBlockHandler, StorageLevel.MEMORY_ONLY,
      IteratorBlock((ArrayBuffer.fill(100)(0)).iterator), blockManager, Some(100))
    // IteratorBlock-MEMORY_ONLY_SER
    testRecordcount(isBlockManagerBasedBlockHandler, StorageLevel.MEMORY_ONLY_SER,
      IteratorBlock((ArrayBuffer.fill(100)(0)).iterator), blockManager, Some(100))
    // IteratorBlock-DISK_ONLY 仅仅使用磁盘存储RDD的数据(未经序列化)
    testRecordcount(isBlockManagerBasedBlockHandler, StorageLevel.DISK_ONLY,
      IteratorBlock((ArrayBuffer.fill(125)(0)).iterator), blockManager, Some(125))
    // IteratorBlock-MEMORY_AND_DISK 
    //RDD的数据直接以Java对象的形式存储于JVM的内存中,如果内存空间不中,某些分区的数据会被存储至磁盘,使用的时候从磁盘读取
    testRecordcount(isBlockManagerBasedBlockHandler, StorageLevel.MEMORY_AND_DISK,
      IteratorBlock((ArrayBuffer.fill(150)(0)).iterator), blockManager, Some(150))
  }
  //创建块管理器
  private def createBlockManager(
      maxMem: Long,
      conf: SparkConf,
      name: String = SparkContext.DRIVER_IDENTIFIER): BlockManager = {
    //创建块传输服务
    val transfer = new NioBlockTransferService(conf, securityMgr)
    //块管理服务
    val manager = new BlockManager(name, rpcEnv, blockManagerMaster, serializer, maxMem, conf,
      mapOutputTracker, shuffleManager, transfer, securityMgr, 0)
    //块管理服务初始化
    manager.initialize("app-id")
    blockManagerBuffer += manager
    manager
  }

  /**
   * Test storing of data using different types of Handler, StorageLevle and ReceivedBlocks
   * and verify the correct record count
   * 使用不同类型的汉德勒的数据的测试存储,存储级别和ReceivedBlocks验证正确的记录数
   */
  private def testRecordcount(isBlockManagedBasedBlockHandler: Boolean,
      sLevel: StorageLevel,
      receivedBlock: ReceivedBlock,
      bManager: BlockManager,
      expectedNumRecords: Option[Long]
      ) {
    blockManager = bManager
    storageLevel = sLevel
    var bId: StreamBlockId = null
    try {
      if (isBlockManagedBasedBlockHandler) {
        // test received block with BlockManager based handler
        //测试接收块blockmanager基础处理
        withBlockManagerBasedBlockHandler { handler =>
          val (blockId, blockStoreResult) = storeSingleBlock(handler, receivedBlock)
          bId = blockId
          assert(blockStoreResult.numRecords === expectedNumRecords,
            "Message count not matches for a " +
            receivedBlock.getClass.getName +
            " being inserted using BlockManagerBasedBlockHandler with " + sLevel)
       }
      } else {
        // test received block with WAL based handler
        //基于WAL(预写式日志)的处理程序的测试接收块
        withWriteAheadLogBasedBlockHandler { handler =>
          val (blockId, blockStoreResult) = storeSingleBlock(handler, receivedBlock)
          bId = blockId
          assert(blockStoreResult.numRecords === expectedNumRecords,
            "Message count not matches for a " +
            receivedBlock.getClass.getName +
            " being inserted using WriteAheadLogBasedBlockHandler with " + sLevel)
        }
      }
    } finally {
     // Removing the Block Id to use same blockManager for next test
     //删除块ID使用相同的blockmanager下一个测试
     blockManager.removeBlock(bId, true)
    }
  }

  /**
   * Test storing of data using different forms of ReceivedBlocks and verify that they succeeded
   * using the given verification function
   * 测试存储采用不同形式的receivedblocks数据验证,他们成功地使用了验证功能
   */
  private def testBlockStoring(receivedBlockHandler: ReceivedBlockHandler)
      (verifyFunc: (Seq[String], Seq[StreamBlockId], Seq[ReceivedBlockStoreResult]) => Unit) {
    val data = Seq.tabulate(100) { _.toString }

    def storeAndVerify(blocks: Seq[ReceivedBlock]) {
      blocks should not be empty
      val (blockIds, storeResults) = storeBlocks(receivedBlockHandler, blocks)
      withClue(s"Testing with ${blocks.head.getClass.getSimpleName}s:") {
        // Verify returns store results have correct block ids
        // 验证返回存储结果是否有正确的块ID
        (storeResults.map { _.blockId }) shouldEqual blockIds

        // Call handler-specific verification function
        //调用处理程序特定的验证功能
        verifyFunc(data, blockIds, storeResults)
      }
    }

    def dataToByteBuffer(b: Seq[String]) = blockManager.dataSerialize(generateBlockId, b.iterator)

    val blocks = data.grouped(10).toSeq

    storeAndVerify(blocks.map { b => IteratorBlock(b.toIterator) })
    storeAndVerify(blocks.map { b => ArrayBufferBlock(new ArrayBuffer ++= b) })
    storeAndVerify(blocks.map { b => ByteBufferBlock(dataToByteBuffer(b)) })
  }

  /** 
   *  Test error handling when blocks that cannot be stored
   *  无法存储的块时的测试错误处理 
   *  */
  private def testErrorHandling(receivedBlockHandler: ReceivedBlockHandler) {
    // Handle error in iterator (e.g. divide-by-zero error)
    //迭代器中的句柄错误
    intercept[Exception] {
      val iterator = (10 to (-10, -1)).toIterator.map { _ / 0 }
      receivedBlockHandler.storeBlock(StreamBlockId(1, 1), IteratorBlock(iterator))
    }

    // Handler error in block manager storing (e.g. too big block)
    //块管理器存储中的处理错误
    intercept[SparkException] {
      val byteBuffer = ByteBuffer.wrap(new Array[Byte](blockManagerSize + 1))
      receivedBlockHandler.storeBlock(StreamBlockId(1, 1), ByteBufferBlock(byteBuffer))
    }
  }

  /** 
   *  Instantiate a BlockManagerBasedBlockHandler and run a code with it
   *  实例化一个blockmanagerbasedblockhandler和它运行代码
   *   */
  private def withBlockManagerBasedBlockHandler(body: BlockManagerBasedBlockHandler => Unit) {
    body(new BlockManagerBasedBlockHandler(blockManager, storageLevel))
  }

  /** 
   *  Instantiate a WriteAheadLogBasedBlockHandler and run a code with it 
   *  实例化一个WriteAheadLogBasedBlockHandler和它运行代码
   *  */
  private def withWriteAheadLogBasedBlockHandler(body: WriteAheadLogBasedBlockHandler => Unit) {
    require(WriteAheadLogUtils.getRollingIntervalSecs(conf, isDriver = false) === 1)
    val receivedBlockHandler = new WriteAheadLogBasedBlockHandler(blockManager, 1,
      storageLevel, conf, hadoopConf, tempDirectory.toString, manualClock)
    try {
      body(receivedBlockHandler)
    } finally {
      receivedBlockHandler.stop()
    }
  }

  /** 
   *  Store blocks using a handler
   *  处理块的存储 
   *  */
  private def storeBlocks(
      receivedBlockHandler: ReceivedBlockHandler,
      blocks: Seq[ReceivedBlock]
    ): (Seq[StreamBlockId], Seq[ReceivedBlockStoreResult]) = {
    val blockIds = Seq.fill(blocks.size)(generateBlockId())
    val storeResults = blocks.zip(blockIds).map {
      case (block, id) =>
        //通过sparkconf设置日志滚动间隔为1000毫秒
        manualClock.advance(500) // log rolling interval set to 1000 ms through SparkConf
        logDebug("Inserting block " + id)
        receivedBlockHandler.storeBlock(id, block)
    }.toList
    logDebug("Done inserting")
    (blockIds, storeResults)
  }

  /** 
   *  Store single block using a handler
   *  使用处理程序存储单个块
   * */
  private def storeSingleBlock(
      handler: ReceivedBlockHandler,
      block: ReceivedBlock
    ): (StreamBlockId, ReceivedBlockStoreResult) = {
    val blockId = generateBlockId
    val blockStoreResult = handler.storeBlock(blockId, block)
    logDebug("Done inserting")
    (blockId, blockStoreResult)
  }

  private def getWriteAheadLogFiles(): Seq[String] = {
    getLogFilesInDirectory(checkpointDirToLogDir(tempDirectory.toString, streamId))
  }

  private def generateBlockId(): StreamBlockId = StreamBlockId(streamId, scala.util.Random.nextLong)
}

