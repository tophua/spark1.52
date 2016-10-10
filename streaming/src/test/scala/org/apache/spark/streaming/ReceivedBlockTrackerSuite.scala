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

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.language.{implicitConversions, postfixOps}
import scala.util.Random

import org.apache.hadoop.conf.Configuration
import org.scalatest.{BeforeAndAfter, Matchers}
import org.scalatest.concurrent.Eventually._

import org.apache.spark.{Logging, SparkConf, SparkException, SparkFunSuite}
import org.apache.spark.storage.StreamBlockId
import org.apache.spark.streaming.receiver.BlockManagerBasedStoreResult
import org.apache.spark.streaming.scheduler._
import org.apache.spark.streaming.util.{WriteAheadLogUtils, FileBasedWriteAheadLogReader}
import org.apache.spark.streaming.util.WriteAheadLogSuite._
import org.apache.spark.util.{Clock, ManualClock, SystemClock, Utils}

class ReceivedBlockTrackerSuite
  extends SparkFunSuite with BeforeAndAfter with Matchers with Logging {

  val hadoopConf = new Configuration()
  val akkaTimeout = 10 seconds
  val streamId = 1

  var allReceivedBlockTrackers = new ArrayBuffer[ReceivedBlockTracker]()
  var checkpointDirectory: File = null
  var conf: SparkConf = null

  before {
    conf = new SparkConf().setMaster("local[2]").setAppName("ReceivedBlockTrackerSuite")
    checkpointDirectory = Utils.createTempDir()
  }

  after {
    allReceivedBlockTrackers.foreach { _.stop() }
    Utils.deleteRecursively(checkpointDirectory)
  }

  test("block addition, and block to batch allocation") {
    //块添加和批量块分配
    val receivedBlockTracker = createTracker(setCheckpointDir = false)
    //默认禁用
    receivedBlockTracker.isWriteAheadLogEnabled should be (false)  // should be disable by default
    receivedBlockTracker.getUnallocatedBlocks(streamId) shouldEqual Seq.empty

    val blockInfos = generateBlockInfos()
    blockInfos.map(receivedBlockTracker.addBlock)

    // Verify added blocks are unallocated blocks
    //验证添加块未分配的块
    receivedBlockTracker.getUnallocatedBlocks(streamId) shouldEqual blockInfos
    receivedBlockTracker.hasUnallocatedReceivedBlocks should be (true)


    // Allocate the blocks to a batch and verify that all of them have been allocated
    //将块分配给一个批处理，并验证所有这些块已被分配
    receivedBlockTracker.allocateBlocksToBatch(1)
    receivedBlockTracker.getBlocksOfBatchAndStream(1, streamId) shouldEqual blockInfos
    receivedBlockTracker.getBlocksOfBatch(1) shouldEqual Map(streamId -> blockInfos)
    receivedBlockTracker.getUnallocatedBlocks(streamId) shouldBe empty
    receivedBlockTracker.hasUnallocatedReceivedBlocks should be (false)

    // Allocate no blocks to another batch
    //没有分配给另一个批处理
    receivedBlockTracker.allocateBlocksToBatch(2)
    receivedBlockTracker.getBlocksOfBatchAndStream(2, streamId) shouldBe empty
    receivedBlockTracker.getBlocksOfBatch(2) shouldEqual Map(streamId -> Seq.empty)

    // Verify that older batches have no operation on batch allocation,
    //确认旧的批对批分配没有操作
    // will return the same blocks as previously allocated.
    //将返回先前分配的相同块
    receivedBlockTracker.allocateBlocksToBatch(1)
    receivedBlockTracker.getBlocksOfBatchAndStream(1, streamId) shouldEqual blockInfos

    blockInfos.map(receivedBlockTracker.addBlock)
    receivedBlockTracker.allocateBlocksToBatch(2)
    receivedBlockTracker.getBlocksOfBatchAndStream(2, streamId) shouldBe empty
    receivedBlockTracker.getUnallocatedBlocks(streamId) shouldEqual blockInfos
  }

  test("recovery and cleanup with write ahead logs") {
    val manualClock = new ManualClock
    // Set the time increment level to twice the rotation interval so that every increment creates
    // a new log file

    def incrementTime() {
      val timeIncrementMillis = 2000L
      manualClock.advance(timeIncrementMillis)
    }

    // Generate and add blocks to the given tracker
    //生成并添加块到给定的跟踪
    def addBlockInfos(tracker: ReceivedBlockTracker): Seq[ReceivedBlockInfo] = {
      val blockInfos = generateBlockInfos()
      blockInfos.map(tracker.addBlock)
      blockInfos
    }

    // Print the data present in the log ahead files in the log directory
    //打印在日志目录中的日志中的数据
    def printLogFiles(message: String) {
      val fileContents = getWriteAheadLogFiles().map { file =>
        (s"\n>>>>> $file: <<<<<\n${getWrittenLogData(file).mkString("\n")}")
      }.mkString("\n")
      logInfo(s"\n\n=====================\n$message\n$fileContents\n=====================\n")
    }

    // Set WAL configuration
    //设置WAL配置
    conf.set("spark.streaming.driver.writeAheadLog.rollingIntervalSecs", "1")
    require(WriteAheadLogUtils.getRollingIntervalSecs(conf, isDriver = true) === 1)

    // Start tracker and add blocks
    //开始跟踪和添加块
    val tracker1 = createTracker(clock = manualClock)
    tracker1.isWriteAheadLogEnabled should be (true)

    val blockInfos1 = addBlockInfos(tracker1)
    tracker1.getUnallocatedBlocks(streamId).toList shouldEqual blockInfos1

    // Verify whether write ahead log has correct contents
    //验证前写日志是否有正确的内容
    val expectedWrittenData1 = blockInfos1.map(BlockAdditionEvent)
    getWrittenLogData() shouldEqual expectedWrittenData1
    getWriteAheadLogFiles() should have size 1

    incrementTime()

    // Recovery without recovery from WAL and verify list of unallocated blocks is empty
    //恢复不恢复从WAL和验证未分配的块列表是空的
    val tracker1_ = createTracker(clock = manualClock, recoverFromWriteAheadLog = false)
    tracker1_.getUnallocatedBlocks(streamId) shouldBe empty
    tracker1_.hasUnallocatedReceivedBlocks should be (false)

    // Restart tracker and verify recovered list of unallocated blocks
    //重新启动跟踪和验证恢复未分配的块列表
    val tracker2 = createTracker(clock = manualClock, recoverFromWriteAheadLog = true)
    val unallocatedBlocks = tracker2.getUnallocatedBlocks(streamId).toList
    unallocatedBlocks shouldEqual blockInfos1
    unallocatedBlocks.foreach { block =>
      block.isBlockIdValid() should be (false)
    }


    // Allocate blocks to batch and verify whether the unallocated blocks got allocated
    //分配块批量验证是否未分配的块被分配
    val batchTime1 = manualClock.getTimeMillis()
    tracker2.allocateBlocksToBatch(batchTime1)
    tracker2.getBlocksOfBatchAndStream(batchTime1, streamId) shouldEqual blockInfos1
    tracker2.getBlocksOfBatch(batchTime1) shouldEqual Map(streamId -> blockInfos1)

    // Add more blocks and allocate to another batch
    //添加更多块，并分配到另一个批次
    incrementTime()
    val batchTime2 = manualClock.getTimeMillis()
    val blockInfos2 = addBlockInfos(tracker2)
    tracker2.allocateBlocksToBatch(batchTime2)
    tracker2.getBlocksOfBatchAndStream(batchTime2, streamId) shouldEqual blockInfos2

    // Verify whether log has correct contents
    //验证日志是否具有正确的内容
    val expectedWrittenData2 = expectedWrittenData1 ++
      Seq(createBatchAllocation(batchTime1, blockInfos1)) ++
      blockInfos2.map(BlockAdditionEvent) ++
      Seq(createBatchAllocation(batchTime2, blockInfos2))
    getWrittenLogData() shouldEqual expectedWrittenData2

    // Restart tracker and verify recovered state
    //重新启动跟踪和验证恢复状态
    incrementTime()
    val tracker3 = createTracker(clock = manualClock, recoverFromWriteAheadLog = true)
    tracker3.getBlocksOfBatchAndStream(batchTime1, streamId) shouldEqual blockInfos1
    tracker3.getBlocksOfBatchAndStream(batchTime2, streamId) shouldEqual blockInfos2
    tracker3.getUnallocatedBlocks(streamId) shouldBe empty

    // Cleanup first batch but not second batch
    //清理第一批，但不是第二批
    val oldestLogFile = getWriteAheadLogFiles().head
    incrementTime()
    tracker3.cleanupOldBatches(batchTime2, waitForCompletion = true)

    // Verify that the batch allocations have been cleaned, and the act has been written to log
    //确认批分配已被清除,并已写入日志
    tracker3.getBlocksOfBatchAndStream(batchTime1, streamId) shouldEqual Seq.empty
    getWrittenLogData(getWriteAheadLogFiles().last) should contain(createBatchCleanup(batchTime1))

    // Verify that at least one log file gets deleted
    //确认至少有一个日志文件被删除
    eventually(timeout(10 seconds), interval(10 millisecond)) {
      getWriteAheadLogFiles() should not contain oldestLogFile
    }
    printLogFiles("After clean")

    // Restart tracker and verify recovered state, specifically whether info about the first
    // batch has been removed, but not the second batch
    //重新启动跟踪和验证恢复状态,具体是否关于第一批信息已被删除,但不是第二批
    incrementTime()
    val tracker4 = createTracker(clock = manualClock, recoverFromWriteAheadLog = true)
    tracker4.getUnallocatedBlocks(streamId) shouldBe empty
    //应该是干净的
    tracker4.getBlocksOfBatchAndStream(batchTime1, streamId) shouldBe empty  // should be cleaned
    tracker4.getBlocksOfBatchAndStream(batchTime2, streamId) shouldEqual blockInfos2
  }

  test("disable write ahead log when checkpoint directory is not set") {
    //当检查点被禁用时,则禁用前写日志记录
    // When checkpoint is disabled, then the write ahead log is disabled
    val tracker1 = createTracker(setCheckpointDir = false)
    tracker1.isWriteAheadLogEnabled should be (false)
  }

  /**
   * Create tracker object with the optional provided clock. Use fake clock if you
   * want to control time by manually incrementing it to test log clean.
   * 用可选提供的时钟创建跟踪对象,如果你想通过手动增加到测试日志清洁控制时间使用假时钟,
   */
  def createTracker(
      setCheckpointDir: Boolean = true,
      recoverFromWriteAheadLog: Boolean = false,
      clock: Clock = new SystemClock): ReceivedBlockTracker = {
    val cpDirOption = if (setCheckpointDir) Some(checkpointDirectory.toString) else None
    val tracker = new ReceivedBlockTracker(
      conf, hadoopConf, Seq(streamId), clock, recoverFromWriteAheadLog, cpDirOption)
    allReceivedBlockTrackers += tracker
    tracker
  }

  /** 
   *  Generate blocks infos using random ids
   *  使用随机生成的块的信息系统 
   *  */
  def generateBlockInfos(): Seq[ReceivedBlockInfo] = {
    List.fill(5)(ReceivedBlockInfo(streamId, Some(0L), None,
      BlockManagerBasedStoreResult(StreamBlockId(streamId, math.abs(Random.nextInt)), Some(0L))))
  }

  /** 
   *  Get all the data written in the given write ahead log file.
   *  获取给定的写在前面的日志文件中的所有数据 
   *  */
  def getWrittenLogData(logFile: String): Seq[ReceivedBlockTrackerLogEvent] = {
    getWrittenLogData(Seq(logFile))
  }

  /**
   * Get all the data written in the given write ahead log files. By default, it will read all
   * files in the test log directory.
   * 获取给定的写在前面的日志文件中的所有数据,默认情况下,它将读取测试日志目录中的所有文件
   */
  def getWrittenLogData(logFiles: Seq[String] = getWriteAheadLogFiles)
    : Seq[ReceivedBlockTrackerLogEvent] = {
    logFiles.flatMap {
      file => new FileBasedWriteAheadLogReader(file, hadoopConf).toSeq
    }.map { byteBuffer =>
      Utils.deserialize[ReceivedBlockTrackerLogEvent](byteBuffer.array)
    }.toList
  }

  /** 
   *  Get all the write ahead log files in the test directory
   *  获取测试目录中的所有写入前的日志文件 
   *  */
  def getWriteAheadLogFiles(): Seq[String] = {
    import ReceivedBlockTracker._
    val logDir = checkpointDirToLogDir(checkpointDirectory.toString)
    getLogFilesInDirectory(logDir).map { _.toString }
  }

  /** 
   *  Create batch allocation object from the given info
   *  从指定的信息创建批分配对象 
   *  */
  def createBatchAllocation(time: Long, blockInfos: Seq[ReceivedBlockInfo])
    : BatchAllocationEvent = {
    BatchAllocationEvent(time, AllocatedBlocks(Map((streamId -> blockInfos))))
  }

  /** 
   *  Create batch clean object from the given info
   *  从给定的信息创建批处理干净的对象 
   *  */
  def createBatchCleanup(time: Long, moreTimes: Long*): BatchCleanupEvent = {
    BatchCleanupEvent((Seq(time) ++ moreTimes).map(Time.apply))
  }

  implicit def millisToTime(milliseconds: Long): Time = Time(milliseconds)

  implicit def timeToMillis(time: Time): Long = time.milliseconds
}
