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

package org.apache.spark.streaming.scheduler

import java.nio.ByteBuffer

import scala.collection.mutable
import scala.language.implicitConversions

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.streaming.Time
import org.apache.spark.streaming.util.{WriteAheadLog, WriteAheadLogUtils}
import org.apache.spark.util.{Clock, Utils}
import org.apache.spark.{Logging, SparkConf}

/** 
 *  Trait representing any event in the ReceivedBlockTracker that updates its state.
 *  特征表示,更新其状态receivedblocktracker任何事件
 *   */
private[streaming] sealed trait ReceivedBlockTrackerLogEvent

private[streaming] case class BlockAdditionEvent(receivedBlockInfo: ReceivedBlockInfo)
  extends ReceivedBlockTrackerLogEvent
private[streaming] case class BatchAllocationEvent(time: Time, allocatedBlocks: AllocatedBlocks)
  extends ReceivedBlockTrackerLogEvent
private[streaming] case class BatchCleanupEvent(times: Seq[Time])
  extends ReceivedBlockTrackerLogEvent


/** 
 *  Class representing the blocks of all the streams allocated to a batch
 *  表示将要分配给一个批处理的所有流的块的类
 *   */
private[streaming]
case class AllocatedBlocks(streamIdToAllocatedBlocks: Map[Int, Seq[ReceivedBlockInfo]]) {
  def getBlocksOfStream(streamId: Int): Seq[ReceivedBlockInfo] = {
    streamIdToAllocatedBlocks.getOrElse(streamId, Seq.empty)
  }
}

/**
 * Class that keep track of all the received blocks, and allocate them to batches
 * when required. All actions taken by this class can be saved to a write ahead log
 * 跟踪所有接收到的块的跟踪的类,并在需要时将它们分配给批处理,
 * 由这个类所采取的所有操作都可以保存到一个写提前日志,使跟踪者的状态,驱动故障后可以恢复
 * (if a checkpoint directory has been provided), so that the state of the tracker
 * (received blocks and block-to-batch allocations) can be recovered after driver failure.
 *
 * Note that when any instance of this class is created with a checkpoint directory,
 * it will try reading events from logs in the directory.
 */
private[streaming] class ReceivedBlockTracker(
    conf: SparkConf,
    hadoopConf: Configuration,
    streamIds: Seq[Int],
    clock: Clock,
    recoverFromWriteAheadLog: Boolean,
    checkpointDirOption: Option[String])
  extends Logging {
  //缓存从blockForPushing中取出一个Block
  private type ReceivedBlockQueue = mutable.Queue[ReceivedBlockInfo]

  private val streamIdToUnallocatedBlockQueues = new mutable.HashMap[Int, ReceivedBlockQueue]
  private val timeToAllocatedBlocks = new mutable.HashMap[Time, AllocatedBlocks]
  private val writeAheadLogOption = createWriteAheadLog()

  private var lastAllocatedBatchTime: Time = null

  // Recover block information from write ahead logs
  //从写入前记录日志中恢复块信息
  if (recoverFromWriteAheadLog) {
    recoverPastEvents()
  }

  /** 
   *  Add received block. This event will get written to the write ahead log (if enabled).
   *  添加接收块,此事件将被写入到写入前日志(如果启用)
   *   */
  def addBlock(receivedBlockInfo: ReceivedBlockInfo): Boolean = synchronized {
    try {
      //在收到了Receiver 报上来的 meta 信息后,先通过 writeToLog()写到 WAL
      writeToLog(BlockAdditionEvent(receivedBlockInfo))
      //再将 meta信息索引起来
      getReceivedBlockQueue(receivedBlockInfo.streamId) += receivedBlockInfo
      logDebug(s"Stream ${receivedBlockInfo.streamId} received " +
        s"block ${receivedBlockInfo.blockStoreResult.blockId}")
      true
    } catch {
      case e: Exception =>
        logError(s"Error adding block $receivedBlockInfo", e)
        false
    }
  }

  /**
   * Allocate all unallocated blocks to the given batch.
   * 所有未分配的块分配到给定的批,此事件将被写入前写日志
   * This event will get written to the write ahead log (if enabled).
   */
  def allocateBlocksToBatch(batchTime: Time): Unit = synchronized {
    if (lastAllocatedBatchTime == null || batchTime > lastAllocatedBatchTime) {
      val streamIdToBlocks = streamIds.map { streamId =>
          (streamId, getReceivedBlockQueue(streamId).dequeueAll(x => true))
      }.toMap
      val allocatedBlocks = AllocatedBlocks(streamIdToBlocks)
      //在收到 JobGenerator的为最新的 batch划分 meta信息的要求后,先通过 writeToLog()写到 WAL
      writeToLog(BatchAllocationEvent(batchTime, allocatedBlocks))
      //再将 meta信息划分到最新的 batch里
      timeToAllocatedBlocks(batchTime) = allocatedBlocks
      lastAllocatedBatchTime = batchTime
      allocatedBlocks
    } else {
      // This situation occurs when:
      // 这种情况发生时：
      // 1. WAL is ended with BatchAllocationEvent, but without BatchCleanupEvent,
      // possibly processed batch job or half-processed batch job need to be processed again,
      // so the batchTime will be equal to lastAllocatedBatchTime.
      // 2. Slow checkpointing makes recovered batch time older than WAL recovered
      // lastAllocatedBatchTime.
      // This situation will only occurs in recovery time.
      logInfo(s"Possibly processed batch $batchTime need to be processed again in WAL recovery")
    }
  }

  /** 
   *  Get the blocks allocated to the given batch. 
   *  获取分配给给定批的块
   *  */
  def getBlocksOfBatch(batchTime: Time): Map[Int, Seq[ReceivedBlockInfo]] = synchronized {
    timeToAllocatedBlocks.get(batchTime).map { _.streamIdToAllocatedBlocks }.getOrElse(Map.empty)
  }

  /** 
   *  Get the blocks allocated to the given batch and stream. 
   *  获取分配给给定批处理和流的块
   *  */
  def getBlocksOfBatchAndStream(batchTime: Time, streamId: Int): Seq[ReceivedBlockInfo] = {
    synchronized {
      timeToAllocatedBlocks.get(batchTime).map {
        _.getBlocksOfStream(streamId)
      }.getOrElse(Seq.empty)
    }
  }

  /** 
   *  Check if any blocks are left to be allocated to batches.
   *  检查是否有任何块被分配给批
   *   */
  def hasUnallocatedReceivedBlocks: Boolean = synchronized {
    !streamIdToUnallocatedBlockQueues.values.forall(_.isEmpty)
  }

  /**
   * Get blocks that have been added but not yet allocated to any batch. This method
   * is primarily used for testing.
   * 获取已添加的块,但尚未分配给任何批处理,此方法主要用于测试
   */
  def getUnallocatedBlocks(streamId: Int): Seq[ReceivedBlockInfo] = synchronized {
    getReceivedBlockQueue(streamId).toSeq
  }

  /**
   * Clean up block information of old batches. If waitForCompletion is true, this method
   * returns only after the files are cleaned up.
   * 清理旧批次的块信息,如果waitforcompletion是true,只返回文件后清理方法。
   */
  def cleanupOldBatches(cleanupThreshTime: Time, waitForCompletion: Boolean): Unit = synchronized {
    require(cleanupThreshTime.milliseconds < clock.getTimeMillis())
    val timesToCleanup = timeToAllocatedBlocks.keys.filter { _ < cleanupThreshTime }.toSeq
    logInfo("Deleting batches " + timesToCleanup)
    //在收到了JobGenerator的清除过时的 meta信息要求后先通过 writeToLog()写到 WAL
    writeToLog(BatchCleanupEvent(timesToCleanup))
    //再将过时的 meta 信息清理掉
    timeToAllocatedBlocks --= timesToCleanup
    //再将 WAL里过时的 meta信息对应的 log清理掉
    writeAheadLogOption.foreach(_.clean(cleanupThreshTime.milliseconds, waitForCompletion))
  }

  /** 
   *  Stop the block tracker.
   *  阻止块跟踪
   *   */
  def stop() {
    writeAheadLogOption.foreach { _.close() }
  }

  /**
   * Recover all the tracker actions from the write ahead logs to recover the state (unallocated
   * and allocated block info) prior to failure.
   * 从写前日志恢复状态恢复所有的跟踪行动(未分配和分配的块信息)失败之前
   */
  private def recoverPastEvents(): Unit = synchronized {
    // Insert the recovered block information
    //插入恢复块信息
    def insertAddedBlock(receivedBlockInfo: ReceivedBlockInfo) {
      logTrace(s"Recovery: Inserting added block $receivedBlockInfo")
      receivedBlockInfo.setBlockIdInvalid()
      getReceivedBlockQueue(receivedBlockInfo.streamId) += receivedBlockInfo
    }

    // Insert the recovered block-to-batch allocations and clear the queue of received blocks
    //将恢复的块插入到批处理分配中,并清除接收块的队列
    // (when the blocks were originally allocated to the batch, the queue must have been cleared).
    //(当块最初被分配给批处理时,队列必须被清除)
    def insertAllocatedBatch(batchTime: Time, allocatedBlocks: AllocatedBlocks) {
      logTrace(s"Recovery: Inserting allocated batch for time $batchTime to " +
        s"${allocatedBlocks.streamIdToAllocatedBlocks}")
      streamIdToUnallocatedBlockQueues.values.foreach { _.clear() }
      lastAllocatedBatchTime = batchTime
      timeToAllocatedBlocks.put(batchTime, allocatedBlocks)
    }

    // Cleanup the batch allocations
    //清理批分配
    def cleanupBatches(batchTimes: Seq[Time]) {
      logTrace(s"Recovery: Cleaning up batches $batchTimes")
      timeToAllocatedBlocks --= batchTimes
    }

    writeAheadLogOption.foreach { writeAheadLog =>
      logInfo(s"Recovering from write ahead logs in ${checkpointDirOption.get}")
      import scala.collection.JavaConversions._
      writeAheadLog.readAll().foreach { byteBuffer =>
        logTrace("Recovering record " + byteBuffer)
        Utils.deserialize[ReceivedBlockTrackerLogEvent](
          byteBuffer.array, Thread.currentThread().getContextClassLoader) match {
          case BlockAdditionEvent(receivedBlockInfo) =>
            insertAddedBlock(receivedBlockInfo)
          case BatchAllocationEvent(time, allocatedBlocks) =>
            insertAllocatedBatch(time, allocatedBlocks)
          case BatchCleanupEvent(batchTimes) =>
            cleanupBatches(batchTimes)
        }
      }
    }
  }

  /** 
   *  Write an update to the tracker to the write ahead log 
   *  向写入前日志的跟踪程序写一个更新
   *  */
  private def writeToLog(record: ReceivedBlockTrackerLogEvent) {
    if (isWriteAheadLogEnabled) {
      logDebug(s"Writing to log $record")
      writeAheadLogOption.foreach { logManager =>
        logManager.write(ByteBuffer.wrap(Utils.serialize(record)), clock.getTimeMillis())
      }
    }
  }

  /** 
   *  Get the queue of received blocks belonging to a particular stream 
   *  获取属于特定流的接收块的队列
   *  */
  private def getReceivedBlockQueue(streamId: Int): ReceivedBlockQueue = {
    streamIdToUnallocatedBlockQueues.getOrElseUpdate(streamId, new ReceivedBlockQueue)
  }

  /** 
   *  Optionally create the write ahead log manager only if the feature is enabled 
   *  仅当启用了该功能时,可以选择创建前写日志管理器
   *  */
  private def createWriteAheadLog(): Option[WriteAheadLog] = {
    checkpointDirOption.map { checkpointDir =>
      val logDir = ReceivedBlockTracker.checkpointDirToLogDir(checkpointDirOption.get)
      WriteAheadLogUtils.createLogForDriver(conf, logDir, hadoopConf)
    }
  }

  /** 
   *  Check if the write ahead log is enabled. This is only used for testing purposes.
   *  检查是否启用了写入日志记录,这只是用于测试的目的
   *   */
  private[streaming] def isWriteAheadLogEnabled: Boolean = writeAheadLogOption.nonEmpty
}

private[streaming] object ReceivedBlockTracker {
  def checkpointDirToLogDir(checkpointDir: String): String = {
    new Path(checkpointDir, "receivedBlockMetadata").toString
  }
}
