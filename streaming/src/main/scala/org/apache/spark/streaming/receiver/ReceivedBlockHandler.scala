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

package org.apache.spark.streaming.receiver

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.{existentials, postfixOps}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.storage._
import org.apache.spark.streaming.receiver.WriteAheadLogBasedBlockHandler._
import org.apache.spark.streaming.util.{WriteAheadLogRecordHandle, WriteAheadLogUtils}
import org.apache.spark.util.{Clock, SystemClock, ThreadUtils}
import org.apache.spark.{Logging, SparkConf, SparkException}

/** 
 *  Trait that represents the metadata related to storage of blocks
 *  表示与存储块相关的元数据
 *   */
private[streaming] trait ReceivedBlockStoreResult {
  // Any implementation of this trait will store a block id
  //任何实现将存储一个块标识
  def blockId: StreamBlockId
  // Any implementation of this trait will have to return the number of records
  //此特性的任何实现将必须返回记录的数量
  def numRecords: Option[Long]
}

/** 
 *  Trait that represents a class that handles the storage of blocks received by receiver
 *  表示处理接收到的块存储的类
 *   */
private[streaming] trait ReceivedBlockHandler {

  /** 
   *  Store a received block with the given block id and return related metadata 
   *  将接收的块存储在给定的块标识和返回相关的元数据中
   *  */
  def storeBlock(blockId: StreamBlockId, receivedBlock: ReceivedBlock): ReceivedBlockStoreResult

  /** 
   *  Cleanup old blocks older than the given threshold time
   *  清理旧块比给定的阈值时间 
   *  */
  def cleanupOldBlocks(threshTime: Long)
}


/**
 * Implementation of [[org.apache.spark.streaming.receiver.ReceivedBlockStoreResult]]
 * that stores the metadata related to storage of blocks using
 * 存储与存储块相关的元数据.
 * [[org.apache.spark.streaming.receiver.BlockManagerBasedBlockHandler]]
 */
private[streaming] case class BlockManagerBasedStoreResult(
      blockId: StreamBlockId, numRecords: Option[Long])
  extends ReceivedBlockStoreResult


/**
 * Implementation of a [[org.apache.spark.streaming.receiver.ReceivedBlockHandler]] which
 * stores the received blocks into a block manager with the specified storage level.
 * 将接收到的块存储到块管理器中指定存储级别(内存或硬盘)
 */
private[streaming] class BlockManagerBasedBlockHandler(
    blockManager: BlockManager, storageLevel: StorageLevel)
  extends ReceivedBlockHandler with Logging {

  def storeBlock(blockId: StreamBlockId, block: ReceivedBlock): ReceivedBlockStoreResult = {

    var numRecords = None: Option[Long]

    val putResult: Seq[(BlockId, BlockStatus)] = block match {
      case ArrayBufferBlock(arrayBuffer) =>
        numRecords = Some(arrayBuffer.size.toLong)
        blockManager.putIterator(blockId, arrayBuffer.iterator, storageLevel,
          tellMaster = true)
      case IteratorBlock(iterator) =>
        val countIterator = new CountingIterator(iterator)
        val putResult = blockManager.putIterator(blockId, countIterator, storageLevel,
          tellMaster = true)
        numRecords = countIterator.count
        putResult
      case ByteBufferBlock(byteBuffer) =>
        blockManager.putBytes(blockId, byteBuffer, storageLevel, tellMaster = true)
      case o =>
        throw new SparkException(
          s"Could not store $blockId to block manager, unexpected block type ${o.getClass.getName}")
    }
    if (!putResult.map { _._1 }.contains(blockId)) {
      throw new SparkException(
        s"Could not store $blockId to block manager with storage level $storageLevel")
    }
    BlockManagerBasedStoreResult(blockId, numRecords)
  }

  def cleanupOldBlocks(threshTime: Long) {
    // this is not used as blocks inserted into the BlockManager are cleared by DStream's clearing
    // of BlockRDDs.
  }
}


/**
 * Implementation of [[org.apache.spark.streaming.receiver.ReceivedBlockStoreResult]]
 * that stores the metadata related to storage of blocks using
 * 使用块存储的元数据
 * [[org.apache.spark.streaming.receiver.WriteAheadLogBasedBlockHandler]]
 */
private[streaming] case class WriteAheadLogBasedStoreResult(
    blockId: StreamBlockId,
    numRecords: Option[Long],
    walRecordHandle: WriteAheadLogRecordHandle
  ) extends ReceivedBlockStoreResult


/**
 * Implementation of a [[org.apache.spark.streaming.receiver.ReceivedBlockHandler]] which
 * stores the received blocks in both, a write ahead log and a block manager.
 * 将接收的块存储在一个写WriteAheadLogBasedBlockHandler和 executor的内存或硬盘
 */
private[streaming] class WriteAheadLogBasedBlockHandler(
    blockManager: BlockManager,
    streamId: Int,
    storageLevel: StorageLevel,
    conf: SparkConf,
    hadoopConf: Configuration,
    checkpointDir: String,
    clock: Clock = new SystemClock
  ) extends ReceivedBlockHandler with Logging {

  private val blockStoreTimeout = conf.getInt(
    "spark.streaming.receiver.blockStoreTimeout", 30).seconds

  private val effectiveStorageLevel = {
    if (storageLevel.deserialized) {
      logWarning(s"Storage level serialization ${storageLevel.deserialized} is not supported when" +
        s" write ahead log is enabled, change to serialization false")
    }
    if (storageLevel.replication > 1) {
      logWarning(s"Storage level replication ${storageLevel.replication} is unnecessary when " +
        s"write ahead log is enabled, change to replication 1")
    }

    StorageLevel(storageLevel.useDisk, storageLevel.useMemory, storageLevel.useOffHeap, false, 1)
  }

  if (storageLevel != effectiveStorageLevel) {
    logWarning(s"User defined storage level $storageLevel is changed to effective storage level " +
      s"$effectiveStorageLevel when write ahead log is enabled")
  }

  // Write ahead log manages
  //提前写日志管理
  private val writeAheadLog = WriteAheadLogUtils.createLogForReceiver(
    conf, checkpointDirToLogDir(checkpointDir, streamId), hadoopConf)

  // For processing futures used in parallel block storing into block manager and write ahead log
  // # threads = 2, so that both writing to BM and WAL can proceed in parallel
  //用于并行块存储到块管理器中,用于处理future和提前写入日志
  implicit private val executionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonFixedThreadPool(2, this.getClass.getSimpleName))

  /**
   * This implementation stores the block into the block manager as well as a write ahead log.
   * It does this in parallel, using Scala Futures, and returns only after the block has
   * been stored in both places.
   * 此实现将块存储到块管理器,以及提前写日志,它这样做是并行,
   */
  def storeBlock(blockId: StreamBlockId, block: ReceivedBlock): ReceivedBlockStoreResult = {

    var numRecords = None: Option[Long]
    // Serialize the block so that it can be inserted into both
    //序列块以便它可以插入到这两个
    val serializedBlock = block match {
      case ArrayBufferBlock(arrayBuffer) =>
        numRecords = Some(arrayBuffer.size.toLong)
        blockManager.dataSerialize(blockId, arrayBuffer.iterator)
      case IteratorBlock(iterator) =>
        val countIterator = new CountingIterator(iterator)
        val serializedBlock = blockManager.dataSerialize(blockId, countIterator)
        numRecords = countIterator.count
        serializedBlock
      case ByteBufferBlock(byteBuffer) =>
        byteBuffer
      case _ =>
        throw new Exception(s"Could not push $blockId to block manager, unexpected block type")
    }

    // Store the block in block manager
    // 存储块在块管理器中
    val storeInBlockManagerFuture = Future {
      val putResult =
        blockManager.putBytes(blockId, serializedBlock, effectiveStorageLevel, tellMaster = true)
      if (!putResult.map { _._1 }.contains(blockId)) {
        throw new SparkException(
          s"Could not store $blockId to block manager with storage level $storageLevel")
      }
    }

    // Store the block in write ahead log
    //存储块提前写日志
    val storeInWriteAheadLogFuture = Future {
      writeAheadLog.write(serializedBlock, clock.getTimeMillis())
    }

    // Combine the futures, wait for both to complete, and return the write ahead log record handle
    //合并futures,等待两个完成,返回写入前的日志记录句柄
    val combinedFuture = storeInBlockManagerFuture.zip(storeInWriteAheadLogFuture).map(_._2)
    val walRecordHandle = Await.result(combinedFuture, blockStoreTimeout)
    WriteAheadLogBasedStoreResult(blockId, numRecords, walRecordHandle)
  }

  def cleanupOldBlocks(threshTime: Long) {
    writeAheadLog.clean(threshTime, false)
  }

  def stop() {
    writeAheadLog.close()
    executionContext.shutdown()
  }
}

private[streaming] object WriteAheadLogBasedBlockHandler {
  def checkpointDirToLogDir(checkpointDir: String, streamId: Int): String = {
    new Path(checkpointDir, new Path("receivedData", streamId.toString)).toString
  }
}

/**
 * A utility that will wrap the Iterator to get the count
 * 一个实用工具,将封装迭代器得到计数
 */
private[streaming] class CountingIterator[T](iterator: Iterator[T]) extends Iterator[T] {
   private var _count = 0

   private def isFullyConsumed: Boolean = !iterator.hasNext

   def hasNext(): Boolean = iterator.hasNext

   def count(): Option[Long] = {
     if (isFullyConsumed) Some(_count) else None
   }

   def next(): T = {
    _count += 1
    iterator.next()
   }
}
