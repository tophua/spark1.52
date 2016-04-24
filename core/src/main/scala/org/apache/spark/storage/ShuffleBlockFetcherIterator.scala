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

import java.io.InputStream
import java.util.concurrent.LinkedBlockingQueue

import scala.collection.mutable.{ArrayBuffer, HashSet, Queue}
import scala.util.control.NonFatal

import org.apache.spark.{Logging, SparkException, TaskContext}
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.shuffle.{BlockFetchingListener, ShuffleClient}
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.util.Utils

/**
 * An iterator that fetches multiple blocks. For local blocks, it fetches from the local block
 * manager. For remote blocks, it fetches them using the provided BlockTransferService.
 *
 * This creates an iterator of (BlockID, InputStream) tuples so the caller can handle blocks
 * in a pipelined fashion as they are received.
 *
 * The implementation throttles the remote fetches to they don't exceed maxBytesInFlight to avoid
 * using too much memory.
 *	即从远端节点或者本地读取中间计算结果
 * @param context [[TaskContext]], used for metrics update
 * @param shuffleClient [[ShuffleClient]] for fetching remote blocks
 * @param blockManager [[BlockManager]] for reading local blocks
 * @param blocksByAddress list of blocks to fetch grouped by the [[BlockManagerId]].
 *                        For each block we also require the size (in bytes as a long field) in
 *                        order to throttle the memory usage.
 * @param maxBytesInFlight max size (in bytes) of remote blocks to fetch at any given point.
 */
private[spark]
final class ShuffleBlockFetcherIterator(
    context: TaskContext,
    shuffleClient: ShuffleClient,
    blockManager: BlockManager,
    blocksByAddress: Seq[(BlockManagerId, Seq[(BlockId, Long)])],
    maxBytesInFlight: Long)
  extends Iterator[(BlockId, InputStream)] with Logging {

  import ShuffleBlockFetcherIterator._

  /**
   * Total number of blocks to fetch. This can be smaller than the total number of blocks
   * in [[blocksByAddress]] because we filter out zero-sized blocks in [[initialize]].
   *
   * This should equal localBlocks.size + remoteBlocks.size.
   */
  private[this] var numBlocksToFetch = 0

  /**
   * The number of blocks proccessed by the caller. The iterator is exhausted when
   * [[numBlocksProcessed]] == [[numBlocksToFetch]].
   */
  private[this] var numBlocksProcessed = 0

  private[this] val startTime = System.currentTimeMillis

  /** Local blocks to fetch, excluding zero-sized blocks. */
  //存入将本地的Block列表
  private[this] val localBlocks = new ArrayBuffer[BlockId]()

  /** Remote blocks to fetch, excluding zero-sized blocks. */
  private[this] val remoteBlocks = new HashSet[BlockId]()

  /**
   * A queue to hold our results. This turns the asynchronous model provided by
   * [[org.apache.spark.network.BlockTransferService]] into a synchronous model (iterator).
   */
  private[this] val results = new LinkedBlockingQueue[FetchResult]

  /**
   * Current [[FetchResult]] being processed. We track this so we can release the current buffer
   * in case of a runtime exception when processing the current buffer.
   */
  @volatile private[this] var currentResult: FetchResult = null

  /**
   * Queue of fetch requests to issue; we'll pull requests off this gradually to make sure that
   * the number of bytes in flight is limited to maxBytesInFlight.
   */
  private[this] val fetchRequests = new Queue[FetchRequest]

  /** Current bytes in flight from our requests */
  private[this] var bytesInFlight = 0L

  private[this] val shuffleMetrics = context.taskMetrics().createShuffleReadMetricsForDependency()

  /**
   * Whether the iterator is still active. If isZombie is true, the callback interface will no
   * longer place fetched blocks into [[results]].
   */
  @volatile private[this] var isZombie = false

  initialize()

  // Decrements the buffer reference count.
  // The currentResult is set to null to prevent releasing the buffer again on cleanup()
  private[storage] def releaseCurrentResultBuffer(): Unit = {
    // Release the current buffer if necessary
    currentResult match {
      case SuccessFetchResult(_, _, _, buf) => buf.release()
      case _ =>
    }
    currentResult = null
  }

  /**
   * Mark the iterator as zombie, and release all buffers that haven't been deserialized yet.
   */
  private[this] def cleanup() {
    isZombie = true
    releaseCurrentResultBuffer()
    // Release buffers in the results queue
    val iter = results.iterator()
    while (iter.hasNext) {
      val result = iter.next()
      result match {
        case SuccessFetchResult(_, _, _, buf) => buf.release()
        case _ =>
      }
    }
  }
/**
 * 获取远程Block,用于远程请求中间结果
 */
  private[this] def sendRequest(req: FetchRequest) {
    logDebug("Sending request for %d blocks (%s) from %s".format(
      req.blocks.size, Utils.bytesToString(req.size), req.address.hostPort))
    bytesInFlight += req.size

    // so we can look up the size of each blockID
    //FetchRequest里封装的blockId,size,address等信息
    val sizeMap = req.blocks.map { case (blockId, size) => (blockId.toString, size) }.toMap
    val blockIds = req.blocks.map(_._1.toString)

    val address = req.address
    //FetchRequest里封装的blockId,size,address等信息,fetchBlocks方法获取其他节点上的中间计算结果
    shuffleClient.fetchBlocks(address.host, address.port, address.executorId, blockIds.toArray,
      new BlockFetchingListener {
        override def onBlockFetchSuccess(blockId: String, buf: ManagedBuffer): Unit = {
          // Only add the buffer to results queue if the iterator is not zombie,
          // i.e. cleanup() has not been called yet.
          if (!isZombie) {
            // Increment the ref count because we need to pass this to a different thread.
            // This needs to be released after use.
            buf.retain()
            results.put(new SuccessFetchResult(BlockId(blockId), address, sizeMap(blockId), buf))
            shuffleMetrics.incRemoteBytesRead(buf.size)
            shuffleMetrics.incRemoteBlocksFetched(1)
          }
          logTrace("Got remote block " + blockId + " after " + Utils.getUsedTimeMs(startTime))
        }

        override def onBlockFetchFailure(blockId: String, e: Throwable): Unit = {
          logError(s"Failed to get block(s) from ${req.address.host}:${req.address.port}", e)
          results.put(new FailureFetchResult(BlockId(blockId), address, e))
        }
      }
    )
  }

  private[this] def splitLocalRemoteBlocks(): ArrayBuffer[FetchRequest] = {
    // Make remote requests at most maxBytesInFlight / 5 in length; the reason to keep them
    // smaller than maxBytesInFlight is to allow multiple, parallel fetches from up to 5
    // nodes, rather than blocking on reading output from one node.
    //maxBytesInFlight:一批请求,这批请求的字节总数不能超maxBytesInFlight,而且每个请求的字节数不能超过
    //maxBytesInFlight的五分之一.可以通过参数Spark.reducer.maxBytesInFlight来控制大小.
    //为什么每个请求不能过超五分之一呢?这样做是为了提高请求的并发度,允许5个请求分别从5个节点获取数据,最大限度
    //利用各节点的资源.
    //每个远程请求的最大尺寸
    val targetRequestSize = math.max(maxBytesInFlight / 5, 1L)//每次请求的数据大小不会超过maxBytesInFlight的五分之一
        
    logDebug("maxBytesInFlight: " + maxBytesInFlight + ", targetRequestSize: " + targetRequestSize)

    // Split local and remote blocks. Remote blocks are further split into FetchRequests of size
    // at most maxBytesInFlight in order to limit the amount of data in flight.
    //缓存需要远程请求的FetchRequest对象
    val remoteRequests = new ArrayBuffer[FetchRequest]

    // Tracks total number of blocks (including zero sized blocks)
    //统计Blocks总数
    var totalBlocks = 0
    for ((address, blockInfos) <- blocksByAddress) {
      totalBlocks += blockInfos.size
      //如果BlockInfo所在的Excutor与当前Executor相同,则将它的BlockId存入localBlocks
      if (address.executorId == blockManager.blockManagerId.executorId) {
        // Filter out zero-sized blocks
        //Block在本地,需要过滤大小为0的Block
        localBlocks ++= blockInfos.filter(_._2 != 0).map(_._1)
        //一共要获取的Block数量
        numBlocksToFetch += localBlocks.size
      } else {//需要远程获取的Block
        //否则将Blockinfo的BlockId和size累加到curBlocks,将blockId存入remoteBlocks,
        val iterator = blockInfos.iterator
        //当前累加到curBlocks中的所有Block的大小之和,用于保证每个远程请求的尺寸不超过targetRequestSize
        var curRequestSize = 0L
        //远程获取的累加缓存,用于保证每个远程请求的尺寸不超过targetRequestSize
        //为什么要累加缓存?如果向一个机器节点频繁地请求字节很小的Block,那么势必造成网络拥塞并增加节点负担
        //将多个小数据量的请求合并一个大的请求避免这些问题,提高系统性能.
        var curBlocks = new ArrayBuffer[(BlockId, Long)]
        while (iterator.hasNext) {
          val (blockId, size) = iterator.next()
          // Skip empty blocks
          //跳过空的Block
          if (size > 0) {
            //将Blockinfo的BlockId和size累加到curBlocks
            curBlocks += ((blockId, size))
            //将blockId存入remoteBlocks
            remoteBlocks += blockId            
            numBlocksToFetch += 1
            curRequestSize += size
          } else if (size < 0) {
            throw new BlockException(blockId, "Negative block size " + size)
          }
          //curRequestSize >= targetRequestSize大于,则新建FetchRequest放入remoteRequests
          //并且为生成下一个FetchRequest做一些准备(curRequestSize设置为0)
          if (curRequestSize >= targetRequestSize) {
            // Add this FetchRequest
            //当前总的size已经可以批量放入一次FetchRequest中
            remoteRequests += new FetchRequest(address, curBlocks)
            curBlocks = new ArrayBuffer[(BlockId, Long)]
            logDebug(s"Creating fetch request of $curRequestSize at $address")
            curRequestSize = 0
          }
        }
        // Add in the final request
        //剩余的请求组成一次FetchRequest
        if (curBlocks.nonEmpty) {
          //如果curBlocks仍然有缓存,新建FetchRequest放入remoteRequests
          remoteRequests += new FetchRequest(address, curBlocks)
        }
      }
    }
    logInfo(s"Getting $numBlocksToFetch non-empty blocks out of $totalBlocks blocks")
    remoteRequests
  }

  /**
   * Fetch the local blocks while we are fetching remote blocks. This is ok because
   * [[ManagedBuffer]]'s memory is allocated lazily when we create the input stream, so all we
   * track in-memory are the ManagedBuffer references themselves.
   * 用于对本地中间计算结果的获取
   */
  private[this] def fetchLocalBlocks() {
    //已经将本地的Block列表存入localBlocks
    val iter = localBlocks.iterator
    while (iter.hasNext) {
      val blockId = iter.next()
      try {
        //
        val buf = blockManager.getBlockData(blockId)
        shuffleMetrics.incLocalBlocksFetched(1)
        shuffleMetrics.incLocalBytesRead(buf.size)
        buf.retain()
        results.put(new SuccessFetchResult(blockId, blockManager.blockManagerId, 0, buf))
      } catch {
        case e: Exception =>
          // If we see an exception, stop immediately.
          logError(s"Error occurred while fetching local blocks", e)
          results.put(new FailureFetchResult(blockId, blockManager.blockManagerId, e))
          return
      }
    }
  }
  /**
   * 读取中间结果初始化过程如下:
   * 1)splitLocalRemoteBlocks划分本地读取和远程读取的Block请求
   * 2)FetchRequest随机排序后存入fetchRequests中
   * 3)遍历fetchRequests中的所有FetchRequest,远程请求Block中间结果
   * 4)调用fetchLocalBlocks获取本地Block
   * 
   */

  private[this] def initialize(): Unit = {
    // Add a task completion callback (called in both success case and failure case) to cleanup.
    context.addTaskCompletionListener(_ => cleanup())

    // Split local and remote blocks.
    //splitLocalRemoteBlocks 用于划分那些Block从本地获取,哪些需要远程拉取,是获取中间计算结果的关键
    val remoteRequests = splitLocalRemoteBlocks()
    // Add the remote requests into our queue in a random order
    fetchRequests ++= Utils.randomize(remoteRequests)

    // Send out initial requests for blocks, up to our maxBytesInFlight
    while (fetchRequests.nonEmpty &&
      (bytesInFlight == 0 || bytesInFlight + fetchRequests.front.size <= maxBytesInFlight)) {
      //遍历fetchRequests中的所有FetchRequest,远程请求Block中间结果
      sendRequest(fetchRequests.dequeue())
    }

    val numFetches = remoteRequests.size - fetchRequests.size
    logInfo("Started " + numFetches + " remote fetches in" + Utils.getUsedTimeMs(startTime))

    // Get Local Blocks
    //调用fetchLocalBlocks获取本地Block
    fetchLocalBlocks()
    logDebug("Got local blocks in " + Utils.getUsedTimeMs(startTime))
  }

  override def hasNext: Boolean = numBlocksProcessed < numBlocksToFetch

  /**
   * Fetches the next (BlockId, InputStream). If a task fails, the ManagedBuffers
   * underlying each InputStream will be freed by the cleanup() method registered with the
   * TaskCompletionListener. However, callers should close() these InputStreams
   * as soon as they are no longer needed, in order to release memory as early as possible.
   *
   * Throws a FetchFailedException if the next block could not be fetched.
   */
  override def next(): (BlockId, InputStream) = {
    numBlocksProcessed += 1
    val startFetchWait = System.currentTimeMillis()
    //中间结果的Block及数据缓存在ShuffleBlockFecherIterator的results中
    currentResult = results.take()
    val result = currentResult
    val stopFetchWait = System.currentTimeMillis()
    shuffleMetrics.incFetchWaitTime(stopFetchWait - startFetchWait)

    result match {
      case SuccessFetchResult(_, _, size, _) => bytesInFlight -= size
      case _ =>
    }
    // Send fetch requests up to maxBytesInFlight
    while (fetchRequests.nonEmpty &&
      (bytesInFlight == 0 || bytesInFlight + fetchRequests.front.size <= maxBytesInFlight)) {
      sendRequest(fetchRequests.dequeue())
    }

    result match {
      case FailureFetchResult(blockId, address, e) =>
        throwFetchFailedException(blockId, address, e)

      case SuccessFetchResult(blockId, address, _, buf) =>
        try {
          (result.blockId, new BufferReleasingInputStream(buf.createInputStream(), this))
        } catch {
          case NonFatal(t) =>
            throwFetchFailedException(blockId, address, t)
        }
    }
  }

  private def throwFetchFailedException(blockId: BlockId, address: BlockManagerId, e: Throwable) = {
    blockId match {
      case ShuffleBlockId(shufId, mapId, reduceId) =>
        throw new FetchFailedException(address, shufId.toInt, mapId.toInt, reduceId, e)
      case _ =>
        throw new SparkException(
          "Failed to get block " + blockId + ", which is not a shuffle block", e)
    }
  }
}

/**
 * Helper class that ensures a ManagedBuffer is release upon InputStream.close()
 */
private class BufferReleasingInputStream(
    private val delegate: InputStream,
    private val iterator: ShuffleBlockFetcherIterator)
  extends InputStream {
  private[this] var closed = false

  override def read(): Int = delegate.read()

  override def close(): Unit = {
    if (!closed) {
      delegate.close()
      iterator.releaseCurrentResultBuffer()
      closed = true
    }
  }

  override def available(): Int = delegate.available()

  override def mark(readlimit: Int): Unit = delegate.mark(readlimit)

  override def skip(n: Long): Long = delegate.skip(n)

  override def markSupported(): Boolean = delegate.markSupported()

  override def read(b: Array[Byte]): Int = delegate.read(b)

  override def read(b: Array[Byte], off: Int, len: Int): Int = delegate.read(b, off, len)

  override def reset(): Unit = delegate.reset()
}

private[storage]
object ShuffleBlockFetcherIterator {

  /**
   * A request to fetch blocks from a remote BlockManager.
   * @param address remote BlockManager to fetch from.
   * @param blocks Sequence of tuple, where the first element is the block id,
   *               and the second element is the estimated size, used to calculate bytesInFlight.
   */
  case class FetchRequest(address: BlockManagerId, blocks: Seq[(BlockId, Long)]) {
    val size = blocks.map(_._2).sum
  }

  /**
   * Result of a fetch from a remote block.
   */
  private[storage] sealed trait FetchResult {
    val blockId: BlockId
    val address: BlockManagerId
  }

  /**
   * Result of a fetch from a remote block successfully.
   * @param blockId block id
   * @param address BlockManager that the block was fetched from.
   * @param size estimated size of the block, used to calculate bytesInFlight.
   *             Note that this is NOT the exact bytes.
   * @param buf [[ManagedBuffer]] for the content.
   */
  private[storage] case class SuccessFetchResult(
      blockId: BlockId,
      address: BlockManagerId,
      size: Long,
      buf: ManagedBuffer)
    extends FetchResult {
    require(buf != null)
    require(size >= 0)
  }

  /**
   * Result of a fetch from a remote block unsuccessfully.
   * @param blockId block id
   * @param address BlockManager that the block was attempted to be fetched from
   * @param e the failure exception
   */
  private[storage] case class FailureFetchResult(
      blockId: BlockId,
      address: BlockManagerId,
      e: Throwable)
    extends FetchResult
}
