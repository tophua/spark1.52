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
import java.util.concurrent.Semaphore

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.future

import org.mockito.Matchers.{any, eq => meq}
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.PrivateMethodTester

import org.apache.spark.{SparkFunSuite, TaskContext}
import org.apache.spark.network._
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.shuffle.BlockFetchingListener
import org.apache.spark.shuffle.FetchFailedException


class ShuffleBlockFetcherIteratorSuite extends SparkFunSuite with PrivateMethodTester {
  // Some of the tests are quite tricky because we are testing the cleanup behavior
  // in the presence of faults.
  //一些测试非常棘手，因为我们正在测试存在故障的清理行为
  /** Creates a mock [[BlockTransferService]] that returns data from the given map. */
  private def createMockTransfer(data: Map[BlockId, ManagedBuffer]): BlockTransferService = {
    val transfer = mock(classOf[BlockTransferService])
    when(transfer.fetchBlocks(any(), any(), any(), any(), any())).thenAnswer(new Answer[Unit] {
      override def answer(invocation: InvocationOnMock): Unit = {
        val blocks = invocation.getArguments()(3).asInstanceOf[Array[String]]
        val listener = invocation.getArguments()(4).asInstanceOf[BlockFetchingListener]

        for (blockId <- blocks) {
          if (data.contains(BlockId(blockId))) {
            listener.onBlockFetchSuccess(blockId, data(BlockId(blockId)))
          } else {
            listener.onBlockFetchFailure(blockId, new BlockNotFoundException(blockId))
          }
        }
      }
    })
    transfer
  }

  // Create a mock managed buffer for testing
  //创建一个用于测试的模拟管理缓冲区
  def createMockManagedBuffer(): ManagedBuffer = {
    val mockManagedBuffer = mock(classOf[ManagedBuffer])
    when(mockManagedBuffer.createInputStream()).thenReturn(mock(classOf[InputStream]))
    mockManagedBuffer
  }

  test("successful 3 local reads + 2 remote reads") {//成功的3本地读取+ 2远程读取
    val blockManager = mock(classOf[BlockManager])
    val localBmId = BlockManagerId("test-client", "test-client", 1)
    doReturn(localBmId).when(blockManager).blockManagerId

    // Make sure blockManager.getBlockData would return the blocks
    //确保blockmanager.getblockdata恢复块
    val localBlocks = Map[BlockId, ManagedBuffer](
      ShuffleBlockId(0, 0, 0) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 1, 0) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 2, 0) -> createMockManagedBuffer())
    localBlocks.foreach { case (blockId, buf) =>
      doReturn(buf).when(blockManager).getBlockData(meq(blockId))
    }

    // Make sure remote blocks would return
    //确保远程块将返回
    val remoteBmId = BlockManagerId("test-client-1", "test-client-1", 2)
    val remoteBlocks = Map[BlockId, ManagedBuffer](
      ShuffleBlockId(0, 3, 0) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 4, 0) -> createMockManagedBuffer())

    val transfer = createMockTransfer(remoteBlocks)

    val blocksByAddress = Seq[(BlockManagerId, Seq[(BlockId, Long)])](
      (localBmId, localBlocks.keys.map(blockId => (blockId, 1.asInstanceOf[Long])).toSeq),
      (remoteBmId, remoteBlocks.keys.map(blockId => (blockId, 1.asInstanceOf[Long])).toSeq)
    )

    val iterator = new ShuffleBlockFetcherIterator(
      TaskContext.empty(),
      transfer,
      blockManager,
      blocksByAddress,
      48 * 1024 * 1024)

    // 3 local blocks fetched in initialization
    //在初始化中获取的3个本地块
    verify(blockManager, times(3)).getBlockData(any())

    for (i <- 0 until 5) {
      assert(iterator.hasNext, s"iterator should have 5 elements but actually has $i elements")
      val (blockId, inputStream) = iterator.next()

      // Make sure we release buffers when a wrapped input stream is closed.
      //确保当一个包装的输入流被关闭时,我们释放缓冲区。
      val mockBuf = localBlocks.getOrElse(blockId, remoteBlocks(blockId))
      // Note: ShuffleBlockFetcherIterator wraps input streams in a BufferReleasingInputStream
      val wrappedInputStream = inputStream.asInstanceOf[BufferReleasingInputStream]
      verify(mockBuf, times(0)).release()
      val delegateAccess = PrivateMethod[InputStream]('delegate)

      verify(wrappedInputStream.invokePrivate(delegateAccess()), times(0)).close()
      wrappedInputStream.close()
      verify(mockBuf, times(1)).release()
      verify(wrappedInputStream.invokePrivate(delegateAccess()), times(1)).close()
      wrappedInputStream.close() // close should be idempotent
      verify(mockBuf, times(1)).release()
      verify(wrappedInputStream.invokePrivate(delegateAccess()), times(1)).close()
    }

    // 3 local blocks, and 2 remote blocks 3个本地块和2个远程块
    // (but from the same block manager so one call to fetchBlocks)
    verify(blockManager, times(3)).getBlockData(any())
    verify(transfer, times(1)).fetchBlocks(any(), any(), any(), any(), any())
  }
  //任务完成释放未用的缓冲区
  test("release current unexhausted buffer in case the task completes early") {
    val blockManager = mock(classOf[BlockManager])
    val localBmId = BlockManagerId("test-client", "test-client", 1)
    doReturn(localBmId).when(blockManager).blockManagerId

    // Make sure remote blocks would return
    val remoteBmId = BlockManagerId("test-client-1", "test-client-1", 2)
    val blocks = Map[BlockId, ManagedBuffer](
      ShuffleBlockId(0, 0, 0) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 1, 0) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 2, 0) -> createMockManagedBuffer())

    // Semaphore to coordinate event sequence in two different threads.
      //在两个不同的线程协调事件序列
    val sem = new Semaphore(0)

    val transfer = mock(classOf[BlockTransferService])
    when(transfer.fetchBlocks(any(), any(), any(), any(), any())).thenAnswer(new Answer[Unit] {
      override def answer(invocation: InvocationOnMock): Unit = {
        val listener = invocation.getArguments()(4).asInstanceOf[BlockFetchingListener]
        future {
          // Return the first two blocks, and wait till task completion before returning the 3rd one
          //返回前两个块,并等到任务完成后,返回第三个
          listener.onBlockFetchSuccess(
            ShuffleBlockId(0, 0, 0).toString, blocks(ShuffleBlockId(0, 0, 0)))
          listener.onBlockFetchSuccess(
            ShuffleBlockId(0, 1, 0).toString, blocks(ShuffleBlockId(0, 1, 0)))
          sem.acquire()
          listener.onBlockFetchSuccess(
            ShuffleBlockId(0, 2, 0).toString, blocks(ShuffleBlockId(0, 2, 0)))
        }
      }
    })

    val blocksByAddress = Seq[(BlockManagerId, Seq[(BlockId, Long)])](
      (remoteBmId, blocks.keys.map(blockId => (blockId, 1.asInstanceOf[Long])).toSeq))

    val taskContext = TaskContext.empty()
    val iterator = new ShuffleBlockFetcherIterator(
      taskContext,
      transfer,
      blockManager,
      blocksByAddress,
      48 * 1024 * 1024)

    verify(blocks(ShuffleBlockId(0, 0, 0)), times(0)).release()
    iterator.next()._2.close() // close() first block's input stream 第一块输入流
    verify(blocks(ShuffleBlockId(0, 0, 0)), times(1)).release()

    // Get the 2nd block but do not exhaust the iterator
    //获取第二个块,但不要耗尽迭代器
    val subIter = iterator.next()._2

    // Complete the task; then the 2nd block buffer should be exhausted
    //完成任务,然后将第二块缓冲区耗尽
    verify(blocks(ShuffleBlockId(0, 1, 0)), times(0)).release()
    taskContext.markTaskCompleted()
    verify(blocks(ShuffleBlockId(0, 1, 0)), times(1)).release()

    // The 3rd block should not be retained because the iterator is already in zombie state
    //第三块不应该被保留,因为迭代器已经处于僵尸状态
    sem.release()
    verify(blocks(ShuffleBlockId(0, 2, 0)), times(0)).retain()
    verify(blocks(ShuffleBlockId(0, 2, 0)), times(0)).release()
  }
  //如果任何远程请求失败,所有的块都失败了
  test("fail all blocks if any of the remote request fails") {
    val blockManager = mock(classOf[BlockManager])
    val localBmId = BlockManagerId("test-client", "test-client", 1)
    doReturn(localBmId).when(blockManager).blockManagerId

    // Make sure remote blocks would return 确保远程块将返回
    val remoteBmId = BlockManagerId("test-client-1", "test-client-1", 2)
    val blocks = Map[BlockId, ManagedBuffer](
      ShuffleBlockId(0, 0, 0) -> mock(classOf[ManagedBuffer]),
      ShuffleBlockId(0, 1, 0) -> mock(classOf[ManagedBuffer]),
      ShuffleBlockId(0, 2, 0) -> mock(classOf[ManagedBuffer])
    )

    // Semaphore to coordinate event sequence in two different threads.
    //信号量在两个不同的线程中协调事件序列
    val sem = new Semaphore(0)

    val transfer = mock(classOf[BlockTransferService])
    when(transfer.fetchBlocks(any(), any(), any(), any(), any())).thenAnswer(new Answer[Unit] {
      override def answer(invocation: InvocationOnMock): Unit = {
        val listener = invocation.getArguments()(4).asInstanceOf[BlockFetchingListener]
        future {
          // Return the first block, and then fail.
          //返回第一个块,然后失败
          listener.onBlockFetchSuccess(
            ShuffleBlockId(0, 0, 0).toString, blocks(ShuffleBlockId(0, 0, 0)))
          listener.onBlockFetchFailure(
            ShuffleBlockId(0, 1, 0).toString, new BlockNotFoundException("blah"))
          listener.onBlockFetchFailure(
            ShuffleBlockId(0, 2, 0).toString, new BlockNotFoundException("blah"))
          sem.release()
        }
      }
    })

    val blocksByAddress = Seq[(BlockManagerId, Seq[(BlockId, Long)])](
      (remoteBmId, blocks.keys.map(blockId => (blockId, 1.asInstanceOf[Long])).toSeq))

    val taskContext = TaskContext.empty()
    val iterator = new ShuffleBlockFetcherIterator(
      taskContext,
      transfer,
      blockManager,
      blocksByAddress,
      48 * 1024 * 1024)

    // Continue only after the mock calls onBlockFetchFailure
    //只有在mock调用onBlockFetchFailure之后才能继续
    sem.acquire()

    // The first block should be returned without an exception, and the last two should throw
    // FetchFailedExceptions (due to failure) 由于故障
    //应该返回第一个块而不会有异常，最后一个块应抛出FetchFailedExceptions（由于失败）
    iterator.next()
    intercept[FetchFailedException] { iterator.next() }
    intercept[FetchFailedException] { iterator.next() }
  }
}
