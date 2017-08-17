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

package org.apache.spark.shuffle.hash

import java.io.{ByteArrayOutputStream, InputStream}
import java.nio.ByteBuffer

import org.mockito.Matchers.{eq => meq, _}
import org.mockito.Mockito.{mock, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import org.apache.spark._
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.shuffle.BaseShuffleHandle
import org.apache.spark.storage.{BlockManager, BlockManagerId, ShuffleBlockId}

/**
 * Wrapper for a managed buffer that keeps track of how many times retain and release are called.
 * 包装管理的缓冲区,保持跟踪多少次保留和释放被调用
 * We need to define this class ourselves instead of using a spy because the NioManagedBuffer class
 * is final (final classes cannot be spied on).
 */
class RecordingManagedBuffer(underlyingBuffer: NioManagedBuffer) extends ManagedBuffer {
  var callsToRetain = 0
  var callsToRelease = 0

  override def size(): Long = underlyingBuffer.size()
  override def nioByteBuffer(): ByteBuffer = underlyingBuffer.nioByteBuffer()
  override def createInputStream(): InputStream = underlyingBuffer.createInputStream()
  override def convertToNetty(): AnyRef = underlyingBuffer.convertToNetty()

  override def retain(): ManagedBuffer = {
    callsToRetain += 1
    underlyingBuffer.retain()
  }
  override def release(): ManagedBuffer = {
    callsToRelease += 1
    underlyingBuffer.release()
  }
}

class HashShuffleReaderSuite extends SparkFunSuite with LocalSparkContext {

  /**
   * This test makes sure that, when data is read from a HashShuffleReader, the underlying
   * ManagedBuffers that contain the data are eventually released.
   * 确保这个可以测试,当数据从一个HashShuffleReader,潜在的managedbuffers包含数据的最终发布
   */
  test("read() releases resources on completion") {//读取释放完成的资源
    val testConf = new SparkConf(false)
    // Create a SparkContext as a convenient way of setting SparkEnv (needed because some of the
    // shuffle code calls SparkEnv.get()).
    //创建一个sparkcontext作为一种实用设定sparkenv,因为一些洗牌代码调用sparkenv get()
    sc = new SparkContext("local", "test", testConf)

    val reduceId = 15
    val shuffleId = 22
    val numMaps = 6
    val keyValuePairsPerMap = 10
    val serializer = new JavaSerializer(testConf)

    // Make a mock BlockManager that will return RecordingManagedByteBuffers of data, so that we
    // can ensure retain() and release() are properly called.
    //模拟recordingmanagedbytebuffers blockmanager将返回数据,这样我们可以确保retain()和release()被称作。
    val blockManager = mock(classOf[BlockManager])

    // Create a return function to use for the mocked wrapForCompression method that just returns
    // the original input stream.
    //创建一个用于模拟wrapforcompression方法只是返回原来的输入流返回功能。
    val dummyCompressionFunction = new Answer[InputStream] {
      override def answer(invocation: InvocationOnMock): InputStream =
        invocation.getArguments()(1).asInstanceOf[InputStream]
    }

    // Create a buffer with some randomly generated key-value pairs to use as the shuffle data
    // from each mappers (all mappers return the same shuffle data).
    //创建一个带有随机生成的键值对用作缓冲区数据的缓冲区。从每个映射(所有映射返回相同的洗牌的数据)。
    val byteOutputStream = new ByteArrayOutputStream()
    val serializationStream = serializer.newInstance().serializeStream(byteOutputStream)
    (0 until keyValuePairsPerMap).foreach { i =>
      serializationStream.writeKey(i)
      serializationStream.writeValue(2*i)
    }

    // Setup the mocked BlockManager to return RecordingManagedBuffers.
    //设置模拟BlockManager返回RecordingManagedBuffers
    val localBlockManagerId = BlockManagerId("test-client", "test-client", 1)
    when(blockManager.blockManagerId).thenReturn(localBlockManagerId)
    //mapId对应RDD的partionsID
    val buffers = (0 until numMaps).map { mapId =>
      // Create a ManagedBuffer with the shuffle data.
      //创建一个managedbuffer的shuffle 数据
      val nioBuffer = new NioManagedBuffer(ByteBuffer.wrap(byteOutputStream.toByteArray))
      val managedBuffer = new RecordingManagedBuffer(nioBuffer)

      // Setup the blockManager mock so the buffer gets returned when the shuffle code tries to
      // fetch shuffle data.
      //设置blockManager模拟,以便shuffle代码尝试时返回缓冲区获取shuffle数据,mapId对应RDD的partionsID
      val shuffleBlockId = ShuffleBlockId(shuffleId, mapId, reduceId)
      when(blockManager.getBlockData(shuffleBlockId)).thenReturn(managedBuffer)
      when(blockManager.wrapForCompression(meq(shuffleBlockId), isA(classOf[InputStream])))
        .thenAnswer(dummyCompressionFunction)

      managedBuffer
    }

    // Make a mocked MapOutputTracker for the shuffle reader to use to determine what
    // shuffle data to read.
    // 让读者使用的洗牌要确定一个模拟 MapOutputTracker将数据混为读
    val mapOutputTracker = mock(classOf[MapOutputTracker])
    when(mapOutputTracker.getMapSizesByExecutorId(shuffleId, reduceId)).thenReturn {
      // Test a scenario where all data is local, to avoid creating a bunch of additional mocks
      // for the code to read data over the network.
      //测试的情况下,所有的数据都是局部的,以避免创建一组额外的模拟用于在网络上读取数据的代码。
      //mapId对应RDD的partionsID
      val shuffleBlockIdsAndSizes = (0 until numMaps).map { mapId =>
        //mapId对应RDD的partionsID
        val shuffleBlockId = ShuffleBlockId(shuffleId, mapId, reduceId)
        (shuffleBlockId, byteOutputStream.size().toLong)
      }
      Seq((localBlockManagerId, shuffleBlockIdsAndSizes))
    }

    // Create a mocked shuffle handle to pass into HashShuffleReader.
    //创建一个模拟洗牌处理进入HashShuffleReader。
    val shuffleHandle = {
      val dependency = mock(classOf[ShuffleDependency[Int, Int, Int]])
      when(dependency.serializer).thenReturn(Some(serializer))
      when(dependency.aggregator).thenReturn(None)
      when(dependency.keyOrdering).thenReturn(None)
      new BaseShuffleHandle(shuffleId, numMaps, dependency)
    }

    val shuffleReader = new HashShuffleReader(
      shuffleHandle,
      reduceId,
      reduceId + 1,
      TaskContext.empty(),
      blockManager,
      mapOutputTracker)

    assert(shuffleReader.read().length === keyValuePairsPerMap * numMaps)

    // Calling .length above will have exhausted the iterator; make sure that exhausting the
    // iterator caused retain and release to be called on each buffer.
    //调用.上面的长度将耗尽迭代器；确保耗尽迭代器导致在每个缓冲区上被调用的保留和释放
    buffers.foreach { buffer =>
      assert(buffer.callsToRetain === 1)
      assert(buffer.callsToRelease === 1)
    }
  }
}
