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

package org.apache.spark

import scala.collection.mutable.ArrayBuffer

import org.mockito.Mockito._
import org.mockito.Matchers.{any, isA}

import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef, RpcCallContext, RpcEnv}
import org.apache.spark.scheduler.{CompressedMapStatus, MapStatus}
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.storage.{BlockManagerId, ShuffleBlockId}

/**
  * MapOutputTracker 保存Shuffle Map Task 输出的位置信息
  * 其中在Drive上的MapOutputTrackerMaster
  * 其中在Executor上的Tracker是MapOutputTrackerWorker
  */
class MapOutputTrackerSuite extends SparkFunSuite {
  private val conf = new SparkConf

  def createRpcEnv(name: String, host: String = "localhost", port: Int = 0,
      securityManager: SecurityManager = new SecurityManager(conf)): RpcEnv = {
    RpcEnv.create(name, host, port, conf, securityManager)
  }

  test("master start and stop") {//主节点开始和停止
    val rpcEnv = createRpcEnv("test")
    //保存Shuffle Map Task 输出的位置信息
    val tracker = new MapOutputTrackerMaster(conf)
    //设置输出的位置信息
    //AkkaRpcEndpointRef(Actor[akka://test/user/MapOutputTracker#1105853817])
    tracker.trackerEndpoint = rpcEnv.setupEndpoint(MapOutputTracker.ENDPOINT_NAME,
      new MapOutputTrackerMasterEndpoint(rpcEnv, tracker, conf))
   // println("Epoch:"+tracker.getEpoch)
    //trackerEndpoint:AkkaRpcEndpointRef(Actor[akka://test/user/MapOutputTracker#508791595])===localhost:44953
   // println("trackerEndpoint:"+tracker.trackerEndpoint+"==="+tracker.trackerEndpoint.address)
    tracker.stop()
    rpcEnv.shutdown()
  }

  test("master register shuffle and fetch") {//主节点注册shuffle和获取
    val rpcEnv = createRpcEnv("test")
    val tracker = new MapOutputTrackerMaster(conf)
    println("tracker:"+tracker.trackerEndpoint)
    tracker.trackerEndpoint = rpcEnv.setupEndpoint(MapOutputTracker.ENDPOINT_NAME,
      new MapOutputTrackerMasterEndpoint(rpcEnv, tracker, conf))
    println("tracker:"+tracker.trackerEndpoint+"===="+tracker.trackerEndpoint.address)
      //shuffleId:10,2数组存储[MapStatus]
    tracker.registerShuffle(10, 2)
    assert(tracker.containsShuffle(10))
    //
    val size1000 = MapStatus.decompressSize(MapStatus.compressSize(1000L))
    val size10000 = MapStatus.decompressSize(MapStatus.compressSize(10000L))
    tracker.registerMapOutput(10, 0, MapStatus(BlockManagerId("a", "hostA", 1000),
        Array(1000L, 10000L)))
    tracker.registerMapOutput(10, 1, MapStatus(BlockManagerId("b", "hostB", 1000),
        Array(10000L, 1000L)))
    val statuses = tracker.getMapSizesByExecutorId(10, 0)
    assert(statuses.toSet ===
      //ShuffleBlockId(shuffleId: Int, mapId: Int, reduceId: Int)
      Seq((BlockManagerId("a", "hostA", 1000), ArrayBuffer((ShuffleBlockId(10, 0, 0), size1000))),
          (BlockManagerId("b", "hostB", 1000), ArrayBuffer((ShuffleBlockId(10, 1, 0), size10000))))
        .toSet)
    tracker.stop()
    rpcEnv.shutdown()
  }

  test("master register and unregister shuffle") {//主节点注册shuffle和注销shuffle
    val rpcEnv = createRpcEnv("test")
    val tracker = new MapOutputTrackerMaster(conf)
    tracker.trackerEndpoint = rpcEnv.setupEndpoint(MapOutputTracker.ENDPOINT_NAME,
      new MapOutputTrackerMasterEndpoint(rpcEnv, tracker, conf))
    tracker.registerShuffle(10, 2)
    val compressedSize1000 = MapStatus.compressSize(1000L)
    val compressedSize10000 = MapStatus.compressSize(10000L)
    tracker.registerMapOutput(10, 0, MapStatus(BlockManagerId("a", "hostA", 1000),
      Array(compressedSize1000, compressedSize10000)))
    tracker.registerMapOutput(10, 1, MapStatus(BlockManagerId("b", "hostB", 1000),
      Array(compressedSize10000, compressedSize1000)))
    assert(tracker.containsShuffle(10))
    assert(tracker.getMapSizesByExecutorId(10, 0).nonEmpty)
    tracker.unregisterShuffle(10)
    assert(!tracker.containsShuffle(10))
    assert(tracker.getMapSizesByExecutorId(10, 0).isEmpty)
    tracker.stop()
    rpcEnv.shutdown()
  }
  //注册和注销Shuffle Map输出和获取
  test("master register shuffle and unregister map output and fetch") {
    val rpcEnv = createRpcEnv("test")
    val tracker = new MapOutputTrackerMaster(conf)
    tracker.trackerEndpoint = rpcEnv.setupEndpoint(MapOutputTracker.ENDPOINT_NAME,
      new MapOutputTrackerMasterEndpoint(rpcEnv, tracker, conf))
    tracker.registerShuffle(10, 2)
    val compressedSize1000 = MapStatus.compressSize(1000L)
    val compressedSize10000 = MapStatus.compressSize(10000L)
    tracker.registerMapOutput(10, 0, MapStatus(BlockManagerId("a", "hostA", 1000),
        Array(compressedSize1000, compressedSize1000, compressedSize1000)))
    tracker.registerMapOutput(10, 1, MapStatus(BlockManagerId("b", "hostB", 1000),
        Array(compressedSize10000, compressedSize1000, compressedSize1000)))

    // As if we had two simultaneous fetch failures
    //我们有两个同时获取失败
    tracker.unregisterMapOutput(10, 0, BlockManagerId("a", "hostA", 1000))
    tracker.unregisterMapOutput(10, 0, BlockManagerId("a", "hostA", 1000))

    // The remaining reduce task might try to grab the output despite the shuffle failure;    
    // this should cause it to fail, and the scheduler will ignore the failure due to the
    // stage already being aborted.
    //剩下的减少任务可能尝试抓住输出,尽管洗牌失败; 这应该导致它失败,并且由于舞台已经被中止,调度程序将忽略该故障。
    intercept[FetchFailedException] { tracker.getMapSizesByExecutorId(10, 1) }

    tracker.stop()
    rpcEnv.shutdown()
  }

  test("remote fetch") {//远程读取
    val hostname = "localhost"
    val rpcEnv = createRpcEnv("spark", hostname, 0, new SecurityManager(conf))
    //rpcEnv:localhost:45916===AkkaRpcEnv(akka://spark)
    println("rpcEnv:"+rpcEnv.address+"==="+rpcEnv)
    val masterTracker = new MapOutputTrackerMaster(conf)
    masterTracker.trackerEndpoint = rpcEnv.setupEndpoint(MapOutputTracker.ENDPOINT_NAME,
      new MapOutputTrackerMasterEndpoint(rpcEnv, masterTracker, conf))
    //spark:AkkaRpcEndpointRef(Actor[akka://spark/user/MapOutputTracker#-1702102346])==localhost:45916
    println("spark:"+masterTracker.trackerEndpoint+"=="+masterTracker.trackerEndpoint.address )
    val slaveRpcEnv = createRpcEnv("spark-slave", hostname, 0, new SecurityManager(conf))
    val slaveTracker = new MapOutputTrackerWorker(conf)

    slaveTracker.trackerEndpoint =
      //根据systemName、address、endpointName获取RpcEndpointRef
      slaveRpcEnv.setupEndpointRef("spark", rpcEnv.address, MapOutputTracker.ENDPOINT_NAME)
    //spark-slave:localhost:39527==AkkaRpcEnv(akka://spark-slave)===AkkaRpcEndpointRef(Actor[akka.tcp://spark@localhost:45916/user/MapOutputTracker#-1702102346])===localhost:45916

    println("spark-slave:"+slaveRpcEnv.address+"=="+slaveRpcEnv+"==="+slaveTracker.trackerEndpoint+"==="+slaveTracker.trackerEndpoint.address)
    masterTracker.registerShuffle(10, 1)
    println("masterTracker.getEpoch-0:"+masterTracker.getEpoch)
    masterTracker.incrementEpoch()
    println("masterTracker.getEpoch-1:"+masterTracker.getEpoch)
    slaveTracker.updateEpoch(masterTracker.getEpoch)
    println("masterTracker.getEpoch-2:"+masterTracker.getEpoch+"==slaveTracker=="+slaveTracker.getEpoch)
    intercept[FetchFailedException] { slaveTracker.getMapSizesByExecutorId(10, 0) }

    val size1000 = MapStatus.decompressSize(MapStatus.compressSize(1000L))
    masterTracker.registerMapOutput(10, 0, MapStatus(
      BlockManagerId("a", "hostA", 1000), Array(1000L)))
    masterTracker.incrementEpoch()
    slaveTracker.updateEpoch(masterTracker.getEpoch)
    println("masterTracker.getEpoch-3:"+masterTracker.getEpoch+"==slaveTracker=="+slaveTracker.getEpoch)
    assert(slaveTracker.getMapSizesByExecutorId(10, 0) ===
      //BlockManagerId:ID of the executor,块管理器的主机名,块管理器的端口
      Seq((BlockManagerId("a", "hostA", 1000), ArrayBuffer((ShuffleBlockId(10, 0, 0), size1000)))))

    masterTracker.unregisterMapOutput(10, 0, BlockManagerId("a", "hostA", 1000))
    masterTracker.incrementEpoch()
    slaveTracker.updateEpoch(masterTracker.getEpoch)
    intercept[FetchFailedException] { slaveTracker.getMapSizesByExecutorId(10, 0) }

    // failure should be cached
    //失败的缓存
    intercept[FetchFailedException] { slaveTracker.getMapSizesByExecutorId(10, 0) }

    masterTracker.stop()
    slaveTracker.stop()
    rpcEnv.shutdown()
    slaveRpcEnv.shutdown()
  }

  test("remote fetch below akka frame size") {//远程读取akka帧大小低于
    val newConf = new SparkConf
    //以MB为单位的driver和executor之间通信信息的大小,设置值越大,driver可以接受越大的计算结果
    newConf.set("spark.akka.frameSize", "1")
    newConf.set("spark.rpc.askTimeout", "1") // Fail fast

    val masterTracker = new MapOutputTrackerMaster(conf)
    val rpcEnv = createRpcEnv("spark")
    val masterEndpoint = new MapOutputTrackerMasterEndpoint(rpcEnv, masterTracker, newConf)
    rpcEnv.setupEndpoint(MapOutputTracker.ENDPOINT_NAME, masterEndpoint)

    // Frame size should be ~123B, and no exception should be thrown
    masterTracker.registerShuffle(10, 1)
    masterTracker.registerMapOutput(10, 0, MapStatus(
      BlockManagerId("88", "mph", 1000), Array.fill[Long](10)(0)))
    val sender = mock(classOf[RpcEndpointRef])
    when(sender.address).thenReturn(RpcAddress("localhost", 12345))
    println("sender:"+masterTracker.getEpoch+"==slaveTracker=="+sender.address)
    val rpcCallContext = mock(classOf[RpcCallContext])
    when(rpcCallContext.sender).thenReturn(sender)
    masterEndpoint.receiveAndReply(rpcCallContext)(GetMapOutputStatuses(10))
    verify(rpcCallContext).reply(any())
    verify(rpcCallContext, never()).sendFailure(any())

//    masterTracker.stop() // this throws an exception 抛出一个异常
    rpcEnv.shutdown()
  }

  test("remote fetch exceeds akka frame size") {//远程读取超过Akka框架大小
    val newConf = new SparkConf
    //以MB为单位的driver和executor之间通信信息的大小,设置值越大,driver可以接受更大的计算结果
    newConf.set("spark.akka.frameSize", "1")
    newConf.set("spark.rpc.askTimeout", "1") // Fail fast 快速失败

    val masterTracker = new MapOutputTrackerMaster(conf)
    val rpcEnv = createRpcEnv("test")
    val masterEndpoint = new MapOutputTrackerMasterEndpoint(rpcEnv, masterTracker, newConf)
    rpcEnv.setupEndpoint(MapOutputTracker.ENDPOINT_NAME, masterEndpoint)

    // Frame size should be ~1.1MB, and MapOutputTrackerMasterEndpoint should throw exception.
    //帧大小应该是 ~1.1MB,应该抛出异常
    // Note that the size is hand-selected here because map output statuses are compressed before
    // being sent.
    masterTracker.registerShuffle(20, 100)
    (0 until 100).foreach { i =>
      masterTracker.registerMapOutput(20, i, new CompressedMapStatus(
        BlockManagerId("999", "mps", 1000), Array.fill[Long](4000000)(0)))
    }
    val sender = mock(classOf[RpcEndpointRef])
    when(sender.address).thenReturn(RpcAddress("localhost", 12345))
    val rpcCallContext = mock(classOf[RpcCallContext])
    when(rpcCallContext.sender).thenReturn(sender)
    masterEndpoint.receiveAndReply(rpcCallContext)(GetMapOutputStatuses(20))
    verify(rpcCallContext, never()).reply(any())
    verify(rpcCallContext).sendFailure(isA(classOf[SparkException]))

//    masterTracker.stop() // this throws an exception 抛出一个异常
    rpcEnv.shutdown()
  }
  //在同一台机器上的多个输出
  test("getLocationsWithLargestOutputs with multiple outputs in same machine") {
    val rpcEnv = createRpcEnv("test")
    val tracker = new MapOutputTrackerMaster(conf)
    tracker.trackerEndpoint = rpcEnv.setupEndpoint(MapOutputTracker.ENDPOINT_NAME,
      new MapOutputTrackerMasterEndpoint(rpcEnv, tracker, conf))
    // Setup 3 map tasks 设置3个Map任务
    // on hostA with output size 2
    // on hostA with output size 2
    // on hostB with output size 3
    tracker.registerShuffle(10, 3)
    tracker.registerMapOutput(10, 0, MapStatus(BlockManagerId("a", "hostA", 1000),
        Array(2L)))
    tracker.registerMapOutput(10, 1, MapStatus(BlockManagerId("a", "hostA", 1000),
        Array(2L)))
    tracker.registerMapOutput(10, 2, MapStatus(BlockManagerId("b", "hostB", 1000),
        Array(3L)))

    // When the threshold is 50%, only host A should be returned as a preferred location
    //当阈值为50%时,只有主机A应该作为一个首选的位置返回,因为它有7个4字节的输出。
    // as it has 4 out of 7 bytes of output.
    val topLocs50 = tracker.getLocationsWithLargestOutputs(10, 0, 1, 0.5)
    assert(topLocs50.nonEmpty)
    assert(topLocs50.get.size === 1)
    assert(topLocs50.get.head === BlockManagerId("a", "hostA", 1000))

    // When the threshold is 20%, both hosts should be returned as preferred locations.
    //当阈值为20%时,两台主机应作为首选位置返回
    val topLocs20 = tracker.getLocationsWithLargestOutputs(10, 0, 1, 0.2)
    assert(topLocs20.nonEmpty)
    assert(topLocs20.get.size === 2)
    assert(topLocs20.get.toSet ===
           Seq(BlockManagerId("a", "hostA", 1000), BlockManagerId("b", "hostB", 1000)).toSet)

    tracker.stop()
    rpcEnv.shutdown()
  }
}
