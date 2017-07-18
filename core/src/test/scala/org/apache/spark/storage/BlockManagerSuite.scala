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

import java.nio.{ByteBuffer, MappedByteBuffer}
import java.util.Arrays

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.language.implicitConversions
import scala.language.postfixOps

import org.mockito.Mockito.{mock, when}
import org.scalatest._
import org.scalatest.concurrent.Eventually._
import org.scalatest.concurrent.Timeouts._

import org.apache.spark.rpc.RpcEnv
import org.apache.spark._
import org.apache.spark.executor.DataReadMethod
import org.apache.spark.network.nio.NioBlockTransferService
import org.apache.spark.scheduler.LiveListenerBus
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer}
import org.apache.spark.shuffle.hash.HashShuffleManager
import org.apache.spark.storage.BlockManagerMessages.BlockManagerHeartbeat
import org.apache.spark.util._


class BlockManagerSuite extends SparkFunSuite with Matchers with BeforeAndAfterEach
  with PrivateMethodTester with ResetSystemProperties {

  private val conf = new SparkConf(false)
  var store: BlockManager = null
  var store2: BlockManager = null
  var rpcEnv: RpcEnv = null
  var master: BlockManagerMaster = null
   //是否启用内部身份验证
  conf.set("spark.authenticate", "false")
  val securityMgr = new SecurityManager(conf)
  val mapOutputTracker = new MapOutputTrackerMaster(conf)
  val shuffleManager = new HashShuffleManager(conf)

  // Reuse a serializer across tests to avoid creating a new thread-local buffer on each test
  conf.set("spark.kryoserializer.buffer", "1m")
  val serializer = new KryoSerializer(conf)

  // Implicitly convert strings to BlockIds for test clarity.
  // 隐式转换为字符串测试清晰的blockids
  implicit def StringToBlockId(value: String): BlockId = new TestBlockId(value)
  def rdd(rddId: Int, splitId: Int): RDDBlockId = RDDBlockId(rddId, splitId)

  private def makeBlockManager(
      maxMem: Long,
      name: String = SparkContext.DRIVER_IDENTIFIER): BlockManager = {
    val transfer = new NioBlockTransferService(conf, securityMgr)
    val manager = new BlockManager(name, rpcEnv, master, serializer, maxMem, conf,
      mapOutputTracker, shuffleManager, transfer, securityMgr, 0)
    manager.initialize("app-id")
    manager
  }

  override def beforeEach(): Unit = {
    rpcEnv = RpcEnv.create("test", "localhost", 0, conf, securityMgr)

    // Set the arch to 64-bit and compressedOops to true to get a deterministic test-case
    System.setProperty("os.arch", "amd64")
    conf.set("os.arch", "amd64")
    conf.set("spark.test.useCompressedOops", "true")
    //0随机 driver侦听的端口
    conf.set("spark.driver.port", rpcEnv.address.port.toString)
    conf.set("spark.storage.unrollFraction", "0.4")
    conf.set("spark.storage.unrollMemoryThreshold", "512")

    master = new BlockManagerMaster(rpcEnv.setupEndpoint("blockmanager",
      new BlockManagerMasterEndpoint(rpcEnv, true, conf, new LiveListenerBus)), conf, true)

    val initialize = PrivateMethod[Unit]('initialize)
    SizeEstimator invokePrivate initialize()
  }

  override def afterEach(): Unit = {
    if (store != null) {
      store.stop()
      store = null
    }
    if (store2 != null) {
      store2.stop()
      store2 = null
    }
    rpcEnv.shutdown()
    rpcEnv.awaitTermination()
    rpcEnv = null
    master = null
  }

  test("StorageLevel object caching") {//存储级对象缓存
    val level1 = StorageLevel(false, false, false, false, 3)
    // this should return the same object as level1
    //这应该返回相同一级的对象
    val level2 = StorageLevel(false, false, false, false, 3)
    // this should return a different object
    //这应该返回一个不同的对象
    val level3 = StorageLevel(false, false, false, false, 2)
    assert(level2 === level1, "level2 is not same as level1")
    assert(level2.eq(level1), "level2 is not the same object as level1")
    assert(level3 != level1, "level3 is same as level1")
    val bytes1 = Utils.serialize(level1)
    val level1_ = Utils.deserialize[StorageLevel](bytes1)
    val bytes2 = Utils.serialize(level2)
    val level2_ = Utils.deserialize[StorageLevel](bytes2)
    assert(level1_ === level1, "Deserialized level1 not same as original level1")
    assert(level1_.eq(level1), "Deserialized level1 not the same object as original level2")
    assert(level2_ === level2, "Deserialized level2 not same as original level2")
    assert(level2_.eq(level1), "Deserialized level2 not the same object as original level1")
  }

  test("BlockManagerId object caching") {//块管理器标识对象缓存
    val id1 = BlockManagerId("e1", "XXX", 1)
    //这应该是1返回相同的对象
    val id2 = BlockManagerId("e1", "XXX", 1) // this should return the same object as id1
    //这应该返回一个不同的对象
    val id3 = BlockManagerId("e1", "XXX", 2) // this should return a different object
    assert(id2 === id1, "id2 is not same as id1")
    assert(id2.eq(id1), "id2 is not the same object as id1")
    assert(id3 != id1, "id3 is same as id1")
    val bytes1 = Utils.serialize(id1)
    val id1_ = Utils.deserialize[BlockManagerId](bytes1)
    val bytes2 = Utils.serialize(id2)
    val id2_ = Utils.deserialize[BlockManagerId](bytes2)
    assert(id1_ === id1, "Deserialized id1 is not same as original id1")
    assert(id1_.eq(id1), "Deserialized id1 is not the same object as original id1")
    assert(id2_ === id2, "Deserialized id2 is not same as original id2")
    assert(id2_.eq(id1), "Deserialized id2 is not the same object as original id1")
  }

  test("BlockManagerId.isDriver() backwards-compatibility with legacy driver ids (SPARK-6716)") {
    assert(BlockManagerId(SparkContext.DRIVER_IDENTIFIER, "XXX", 1).isDriver)
    assert(BlockManagerId(SparkContext.LEGACY_DRIVER_IDENTIFIER, "XXX", 1).isDriver)
    assert(!BlockManagerId("notADriverIdentifier", "XXX", 1).isDriver)
  }

  test("master + 1 manager interaction") {//主节点+ 1管理合作
    store = makeBlockManager(20000)
    val a1 = new Array[Byte](4000)
    val a2 = new Array[Byte](4000)
    val a3 = new Array[Byte](4000)

    // Putting a1, a2  and a3 in memory and telling master only about a1 and a2
    //把A1,A2和A3在内存中告诉主节点只有A1和A2
    store.putSingle("a1", a1, StorageLevel.MEMORY_ONLY)
    store.putSingle("a2", a2, StorageLevel.MEMORY_ONLY)
    store.putSingle("a3", a3, StorageLevel.MEMORY_ONLY, tellMaster = false)

    // Checking whether blocks are in memory
    //检查块是否在内存中
    assert(store.getSingle("a1").isDefined, "a1 was not in store")
    assert(store.getSingle("a2").isDefined, "a2 was not in store")
    assert(store.getSingle("a3").isDefined, "a3 was not in store")

    // Checking whether master knows about the blocks or not
    //检查是否主节点知道
    assert(master.getLocations("a1").size > 0, "master was not told about a1")
    assert(master.getLocations("a2").size > 0, "master was not told about a2")
    assert(master.getLocations("a3").size === 0, "master was told about a3")

    // Drop a1 and a2 from memory; this should be reported back to the master
    //内存中删除A1和A2的,这应该报告给主节点
    store.dropFromMemory("a1", null: Either[Array[Any], ByteBuffer])
    store.dropFromMemory("a2", null: Either[Array[Any], ByteBuffer])
    assert(store.getSingle("a1") === None, "a1 not removed from store")
    assert(store.getSingle("a2") === None, "a2 not removed from store")
    assert(master.getLocations("a1").size === 0, "master did not remove a1")
    assert(master.getLocations("a2").size === 0, "master did not remove a2")
  }

  test("master + 2 managers interaction") {//主节点+ 2管理合作
    store = makeBlockManager(2000, "exec1")
    store2 = makeBlockManager(2000, "exec2")

    val peers = master.getPeers(store.blockManagerId)
    assert(peers.size === 1, "master did not return the other manager as a peer")
    assert(peers.head === store2.blockManagerId, "peer returned by master is not the other manager")

    val a1 = new Array[Byte](400)
    val a2 = new Array[Byte](400)
    store.putSingle("a1", a1, StorageLevel.MEMORY_ONLY_2)
    store2.putSingle("a2", a2, StorageLevel.MEMORY_ONLY_2)
    assert(master.getLocations("a1").size === 2, "master did not report 2 locations for a1")
    assert(master.getLocations("a2").size === 2, "master did not report 2 locations for a2")
  }

  test("removing block") {//删除块
    store = makeBlockManager(20000)
    val a1 = new Array[Byte](4000)
    val a2 = new Array[Byte](4000)
    val a3 = new Array[Byte](4000)

    // Putting a1, a2 and a3 in memory and telling master only about a1 and a2
    //把A1,A2和A3存储内存中,只有A1和A2告诉master
    store.putSingle("a1-to-remove", a1, StorageLevel.MEMORY_ONLY)
    store.putSingle("a2-to-remove", a2, StorageLevel.MEMORY_ONLY)
    store.putSingle("a3-to-remove", a3, StorageLevel.MEMORY_ONLY, tellMaster = false)

    // Checking whether blocks are in memory and memory size
    //检查块是否在内存大小
    val memStatus = master.getMemoryStatus.head._2
    assert(memStatus._1 == 20000L, "total memory " + memStatus._1 + " should equal 20000")
    assert(memStatus._2 <= 12000L, "remaining memory " + memStatus._2 + " should <= 12000")
    assert(store.getSingle("a1-to-remove").isDefined, "a1 was not in store")
    assert(store.getSingle("a2-to-remove").isDefined, "a2 was not in store")
    assert(store.getSingle("a3-to-remove").isDefined, "a3 was not in store")

    // Checking whether master knows about the blocks or not
    assert(master.getLocations("a1-to-remove").size > 0, "master was not told about a1")
    assert(master.getLocations("a2-to-remove").size > 0, "master was not told about a2")
    assert(master.getLocations("a3-to-remove").size === 0, "master was told about a3")

    // Remove a1 and a2 and a3. Should be no-op for a3.
    //删除a1,a2和 a3,应该没有A3
    master.removeBlock("a1-to-remove")
    master.removeBlock("a2-to-remove")
    master.removeBlock("a3-to-remove")

    eventually(timeout(1000 milliseconds), interval(10 milliseconds)) {
      store.getSingle("a1-to-remove") should be (None)
      master.getLocations("a1-to-remove") should have size 0
    }
    eventually(timeout(1000 milliseconds), interval(10 milliseconds)) {
      store.getSingle("a2-to-remove") should be (None)
      master.getLocations("a2-to-remove") should have size 0
    }
    eventually(timeout(1000 milliseconds), interval(10 milliseconds)) {
      store.getSingle("a3-to-remove") should not be (None)
      master.getLocations("a3-to-remove") should have size 0
    }
    eventually(timeout(1000 milliseconds), interval(10 milliseconds)) {
      val memStatus = master.getMemoryStatus.head._2
      memStatus._1 should equal (20000L)
      memStatus._2 should equal (20000L)
    }
  }

  test("removing rdd") {//删除RDD
    store = makeBlockManager(20000)
    val a1 = new Array[Byte](4000)
    val a2 = new Array[Byte](4000)
    val a3 = new Array[Byte](4000)
    // Putting a1, a2 and a3 in memory.
    //存储a1,a2,a3到内存中
    store.putSingle(rdd(0, 0), a1, StorageLevel.MEMORY_ONLY)
    store.putSingle(rdd(0, 1), a2, StorageLevel.MEMORY_ONLY)
    store.putSingle("nonrddblock", a3, StorageLevel.MEMORY_ONLY)
    master.removeRdd(0, blocking = false)

    eventually(timeout(1000 milliseconds), interval(10 milliseconds)) {
      store.getSingle(rdd(0, 0)) should be (None)
      master.getLocations(rdd(0, 0)) should have size 0
    }
    eventually(timeout(1000 milliseconds), interval(10 milliseconds)) {
      store.getSingle(rdd(0, 1)) should be (None)
      master.getLocations(rdd(0, 1)) should have size 0
    }
    eventually(timeout(1000 milliseconds), interval(10 milliseconds)) {
      store.getSingle("nonrddblock") should not be (None)
      master.getLocations("nonrddblock") should have size (1)
    }

    store.putSingle(rdd(0, 0), a1, StorageLevel.MEMORY_ONLY)
    store.putSingle(rdd(0, 1), a2, StorageLevel.MEMORY_ONLY)
    master.removeRdd(0, blocking = true)
    store.getSingle(rdd(0, 0)) should be (None)
    master.getLocations(rdd(0, 0)) should have size 0
    store.getSingle(rdd(0, 1)) should be (None)
    master.getLocations(rdd(0, 1)) should have size 0
  }

  test("removing broadcast") {//删除广播变更
    store = makeBlockManager(2000)
    val driverStore = store
    val executorStore = makeBlockManager(2000, "executor")
    val a1 = new Array[Byte](400)
    val a2 = new Array[Byte](400)
    val a3 = new Array[Byte](400)
    val a4 = new Array[Byte](400)

    val broadcast0BlockId = BroadcastBlockId(0)
    val broadcast1BlockId = BroadcastBlockId(1)
    val broadcast2BlockId = BroadcastBlockId(2)
    val broadcast2BlockId2 = BroadcastBlockId(2, "_")

    // insert broadcast blocks in both the stores
    //在两个存储中插入广播块
    Seq(driverStore, executorStore).foreach { case s =>
      s.putSingle(broadcast0BlockId, a1, StorageLevel.DISK_ONLY)
      s.putSingle(broadcast1BlockId, a2, StorageLevel.DISK_ONLY)
      s.putSingle(broadcast2BlockId, a3, StorageLevel.DISK_ONLY)
      s.putSingle(broadcast2BlockId2, a4, StorageLevel.DISK_ONLY)
    }

    // verify whether the blocks exist in both the stores
    //验证块是否存在于两个存储中
    Seq(driverStore, executorStore).foreach { case s =>
      s.getLocal(broadcast0BlockId) should not be (None)
      s.getLocal(broadcast1BlockId) should not be (None)
      s.getLocal(broadcast2BlockId) should not be (None)
      s.getLocal(broadcast2BlockId2) should not be (None)
    }

    // remove broadcast 0 block only from executors
    //从执行器删除0块广播
    master.removeBroadcast(0, removeFromMaster = false, blocking = true)

    // only broadcast 0 block should be removed from the executor store
    //只有广播0块应该从执行器存储区中删除
    executorStore.getLocal(broadcast0BlockId) should be (None)
    executorStore.getLocal(broadcast1BlockId) should not be (None)
    executorStore.getLocal(broadcast2BlockId) should not be (None)

    // nothing should be removed from the driver store
    //应该从驱动程序存储中删除任何东西
    driverStore.getLocal(broadcast0BlockId) should not be (None)
    driverStore.getLocal(broadcast1BlockId) should not be (None)
    driverStore.getLocal(broadcast2BlockId) should not be (None)

    // remove broadcast 0 block from the driver as well
    //从驱动程序中删除广播0块
    master.removeBroadcast(0, removeFromMaster = true, blocking = true)
    driverStore.getLocal(broadcast0BlockId) should be (None)
    driverStore.getLocal(broadcast1BlockId) should not be (None)

    // remove broadcast 1 block from both the stores asynchronously
    //从异步存储的两个存储区中删除广播1块,并验证所有广播的1个块已被删除
    // and verify all broadcast 1 blocks have been removed
    master.removeBroadcast(1, removeFromMaster = true, blocking = false)
    eventually(timeout(1000 milliseconds), interval(10 milliseconds)) {
      driverStore.getLocal(broadcast1BlockId) should be (None)
      executorStore.getLocal(broadcast1BlockId) should be (None)
    }

    // remove broadcast 2 from both the stores asynchronously
    //从异步存储的2个广播中删除,并验证所有广播的2个块已被删除
    // and verify all broadcast 2 blocks have been removed
    master.removeBroadcast(2, removeFromMaster = true, blocking = false)
    eventually(timeout(1000 milliseconds), interval(10 milliseconds)) {
      driverStore.getLocal(broadcast2BlockId) should be (None)
      driverStore.getLocal(broadcast2BlockId2) should be (None)
      executorStore.getLocal(broadcast2BlockId) should be (None)
      executorStore.getLocal(broadcast2BlockId2) should be (None)
    }
    executorStore.stop()
    driverStore.stop()
    store = null
  }

  test("reregistration on heart beat") {//重新注册第一个的心跳
    store = makeBlockManager(2000)
    val a1 = new Array[Byte](400)

    store.putSingle("a1", a1, StorageLevel.MEMORY_ONLY)

    assert(store.getSingle("a1").isDefined, "a1 was not in store")
    assert(master.getLocations("a1").size > 0, "master was not told about a1")

    master.removeExecutor(store.blockManagerId.executorId)
    assert(master.getLocations("a1").size == 0, "a1 was not removed from master")

    val reregister = !master.driverEndpoint.askWithRetry[Boolean](
      BlockManagerHeartbeat(store.blockManagerId))
    assert(reregister == true)
  }

  test("reregistration on block update") {//重新注册更新块
    store = makeBlockManager(2000)
    val a1 = new Array[Byte](400)
    val a2 = new Array[Byte](400)

    store.putSingle("a1", a1, StorageLevel.MEMORY_ONLY)
    assert(master.getLocations("a1").size > 0, "master was not told about a1")

    master.removeExecutor(store.blockManagerId.executorId)
    assert(master.getLocations("a1").size == 0, "a1 was not removed from master")

    store.putSingle("a2", a2, StorageLevel.MEMORY_ONLY)
    store.waitForAsyncReregister()

    assert(master.getLocations("a1").size > 0, "a1 was not reregistered with master")
    assert(master.getLocations("a2").size > 0, "master was not told about a2")
  }

  test("reregistration doesn't dead lock") {//注册不锁死
    store = makeBlockManager(2000)
    val a1 = new Array[Byte](400)
    val a2 = List(new Array[Byte](400))

    // try many times to trigger any deadlocks
    //尝试了很多次触发死锁
    for (i <- 1 to 100) {
      master.removeExecutor(store.blockManagerId.executorId)
      val t1 = new Thread {
        override def run() {
          store.putIterator("a2", a2.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
        }
      }
      val t2 = new Thread {
        override def run() {
          store.putSingle("a1", a1, StorageLevel.MEMORY_ONLY)
        }
      }
      val t3 = new Thread {
        override def run() {
          store.reregister()
        }
      }

      t1.start()
      t2.start()
      t3.start()
      t1.join()
      t2.join()
      t3.join()

      store.dropFromMemory("a1", null: Either[Array[Any], ByteBuffer])
      store.dropFromMemory("a2", null: Either[Array[Any], ByteBuffer])
      store.waitForAsyncReregister()
    }
  }
  //正确的blockresult返回调用get()
  test("correct BlockResult returned from get() calls") {
    store = makeBlockManager(12000)
    val list1 = List(new Array[Byte](2000), new Array[Byte](2000))
    val list2 = List(new Array[Byte](500), new Array[Byte](1000), new Array[Byte](1500))
    val list1SizeEstimate = SizeEstimator.estimate(list1.iterator.toArray)
    val list2SizeEstimate = SizeEstimator.estimate(list2.iterator.toArray)
    store.putIterator("list1", list1.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    store.putIterator("list2memory", list2.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    store.putIterator("list2disk", list2.iterator, StorageLevel.DISK_ONLY, tellMaster = true)
    val list1Get = store.get("list1")
    assert(list1Get.isDefined, "list1 expected to be in store")
    assert(list1Get.get.data.size === 2)
    assert(list1Get.get.bytes === list1SizeEstimate)
    assert(list1Get.get.readMethod === DataReadMethod.Memory)
    val list2MemoryGet = store.get("list2memory")
    assert(list2MemoryGet.isDefined, "list2memory expected to be in store")
    assert(list2MemoryGet.get.data.size === 3)
    assert(list2MemoryGet.get.bytes === list2SizeEstimate)
    assert(list2MemoryGet.get.readMethod === DataReadMethod.Memory)
    val list2DiskGet = store.get("list2disk")
    assert(list2DiskGet.isDefined, "list2memory expected to be in store")
    assert(list2DiskGet.get.data.size === 3)
    // We don't know the exact size of the data on disk, but it should certainly be > 0.
    //我们不知道磁盘上的数据的确切大小,但它肯定是> 0
    assert(list2DiskGet.get.bytes > 0)
    assert(list2DiskGet.get.readMethod === DataReadMethod.Disk)
  }

  test("in-memory LRU storage") {//在内存中存储的(LRU)近期最少使用算法
    store = makeBlockManager(12000)
    val a1 = new Array[Byte](4000)
    val a2 = new Array[Byte](4000)
    val a3 = new Array[Byte](4000)
    store.putSingle("a1", a1, StorageLevel.MEMORY_ONLY)
    store.putSingle("a2", a2, StorageLevel.MEMORY_ONLY)
    store.putSingle("a3", a3, StorageLevel.MEMORY_ONLY)
    assert(store.getSingle("a2").isDefined, "a2 was not in store")
    assert(store.getSingle("a3").isDefined, "a3 was not in store")
    assert(store.getSingle("a1") === None, "a1 was in store")
    assert(store.getSingle("a2").isDefined, "a2 was not in store")
    // At this point a2 was gotten last, so LRU will getSingle rid of a3
    //在这一点上是得到A2,所以LRU将getsingle摆脱A3
    store.putSingle("a1", a1, StorageLevel.MEMORY_ONLY)
    assert(store.getSingle("a1").isDefined, "a1 was not in store")
    assert(store.getSingle("a2").isDefined, "a2 was not in store")
    assert(store.getSingle("a3") === None, "a3 was in store")
  }

  test("in-memory LRU storage with serialization") {//在内存中存储序列化的(LRU)近期最少使用算法
    store = makeBlockManager(12000)
    val a1 = new Array[Byte](4000)
    val a2 = new Array[Byte](4000)
    val a3 = new Array[Byte](4000)
    store.putSingle("a1", a1, StorageLevel.MEMORY_ONLY_SER)
    store.putSingle("a2", a2, StorageLevel.MEMORY_ONLY_SER)
    store.putSingle("a3", a3, StorageLevel.MEMORY_ONLY_SER)
    assert(store.getSingle("a2").isDefined, "a2 was not in store")
    assert(store.getSingle("a3").isDefined, "a3 was not in store")
    assert(store.getSingle("a1") === None, "a1 was in store")
    assert(store.getSingle("a2").isDefined, "a2 was not in store")
    // At this point a2 was gotten last, so LRU will getSingle rid of a3
    store.putSingle("a1", a1, StorageLevel.MEMORY_ONLY_SER)
    assert(store.getSingle("a1").isDefined, "a1 was not in store")
    assert(store.getSingle("a2").isDefined, "a2 was not in store")
    assert(store.getSingle("a3") === None, "a3 was in store")
  }

  test("in-memory LRU for partitions of same RDD") {//对于相同RDD分区内存LRU
    store = makeBlockManager(12000)
    val a1 = new Array[Byte](4000)
    val a2 = new Array[Byte](4000)
    val a3 = new Array[Byte](4000)
    store.putSingle(rdd(0, 1), a1, StorageLevel.MEMORY_ONLY)
    store.putSingle(rdd(0, 2), a2, StorageLevel.MEMORY_ONLY)
    store.putSingle(rdd(0, 3), a3, StorageLevel.MEMORY_ONLY)
    // Even though we accessed rdd_0_3 last, it should not have replaced partitions 1 and 2
    //即使我们最后访问rdd_0_3,它不应该取代分区1和2从相同的RDD
    // from the same RDD
    assert(store.getSingle(rdd(0, 3)) === None, "rdd_0_3 was in store")
    assert(store.getSingle(rdd(0, 2)).isDefined, "rdd_0_2 was not in store")
    assert(store.getSingle(rdd(0, 1)).isDefined, "rdd_0_1 was not in store")
    // Check that rdd_0_3 doesn't replace them even after further accesses
    //检查rdd_0_3不能代替他们还要经过进一步的访问
    assert(store.getSingle(rdd(0, 3)) === None, "rdd_0_3 was in store")
    assert(store.getSingle(rdd(0, 3)) === None, "rdd_0_3 was in store")
    assert(store.getSingle(rdd(0, 3)) === None, "rdd_0_3 was in store")
  }

  test("in-memory LRU for partitions of multiple RDDs") {//对于多分区内存LRU RDDS
    store = makeBlockManager(12000)
    store.putSingle(rdd(0, 1), new Array[Byte](4000), StorageLevel.MEMORY_ONLY)
    store.putSingle(rdd(0, 2), new Array[Byte](4000), StorageLevel.MEMORY_ONLY)
    store.putSingle(rdd(1, 1), new Array[Byte](4000), StorageLevel.MEMORY_ONLY)
    // At this point rdd_1_1 should've replaced rdd_0_1
    //此时rdd_1_1应该已经取代了rdd_0_1
    assert(store.memoryStore.contains(rdd(1, 1)), "rdd_1_1 was not in store")
    assert(!store.memoryStore.contains(rdd(0, 1)), "rdd_0_1 was in store")
    assert(store.memoryStore.contains(rdd(0, 2)), "rdd_0_2 was not in store")
    // Do a get() on rdd_0_2 so that it is the most recently used item
    //在rdd_0_2上执行get（），以便它是最近使用的item
    assert(store.getSingle(rdd(0, 2)).isDefined, "rdd_0_2 was not in store")
    // Put in more partitions from RDD 0; they should replace rdd_1_1
    //从RDD 0放入更多的分区; 他们应该取代rdd_1_1
    store.putSingle(rdd(0, 3), new Array[Byte](4000), StorageLevel.MEMORY_ONLY)
    store.putSingle(rdd(0, 4), new Array[Byte](4000), StorageLevel.MEMORY_ONLY)
    // Now rdd_1_1 should be dropped to add rdd_0_3, but then rdd_0_2 should *not* be dropped
    // when we try to add rdd_0_4.
    //现在rdd_1_1应该被删除以添加rdd_0_3，但是rdd_0_2应该不会被删除
    //当我们尝试添加rdd_0_4。
    assert(!store.memoryStore.contains(rdd(1, 1)), "rdd_1_1 was in store")
    assert(!store.memoryStore.contains(rdd(0, 1)), "rdd_0_1 was in store")
    assert(!store.memoryStore.contains(rdd(0, 4)), "rdd_0_4 was in store")
    assert(store.memoryStore.contains(rdd(0, 2)), "rdd_0_2 was not in store")
    assert(store.memoryStore.contains(rdd(0, 3)), "rdd_0_3 was not in store")
  }

  test("tachyon storage") {//
    // TODO Make the spark.test.tachyon.enable true after using tachyon 0.5.0 testing jar.
    val tachyonUnitTestEnabled = conf.getBoolean("spark.test.tachyon.enable", false)
    conf.set(ExternalBlockStore.BLOCK_MANAGER_NAME, ExternalBlockStore.DEFAULT_BLOCK_MANAGER_NAME)
    if (tachyonUnitTestEnabled) {
      store = makeBlockManager(1200)
      val a1 = new Array[Byte](400)
      val a2 = new Array[Byte](400)
      val a3 = new Array[Byte](400)
      store.putSingle("a1", a1, StorageLevel.OFF_HEAP)
      store.putSingle("a2", a2, StorageLevel.OFF_HEAP)
      store.putSingle("a3", a3, StorageLevel.OFF_HEAP)
      assert(store.getSingle("a3").isDefined, "a3 was in store")
      assert(store.getSingle("a2").isDefined, "a2 was in store")
      assert(store.getSingle("a1").isDefined, "a1 was in store")
    } else {
      info("tachyon storage test disabled.")
    }
  }

  test("on-disk storage") {//在磁盘上存储
    store = makeBlockManager(1200)
    val a1 = new Array[Byte](400)
    val a2 = new Array[Byte](400)
    val a3 = new Array[Byte](400)
    store.putSingle("a1", a1, StorageLevel.DISK_ONLY)
    store.putSingle("a2", a2, StorageLevel.DISK_ONLY)
    store.putSingle("a3", a3, StorageLevel.DISK_ONLY)
    assert(store.getSingle("a2").isDefined, "a2 was in store")
    assert(store.getSingle("a3").isDefined, "a3 was in store")
    assert(store.getSingle("a1").isDefined, "a1 was in store")
  }

  test("disk and memory storage") {//在内存上存储
    store = makeBlockManager(12000)
    val a1 = new Array[Byte](4000)
    val a2 = new Array[Byte](4000)
    val a3 = new Array[Byte](4000)
    store.putSingle("a1", a1, StorageLevel.MEMORY_AND_DISK)
    store.putSingle("a2", a2, StorageLevel.MEMORY_AND_DISK)
    store.putSingle("a3", a3, StorageLevel.MEMORY_AND_DISK)
    assert(store.getSingle("a2").isDefined, "a2 was not in store")
    assert(store.getSingle("a3").isDefined, "a3 was not in store")
    assert(store.memoryStore.getValues("a1") == None, "a1 was in memory store")
    assert(store.getSingle("a1").isDefined, "a1 was not in store")
    assert(store.memoryStore.getValues("a1").isDefined, "a1 was not in memory store")
  }

  test("disk and memory storage with getLocalBytes") {//在磁盘和内存存储获本地二进制
    store = makeBlockManager(12000)
    val a1 = new Array[Byte](4000)
    val a2 = new Array[Byte](4000)
    val a3 = new Array[Byte](4000)
    store.putSingle("a1", a1, StorageLevel.MEMORY_AND_DISK)
    store.putSingle("a2", a2, StorageLevel.MEMORY_AND_DISK)
    store.putSingle("a3", a3, StorageLevel.MEMORY_AND_DISK)
    assert(store.getLocalBytes("a2").isDefined, "a2 was not in store")
    assert(store.getLocalBytes("a3").isDefined, "a3 was not in store")
    assert(store.memoryStore.getValues("a1") == None, "a1 was in memory store")
    assert(store.getLocalBytes("a1").isDefined, "a1 was not in store")
    assert(store.memoryStore.getValues("a1").isDefined, "a1 was not in memory store")
  }

  test("disk and memory storage with serialization") {//磁盘和存储器存储序列化
    store = makeBlockManager(12000)
    val a1 = new Array[Byte](4000)
    val a2 = new Array[Byte](4000)
    val a3 = new Array[Byte](4000)
    store.putSingle("a1", a1, StorageLevel.MEMORY_AND_DISK_SER)
    store.putSingle("a2", a2, StorageLevel.MEMORY_AND_DISK_SER)
    store.putSingle("a3", a3, StorageLevel.MEMORY_AND_DISK_SER)
    assert(store.getSingle("a2").isDefined, "a2 was not in store")
    assert(store.getSingle("a3").isDefined, "a3 was not in store")
    assert(store.memoryStore.getValues("a1") == None, "a1 was in memory store")
    assert(store.getSingle("a1").isDefined, "a1 was not in store")
    assert(store.memoryStore.getValues("a1").isDefined, "a1 was not in memory store")
  }
//磁盘和存储器存储序列化获得本地或者远程数据
  test("disk and memory storage with serialization and getLocalBytes") {
    store = makeBlockManager(12000)
    val a1 = new Array[Byte](4000)
    val a2 = new Array[Byte](4000)
    val a3 = new Array[Byte](4000)
    store.putSingle("a1", a1, StorageLevel.MEMORY_AND_DISK_SER)
    store.putSingle("a2", a2, StorageLevel.MEMORY_AND_DISK_SER)
    store.putSingle("a3", a3, StorageLevel.MEMORY_AND_DISK_SER)
    assert(store.getLocalBytes("a2").isDefined, "a2 was not in store")
    assert(store.getLocalBytes("a3").isDefined, "a3 was not in store")
    assert(store.memoryStore.getValues("a1") == None, "a1 was in memory store")
    assert(store.getLocalBytes("a1").isDefined, "a1 was not in store")
    assert(store.memoryStore.getValues("a1").isDefined, "a1 was not in memory store")
  }

  test("LRU with mixed storage levels") {
    store = makeBlockManager(12000)
    val a1 = new Array[Byte](4000)
    val a2 = new Array[Byte](4000)
    val a3 = new Array[Byte](4000)
    val a4 = new Array[Byte](4000)
    // First store a1 and a2, both in memory, and a3, on disk only
    //第一个存储a1和a2在内存中,a2在磁盘
    store.putSingle("a1", a1, StorageLevel.MEMORY_ONLY_SER)
    store.putSingle("a2", a2, StorageLevel.MEMORY_ONLY_SER)
    store.putSingle("a3", a3, StorageLevel.DISK_ONLY)
    // At this point LRU should not kick in because a3 is only on disk
    //在这一点上不应该因为LRU踢A3只有在磁盘
    assert(store.getSingle("a1").isDefined, "a1 was not in store")
    assert(store.getSingle("a2").isDefined, "a2 was not in store")
    assert(store.getSingle("a3").isDefined, "a3 was not in store")
    // Now let's add in a4, which uses both disk and memory; a1 should drop out
    //现在让我们添加在A4,使用磁盘和内存;A1应该退出
    store.putSingle("a4", a4, StorageLevel.MEMORY_AND_DISK_SER)
    assert(store.getSingle("a1") == None, "a1 was in store")
    assert(store.getSingle("a2").isDefined, "a2 was not in store")
    assert(store.getSingle("a3").isDefined, "a3 was not in store")
    assert(store.getSingle("a4").isDefined, "a4 was not in store")
  }

  test("in-memory LRU with streams") {//在内存LRU流
    store = makeBlockManager(12000)
    val list1 = List(new Array[Byte](2000), new Array[Byte](2000))
    val list2 = List(new Array[Byte](2000), new Array[Byte](2000))
    val list3 = List(new Array[Byte](2000), new Array[Byte](2000))
    store.putIterator("list1", list1.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    store.putIterator("list2", list2.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    store.putIterator("list3", list3.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    assert(store.get("list2").isDefined, "list2 was not in store")
    assert(store.get("list2").get.data.size === 2)
    assert(store.get("list3").isDefined, "list3 was not in store")
    assert(store.get("list3").get.data.size === 2)
    assert(store.get("list1") === None, "list1 was in store")
    assert(store.get("list2").isDefined, "list2 was not in store")
    assert(store.get("list2").get.data.size === 2)
    // At this point list2 was gotten last, so LRU will getSingle rid of list3
    store.putIterator("list1", list1.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    assert(store.get("list1").isDefined, "list1 was not in store")
    assert(store.get("list1").get.data.size === 2)
    assert(store.get("list2").isDefined, "list2 was not in store")
    assert(store.get("list2").get.data.size === 2)
    assert(store.get("list3") === None, "list1 was in store")
  }

  test("LRU with mixed storage levels and streams") {
    store = makeBlockManager(12000)
    val list1 = List(new Array[Byte](2000), new Array[Byte](2000))
    val list2 = List(new Array[Byte](2000), new Array[Byte](2000))
    val list3 = List(new Array[Byte](2000), new Array[Byte](2000))
    val list4 = List(new Array[Byte](2000), new Array[Byte](2000))
    // First store list1 and list2, both in memory, and list3, on disk only
    //第一个存储列表1和列表2,都在内存中,list3只在磁盘上
    store.putIterator("list1", list1.iterator, StorageLevel.MEMORY_ONLY_SER, tellMaster = true)
    store.putIterator("list2", list2.iterator, StorageLevel.MEMORY_ONLY_SER, tellMaster = true)
    store.putIterator("list3", list3.iterator, StorageLevel.DISK_ONLY, tellMaster = true)
    val listForSizeEstimate = new ArrayBuffer[Any]
    listForSizeEstimate ++= list1.iterator
    val listSize = SizeEstimator.estimate(listForSizeEstimate)
    // At this point LRU should not kick in because list3 is only on disk
    //在这一点上不应该踢在LRU因为list3只有在磁盘
    assert(store.get("list1").isDefined, "list1 was not in store")
    assert(store.get("list1").get.data.size === 2)
    assert(store.get("list2").isDefined, "list2 was not in store")
    assert(store.get("list2").get.data.size === 2)
    assert(store.get("list3").isDefined, "list3 was not in store")
    assert(store.get("list3").get.data.size === 2)
    assert(store.get("list1").isDefined, "list1 was not in store")
    assert(store.get("list1").get.data.size === 2)
    assert(store.get("list2").isDefined, "list2 was not in store")
    assert(store.get("list2").get.data.size === 2)
    assert(store.get("list3").isDefined, "list3 was not in store")
    assert(store.get("list3").get.data.size === 2)
    // Now let's add in list4, which uses both disk and memory; list1 should drop out
    //现在让我们添加在list4,它使用磁盘和内存,list1应该退出
    store.putIterator("list4", list4.iterator, StorageLevel.MEMORY_AND_DISK_SER, tellMaster = true)
    assert(store.get("list1") === None, "list1 was in store")
    assert(store.get("list2").isDefined, "list2 was not in store")
    assert(store.get("list2").get.data.size === 2)
    assert(store.get("list3").isDefined, "list3 was not in store")
    assert(store.get("list3").get.data.size === 2)
    assert(store.get("list4").isDefined, "list4 was not in store")
    assert(store.get("list4").get.data.size === 2)
  }

  test("negative byte values in ByteBufferInputStream") {
    val buffer = ByteBuffer.wrap(Array[Int](254, 255, 0, 1, 2).map(_.toByte).toArray)
    val stream = new ByteBufferInputStream(buffer)
    val temp = new Array[Byte](10)
    assert(stream.read() === 254, "unexpected byte read")
    assert(stream.read() === 255, "unexpected byte read")
    assert(stream.read() === 0, "unexpected byte read")
    assert(stream.read(temp, 0, temp.length) === 2, "unexpected number of bytes read")
    assert(stream.read() === -1, "end of stream not signalled")
    assert(stream.read(temp, 0, temp.length) === -1, "end of stream not signalled")
  }

  test("overly large block") {//过大的块
    store = makeBlockManager(5000)
    store.putSingle("a1", new Array[Byte](10000), StorageLevel.MEMORY_ONLY)
    assert(store.getSingle("a1") === None, "a1 was in store")
    store.putSingle("a2", new Array[Byte](10000), StorageLevel.MEMORY_AND_DISK)
    assert(store.memoryStore.getValues("a2") === None, "a2 was in memory store")
    assert(store.getSingle("a2").isDefined, "a2 was not in store")
  }

  test("block compression") {//块压缩
    try {
      conf.set("spark.shuffle.compress", "true")
      store = makeBlockManager(20000, "exec1")
      store.putSingle(ShuffleBlockId(0, 0, 0), new Array[Byte](1000), StorageLevel.MEMORY_ONLY_SER)
      assert(store.memoryStore.getSize(ShuffleBlockId(0, 0, 0)) <= 100,
        "shuffle_0_0_0 was not compressed")
      store.stop()
      store = null

      conf.set("spark.shuffle.compress", "false")
      store = makeBlockManager(20000, "exec2")
      store.putSingle(ShuffleBlockId(0, 0, 0), new Array[Byte](10000), StorageLevel.MEMORY_ONLY_SER)
      assert(store.memoryStore.getSize(ShuffleBlockId(0, 0, 0)) >= 10000,
        "shuffle_0_0_0 was compressed")
      store.stop()
      store = null
     //是否在发送之前压缩广播变量
      conf.set("spark.broadcast.compress", "true")
      store = makeBlockManager(20000, "exec3")
      store.putSingle(BroadcastBlockId(0), new Array[Byte](10000), StorageLevel.MEMORY_ONLY_SER)
      assert(store.memoryStore.getSize(BroadcastBlockId(0)) <= 1000,
        "broadcast_0 was not compressed")
      store.stop()
      store = null
      //是否在发送之前压缩广播变量
      conf.set("spark.broadcast.compress", "false")
      store = makeBlockManager(20000, "exec4")
      store.putSingle(BroadcastBlockId(0), new Array[Byte](10000), StorageLevel.MEMORY_ONLY_SER)
      assert(store.memoryStore.getSize(BroadcastBlockId(0)) >= 10000, "broadcast_0 was compressed")
      store.stop()
      store = null
 //是否压缩RDD分区
      conf.set("spark.rdd.compress", "true")
      store = makeBlockManager(20000, "exec5")
      store.putSingle(rdd(0, 0), new Array[Byte](10000), StorageLevel.MEMORY_ONLY_SER)
      assert(store.memoryStore.getSize(rdd(0, 0)) <= 1000, "rdd_0_0 was not compressed")
      store.stop()
      store = null
 //是否压缩RDD分区
      conf.set("spark.rdd.compress", "false")
      store = makeBlockManager(20000, "exec6")
      store.putSingle(rdd(0, 0), new Array[Byte](10000), StorageLevel.MEMORY_ONLY_SER)
      assert(store.memoryStore.getSize(rdd(0, 0)) >= 10000, "rdd_0_0 was compressed")
      store.stop()
      store = null

      // Check that any other block types are also kept uncompressed
      //检查任何其他块类型也未压缩
      store = makeBlockManager(20000, "exec7")
      store.putSingle("other_block", new Array[Byte](10000), StorageLevel.MEMORY_ONLY)
      assert(store.memoryStore.getSize("other_block") >= 10000, "other_block was compressed")
      store.stop()
      store = null
    } finally {
      System.clearProperty("spark.shuffle.compress")
      //是否在发送之前压缩广播变量
      System.clearProperty("spark.broadcast.compress")
       //是否压缩RDD分区
      System.clearProperty("spark.rdd.compress")
    }
  }

  test("block store put failure") {//块存储故障
    // Use Java serializer so we can create an unserializable error.
    //使用java序列化程序,所以我们可以创建一个未序列化的错差
    val transfer = new NioBlockTransferService(conf, securityMgr)
    store = new BlockManager(SparkContext.DRIVER_IDENTIFIER, rpcEnv, master,
      new JavaSerializer(conf), 1200, conf, mapOutputTracker, shuffleManager, transfer, securityMgr,
      0)

    // The put should fail since a1 is not serializable.
    //一个未序列化的存储失败
    class UnserializableClass
    val a1 = new UnserializableClass
    intercept[java.io.NotSerializableException] {
      store.putSingle("a1", a1, StorageLevel.DISK_ONLY)
    }

    // Make sure get a1 doesn't hang and returns None.
    //确保获得A1暂停,返回None,
    failAfter(1 second) {
      assert(store.getSingle("a1") == None, "a1 should not be in store")
    }
  }
  //内存映射和非内存映射文件的读取是等效的
  test("reads of memory-mapped and non memory-mapped files are equivalent") {
  //以字节为单位的块大小,用于磁盘读取一个块大小时进行内存映射。这可以防止Spark在内存映射时使用很小块,
//一般情况下,对块进行内存映射的开销接近或低于操作系统的页大小
    val confKey = "spark.storage.memoryMapThreshold"

    // Create a non-trivial (not all zeros) byte array
    //创建一个不重要（不是所有的零）字节数组
    var counter = 0.toByte
    def incr: Byte = {counter = (counter + 1).toByte; counter;}
    val bytes = Array.fill[Byte](1000)(incr)
    val byteBuffer = ByteBuffer.wrap(bytes)

    val blockId = BlockId("rdd_1_2")

    // This sequence of mocks makes these tests fairly brittle. It would
    // be nice to refactor classes involved in disk storage in a way that
    // allows for easier testing.
    //这个模拟序列使得这些测试相当脆弱。 它会以很好的方式重构涉及磁盘存储的类允许更容易的测试。
    val blockManager = mock(classOf[BlockManager])
    when(blockManager.conf).thenReturn(conf.clone.set(confKey, "0"))
    val diskBlockManager = new DiskBlockManager(blockManager, conf)

    val diskStoreMapped = new DiskStore(blockManager, diskBlockManager)
    diskStoreMapped.putBytes(blockId, byteBuffer, StorageLevel.DISK_ONLY)
    val mapped = diskStoreMapped.getBytes(blockId).get

    when(blockManager.conf).thenReturn(conf.clone.set(confKey, "1m"))
    val diskStoreNotMapped = new DiskStore(blockManager, diskBlockManager)
    diskStoreNotMapped.putBytes(blockId, byteBuffer, StorageLevel.DISK_ONLY)
    val notMapped = diskStoreNotMapped.getBytes(blockId).get

    // Not possible to do isInstanceOf due to visibility of HeapByteBuffer
    //由于HeapByteBuffer的可见性,不可能做isInstanceOf
    assert(notMapped.getClass.getName.endsWith("HeapByteBuffer"),
      "Expected HeapByteBuffer for un-mapped read")
    assert(mapped.isInstanceOf[MappedByteBuffer], "Expected MappedByteBuffer for mapped read")

    def arrayFromByteBuffer(in: ByteBuffer): Array[Byte] = {
      val array = new Array[Byte](in.remaining())
      in.get(array)
      array
    }

    val mappedAsArray = arrayFromByteBuffer(mapped)
    val notMappedAsArray = arrayFromByteBuffer(notMapped)
    assert(Arrays.equals(mappedAsArray, bytes))
    assert(Arrays.equals(notMappedAsArray, bytes))
  }

  test("updated block statuses") {//更新块的状态
    store = makeBlockManager(12000)
    val list = List.fill(2)(new Array[Byte](2000))
    val bigList = List.fill(8)(new Array[Byte](2000))

    // 1 updated block (i.e. list1)
    //1 更新块(i.e. list1)
    val updatedBlocks1 =
      store.putIterator("list1", list.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    assert(updatedBlocks1.size === 1)
    assert(updatedBlocks1.head._1 === TestBlockId("list1"))
    assert(updatedBlocks1.head._2.storageLevel === StorageLevel.MEMORY_ONLY)

    // 1 updated block (i.e. list2)
    //1 更新块(i.e. list2)
    val updatedBlocks2 =
      store.putIterator("list2", list.iterator, StorageLevel.MEMORY_AND_DISK, tellMaster = true)
    assert(updatedBlocks2.size === 1)
    assert(updatedBlocks2.head._1 === TestBlockId("list2"))
    assert(updatedBlocks2.head._2.storageLevel === StorageLevel.MEMORY_ONLY)

    // 2 updated blocks - list1 is kicked out of memory while list3 is added
    //2 更新块- list1踢出内存而list3添加
    val updatedBlocks3 =
      store.putIterator("list3", list.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    assert(updatedBlocks3.size === 2)
    updatedBlocks3.foreach { case (id, status) =>
      id match {
        case TestBlockId("list1") => assert(status.storageLevel === StorageLevel.NONE)
        case TestBlockId("list3") => assert(status.storageLevel === StorageLevel.MEMORY_ONLY)
        case _ => fail("Updated block is neither list1 nor list3")
      }
    }
    assert(store.memoryStore.contains("list3"), "list3 was not in memory store")

    // 2 updated blocks - list2 is kicked out of memory (but put on disk) while list4 is added
    //2 更新块-ist2踢出内存(但放在磁盘上)而list4添加
    val updatedBlocks4 =
      store.putIterator("list4", list.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    assert(updatedBlocks4.size === 2)
    updatedBlocks4.foreach { case (id, status) =>
      id match {
        case TestBlockId("list2") => assert(status.storageLevel === StorageLevel.DISK_ONLY)
        case TestBlockId("list4") => assert(status.storageLevel === StorageLevel.MEMORY_ONLY)
        case _ => fail("Updated block is neither list2 nor list4")
      }
    }
    assert(store.diskStore.contains("list2"), "list2 was not in disk store")
    assert(store.memoryStore.contains("list4"), "list4 was not in memory store")

    // No updated blocks - list5 is too big to fit in store and nothing is kicked out
    //没有更新的块-列表5太大,适合在存储并没有踢出
    val updatedBlocks5 =
      store.putIterator("list5", bigList.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    assert(updatedBlocks5.size === 0)

    // memory store contains only list3 and list4
    //存储器存储只包含list3和list4
    assert(!store.memoryStore.contains("list1"), "list1 was in memory store")
    assert(!store.memoryStore.contains("list2"), "list2 was in memory store")
    assert(store.memoryStore.contains("list3"), "list3 was not in memory store")
    assert(store.memoryStore.contains("list4"), "list4 was not in memory store")
    assert(!store.memoryStore.contains("list5"), "list5 was in memory store")

    // disk store contains only list2
    //磁盘存储只包含list2
    assert(!store.diskStore.contains("list1"), "list1 was in disk store")
    assert(store.diskStore.contains("list2"), "list2 was not in disk store")
    assert(!store.diskStore.contains("list3"), "list3 was in disk store")
    assert(!store.diskStore.contains("list4"), "list4 was in disk store")
    assert(!store.diskStore.contains("list5"), "list5 was in disk store")
  }

  test("query block statuses") {//查询块的状态
    store = makeBlockManager(12000)
    val list = List.fill(2)(new Array[Byte](2000))

    // Tell master. By LRU, only list2 and list3 remains.
    //告诉主人,通过LRU,只有清单和目录3仍然。
    store.putIterator("list1", list.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    store.putIterator("list2", list.iterator, StorageLevel.MEMORY_AND_DISK, tellMaster = true)
    store.putIterator("list3", list.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)

    // getLocations and getBlockStatus should yield the same locations
    //getLocations 和 getBlockStatus应产生相同的位置
    assert(store.master.getLocations("list1").size === 0)
    assert(store.master.getLocations("list2").size === 1)
    assert(store.master.getLocations("list3").size === 1)
    assert(store.master.getBlockStatus("list1", askSlaves = false).size === 0)
    assert(store.master.getBlockStatus("list2", askSlaves = false).size === 1)
    assert(store.master.getBlockStatus("list3", askSlaves = false).size === 1)
    assert(store.master.getBlockStatus("list1", askSlaves = true).size === 0)
    assert(store.master.getBlockStatus("list2", askSlaves = true).size === 1)
    assert(store.master.getBlockStatus("list3", askSlaves = true).size === 1)

    // This time don't tell master and see what happens. By LRU, only list5 and list6 remains.
    //这一次不要调用主节点,看看会发生什么,通过LRU,只有列表6和list6仍然保留
    store.putIterator("list4", list.iterator, StorageLevel.MEMORY_ONLY, tellMaster = false)
    store.putIterator("list5", list.iterator, StorageLevel.MEMORY_AND_DISK, tellMaster = false)
    store.putIterator("list6", list.iterator, StorageLevel.MEMORY_ONLY, tellMaster = false)

    // getLocations should return nothing because the master is not informed
    // getLocations应该返回无因为主节点是没有通知
    // getBlockStatus without asking slaves should have the same result 不要求子节点有相同的结果
    // getBlockStatus with asking slaves, however, should return the actual block statuses
    //getBlockStatus 然而,应该返回实际的块状态
    assert(store.master.getLocations("list4").size === 0)
    assert(store.master.getLocations("list5").size === 0)
    assert(store.master.getLocations("list6").size === 0)
    assert(store.master.getBlockStatus("list4", askSlaves = false).size === 0)
    assert(store.master.getBlockStatus("list5", askSlaves = false).size === 0)
    assert(store.master.getBlockStatus("list6", askSlaves = false).size === 0)
    assert(store.master.getBlockStatus("list4", askSlaves = true).size === 0)
    assert(store.master.getBlockStatus("list5", askSlaves = true).size === 1)
    assert(store.master.getBlockStatus("list6", askSlaves = true).size === 1)
  }

  test("get matching blocks") {//获得匹配的块
    store = makeBlockManager(12000)
    val list = List.fill(2)(new Array[Byte](100))

    // insert some blocks
    //插入块
    store.putIterator("list1", list.iterator, StorageLevel.MEMORY_AND_DISK, tellMaster = true)
    store.putIterator("list2", list.iterator, StorageLevel.MEMORY_AND_DISK, tellMaster = true)
    store.putIterator("list3", list.iterator, StorageLevel.MEMORY_AND_DISK, tellMaster = true)

    // getLocations and getBlockStatus should yield the same locations
    //getLocations和getBlockStatus应该产生相同的位置
    assert(store.master.getMatchingBlockIds(_.toString.contains("list"), askSlaves = false).size
      === 3)
    assert(store.master.getMatchingBlockIds(_.toString.contains("list1"), askSlaves = false).size
      === 1)

    // insert some more blocks
      //插入一些块
    store.putIterator("newlist1", list.iterator, StorageLevel.MEMORY_AND_DISK, tellMaster = true)
    store.putIterator("newlist2", list.iterator, StorageLevel.MEMORY_AND_DISK, tellMaster = false)
    store.putIterator("newlist3", list.iterator, StorageLevel.MEMORY_AND_DISK, tellMaster = false)

    // getLocations and getBlockStatus should yield the same locations
    //getlocations和getblockstatus应该产生相同的位置
    assert(store.master.getMatchingBlockIds(_.toString.contains("newlist"), askSlaves = false).size
      === 1)
    assert(store.master.getMatchingBlockIds(_.toString.contains("newlist"), askSlaves = true).size
      === 3)

    val blockIds = Seq(RDDBlockId(1, 0), RDDBlockId(1, 1), RDDBlockId(2, 0))
    blockIds.foreach { blockId =>
      store.putIterator(blockId, list.iterator, StorageLevel.MEMORY_ONLY, tellMaster = true)
    }
    val matchedBlockIds = store.master.getMatchingBlockIds(_ match {
      case RDDBlockId(1, _) => true
      case _ => false
    }, askSlaves = true)
    assert(matchedBlockIds.toSet === Set(RDDBlockId(1, 0), RDDBlockId(1, 1)))
  }
  //修正了缓存替换相同的RDD规则
  test("SPARK-1194 regression: fix the same-RDD rule for cache replacement") {
    store = makeBlockManager(12000)
    store.putSingle(rdd(0, 0), new Array[Byte](4000), StorageLevel.MEMORY_ONLY)
    store.putSingle(rdd(1, 0), new Array[Byte](4000), StorageLevel.MEMORY_ONLY)
    // Access rdd_1_0 to ensure it's not least recently used.
    //为了确保这不是最近最少使用的访问rdd_1_0。
    assert(store.getSingle(rdd(1, 0)).isDefined, "rdd_1_0 was not in store")
    // According to the same-RDD rule, rdd_1_0 should be replaced here.
    //根据同一法规则,rdd_1_0应更换这里。
    store.putSingle(rdd(0, 1), new Array[Byte](4000), StorageLevel.MEMORY_ONLY)
    // rdd_1_0 should have been replaced, even it's not least recently used.
    //rdd_1_0应该被取代,即使它不是最近最少使用。
    assert(store.memoryStore.contains(rdd(0, 0)), "rdd_0_0 was not in store")
    assert(store.memoryStore.contains(rdd(0, 1)), "rdd_0_1 was not in store")
    assert(!store.memoryStore.contains(rdd(1, 0)), "rdd_1_0 was in store")
  }

  test("reserve/release unroll memory") {//储备/释放展开内存
    store = makeBlockManager(12000)
    val memoryStore = store.memoryStore
    assert(memoryStore.currentUnrollMemory === 0)
    assert(memoryStore.currentUnrollMemoryForThisTask === 0)

    // Reserve 储备
    memoryStore.reserveUnrollMemoryForThisTask(100)
    assert(memoryStore.currentUnrollMemoryForThisTask === 100)
    memoryStore.reserveUnrollMemoryForThisTask(200)
    assert(memoryStore.currentUnrollMemoryForThisTask === 300)
    memoryStore.reserveUnrollMemoryForThisTask(500)
    assert(memoryStore.currentUnrollMemoryForThisTask === 800)
    memoryStore.reserveUnrollMemoryForThisTask(1000000)
    assert(memoryStore.currentUnrollMemoryForThisTask === 800) // not granted
    // Release 释放
    memoryStore.releaseUnrollMemoryForThisTask(100)
    assert(memoryStore.currentUnrollMemoryForThisTask === 700)
    memoryStore.releaseUnrollMemoryForThisTask(100)
    assert(memoryStore.currentUnrollMemoryForThisTask === 600)
    // Reserve again 再次储备
    memoryStore.reserveUnrollMemoryForThisTask(4400)
    assert(memoryStore.currentUnrollMemoryForThisTask === 5000)
    memoryStore.reserveUnrollMemoryForThisTask(20000)
    assert(memoryStore.currentUnrollMemoryForThisTask === 5000) // not granted
    // Release again  再次释放
    memoryStore.releaseUnrollMemoryForThisTask(1000)
    assert(memoryStore.currentUnrollMemoryForThisTask === 4000)
    memoryStore.releaseUnrollMemoryForThisTask() // release all
    assert(memoryStore.currentUnrollMemoryForThisTask === 0)
  }

  /**
   * Verify the result of MemoryStore#unrollSafely is as expected.
   * 验证的结果是内存中# unrollsafely预期
   */
  private def verifyUnroll(
      expected: Iterator[Any],
      result: Either[Array[Any], Iterator[Any]],
      shouldBeArray: Boolean): Unit = {
    val actual: Iterator[Any] = result match {
      case Left(arr: Array[Any]) =>
        assert(shouldBeArray, "expected iterator from unroll!")
        arr.iterator
      case Right(it: Iterator[Any]) =>
        assert(!shouldBeArray, "expected array from unroll!")
        it
      case _ =>
        fail("unroll returned neither an iterator nor an array...")
    }
    expected.zip(actual).foreach { case (e, a) =>
      assert(e === a, "unroll did not return original values!")
    }
  }

  test("safely unroll blocks") {//安全展开块
    store = makeBlockManager(12000)
    val smallList = List.fill(40)(new Array[Byte](100))
    val bigList = List.fill(40)(new Array[Byte](1000))
    val memoryStore = store.memoryStore
    val droppedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]
    assert(memoryStore.currentUnrollMemoryForThisTask === 0)

    // Unroll with all the space in the world. This should succeed and return an array.
    //将展开所有空间,这应该成功并返回一个数组
    var unrollResult = memoryStore.unrollSafely("unroll", smallList.iterator, droppedBlocks)
    verifyUnroll(smallList.iterator, unrollResult, shouldBeArray = true)
    assert(memoryStore.currentUnrollMemoryForThisTask === 0)
    memoryStore.releasePendingUnrollMemoryForThisTask()

    // Unroll with not enough space. This should succeed after kicking out someBlock1.
    //将没有足够的空间,这应该被踢出后someblock1成功
    store.putIterator("someBlock1", smallList.iterator, StorageLevel.MEMORY_ONLY)
    store.putIterator("someBlock2", smallList.iterator, StorageLevel.MEMORY_ONLY)
    unrollResult = memoryStore.unrollSafely("unroll", smallList.iterator, droppedBlocks)
    verifyUnroll(smallList.iterator, unrollResult, shouldBeArray = true)
    assert(memoryStore.currentUnrollMemoryForThisTask === 0)
    assert(droppedBlocks.size === 1)
    assert(droppedBlocks.head._1 === TestBlockId("someBlock1"))
    droppedBlocks.clear()
    memoryStore.releasePendingUnrollMemoryForThisTask()

    // Unroll huge block with not enough space. Even after ensuring free space of 12000 * 0.4 =
    // 4800 bytes, there is still not enough room to unroll this block. This returns an iterator.
    //打开巨大的块没有足够的空间,即使在保证了自由空间
    // In the mean time, however, we kicked out someBlock2 before giving up.
    store.putIterator("someBlock3", smallList.iterator, StorageLevel.MEMORY_ONLY)
    unrollResult = memoryStore.unrollSafely("unroll", bigList.iterator, droppedBlocks)
    verifyUnroll(bigList.iterator, unrollResult, shouldBeArray = false)
    assert(memoryStore.currentUnrollMemoryForThisTask > 0) // we returned an iterator
    assert(droppedBlocks.size === 1)
    assert(droppedBlocks.head._1 === TestBlockId("someBlock2"))
    droppedBlocks.clear()
  }

  test("safely unroll blocks through putIterator") {//安全地展开通过putiterator块
    store = makeBlockManager(12000)
    val memOnly = StorageLevel.MEMORY_ONLY
    val memoryStore = store.memoryStore
    val smallList = List.fill(40)(new Array[Byte](100))
    val bigList = List.fill(40)(new Array[Byte](1000))
    def smallIterator: Iterator[Any] = smallList.iterator.asInstanceOf[Iterator[Any]]
    def bigIterator: Iterator[Any] = bigList.iterator.asInstanceOf[Iterator[Any]]
    assert(memoryStore.currentUnrollMemoryForThisTask === 0)

    // Unroll with plenty of space. This should succeed and cache both blocks.
    //将有很多的空间,这应该成功并缓存两个块。
    val result1 = memoryStore.putIterator("b1", smallIterator, memOnly, returnValues = true)
    val result2 = memoryStore.putIterator("b2", smallIterator, memOnly, returnValues = true)
    assert(memoryStore.contains("b1"))
    assert(memoryStore.contains("b2"))
    assert(result1.size > 0) // unroll was successful展开成功
    assert(result2.size > 0)
    assert(result1.data.isLeft) // unroll did not drop this block to disk 展开不放弃这一块磁盘
    assert(result2.data.isLeft)
    assert(memoryStore.currentUnrollMemoryForThisTask === 0)

    // Re-put these two blocks so block manager knows about them too. Otherwise, block manager
    // would not know how to drop them from memory later.
    //重新整理这两个块，所以块管理也知道他们。 否则，块管理器不知道如何将它们从内存中删除。
    memoryStore.remove("b1")
    memoryStore.remove("b2")
    store.putIterator("b1", smallIterator, memOnly)
    store.putIterator("b2", smallIterator, memOnly)
    // Unroll with not enough space. This should succeed but kick out b1 in the process.
    //展开空间不足,这应该是成功的，但在这个过程中踢出b1。
    val result3 = memoryStore.putIterator("b3", smallIterator, memOnly, returnValues = true)
    assert(result3.size > 0)
    assert(result3.data.isLeft)
    assert(!memoryStore.contains("b1"))
    assert(memoryStore.contains("b2"))
    assert(memoryStore.contains("b3"))
    assert(memoryStore.currentUnrollMemoryForThisTask === 0)
    memoryStore.remove("b3")
    store.putIterator("b3", smallIterator, memOnly)

    // Unroll huge block with not enough space. This should fail and kick out b2 in the process.
    //打开巨大的块没有足够的空间,
    val result4 = memoryStore.putIterator("b4", bigIterator, memOnly, returnValues = true)
    assert(result4.size === 0) // unroll was unsuccessful
    assert(result4.data.isLeft)
    assert(!memoryStore.contains("b1"))
    assert(!memoryStore.contains("b2"))
    assert(memoryStore.contains("b3"))
    assert(!memoryStore.contains("b4"))
    //我们返回了一个迭代器
    assert(memoryStore.currentUnrollMemoryForThisTask > 0) // we returned an iterator
  }

  /**
   * This test is essentially identical to the preceding one, except that it uses MEMORY_AND_DISK.
   * 测试基本上是相同的前一个
   */
  test("safely unroll blocks through putIterator (disk)") {//安全地展开,通过putiterator块
    store = makeBlockManager(12000)
    val memAndDisk = StorageLevel.MEMORY_AND_DISK
    val memoryStore = store.memoryStore
    val diskStore = store.diskStore
    val smallList = List.fill(40)(new Array[Byte](100))
    val bigList = List.fill(40)(new Array[Byte](1000))
    def smallIterator: Iterator[Any] = smallList.iterator.asInstanceOf[Iterator[Any]]
    def bigIterator: Iterator[Any] = bigList.iterator.asInstanceOf[Iterator[Any]]
    assert(memoryStore.currentUnrollMemoryForThisTask === 0)

    store.putIterator("b1", smallIterator, memAndDisk)
    store.putIterator("b2", smallIterator, memAndDisk)

    // Unroll with not enough space. This should succeed but kick out b1 in the process.
    //将没有足够的空间
    // Memory store should contain b2 and b3, while disk store should contain only b1
    val result3 = memoryStore.putIterator("b3", smallIterator, memAndDisk, returnValues = true)
    assert(result3.size > 0)
    assert(!memoryStore.contains("b1"))
    assert(memoryStore.contains("b2"))
    assert(memoryStore.contains("b3"))
    assert(diskStore.contains("b1"))
    assert(!diskStore.contains("b2"))
    assert(!diskStore.contains("b3"))
    memoryStore.remove("b3")
    store.putIterator("b3", smallIterator, StorageLevel.MEMORY_ONLY)
    assert(memoryStore.currentUnrollMemoryForThisTask === 0)

    // Unroll huge block with not enough space. This should fail and drop the new block to disk
    //展开巨大的块没有足够的空间,这应该失败,并将新块放在磁盘上
    // directly in addition to kicking out b2 in the process. Memory store should contain only
    //除了直接踢出B2的过程中,
    // b3, while disk store should contain b1, b2 and b4.
    //内存存储应该只包含B,而磁盘存储应含有B1、B2和B4
    val result4 = memoryStore.putIterator("b4", bigIterator, memAndDisk, returnValues = true)
    assert(result4.size > 0)
    //展开从磁盘返回字节
    assert(result4.data.isRight) // unroll returned bytes from disk
    assert(!memoryStore.contains("b1"))
    assert(!memoryStore.contains("b2"))
    assert(memoryStore.contains("b3"))
    assert(!memoryStore.contains("b4"))
    assert(diskStore.contains("b1"))
    assert(diskStore.contains("b2"))
    assert(!diskStore.contains("b3"))
    assert(diskStore.contains("b4"))
    assert(memoryStore.currentUnrollMemoryForThisTask > 0) // we returned an iterator 我们返回了一个迭代器
  }

  test("multiple unrolls by the same thread") {//同一个线程多展开
    store = makeBlockManager(12000)
    val memOnly = StorageLevel.MEMORY_ONLY
    val memoryStore = store.memoryStore
    val smallList = List.fill(40)(new Array[Byte](100))
    def smallIterator: Iterator[Any] = smallList.iterator.asInstanceOf[Iterator[Any]]
    assert(memoryStore.currentUnrollMemoryForThisTask === 0)

    // All unroll memory used is released because unrollSafely returned an array
    //所有使用的内存被释放因为unrollsafely将返回一个数组
    memoryStore.putIterator("b1", smallIterator, memOnly, returnValues = true)
    assert(memoryStore.currentUnrollMemoryForThisTask === 0)
    memoryStore.putIterator("b2", smallIterator, memOnly, returnValues = true)
    assert(memoryStore.currentUnrollMemoryForThisTask === 0)

    // Unroll memory is not released because unrollSafely returned an iterator
    // that still depends on the underlying vector used in the process
    //打开内存不释放unrollsafely返回一个迭代器,因为仍然依赖于底层的载体在使用过程中
    memoryStore.putIterator("b3", smallIterator, memOnly, returnValues = true)
    val unrollMemoryAfterB3 = memoryStore.currentUnrollMemoryForThisTask
    assert(unrollMemoryAfterB3 > 0)

    // The unroll memory owned by this thread builds on top of its value after the previous unrolls
    //此线程拥有的展开内存在上一个展开之后建立在其值之上
    memoryStore.putIterator("b4", smallIterator, memOnly, returnValues = true)
    val unrollMemoryAfterB4 = memoryStore.currentUnrollMemoryForThisTask
    assert(unrollMemoryAfterB4 > unrollMemoryAfterB3)

    // ... but only to a certain extent (until we run out of free space to grant new unroll memory)
    //...但只有在一定程度上（直到我们用尽了空闲空间才能授予新的展开内存）
    memoryStore.putIterator("b5", smallIterator, memOnly, returnValues = true)
    val unrollMemoryAfterB5 = memoryStore.currentUnrollMemoryForThisTask
    memoryStore.putIterator("b6", smallIterator, memOnly, returnValues = true)
    val unrollMemoryAfterB6 = memoryStore.currentUnrollMemoryForThisTask
    memoryStore.putIterator("b7", smallIterator, memOnly, returnValues = true)
    val unrollMemoryAfterB7 = memoryStore.currentUnrollMemoryForThisTask
    assert(unrollMemoryAfterB5 === unrollMemoryAfterB4)
    assert(unrollMemoryAfterB6 === unrollMemoryAfterB4)
    assert(unrollMemoryAfterB7 === unrollMemoryAfterB4)
  }
  //延迟创造大字节缓冲区,避免内存溢出,如果它不能被放在内存中
  test("lazily create a big ByteBuffer to avoid OOM if it cannot be put into MemoryStore") {
    store = makeBlockManager(12000)
    val memoryStore = store.memoryStore
    val blockId = BlockId("rdd_3_10")
    val result = memoryStore.putBytes(blockId, 13000, () => {
      fail("A big ByteBuffer that cannot be put into MemoryStore should not be created")
    })
    assert(result.size === 13000)
    assert(result.data === null)
    assert(result.droppedBlocks === Nil)
  }

  test("put a small ByteBuffer to MemoryStore") {//把一个小ByteBuffer到内存中
    store = makeBlockManager(12000)
    val memoryStore = store.memoryStore
    val blockId = BlockId("rdd_3_10")
    var bytes: ByteBuffer = null
    val result = memoryStore.putBytes(blockId, 10000, () => {
      bytes = ByteBuffer.allocate(10000)
      bytes
    })
    assert(result.size === 10000)
    assert(result.data === Right(bytes))
    assert(result.droppedBlocks === Nil)
  }
}
