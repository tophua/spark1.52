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

package org.apache.spark.broadcast

import scala.util.Random

import org.scalatest.Assertions

import org.apache.spark._
import org.apache.spark.io.SnappyCompressionCodec
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.storage._

// Dummy(虚拟的) class that creates a broadcast variable but doesn't use it
//虚拟类创建一个广播变量,但不使用它
class DummyBroadcastClass(rdd: RDD[Int]) extends Serializable {
  @transient val list = List(1, 2, 3, 4)
  val broadcast = rdd.context.broadcast(list)
  val bid = broadcast.id
  println("bid:"+bid)
  def doSomething(): Set[(Int, Boolean)] = {
    rdd.map { x =>
      val bm = SparkEnv.get.blockManager
      // Check if broadcast block was fetched
      //检查广播块是否被取出
      val isFound = bm.getLocal(BroadcastBlockId(bid)).isDefined
      (x, isFound)
    }.collect().toSet
  }
}

class BroadcastSuite extends SparkFunSuite with LocalSparkContext {

  private val httpConf = broadcastConf("HttpBroadcastFactory")//HTTP广播工厂
  private val torrentConf = broadcastConf("TorrentBroadcastFactory")//

  test("Using HttpBroadcast locally") {//使用HTTP本地广播
    sc = new SparkContext("local", "test", httpConf)
    val list = List[Int](1, 2, 3, 4)
    val broadcast = sc.broadcast(list)
    val results = sc.parallelize(1 to 2).map(x => (x, broadcast.value.sum))
    assert(results.collect().toSet === Set((1, 10), (2, 10)))
  }
  //从多个线程访问HTTP广播变量
  test("Accessing HttpBroadcast variables from multiple threads") {
    sc = new SparkContext("local[10]", "test", httpConf)
    val list = List[Int](1, 2, 3, 4)
    val broadcast = sc.broadcast(list)
    val results = sc.parallelize(1 to 10).map(x => (x, broadcast.value.sum))
    assert(results.collect().toSet === (1 to 10).map(x => (x, 10)).toSet)
  }
  //在一个本地集群访问HTTP广播变量
  test("Accessing HttpBroadcast variables in a local cluster") {
    val numSlaves = 4
    val conf = httpConf.clone
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //是否在发送之前压缩广播变量
    conf.set("spark.broadcast.compress", "true")
    //sc = new SparkContext("local-cluster[%d, 1, 1024]".format(numSlaves), "test", conf)
    sc = new SparkContext("local[*]", "test", conf)
    val list = List[Int](1, 2, 3, 4)
    val broadcast = sc.broadcast(list)
    val results = sc.parallelize(1 to numSlaves).map(x => (x, broadcast.value.sum))
    assert(results.collect().toSet === (1 to numSlaves).map(x => (x, 10)).toSet)
  }
  //使用TorrentBroadcast本地广播
  test("Using TorrentBroadcast locally") {
    sc = new SparkContext("local", "test", torrentConf)
    val list = List[Int](1, 2, 3, 4)
    val broadcast = sc.broadcast(list)
    val results = sc.parallelize(1 to 2).map(x => (x, broadcast.value.sum))
    assert(results.collect().toSet === Set((1, 10), (2, 10)))
  }
  //多线程访问TorrentBroadcast变量
  test("Accessing TorrentBroadcast variables from multiple threads") {
    sc = new SparkContext("local[10]", "test", torrentConf)
    val list = List[Int](1, 2, 3, 4)
    val broadcast = sc.broadcast(list)
    val results = sc.parallelize(1 to 10).map(x => (x, broadcast.value.sum))
    assert(results.collect().toSet === (1 to 10).map(x => (x, 10)).toSet)
  }
  //本地群集访问Torrent广播变量
  test("Accessing TorrentBroadcast variables in a local cluster") {
    val numSlaves = 4
    val conf = torrentConf.clone
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //是否在发送之前压缩广播变量
    conf.set("spark.broadcast.compress", "true")
    //sc = new SparkContext("local-cluster[%d, 1, 1024]".format(numSlaves), "test", conf)
    sc = new SparkContext("local[*]", "test", conf)
    val list = List[Int](1, 2, 3, 4)
    val broadcast = sc.broadcast(list)
    val results = sc.parallelize(1 to numSlaves).map(x => (x, broadcast.value.sum))
    assert(results.collect().toSet === (1 to numSlaves).map(x => (x, 10)).toSet)
  }
  //
  test("TorrentBroadcast's blockifyObject and unblockifyObject are inverses") {//相互操作
    import org.apache.spark.broadcast.TorrentBroadcast._
    val blockSize = 1024
    val conf = new SparkConf()
    val compressionCodec = Some(new SnappyCompressionCodec(conf))
    val serializer = new JavaSerializer(conf)//序列化
    val seed = 42
    val rand = new Random(seed)//随机数
    for (trial <- 1 to 100) {
      val size = 1 + rand.nextInt(1024 * 10)
      val data: Array[Byte] = new Array[Byte](size)
      rand.nextBytes(data)
      //参数为data数组,块大小,序列化对象,压缩方式。
      val blocks = blockifyObject(data, blockSize, serializer, compressionCodec)
      //反相操作blockifyObject对象,获得数据数组
      val unblockified = unBlockifyObject[Array[Byte]](blocks, serializer, compressionCodec)
      assert(unblockified === data)
    }
  }
//测试Torrent延迟广播变量
  ignore("Test Lazy Broadcast variables with TorrentBroadcast") {
    val numSlaves = 2
    val conf = torrentConf.clone
    //sc = new SparkContext("local-cluster[%d, 1, 1024]".format(numSlaves), "test", conf)
    sc = new SparkContext("local[*]", "test", conf)
    val rdd = sc.parallelize(1 to numSlaves)

    val results = new DummyBroadcastClass(rdd).doSomething()

    assert(results.toSet === (1 to numSlaves).map(x => (x, false)).toSet)
  }
  //在执行未持久化HTTP广播变量在本地模式
  test("Unpersisting HttpBroadcast on executors only in local mode") {
    testUnpersistHttpBroadcast(distributed = false, removeFromDriver = false)
  }
  //在本地模式下,对执行程序和驱动程序进行HANDP广播
  test("Unpersisting HttpBroadcast on executors and driver in local mode") {
    testUnpersistHttpBroadcast(distributed = false, removeFromDriver = true)
  }
  //在分布式模式下,仅对执行程序执行HttpBroadcast
  test("Unpersisting HttpBroadcast on executors only in distributed mode") {
   // testUnpersistHttpBroadcast(distributed = true, removeFromDriver = false)
  }
  //在分布式模式下对执行程序和驱动程序进行HipppBroadcast的分散化
  test("Unpersisting HttpBroadcast on executors and driver in distributed mode") {
   // testUnpersistHttpBroadcast(distributed = true, removeFromDriver = true)
  }
  //在本地模式下,仅对执行者进行TorrentBroadcast的Unpersisting
  test("Unpersisting TorrentBroadcast on executors only in local mode") {
   // testUnpersistTorrentBroadcast(distributed = false, removeFromDriver = false)
  }
  //在本地模式下对执行程序和驱动程序进行TorrentBroadcast的Unpersisting
  test("Unpersisting TorrentBroadcast on executors and driver in local mode") {
   //testUnpersistTorrentBroadcast(distributed = false, removeFromDriver = true)
  }
  //TorrentBroadcast仅在分布式模式下执行
  test("Unpersisting TorrentBroadcast on executors only in distributed mode") {
   //testUnpersistTorrentBroadcast(distributed = true, removeFromDriver = false)
  }
  //在分布式模式下对执行程序和驱动程序进行漫游
  test("Unpersisting TorrentBroadcast on executors and driver in distributed mode") {
   // testUnpersistTorrentBroadcast(distributed = true, removeFromDriver = true)
  }
  //使用广播后直接销毁打印
  test("Using broadcast after destroy prints callsite") {
    //sc = new SparkContext("local", "test")
    //testPackage.runCallSiteTest(sc)
  }
  //SparkContext停止后,不能创建广播变量
 test("Broadcast variables cannot be created after SparkContext is stopped (SPARK-5065)") {
    sc = new SparkContext("local", "test")
    sc.stop()
    val thrown = intercept[IllegalStateException] {
      sc.broadcast(Seq(1, 2, 3))
    }
    assert(thrown.getMessage.toLowerCase.contains("stopped"))
  }

  /**
   * Verify the persistence of state associated with an HttpBroadcast in either local mode or
   * local-cluster mode (when distributed = true).
   * 验证本地或者本地集群模式Http广播变量持久化状态
   * This test creates a broadcast variable, uses it on all executors, and then unpersists it.
   * 此测试创建一个广播变量,使用它在所有的执行者,不持久化它,
   * In between each step, this test verifies that the broadcast blocks and the broadcast file
   * 在每一步之间,此测试验证广播块和广播文件只存在于预期的节点上
   * are present only on the expected nodes.
   */
  private def testUnpersistHttpBroadcast(distributed: Boolean, removeFromDriver: Boolean) {
    val numSlaves = if (distributed) 2 else 0//distributed 是否分布式

    // Verify that the broadcast file is created, and blocks are persisted only on the driver
    //验证创建广播文件,块仅持久化驱动程序上
    def afterCreation(broadcastId: Long, bmm: BlockManagerMaster) {
      val blockId = BroadcastBlockId(broadcastId)
      //根据blockId向Master返回该Block的状态
      val statuses = bmm.getBlockStatus(blockId, askSlaves = true)
      assert(statuses.size === 1)
      //case(BlockManagerId,BlockStatus)
      statuses.head match { case (bm, status) =>
        assert(bm.isDriver, "Block should only be on the driver")//块应该只在驱动程序
        assert(status.storageLevel === StorageLevel.MEMORY_AND_DISK)//存储级别
        assert(status.memSize > 0, "Block should be in memory store on the driver")//块应该在驱动器上的内存存储
        assert(status.diskSize === 0, "Block should not be in disk store on the driver")//块不应该在驱动器上的磁盘存储中
      }
      if (distributed) {
        // this file is only generated in distributed mode
        //此文件只在分布式模式下生成
        assert(HttpBroadcast.getFile(blockId.broadcastId).exists, "Broadcast file not found!")//未找到广播文件
      }
    }

    // Verify that blocks are persisted in both the executors and the driver
    //验证执行者和驱动器持久化的块
    def afterUsingBroadcast(broadcastId: Long, bmm: BlockManagerMaster) {
      val blockId = BroadcastBlockId(broadcastId)
      //根据blockId向Master返回该Block的状态
      val statuses = bmm.getBlockStatus(blockId, askSlaves = true)
      assert(statuses.size === numSlaves + 1)//
      statuses.foreach { case (_, status) =>
        assert(status.storageLevel === StorageLevel.MEMORY_AND_DISK)//存储级别
        assert(status.memSize > 0, "Block should be in memory store")//块应该在内存中存储
        assert(status.diskSize === 0, "Block should not be in disk store")//块不应该在磁盘存储
      }
    }

    // Verify that blocks are unpersisted on all executors, and on all nodes if removeFromDriver
    //验证块在执行者未持久化块,并在所有节点上,如果删除从驱动程序是真的
    // is true. In the latter case, also verify that the broadcast file is deleted on the driver.
    //在后一种情况下,还验证了在驱动程序上删除广播文件
    def afterUnpersist(broadcastId: Long, bmm: BlockManagerMaster) {
      val blockId = BroadcastBlockId(broadcastId)
      //根据blockId向Master返回该Block的状态
      val statuses = bmm.getBlockStatus(blockId, askSlaves = true)
      val expectedNumBlocks = if (removeFromDriver) 0 else 1//期望的块数
      val possiblyNot = if (removeFromDriver) "" else " not"
      assert(statuses.size === expectedNumBlocks,
        "Block should%s be unpersisted on the driver".format(possiblyNot))
      if (distributed && removeFromDriver) {
        // this file is only generated in distributed mode
        //此文件只在分布式模式下生成
        assert(!HttpBroadcast.getFile(blockId.broadcastId).exists,
          "Broadcast file should%s be deleted".format(possiblyNot))
      }
    }

    testUnpersistBroadcast(distributed, numSlaves, httpConf, afterCreation,
      afterUsingBroadcast, afterUnpersist, removeFromDriver)
  }

  /**
   * Verify the persistence of state associated with an TorrentBroadcast in a local-cluster.
   * 验证在本地群集中的Torrent广播变量关联的持久性状态,
   * This test creates a broadcast variable, uses it on all executors, and then unpersists it.
   * 此测试创建一个广播变量,使用它在所有的执行者,然后未持久化它
   * In between each step, this test verifies that the broadcast blocks are present only on the
   * 在每一步之间,此测试验证广播块仅存在于预期的节点上
   * expected nodes.
   */
  private def testUnpersistTorrentBroadcast(distributed: Boolean, removeFromDriver: Boolean) {
    val numSlaves = if (distributed) 2 else 0

    // Verify that blocks are persisted only on the driver
    //验证仅在驱动程序上持久化的块
    def afterCreation(broadcastId: Long, bmm: BlockManagerMaster) {
      var blockId = BroadcastBlockId(broadcastId)
      var statuses = bmm.getBlockStatus(blockId, askSlaves = true)
      assert(statuses.size === 1)

      blockId = BroadcastBlockId(broadcastId, "piece0")
      statuses = bmm.getBlockStatus(blockId, askSlaves = true)
      assert(statuses.size === 1)
    }

    // Verify that blocks are persisted in both the executors and the driver
    //验证执行者和驱动器的块的持久化
    def afterUsingBroadcast(broadcastId: Long, bmm: BlockManagerMaster) {
      var blockId = BroadcastBlockId(broadcastId)
      val statuses = bmm.getBlockStatus(blockId, askSlaves = true)
      assert(statuses.size === numSlaves + 1)

      blockId = BroadcastBlockId(broadcastId, "piece0")
      assert(statuses.size === numSlaves + 1)
    }

    // Verify that blocks are unpersisted on all executors, and on all nodes if removeFromDriver
    //验证块在执行者未持久化块,并在所有节点上,如果删除从驱动程序是真的
    // is true.
    def afterUnpersist(broadcastId: Long, bmm: BlockManagerMaster) {
      var blockId = BroadcastBlockId(broadcastId)
      var expectedNumBlocks = if (removeFromDriver) 0 else 1
      var statuses = bmm.getBlockStatus(blockId, askSlaves = true)
      assert(statuses.size === expectedNumBlocks)

      blockId = BroadcastBlockId(broadcastId, "piece0")
      expectedNumBlocks = if (removeFromDriver) 0 else 1
      statuses = bmm.getBlockStatus(blockId, askSlaves = true)
      assert(statuses.size === expectedNumBlocks)
    }

    testUnpersistBroadcast(distributed, numSlaves, torrentConf, afterCreation,
      afterUsingBroadcast, afterUnpersist, removeFromDriver)
  }

  /**
   * This test runs in 4 steps:
   * 本测试运行在4个步骤：
   * 1) Create broadcast variable, and verify that all state is persisted on the driver.
   * 		创建广播变量,验证驱动器上持久化所有状态
   * 2) Use the broadcast variable on all executors, and verify that all state is persisted
   * 	   所有执行者使用广播变量,确认所有持久化状态在驱动器和执行器
   *    on both the driver and the executors.
   * 3) Unpersist the broadcast, and verify that all state is removed where they should be.
   * 	  未持久化广播变量,他们应该并确认所有的状态都被删除
   * 4) [Optional] If removeFromDriver is false, we verify that the broadcast is re-usable.
   * 		如果removeFromDriver为flase,我们验证广播是可重复使用
   */
  private def testUnpersistBroadcast(
      distributed: Boolean,
      //只用于当分布式
      numSlaves: Int,  // used only when distributed = true
      broadcastConf: SparkConf,
      afterCreation: (Long, BlockManagerMaster) => Unit,
      afterUsingBroadcast: (Long, BlockManagerMaster) => Unit,
      afterUnpersist: (Long, BlockManagerMaster) => Unit,
      removeFromDriver: Boolean) {

    sc = if (distributed) {//是否分布式
      val _sc =
        new SparkContext("local-cluster[%d, 1, 1024]".format(numSlaves), "test", broadcastConf)
      // Wait until all salves are up
      //等到所有的从节点
      _sc.jobProgressListener.waitUntilExecutorsUp(numSlaves, 10000)
      _sc
    } else {
      new SparkContext("local", "test", broadcastConf)
    }
    val blockManagerMaster = sc.env.blockManager.master
    val list = List[Int](1, 2, 3, 4)

    // Create broadcast variable
    //创建广播变量
    
    val broadcast = sc.broadcast(list)
    afterCreation(broadcast.id, blockManagerMaster)

    // Use broadcast variable on all executors
    //所有执行者使用广播变量
    val partitions = 10
    assert(partitions > numSlaves)
    val results = sc.parallelize(1 to partitions, partitions).map(x => (x, broadcast.value.sum))
    assert(results.collect().toSet === (1 to partitions).map(x => (x, list.sum)).toSet)
    afterUsingBroadcast(broadcast.id, blockManagerMaster)

    // Unpersist broadcast
    //删除广播变量
    if (removeFromDriver) {
      broadcast.destroy(blocking = true)
    } else {
      broadcast.unpersist(blocking = true)
    }
    afterUnpersist(broadcast.id, blockManagerMaster)

    // If the broadcast is removed from driver, all subsequent uses of the broadcast variable
    //如果广播变量被从驱动程序中删除,广播变量的所有后续使用将抛出异常,结果应该和以前一样
    // should throw SparkExceptions. Otherwise, the result should be the same as before.
    if (removeFromDriver) {
      // Using this variable on the executors crashes them, which hangs the test.
      //使用执行者对广播变量销毁,悬挂试验
      // Instead, crash the driver by directly accessing the broadcast value.
      //广播变量值不能访问
      intercept[SparkException] { broadcast.value }
      intercept[SparkException] { broadcast.unpersist() }
      intercept[SparkException] { broadcast.destroy(blocking = true) }
    } else {
      val results = sc.parallelize(1 to partitions, partitions).map(x => (x, broadcast.value.sum))
      assert(results.collect().toSet === (1 to partitions).map(x => (x, list.sum)).toSet)
    }
  }

  /** 
   *  Helper method to create a SparkConf that uses the given broadcast factory. 
   *  辅助方法给定广播工厂来创建一个SparkConf使用
   *  
   *  */
  private def broadcastConf(factoryName: String): SparkConf = {
    val conf = new SparkConf
    //广播的实现类
    conf.set("spark.broadcast.factory", "org.apache.spark.broadcast.%s".format(factoryName))
    conf
  }
}

package object testPackage extends Assertions {

  def runCallSiteTest(sc: SparkContext) {
    val broadcast = sc.broadcast(Array(1, 2, 3, 4))
    broadcast.destroy()
    val thrown = intercept[SparkException] { broadcast.value }
    assert(thrown.getMessage.contains("BroadcastSuite.scala"))
  }

}
