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

import org.scalatest.concurrent.Timeouts._
import org.scalatest.Matchers
import org.scalatest.time.{Millis, Span}

import org.apache.spark.storage.{RDDBlockId, StorageLevel}

class NotSerializableClass
class NotSerializableExn(val notSer: NotSerializableClass) extends Throwable() {}

//分布式
class DistributedSuite extends SparkFunSuite with Matchers with LocalSparkContext {
  //1 CPU核数,1024内存数
  //val clusterUrl = "local-cluster[2,1,1024]"
  /**
  val clusterUrl = "local"
  test("task throws not serializable exception") {//task任务抛出不能序列化异常
    // Ensures that executors do not crash when an exn is not serializable. If executors crash,
    // this test will hang. Correct behavior is that executors don't crash but fail tasks
    // and the scheduler throws a SparkException.
    //确保执行者(executors)不死机时,生成不可序列化,如果执行者(executors)死机,这个测试将挂起,
    //正确的方法是执行者不崩溃,但失败的任务,并任务调度器抛出Spark异常
    // numSlaves must be less than numPartitions
    //从节点数必须小于分区数
    val numSlaves = 3 //从节点
    val numPartitions = 10//分区数
   val conf = new SparkConf
    //sc = new SparkContext("local-cluster[%s,1,1024]".format(numSlaves), "test")
    sc = new SparkContext("local[*]".format(numSlaves), "test")
    val data = sc.parallelize(1 to 100, numPartitions).
      map(x => throw new NotSerializableExn(new NotSerializableClass))
    intercept[SparkException] {//截获异常      
      data.count()
    }
     println("task throws not :"+data.count())
    resetSparkContext()
  }

  test("local-cluster format") {//本地集群格式
    //sc = new SparkContext("local-cluster[2,1,1024]", "test")
    sc = new SparkContext("local[*]", "test")
    assert(sc.parallelize(1 to 2, 2).count() == 2)
   /* resetSparkContext()
    sc = new SparkContext("local-cluster[2 , 1 , 1024]", "test")
    assert(sc.parallelize(1 to 2, 2).count() == 2)
    resetSparkContext()
    sc = new SparkContext("local-cluster[2, 1, 1024]", "test")
    assert(sc.parallelize(1 to 2, 2).count() == 2)
    resetSparkContext()
    sc = new SparkContext("local-cluster[ 2, 1, 1024 ]", "test")
    assert(sc.parallelize(1 to 2, 2).count() == 2)
    resetSparkContext()*/
  }

  test("simple groupByKey") {//简单的以Key分组
    sc = new SparkContext(clusterUrl, "test")
    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (2, 1)), 5)
    //元组分组,参数分区数
    val groups = pairs.groupByKey(5).collect()
    assert(groups.size === 2)
    //元组
    val valuesFor1 = groups.find(_._1 == 1).get._2 //取出数组分组为1的第二个元素列表
    assert(valuesFor1.toList.sorted === List(1, 2, 3))
    val valuesFor2 = groups.find(_._1 == 2).get._2//取出数组分组为2的第二个元素列表
    assert(valuesFor2.toList.sorted === List(1))
  }

  test("groupByKey where map output sizes exceed maxMbInFlight") {
     //在Shuffle的时候,每个Reducer任务获取缓存数据指定大小(以兆字节为单位) 
    val conf = new SparkConf().set("spark.reducer.maxSizeInFlight", "1m")
    sc = new SparkContext(clusterUrl, "test", conf)
    // This data should be around 20 MB, so even with 4 mappers and 2 reducers, each map output
    // file should be about 2.5 MB
    val pairs = sc.parallelize(1 to 2000, 4).map(x => (x % 16, new Array[Byte](10000)))
    val groups = pairs.groupByKey(2).map(x => (x._1, x._2.size)).collect()
    assert(groups.length === 16)
    assert(groups.map(_._2).sum === 2000)
  }

  test("accumulators") {
    //共享变量,累加器变量
    sc = new SparkContext(clusterUrl, "test")
    val accum = sc.accumulator(0)
    sc.parallelize(1 to 10, 10).foreach(x => accum += x)
    assert(accum.value === 55)
  }

  test("broadcast variables") {
    //共享变量 ,广播共享变量在每个节点上保持一份read-only的变量
    sc = new SparkContext(clusterUrl, "test")
    val array = new Array[Int](100)
    val bv = sc.broadcast(array)
    array(2) = 3     // Change the array -- this should not be seen on workers
    val rdd = sc.parallelize(1 to 10, 10)
    val sum = rdd.map(x => bv.value.sum).reduce(_ + _)
    assert(sum === 0)
  }

  test("repeatedly failing task") {//多次失败的任务
    sc = new SparkContext(clusterUrl, "test")
    val accum = sc.accumulator(0)
    val thrown = intercept[SparkException] {
      // scalastyle:off println
      sc.parallelize(1 to 10, 10).foreach(x => println(x / 0))
      // scalastyle:on println
    }
    assert(thrown.getClass === classOf[SparkException])
    assert(thrown.getMessage.contains("failed 4 times"))
  }

  test("repeatedly failing task that crashes JVM") {//多次失败的任务
    // Ensures that if a task fails in a way that crashes the JVM, the job eventually fails rather
    // than hanging due to retrying the failed task infinitely many times (eventually the
    // standalone scheduler will remove the application, causing the job to hang waiting to
    // reconnect to the master).
    sc = new SparkContext(clusterUrl, "test")
    failAfter(Span(100000, Millis)) {
      val thrown = intercept[SparkException] {
        // One of the tasks always fails.
        sc.parallelize(1 to 10, 2).foreach { x => if (x == 1) System.exit(42) }
      }
      assert(thrown.getClass === classOf[SparkException])
      assert(thrown.getMessage.contains("failed 4 times"))
    }
  }

  test("caching") {//缓存
    sc = new SparkContext(clusterUrl, "test")
    val data = sc.parallelize(1 to 1000, 10).cache()
    assert(data.count() === 1000)
    assert(data.count() === 1000)
    assert(data.count() === 1000)
  }

  test("caching on disk") {//缓存到磁盘
    sc = new SparkContext(clusterUrl, "test")
    val data = sc.parallelize(1 to 1000, 10).persist(StorageLevel.DISK_ONLY)
    assert(data.count() === 1000)
    assert(data.count() === 1000)
    assert(data.count() === 1000)
  }
/**
  test("caching in memory, replicated") {//在内存中缓存,复制
    sc = new SparkContext(clusterUrl, "test")
    val data = sc.parallelize(1 to 1000, 10).persist(StorageLevel.MEMORY_ONLY_2)
    assert(data.count() === 1000)
    assert(data.count() === 1000)
    assert(data.count() === 1000)
  }**/

  test("caching in memory, serialized, replicated") {//缓存在内存中,序列化,复制
  /*  sc = new SparkContext(clusterUrl, "test")
    val data = sc.parallelize(1 to 1000, 10).persist(StorageLevel.MEMORY_ONLY_SER_2)
    assert(data.count() === 1000)
    assert(data.count() === 1000)
    assert(data.count() === 1000)*/
  }

  test("caching on disk, replicated") {//复制磁盘上的缓存
    sc = new SparkContext(clusterUrl, "test")
    val data = sc.parallelize(1 to 1000, 10).persist(StorageLevel.DISK_ONLY_2)
    assert(data.count() === 1000)
    assert(data.count() === 1000)
    assert(data.count() === 1000)
  }

  test("caching in memory and disk, replicated") {//在内存和磁盘中缓存,复制
    sc = new SparkContext(clusterUrl, "test")
    val data = sc.parallelize(1 to 1000, 10).persist(StorageLevel.MEMORY_AND_DISK_2)
    assert(data.count() === 1000)
    assert(data.count() === 1000)
    assert(data.count() === 1000)
  }

  test("caching in memory and disk, serialized, replicated") {//在内存和磁盘,缓存序列化,复制
    sc = new SparkContext(clusterUrl, "test")
    val data = sc.parallelize(1 to 1000, 10).persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    assert(data.count() === 1000)
    assert(data.count() === 1000)
    assert(data.count() === 1000)

    // Get all the locations of the first partition and try to fetch the partitions
    //获取第一个分区的所有位置,并试图从这些位置取分区
    // from those locations.
    val blockIds = data.partitions.indices.map(index => RDDBlockId(data.id, index)).toArray
    val blockId = blockIds(0)
    val blockManager = SparkEnv.get.blockManager
    val blockTransfer = SparkEnv.get.blockTransferService
    blockManager.master.getLocations(blockId).foreach { cmId =>
      val bytes = blockTransfer.fetchBlockSync(cmId.host, cmId.port, cmId.executorId,
        blockId.toString)
      val deserialized = blockManager.dataDeserialize(blockId, bytes.nioByteBuffer())
        .asInstanceOf[Iterator[Int]].toList
      assert(deserialized === (1 to 100).toList)
    }
  }
   //没有内存时没有分区的计算而不缓存
  test("compute without caching when no partitions fit in memory") {
    sc = new SparkContext(clusterUrl, "test")
    // data will be 4 million * 4 bytes = 16 MB in size, but our memoryFraction set the cache
    // to only 50 KB (0.0001 of 512 MB), so no partitions should fit in memory
    val data = sc.parallelize(1 to 4000000, 2).persist(StorageLevel.MEMORY_ONLY_SER)
    assert(data.count() === 4000000)
    assert(data.count() === 4000000)
    assert(data.count() === 4000000)
  }

  test("compute when only some partitions fit in memory") {
    //Spark用于缓存的内存大小所占用的Java堆的比率
    val conf = new SparkConf().set("spark.storage.memoryFraction", "0.01")
    sc = new SparkContext(clusterUrl, "test", conf)
    // data will be 4 million * 4 bytes = 16 MB in size, but our memoryFraction set the cache
    // to only 5 MB (0.01 of 512 MB), so not all of it will fit in memory; we use 20 partitions
    // to make sure that *some* of them do fit though
    val data = sc.parallelize(1 to 4000000, 20).persist(StorageLevel.MEMORY_ONLY_SER)
    assert(data.count() === 4000000)
    assert(data.count() === 4000000)
    assert(data.count() === 4000000)
  }
  //将环境变量传递给集群
  test("passing environment variables to cluster") {
    sc = new SparkContext(clusterUrl, "test", null, Nil, Map("TEST_VAR" -> "TEST_VALUE"))
    val values = sc.parallelize(1 to 2, 2).map(x => System.getenv("TEST_VAR")).collect()
    assert(values.toSeq === Seq("TEST_VALUE", "TEST_VALUE"))
  }

  test("recover from node failures") {//从节点故障恢复
    import DistributedSuite.{markNodeIfIdentity, failOnMarkedIdentity}
    DistributedSuite.amMaster = true
    sc = new SparkContext(clusterUrl, "test")
    val data = sc.parallelize(Seq(true, true), 2)
    assert(data.count === 2) // force executors to start
    assert(data.map(markNodeIfIdentity).collect.size === 2)
    assert(data.map(failOnMarkedIdentity).collect.size === 2)
  }
  //在shuffle-map中从重复节点故障恢复
  test("recover from repeated node failures during shuffle-map") {
    import DistributedSuite.{markNodeIfIdentity, failOnMarkedIdentity}
    DistributedSuite.amMaster = true
    sc = new SparkContext(clusterUrl, "test")
    for (i <- 1 to 3) {
      val data = sc.parallelize(Seq(true, false), 2)
      assert(data.count === 2)
      assert(data.map(markNodeIfIdentity).collect.size === 2)
      assert(data.map(failOnMarkedIdentity).map(x => x -> x).groupByKey.count === 2)
    }
  }
  //在shuffle-reduce中从重复节点故障恢复
  test("recover from repeated node failures during shuffle-reduce") {
    import DistributedSuite.{markNodeIfIdentity, failOnMarkedIdentity}
    DistributedSuite.amMaster = true
    sc = new SparkContext(clusterUrl, "test")
    for (i <- 1 to 3) {
      val data = sc.parallelize(Seq(true, true), 2)
      assert(data.count === 2)
      assert(data.map(markNodeIfIdentity).collect.size === 2)
      // This relies on mergeCombiners being used to perform the actual reduce for this
      // test to actually be testing what it claims.
      val grouped = data.map(x => x -> x).combineByKey(
                      x => x,
                      (x: Boolean, y: Boolean) => x,
                      (x: Boolean, y: Boolean) => failOnMarkedIdentity(x)
                    )
      assert(grouped.collect.size === 1)
    }
  }
//从复制节点故障恢复
  test("recover from node failures with replication") {
    import DistributedSuite.{markNodeIfIdentity, failOnMarkedIdentity}
    DistributedSuite.amMaster = true
    // Using more than two nodes so we don't have a symmetric communication pattern and might
    // cache a partially correct list of peers.
    //使用两个以上的节点,所以我们没有一个对称的通信模式,并可能缓存部分正确的对等节点列表
   // sc = new SparkContext("local-cluster[3,1,1024]", "test")

    sc = new SparkContext("local[*]", "test")
    for (i <- 1 to 3) {
      val data = sc.parallelize(Seq(true, false, false, false), 4)
      data.persist(StorageLevel.MEMORY_ONLY_2)

      assert(data.count === 4)
      assert(data.map(markNodeIfIdentity).collect.size === 4)
      assert(data.map(failOnMarkedIdentity).collect.size === 4)

      // Create a new replicated RDD to make sure that cached peer information doesn't cause
      // problems.
      //创建一个新的复制RDD确保缓存节点信息不会造成问题
      val data2 = sc.parallelize(Seq(true, true), 2).persist(StorageLevel.MEMORY_ONLY_2)
      assert(data2.count === 2)
    }
  }

  test("unpersist RDDs") {//未持久化RDD
    DistributedSuite.amMaster = true
    //sc = new SparkContext("local-cluster[3,1,1024]", "test")
    sc = new SparkContext("local[*]", "test")
    val data = sc.parallelize(Seq(true, false, false, false), 4)
    data.persist(StorageLevel.MEMORY_ONLY_2)
    data.count
    assert(sc.persistentRdds.isEmpty === false)
    data.unpersist()
    assert(sc.persistentRdds.isEmpty === true)

    failAfter(Span(3000, Millis)) {
      try {
        while (! sc.getRDDStorageInfo.isEmpty) {
          Thread.sleep(200)
        }
      } catch {
        case _: Throwable => { Thread.sleep(10) }
          // Do nothing. We might see exceptions because block manager
          // is racing this thread to remove entries from the driver.
      }
    }
  }

}

object DistributedSuite {
  // Indicates whether this JVM is marked for failure.
  // 标示此JVM标记为失败
  var mark = false

  // Set by test to remember if we are in the driver program so we can assert
  // that we are not.

  var amMaster = false

  // Act like an identity function, but if the argument is true, set mark to true.
  def markNodeIfIdentity(item: Boolean): Boolean = {
    if (item) {
      assert(!amMaster)
      mark = true
    }
    item
  }

  // Act like an identity function, but if mark was set to true previously, fail,
  // crashing the entire JVM.
  //但如果标记被设置为真,则失败,整个JVM崩溃
  def failOnMarkedIdentity(item: Boolean): Boolean = {
    if (mark) {
      System.exit(42)
    }
    item
  }**/
}
