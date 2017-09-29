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

import java.lang.ref.WeakReference

import scala.collection.mutable.{HashSet, SynchronizedSet}
import scala.language.existentials
import scala.util.Random

import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.PatienceConfiguration
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._

import org.apache.spark.rdd.{ReliableRDDCheckpointData, RDD}
import org.apache.spark.storage._
import org.apache.spark.shuffle.hash.HashShuffleManager
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.storage.BroadcastBlockId
import org.apache.spark.storage.RDDBlockId
import org.apache.spark.storage.ShuffleBlockId
import org.apache.spark.storage.ShuffleIndexBlockId

/**
 * An abstract base class for context cleaner tests, which sets up a context with a config
 * 上下文清理测试的抽象基类,设置一个适合于清理测试的配置,并提供了一些实用功能
 * suitable for cleaner tests and provides some utility functions. Subclasses can use different
 * 子类可以使用不同的配置选项,一个不同的Shuffle管理器类
 * config options, in particular(详细的), a different shuffle manager class
 */
abstract class ContextCleanerSuiteBase(val shuffleManager: Class[_] = classOf[HashShuffleManager])
  extends SparkFunSuite with BeforeAndAfter with LocalSparkContext
{
  implicit val defaultTimeout = timeout(10000 millis)
  val conf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("ContextCleanerSuite")
    .set("spark.cleaner.referenceTracking.blocking", "true")
    .set("spark.cleaner.referenceTracking.blocking.shuffle", "true")
    .set("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
    .set("spark.shuffle.manager", shuffleManager.getName)

  before {
    sc = new SparkContext(conf)
  }

  after {
    if (sc != null) {
      sc.stop()
      sc = null
    }
  }

  // ------ Helper functions ------

  protected def newRDD() = sc.makeRDD(1 to 10)
  protected def newPairRDD() = newRDD().map(_ -> 1)
  protected def newShuffleRDD() = newPairRDD().reduceByKey(_ + _)
  protected def newBroadcast() = sc.broadcast(1 to 100)

  protected def newRDDWithShuffleDependencies(): (RDD[_], Seq[ShuffleDependency[_, _, _]]) = {
    def getAllDependencies(rdd: RDD[_]): Seq[Dependency[_]] = {
      println("dependencies:"+rdd.dependencies)
      rdd.dependencies ++ rdd.dependencies.flatMap { dep =>
        getAllDependencies(dep.rdd)
      }
    }
    val rdd = newShuffleRDD()

    // Get all the shuffle dependencies 获取所有的shuffle依赖
    val shuffleDeps = getAllDependencies(rdd)
      .filter(_.isInstanceOf[ShuffleDependency[_, _, _]])
      .map(_.asInstanceOf[ShuffleDependency[_, _, _]])
    (rdd, shuffleDeps)
  }

  protected def randomRdd() = {
    val rdd: RDD[_] = Random.nextInt(3) match {//包含3的随机数
      case 0 => newRDD()
      case 1 => newShuffleRDD()
      case 2 => newPairRDD.join(newPairRDD())
    }
    //nextBoolean方法调用返回下一个伪均匀分布的boolean值
    if (Random.nextBoolean()) rdd.persist()
    rdd.count()
    rdd
  }

  /** 
   *  Run GC(垃圾回收) and make sure it actually has run
   *  垃圾回收确保它实际上已经运行
   *   */
  protected def runGC() {
    //WeakReference 弱引用,在内存不足时,垃圾回收器会回收此对象,所以在每次使用此对象时,要检查其是否被回收
    val weakRef = new WeakReference(new Object())
    val startTime = System.currentTimeMillis//(毫秒时间)
    System.gc() // 运行垃圾收集,通常*运行GC  Make a best effort to run the garbage collection. It *usually* runs GC.
    // Wait until a weak reference object has been GCed
    //等到一个弱引用对象已垃圾回收,10000毫秒==10秒
    while (System.currentTimeMillis - startTime < 10000 && weakRef.get != null) {
      System.gc()
      Thread.sleep(200)//毫秒
    }
  }

  protected def cleaner = sc.cleaner.get
}


/**
 * Basic ContextCleanerSuite, which uses sort-based shuffle
 * 基础上下文件清理套件,它使用基于排序的Shuffle
 */
class ContextCleanerSuite extends ContextCleanerSuiteBase {
  test("cleanup RDD") {
    val rdd = newRDD().persist()
    val collected = rdd.collect().toList
    val tester = new CleanerTester(sc, rddIds = Seq(rdd.id))

    // Explicit cleanup 显示清理
    //参数blocking是否堵塞
    cleaner.doCleanupRDD(rdd.id, blocking = true)
    tester.assertCleanup()

    // Verify that RDDs can be re-executed after cleaning up
    // 验证RDDS可以重新清理后执行
    assert(rdd.collect().toList === collected)
  }

  test("cleanup shuffle") {//清理shuffle
    val (rdd, shuffleDeps) = newRDDWithShuffleDependencies()
    val collected = rdd.collect().toList
    val tester = new CleanerTester(sc, shuffleIds = shuffleDeps.map(_.shuffleId))

    // Explicit cleanup 显式的清除
    shuffleDeps.foreach(s => cleaner.doCleanupShuffle(s.shuffleId, blocking = true))
    tester.assertCleanup()

    // Verify that shuffles can be re-executed after cleaning up
    //验证重新清理后执行
    assert(rdd.collect().toList.equals(collected))
  }

  test("cleanup broadcast") {//清理广播
    val broadcast = newBroadcast()
    val tester = new CleanerTester(sc, broadcastIds = Seq(broadcast.id))

    // Explicit cleanup  显式清除
    cleaner.doCleanupBroadcast(broadcast.id, blocking = true)
    tester.assertCleanup()
  }

  test("automatically cleanup RDD") {//自动清理RDD
    var rdd = newRDD().persist()
    rdd.count()

    // Test that GC does not cause RDD cleanup due to a strong reference
    //试验GC 清理RDD不会引起强引用
    val preGCTester = new CleanerTester(sc, rddIds = Seq(rdd.id))
    runGC()
    intercept[Exception] {
      preGCTester.assertCleanup()(timeout(1000 millis))
    }

    // Test that GC causes RDD cleanup after dereferencing the RDD
    //测试 GC引起RDD清理后废弃的RDD
    // Note rdd is used after previous GC to avoid early collection by the JVM
    val postGCTester = new CleanerTester(sc, rddIds = Seq(rdd.id))
    rdd = null // Make RDD out of scope
    runGC()
    postGCTester.assertCleanup()
  }

  test("automatically cleanup shuffle") {//自动清理Shuffle
    var rdd = newShuffleRDD()
    rdd.count()

    // Test that GC does not cause shuffle cleanup due to a strong reference
    //测试垃圾回收,不会因为强引用而导致Shuffle清理
    val preGCTester = new CleanerTester(sc, shuffleIds = Seq(0))
    runGC()
    intercept[Exception] {
      preGCTester.assertCleanup()(timeout(1000 millis))
    }
    rdd.count()  // Defeat early collection by the JVM 由JVM早期采集失败

    // Test that GC causes shuffle cleanup after dereferencing the RDD
    //测试垃圾回收,原因Shuffle清理后引用的RDD
    val postGCTester = new CleanerTester(sc, shuffleIds = Seq(0))
    //使RDD超出范围,相应的Shuffle超出范围
    rdd = null  // Make RDD out of scope, so that corresponding shuffle goes out of scope
    runGC()
    postGCTester.assertCleanup()
  }

  test("automatically cleanup broadcast") {//自动清理广播
    var broadcast = newBroadcast()

    // Test that GC does not cause broadcast cleanup due to a strong reference
    //测试垃圾,不会造成广播清理强引用
    val preGCTester = new CleanerTester(sc, broadcastIds = Seq(broadcast.id))
    runGC()
    intercept[Exception] {
      preGCTester.assertCleanup()(timeout(1000 millis))
    }

    // Test that GC causes broadcast cleanup after dereferencing the broadcast variable
    //测试垃圾回收,广播清理后的引用广播变量,
    // Note broadcast is used after previous GC to avoid early collection by the JVM
    //注意广播后用以前的GC在JVM避免早期回收
    val postGCTester = new CleanerTester(sc, broadcastIds = Seq(broadcast.id))
    broadcast = null  // Make broadcast variable out of scope 使广播变量的范围
    runGC()
    postGCTester.assertCleanup()
  }

  test("automatically cleanup normal checkpoint") {//自动清理正常检查点
    val checkpointDir = java.io.File.createTempFile("temp", "")
    /**
     * delete为直接删除
     * deleteOnExit文档解释为:在虚拟机终止时,请求删除此抽象路径名表示的文件或目录。
     * 程序运行deleteOnExit成功后,File并没有直接删除,而是在虚拟机正常运行结束后才会删除
     */
    checkpointDir.deleteOnExit()
    checkpointDir.delete()
    var rdd = newPairRDD()
    sc.setCheckpointDir(checkpointDir.toString)
    rdd.checkpoint()
    rdd.cache()
    rdd.collect()
    var rddId = rdd.id

    // Confirm the checkpoint directory exists
    //确认检查点目录存在
    assert(ReliableRDDCheckpointData.checkpointPath(sc, rddId).isDefined)//分布式检查点是否定义
    val path = ReliableRDDCheckpointData.checkpointPath(sc, rddId).get//
    val fs = path.getFileSystem(sc.hadoopConfiguration)
    assert(fs.exists(path))

    // the checkpoint is not cleaned by default (without the configuration set)
    //默认情况下不清理检查点(没有配置集)
    var postGCTester = new CleanerTester(sc, Seq(rddId), Nil, Nil, Seq(rddId))
    //使RDD超出范围,好吧,如果提前收集
    rdd = null // Make RDD out of scope, ok if collected earlier
    runGC()
    postGCTester.assertCleanup()
    assert(!fs.exists(ReliableRDDCheckpointData.checkpointPath(sc, rddId).get))

    // Verify that checkpoints are NOT cleaned up if the config is not enabled
    //如果没有启用配置,则验证检查点未被清理
    sc.stop()
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("cleanupCheckpoint")
      .set("spark.cleaner.referenceTracking.cleanCheckpoints", "false")
    sc = new SparkContext(conf)
    rdd = newPairRDD()
    sc.setCheckpointDir(checkpointDir.toString)
    rdd.checkpoint()
    rdd.cache()
    rdd.collect()
    rddId = rdd.id

    // Confirm the checkpoint directory exists
    //确认检查点目录存在
    assert(fs.exists(ReliableRDDCheckpointData.checkpointPath(sc, rddId).get))

    // Reference rdd to defeat any early collection by the JVM
    //引用作废RDD,JVM早期垃圾收回
    rdd.count()

    // Test that GC causes checkpoint data cleanup after dereferencing the RDD
    //测试GC(垃圾收回)检查站数据清理后引用RDD
    postGCTester = new CleanerTester(sc, Seq(rddId))
    rdd = null // Make RDD out of scope 使RDD超出范围
    runGC()
    postGCTester.assertCleanup()
    assert(fs.exists(ReliableRDDCheckpointData.checkpointPath(sc, rddId).get))//检查点未被清理
  }

  test("automatically clean up local checkpoint") {//自动清理本地检查点
    // Note that this test is similar to the RDD cleanup
    //请注意,这个测试是类似于RDD清理
    // test because the same underlying mechanism is used!
    //测试,因为使用相同的基本机制
    var rdd = newPairRDD().localCheckpoint()
    assert(rdd.checkpointData.isDefined)
    assert(rdd.checkpointData.get.checkpointRDD.isEmpty)
    rdd.count()
    assert(rdd.checkpointData.get.checkpointRDD.isDefined)

    // Test that GC does not cause checkpoint cleanup due to a strong reference
    //测试垃圾收回,不会因为强引用而导致检查点清理
    val preGCTester = new CleanerTester(sc, rddIds = Seq(rdd.id))
    runGC()
    intercept[Exception] {
      preGCTester.assertCleanup()(timeout(1000 millis))
    }

    // Test that RDD going out of scope does cause the checkpoint blocks to be cleaned up
    //测试,RDD超出范围导致检查点块被清理
    val postGCTester = new CleanerTester(sc, rddIds = Seq(rdd.id))
    rdd = null
    runGC()
    postGCTester.assertCleanup()
  }
  //自动清理RDD +Shuffle+广播
  test("automatically cleanup RDD + shuffle + broadcast") {
    val numRdds = 100
    val numBroadcasts = 4 // Broadcasts are more costly 广播是更昂贵
    val rddBuffer = (1 to numRdds).map(i => randomRdd()).toBuffer
    val broadcastBuffer = (1 to numBroadcasts).map(i => newBroadcast()).toBuffer
    val rddIds = sc.persistentRdds.keys.toSeq
    val shuffleIds = 0 until sc.newShuffleId
    val broadcastIds = broadcastBuffer.map(_.id)

    val preGCTester = new CleanerTester(sc, rddIds, shuffleIds, broadcastIds)
    runGC()
    intercept[Exception] {
      preGCTester.assertCleanup()(timeout(1000 millis))
    }

    // Test that GC triggers the cleanup of all variables after the dereferencing them
    //试验表明,GC触发清理引用后的所有变量
    val postGCTester = new CleanerTester(sc, rddIds, shuffleIds, broadcastIds)
    broadcastBuffer.clear()
    rddBuffer.clear()
    runGC()
    postGCTester.assertCleanup()

    // Make sure the broadcasted task closure no longer exists after GC.
    //确保广播任务关闭后GC不再存在
    val taskClosureBroadcastId = broadcastIds.max + 1
    assert(sc.env.blockManager.master.getMatchingBlockIds({
      case BroadcastBlockId(`taskClosureBroadcastId`, _) => true
      case _ => false
    }, askSlaves = true).isEmpty)
  }
  //自动清理RDD+Shuffle+分布式模式广播
  test("automatically cleanup RDD + shuffle + broadcast in distributed mode") {
    sc.stop()

    val conf2 = new SparkConf()
//      .setMaster("local-cluster[2, 1, 1024]")
      .setMaster("local[*]")
      .setAppName("ContextCleanerSuite")
      .set("spark.cleaner.referenceTracking.blocking", "true")
      .set("spark.cleaner.referenceTracking.blocking.shuffle", "true")
      .set("spark.shuffle.manager", shuffleManager.getName)
    sc = new SparkContext(conf2)

    val numRdds = 10
    val numBroadcasts = 4 // Broadcasts are more costly
    val rddBuffer = (1 to numRdds).map(i => randomRdd()).toBuffer
    val broadcastBuffer = (1 to numBroadcasts).map(i => newBroadcast()).toBuffer
    val rddIds = sc.persistentRdds.keys.toSeq
    val shuffleIds = 0 until sc.newShuffleId
    val broadcastIds = broadcastBuffer.map(_.id)

    val preGCTester = new CleanerTester(sc, rddIds, shuffleIds, broadcastIds)
    runGC()
    intercept[Exception] {
      preGCTester.assertCleanup()(timeout(1000 millis))
    }

    // Test that GC triggers the cleanup of all variables after the dereferencing them
    val postGCTester = new CleanerTester(sc, rddIds, shuffleIds, broadcastIds)
    broadcastBuffer.clear()
    rddBuffer.clear()
    runGC()
    postGCTester.assertCleanup()

    // Make sure the broadcasted task closure no longer exists after GC.
    // 确保广播任务关闭后GC不再存在
    val taskClosureBroadcastId = broadcastIds.max + 1
    assert(sc.env.blockManager.master.getMatchingBlockIds({
      case BroadcastBlockId(`taskClosureBroadcastId`, _) => true
      case _ => false
    }, askSlaves = true).isEmpty)
  }
}


/**
 * A copy of the shuffle tests for sort-based shuffle
 * 复制Shuffle测试排序为基础的Shuffle
 */
class SortShuffleContextCleanerSuite extends ContextCleanerSuiteBase(classOf[SortShuffleManager]) {
  test("cleanup shuffle") {
    val (rdd, shuffleDeps) = newRDDWithShuffleDependencies()
    val collected = rdd.collect().toList
    val tester = new CleanerTester(sc, shuffleIds = shuffleDeps.map(_.shuffleId))

    // Explicit cleanup
    //显示清理
    shuffleDeps.foreach(s => cleaner.doCleanupShuffle(s.shuffleId, blocking = true))
    tester.assertCleanup()

    // Verify that shuffles can be re-executed after cleaning up
    //验证将可以重新清理后执行
    assert(rdd.collect().toList.equals(collected))
  }

  test("automatically cleanup shuffle") {//自动清理Shuffle
    var rdd = newShuffleRDD()
    rdd.count()

    // Test that GC does not cause shuffle cleanup due to a strong reference
    //测试GC,不会因为强引用而导致shuffle清理
    val preGCTester = new CleanerTester(sc, shuffleIds = Seq(0))
    runGC()
    intercept[Exception] {
      preGCTester.assertCleanup()(timeout(1000 millis))
    }
    rdd.count()  // Defeat early collection by the JVM 由JVM早期采集失败

    // Test that GC causes shuffle cleanup after dereferencing the RDD
    //测试GC,Shuffle清理后废弃的RDD
    val postGCTester = new CleanerTester(sc, shuffleIds = Seq(0))
    //标记RDD超出范围,因此,相应的Shuffle会超出范围
    rdd = null  // Make RDD out of scope, so that corresponding shuffle goes out of scope
    runGC()
    postGCTester.assertCleanup()
  }
  //自动清理RDD +Shuffle+广播在分布式模式
  test("automatically cleanup RDD + shuffle + broadcast in distributed mode") {
    sc.stop()

    val conf2 = new SparkConf()
     // .setMaster("local-cluster[2, 1, 1024]")
      .setMaster("local[*]")
      .setAppName("ContextCleanerSuite")
      .set("spark.cleaner.referenceTracking.blocking", "true")
      .set("spark.cleaner.referenceTracking.blocking.shuffle", "true")
      .set("spark.shuffle.manager", shuffleManager.getName)
    sc = new SparkContext(conf2)

    val numRdds = 10
    val numBroadcasts = 4 // Broadcasts are more costly 广播是更昂贵
    val rddBuffer = (1 to numRdds).map(i => randomRdd).toBuffer
    val broadcastBuffer = (1 to numBroadcasts).map(i => newBroadcast).toBuffer
    val rddIds = sc.persistentRdds.keys.toSeq
    val shuffleIds = 0 until sc.newShuffleId()
    val broadcastIds = broadcastBuffer.map(_.id)

    val preGCTester = new CleanerTester(sc, rddIds, shuffleIds, broadcastIds)
    runGC()
    intercept[Exception] {
      preGCTester.assertCleanup()(timeout(1000 millis))
    }

    // Test that GC triggers the cleanup of all variables after the dereferencing them
    val postGCTester = new CleanerTester(sc, rddIds, shuffleIds, broadcastIds)
    broadcastBuffer.clear()
    rddBuffer.clear()
    runGC()
    postGCTester.assertCleanup()

    // Make sure the broadcasted task closure no longer exists after GC.
    val taskClosureBroadcastId = broadcastIds.max + 1
    assert(sc.env.blockManager.master.getMatchingBlockIds({
      case BroadcastBlockId(`taskClosureBroadcastId`, _) => true
      case _ => false
    }, askSlaves = true).isEmpty)
  }
}


/**
 * Class to test whether RDDs, shuffles, etc. have been successfully cleaned.
 * 已成功清洗
 * The checkpoint here refers only to normal (reliable) checkpoints, not local checkpoints.
 * 这里的检查点只指正常(可靠)的检查点,没有本地检查点
 */
class CleanerTester(
    sc: SparkContext,
    rddIds: Seq[Int] = Seq.empty,
    shuffleIds: Seq[Int] = Seq.empty,
    broadcastIds: Seq[Long] = Seq.empty,
    checkpointIds: Seq[Long] = Seq.empty)
  extends Logging {

  val toBeCleanedRDDIds = new HashSet[Int] with SynchronizedSet[Int] ++= rddIds
  val toBeCleanedShuffleIds = new HashSet[Int] with SynchronizedSet[Int] ++= shuffleIds
  val toBeCleanedBroadcstIds = new HashSet[Long] with SynchronizedSet[Long] ++= broadcastIds
  val toBeCheckpointIds = new HashSet[Long] with SynchronizedSet[Long] ++= checkpointIds
  val isDistributed = !sc.isLocal

  val cleanerListener = new CleanerListener {
    def rddCleaned(rddId: Int): Unit = {
      toBeCleanedRDDIds -= rddId
      logInfo("RDD " + rddId + " cleaned")
    }

    def shuffleCleaned(shuffleId: Int): Unit = {
      toBeCleanedShuffleIds -= shuffleId
      logInfo("Shuffle " + shuffleId + " cleaned")
    }

    def broadcastCleaned(broadcastId: Long): Unit = {
      toBeCleanedBroadcstIds -= broadcastId
      logInfo("Broadcast " + broadcastId + " cleaned")
    }

    def accumCleaned(accId: Long): Unit = {
      logInfo("Cleaned accId " + accId + " cleaned")
    }

    def checkpointCleaned(rddId: Long): Unit = {
      toBeCheckpointIds -= rddId
      logInfo("checkpoint  " + rddId + " cleaned")
    }
  }

  val MAX_VALIDATION_ATTEMPTS = 10
  val VALIDATION_ATTEMPT_INTERVAL = 100

  logInfo("Attempting to validate before cleanup:\n" + uncleanedResourcesToString)
  preCleanupValidate()
  sc.cleaner.get.attachListener(cleanerListener)

  /** 
   *  Assert that all the stuff has been cleaned up 
   *  断言所有的东西都被清理过了
   *  */
  def assertCleanup()(implicit waitTimeout: PatienceConfiguration.Timeout) {
    try {
      eventually(waitTimeout, interval(100 millis)) {
        assert(isAllCleanedUp)
      }
      postCleanupValidate()
    } finally {
      //清理留下的资源
      logInfo("Resources left from cleaning up:\n" + uncleanedResourcesToString)
    }
  }

  /**
   *  Verify that RDDs, shuffles, etc. occupy resources 
   *  验证RDDS,Shuffle,占用资源等
   *  */
  private def preCleanupValidate() {
    assert(rddIds.nonEmpty || shuffleIds.nonEmpty || broadcastIds.nonEmpty ||
      checkpointIds.nonEmpty, "Nothing to cleanup")

    // Verify the RDDs have been persisted and blocks are present
    //验证RDDS已存在持久化块
    rddIds.foreach { rddId =>
      assert(
        sc.persistentRdds.contains(rddId),
        "RDD " + rddId + " have not been persisted, cannot start cleaner test"
      )

      assert(
        !getRDDBlocks(rddId).isEmpty,
        "Blocks of RDD " + rddId + " cannot be found in block manager, " +
          "cannot start cleaner test"
      )
    }

    // Verify the shuffle ids are registered and blocks are present
    //验证注册Shuffle的ids存在的块
    shuffleIds.foreach { shuffleId =>
      assert(
        mapOutputTrackerMaster.containsShuffle(shuffleId),
        "Shuffle " + shuffleId + " have not been registered, cannot start cleaner test"
      )

      assert(
        !getShuffleBlocks(shuffleId).isEmpty,
        "Blocks of shuffle " + shuffleId + " cannot be found in block manager, " +
          "cannot start cleaner test"
      )
    }

    // Verify that the broadcast blocks are present
    //确认广播块的存在
    broadcastIds.foreach { broadcastId =>
      assert(
        !getBroadcastBlocks(broadcastId).isEmpty,
        "Blocks of broadcast " + broadcastId + "cannot be found in block manager, " +
          "cannot start cleaner test"
      )
    }
  }

  /**
   * Verify that RDDs, shuffles, etc. do not occupy resources. Tests multiple times as there is
   * 验证RDDS,Shuffle,不占用资源等,测试多次,没有保证多长时间,它会采取清理的资源
   * as there is not guarantee on how long it will take clean up the resources.
   */
  private def postCleanupValidate() {
    // Verify the RDDs have been persisted and blocks are present
    //验证RDDS持久化存在的块
    rddIds.foreach { rddId =>
      assert(
        !sc.persistentRdds.contains(rddId),
        "RDD " + rddId + " was not cleared from sc.persistentRdds"
      )

      assert(
        getRDDBlocks(rddId).isEmpty,
        "Blocks of RDD " + rddId + " were not cleared from block manager"
      )
    }

    // Verify the shuffle ids are registered and blocks are present
    //验证Shuffle注册ids存在的块
    shuffleIds.foreach { shuffleId =>
      assert(
        !mapOutputTrackerMaster.containsShuffle(shuffleId),
        "Shuffle " + shuffleId + " was not deregistered from map output tracker"
      )

      assert(
        getShuffleBlocks(shuffleId).isEmpty,
        "Blocks of shuffle " + shuffleId + " were not cleared from block manager"
      )
    }

    // Verify that the broadcast blocks are present
    //检查广播块的存在
    broadcastIds.foreach { broadcastId =>
      assert(
        getBroadcastBlocks(broadcastId).isEmpty,
        "Blocks of broadcast " + broadcastId + " were not cleared from block manager"
      )
    }
  }

  private def uncleanedResourcesToString = {
    s"""
      |\tRDDs = ${toBeCleanedRDDIds.toSeq.sorted.mkString("[", ", ", "]")}
      |\tShuffles = ${toBeCleanedShuffleIds.toSeq.sorted.mkString("[", ", ", "]")}
      |\tBroadcasts = ${toBeCleanedBroadcstIds.toSeq.sorted.mkString("[", ", ", "]")}
    """.stripMargin
  }

  private def isAllCleanedUp =
    toBeCleanedRDDIds.isEmpty &&
    toBeCleanedShuffleIds.isEmpty &&
    toBeCleanedBroadcstIds.isEmpty &&
    toBeCheckpointIds.isEmpty

  private def getRDDBlocks(rddId: Int): Seq[BlockId] = {
    blockManager.master.getMatchingBlockIds( _ match {
      case RDDBlockId(`rddId`, _) => true
      case _ => false
    }, askSlaves = true)
  }

  private def getShuffleBlocks(shuffleId: Int): Seq[BlockId] = {
    blockManager.master.getMatchingBlockIds( _ match {
      case ShuffleBlockId(`shuffleId`, _, _) => true
      case ShuffleIndexBlockId(`shuffleId`, _, _) => true
      case _ => false
    }, askSlaves = true)
  }

  private def getBroadcastBlocks(broadcastId: Long): Seq[BlockId] = {
    blockManager.master.getMatchingBlockIds( _ match {
      case BroadcastBlockId(`broadcastId`, _) => true
      case _ => false
    }, askSlaves = true)
  }

  private def blockManager = sc.env.blockManager
 private def mapOutputTrackerMaster = sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
}
