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

package org.apache.spark.streaming

import scala.util.Random

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.rdd.BlockRDD
import org.apache.spark.storage.{StorageLevel, StreamBlockId}
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.rdd.WriteAheadLogBackedBlockRDD
import org.apache.spark.streaming.receiver.{BlockManagerBasedStoreResult, Receiver, WriteAheadLogBasedStoreResult}
import org.apache.spark.streaming.scheduler.ReceivedBlockInfo
import org.apache.spark.streaming.util.{WriteAheadLogRecordHandle, WriteAheadLogUtils}
import org.apache.spark.{SparkConf, SparkEnv}
/**
 * 
 * ReceiverInputDStream主要负责数据的产生与导入,
 * 它除了需要像其它 DStream那样在某个 batch里实例化 RDD以外,还需要额外的 Receiver为这个RDD生产数据!
 * ReceiverInputDStream保存关于历次batch的源头数据条数,历次batch计算花费的时间,用来实时计算准确的流量控制信息;
 * Spark Streaming 在程序刚开始运行时:
 * (1)由 Receiver 的总指挥 ReceiverTracker分发多个 job(每个job有 1个 task),
 * 		到多个executor上分别启动 ReceiverSupervisor实例
 * (2)每个 ReceiverSupervisor启动后将马上生成一个用户提供的 Receiver实现的实例 
 * 		该 Receiver 实现可以持续产生或者持续接收系统外数据,
 * 		比如 TwitterReceiver可以实时爬取 twitter数据 ——并在 Receiver 实例生成后调用 Receiver.onStart()
 * 
 * ReceiverInputDStream在每个batch去检查 ReceiverTracker收到的块数据 meta信息,界定哪些新数据需要在本 batch内处理,
 * 然后生成相应的 RDD实例去处理这些块数据
 */
class ReceiverInputDStreamSuite extends TestSuiteBase with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    StreamingContext.getActive().map { _.stop() }
  }
  //创建空的blockrdd时没有块信息WAL(预写式日志)
  testWithoutWAL("createBlockRDD creates empty BlockRDD when no block info") { receiverStream =>
    val rdd = receiverStream.createBlockRDD(Time(0), Seq.empty)
    assert(rdd.isInstanceOf[BlockRDD[_]])
    //判断是否能转换预写日志块
    val isInstance=rdd.isInstanceOf[WriteAheadLogBackedBlockRDD[_]]
    //println("======"+isInstance)
    assert(!rdd.isInstanceOf[WriteAheadLogBackedBlockRDD[_]])
    assert(rdd.isEmpty())
  }
  //创建块的信息正确blockrdd WAL(预写式日志)
  testWithoutWAL("createBlockRDD creates correct BlockRDD with block info") { receiverStream =>
    val blockInfos = Seq.fill(5) { createBlockInfo(withWALInfo = false) }
    //List(input-0--2296022686498889608,input-0-8033641072805079561,input-0-4856295788039827092,input-0--5257505035776562917,input-0-252513990646127979)
    val blockIds = blockInfos.map(x=>{
        println("======"+x.blockId)      
        x.blockId
              })

    //Verify that there are some blocks that are present, and some that are not
    //确认有一些块是否存在
    require(blockIds.forall(blockId => SparkEnv.get.blockManager.master.contains(blockId)))
    //创建一个BlockRDD
    val rdd = receiverStream.createBlockRDD(Time(0), blockInfos)
    assert(rdd.isInstanceOf[BlockRDD[_]])
    //判断没有预写日志BlockRDD
    assert(!rdd.isInstanceOf[WriteAheadLogBackedBlockRDD[_]])
    val blockRDD = rdd.asInstanceOf[BlockRDD[_]]
    /**
     * blockIds=
     * ======input-0--6065065102381848782
     *  ======input-0--2488062752272461646
     *  ======input-0-6777333610868239819
     *  ======input-0-1311392156326201126
     *  ======input-0-7149492482940956743
     */
    assert(blockRDD.blockIds.toSeq === blockIds)
  }
  //创建blockrdd块过虑不存在WAL(预写式日志)
  testWithoutWAL("createBlockRDD filters non-existent blocks before creating BlockRDD") {
    receiverStream =>
      val presentBlockInfos = Seq.fill(2)(createBlockInfo(withWALInfo = false, createBlock = true))
      val absentBlockInfos = Seq.fill(3)(createBlockInfo(withWALInfo = false, createBlock = false))
      val blockInfos = presentBlockInfos ++ absentBlockInfos
      val blockIds = blockInfos.map(_.blockId)

      // Verify that there are some blocks that are present, and some that are not
      //确认一些块是否存在
      require(blockIds.exists(blockId => SparkEnv.get.blockManager.master.contains(blockId)))
      require(blockIds.exists(blockId => !SparkEnv.get.blockManager.master.contains(blockId)))

      val rdd = receiverStream.createBlockRDD(Time(0), blockInfos)
      assert(rdd.isInstanceOf[BlockRDD[_]])
      val blockRDD = rdd.asInstanceOf[BlockRDD[_]]
      assert(blockRDD.blockIds.toSeq === presentBlockInfos.map { _.blockId})
  }
  //创建空的WALBackedBlockRDD没有块信息 WAL(预写式日志)
  testWithWAL("createBlockRDD creates empty WALBackedBlockRDD when no block info") {
    receiverStream =>
      val rdd = receiverStream.createBlockRDD(Time(0), Seq.empty)
      assert(rdd.isInstanceOf[WriteAheadLogBackedBlockRDD[_]])
      assert(rdd.isEmpty())
  }
  //WAL(预写式日志)
  testWithWAL(
    "createBlockRDD creates correct WALBackedBlockRDD with all block info having WAL info") {
    receiverStream =>
      val blockInfos = Seq.fill(5) { createBlockInfo(withWALInfo = true) }
      val blockIds = blockInfos.map(_.blockId)
      val rdd = receiverStream.createBlockRDD(Time(0), blockInfos)
      assert(rdd.isInstanceOf[WriteAheadLogBackedBlockRDD[_]])
      val blockRDD = rdd.asInstanceOf[WriteAheadLogBackedBlockRDD[_]]
      assert(blockRDD.blockIds.toSeq === blockIds)
     // val seq=blockRDD.walRecordHandles.toSeq
      //println("==="+seq.mkString(","))
      assert(blockRDD.walRecordHandles.toSeq === blockInfos.map { _.walRecordHandleOption.get })
  }
  //创建blockrdd当某块信息没有WAL信息 WAL(预写式日志)
  testWithWAL("createBlockRDD creates BlockRDD when some block info dont have WAL info") {
    receiverStream =>
      val blockInfos1 = Seq.fill(2) { createBlockInfo(withWALInfo = true) }
      val blockInfos2 = Seq.fill(3) { createBlockInfo(withWALInfo = false) }
      val blockInfos = blockInfos1 ++ blockInfos2
      val blockIds = blockInfos.map(_.blockId)
      val rdd = receiverStream.createBlockRDD(Time(0), blockInfos)
      assert(rdd.isInstanceOf[BlockRDD[_]])
      val blockRDD = rdd.asInstanceOf[BlockRDD[_]]
      assert(blockRDD.blockIds.toSeq === blockIds)
  }

  //没有启用WAL(预写式日志)
  private def testWithoutWAL(msg: String)(body: ReceiverInputDStream[_] => Unit): Unit = {
    
    test(s"Without WAL enabled: $msg") {
      runTest(enableWAL = false, body)
    }
  }
  //WAL(预写式日志)
  private def testWithWAL(msg: String)(body: ReceiverInputDStream[_] => Unit): Unit = {
    test(s"With WAL enabled: $msg") {
      runTest(enableWAL = true, body)
    }
  }
  //WAL(预写式日志)
  private def runTest(enableWAL: Boolean, body: ReceiverInputDStream[_] => Unit): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[4]").setAppName("ReceiverInputDStreamSuite")
    //设置预写式日志是否可用
    conf.set(WriteAheadLogUtils.RECEIVER_WAL_ENABLE_CONF_KEY, enableWAL.toString)
    //判断预写日志是否可用的状态
    require(WriteAheadLogUtils.enableReceiverLog(conf) === enableWAL)
    //分隔的时间叫作批次间隔
    val ssc = new StreamingContext(conf, Seconds(1))
    val receiverStream = new ReceiverInputDStream[Int](ssc) {
      override def getReceiver(): Receiver[Int] = null
    }
    withStreamingContext(ssc) { ssc =>
      body(receiverStream)
    }
  }

  /**
   * Create a block info for input to the ReceiverInputDStream.createBlockRDD
   * 创建一个用于输入到createBlockRDD块信息
   * @param withWALInfo Create block with  WAL(预写式日志) info in it
   * @param createBlock Actually create the block in the BlockManager
   * 				实际在块管理中创建块
   * @return
   */
  private def createBlockInfo(
      withWALInfo: Boolean,
      createBlock: Boolean = true): ReceivedBlockInfo = {
    val blockId = new StreamBlockId(0, Random.nextLong())
    if (createBlock) {//判断是否创建块
      //存储块,是否调用master
      SparkEnv.get.blockManager.putSingle(blockId, 1, StorageLevel.MEMORY_ONLY, tellMaster = true)
      //判断master是否包含blockId
      require(SparkEnv.get.blockManager.master.contains(blockId))
    }
    val storeResult = if (withWALInfo) {//预写日志信息
      //预写日志记录存储
      new WriteAheadLogBasedStoreResult(blockId, None, new WriteAheadLogRecordHandle { })
    } else {
      //管理块记录存储
      new BlockManagerBasedStoreResult(blockId, None)
    }
    new ReceivedBlockInfo(0, None, None, storeResult)
  }
}
