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
package org.apache.spark.streaming.rdd

import java.io.File

import scala.util.Random

import org.apache.hadoop.conf.Configuration
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.spark.storage.{BlockId, BlockManager, StorageLevel, StreamBlockId}
import org.apache.spark.streaming.util.{FileBasedWriteAheadLogSegment, FileBasedWriteAheadLogWriter}
import org.apache.spark.util.Utils
import org.apache.spark.{SparkConf, SparkContext, SparkException, SparkFunSuite}
/**
 * 支持提前写入块日志RDD测试套件
 */
class WriteAheadLogBackedBlockRDDSuite
  extends SparkFunSuite with BeforeAndAfterAll with BeforeAndAfterEach {

  val conf = new SparkConf()
    .setMaster("local[2]")
    .setAppName(this.getClass.getSimpleName)

  val hadoopConf = new Configuration()

  var sparkContext: SparkContext = null
  var blockManager: BlockManager = null
  var dir: File = null

  override def beforeEach(): Unit = {
    //创建临时目录
    dir = Utils.createTempDir()
  }

  override def afterEach(): Unit = {
    //递归删除目录
    Utils.deleteRecursively(dir)
  }

  override def beforeAll(): Unit = {
    
    sparkContext = new SparkContext(conf)
    blockManager = sparkContext.env.blockManager
  }

  override def afterAll(): Unit = {
    //将测试依赖项引入核心测试更为简单
    // Copied from LocalSparkContext, simpler than to introduced test dependencies to core tests.
    sparkContext.stop()
    System.clearProperty("spark.driver.port")
  }
 //在两个块管理器中读取数据并写日志
  test("Read data available in both block manager and write ahead log") {
    testRDD(numPartitions = 5, numPartitionsInBM = 5, numPartitionsInWAL = 5)
  }
 //读取数据只在块管理器,而不是在写提前日志
  test("Read data available only in block manager, not in write ahead log") {
    testRDD(numPartitions = 5, numPartitionsInBM = 5, numPartitionsInWAL = 0)
  }
 //在写前面的日志,而不是在块管理器读取数据
  test("Read data available only in write ahead log, not in block manager") {
    testRDD(numPartitions = 5, numPartitionsInBM = 0, numPartitionsInWAL = 5)
  }
  //读取部分可用的块管理器的数据,并在前面的日志中休息
  test("Read data with partially available in block manager, and rest in write ahead log") {
    testRDD(numPartitions = 5, numPartitionsInBM = 3, numPartitionsInWAL = 2)
  }
  //试验isblockvalid跳过块的读取从blockmanager
  test("Test isBlockValid skips block fetching from BlockManager") {
    testRDD(
      numPartitions = 5, numPartitionsInBM = 5, numPartitionsInWAL = 0, testIsBlockValid = true)
  }
  //测试是否删除有效的RDD数据块管理器
  test("Test whether RDD is valid after removing blocks from block manager") {
    testRDD(
      numPartitions = 5, numPartitionsInBM = 5, numPartitionsInWAL = 5, testBlockRemove = true)
  }
  //从写入提前日志返回到块管理器恢复的块的测试存储
  test("Test storing of blocks recovered from write ahead log back into block manager") {
    testRDD(
      numPartitions = 5, numPartitionsInBM = 0, numPartitionsInWAL = 5, testStoreInBM = true)
  }

  /**
   * Test the WriteAheadLogBackedRDD, by writing some partitions of the data to block manager
   * and the rest to a write ahead log, and then reading reading it all back using the RDD.
   * 通过写一些分区的数据块管理器和其他的一个写提前日志,然后使用RDD读取回来,
   * It can also test if the partitions that were read from the log were again stored in
   * block manager.
   * 它也可以测试,如果从日志中读取的分区再次被存储在块管理器中
   *
   * @param numPartitions Number of partitions in RDD 在RDD分区数
   * @param numPartitionsInBM Number of partitions to write to the BlockManager.
   * 												    块写的分区数
   *                          Partitions 0 to (numPartitionsInBM-1) will be written to BlockManager
   * @param numPartitionsInWAL Number of partitions to write to the Write Ahead Log.
   * 													写入前写入日志的分区数
   *                           Partitions (numPartitions - 1 - numPartitionsInWAL) to
   *                           (numPartitions - 1) will be written to WAL
   * @param testIsBlockValid Test whether setting isBlockValid to false skips block fetching
   * 												 测试是否设置isblockvalid假跳过块的读取
   * @param testBlockRemove Test whether calling rdd.removeBlock() makes the RDD still usable with
   *                        reads falling back to the WAL
   *                        测试是否调用RDD.removeblock()使得RDD还可从日志系统读回来
   * @param testStoreInBM   Test whether blocks read from log are stored back into block manager
   *												测试从日志中读取的块是否被存储到块管理器中
   * Example with numPartitions = 5, numPartitionsInBM = 3, and numPartitionsInWAL = 4
   * numPartitions总分区数,numPartitionsInBM块写入的分区 数,numPartitionsInWAL 写入前日志分区
   *
   *   numPartitionsInBM = 3
   *   |------------------|
   *   |                  |
   *    0       1       2       3       4
   *           |                         |
   *           |-------------------------|
   *              numPartitionsInWAL = 4
   */
  private def testRDD(
      numPartitions: Int,
      numPartitionsInBM: Int,
      numPartitionsInWAL: Int,
      testIsBlockValid: Boolean = false,
      testBlockRemove: Boolean = false,
      testStoreInBM: Boolean = false
    ) {
    require(numPartitionsInBM <= numPartitions,
      "Can't put more partitions in BlockManager than that in RDD")
    require(numPartitionsInWAL <= numPartitions,
      "Can't put more partitions in write ahead log than that in RDD")
    val data = Seq.fill(numPartitions, 10)(scala.util.Random.nextString(50))
    data.foreach { x => println }
    // Put the necessary blocks in the block manager
    //把必要的块放在块管理器中
    val blockIds = Array.fill(numPartitions)(StreamBlockId(Random.nextInt(), Random.nextInt()))
    blockIds.foreach { x => println }
    data.zip(blockIds).take(numPartitionsInBM).foreach { case(block, blockId) =>
      blockManager.putIterator(blockId, block.iterator, StorageLevel.MEMORY_ONLY_SER)
    }

    // Generate write ahead log record handles
    //生成写入前日志记录句柄
    val recordHandles = generateFakeRecordHandles(numPartitions - numPartitionsInWAL) ++
      generateWALRecordHandles(data.takeRight(numPartitionsInWAL),
        blockIds.takeRight(numPartitionsInWAL))

    // Make sure that the left `numPartitionsInBM` blocks are in block manager, and others are not
    //确保左` numpartitionsinbm `块是块的管理，而不是别人
    require(
      blockIds.take(numPartitionsInBM).forall(blockManager.get(_).nonEmpty),
      "Expected blocks not in BlockManager"
    )
    require(
      blockIds.takeRight(numPartitions - numPartitionsInBM).forall(blockManager.get(_).isEmpty),
      "Unexpected blocks in BlockManager"
    )

    // Make sure that the right `numPartitionsInWAL` blocks are in WALs, and other are not
    //确保正确的` numpartitionsinwal `块在预写日志系统 ,和其他不
    require(
      recordHandles.takeRight(numPartitionsInWAL).forall(s =>
        new File(s.path.stripPrefix("file://")).exists()),
      "Expected blocks not in write ahead log"
    )
    require(
      recordHandles.take(numPartitions - numPartitionsInWAL).forall(s =>
        !new File(s.path.stripPrefix("file://")).exists()),
      "Unexpected blocks in write ahead log"
    )

    // Create the RDD and verify whether the returned data is correct
    //创建RDD和验证是否返回的正确数据
    val rdd = new WriteAheadLogBackedBlockRDD[String](sparkContext, blockIds.toArray,
      recordHandles.toArray, storeInBlockManager = false)
    assert(rdd.collect() === data.flatten)

    // Verify that the block fetching is skipped when isBlockValid is set to false.
    //验证该块的读取是跳过时，isblockvalid设置为false
    // This is done by using a RDD whose data is only in memory but is set to skip block fetching
    //这是通过使用一个RDD的数据是在内存中进行但设置跳过块的读取使用RDD会抛出异常,
    // Using that RDD will throw exception, as it skips block fetching even if the blocks are in
    //它跳过块的读取即使块在BlockManager
    // in BlockManager.
    if (testIsBlockValid) {
      require(numPartitionsInBM === numPartitions, "All partitions must be in BlockManager")
      require(numPartitionsInWAL === 0, "No partitions must be in WAL")
      val rdd2 = new WriteAheadLogBackedBlockRDD[String](sparkContext, blockIds.toArray,
        recordHandles.toArray, isBlockIdValid = Array.fill(blockIds.length)(false))
      intercept[SparkException] {
        rdd2.collect()
      }
    }

    // Verify that the RDD is not invalid after the blocks are removed and can still read data
    // from write ahead log
    //验证了RDD是无效后的块删除,还可以读取数据提前写日志
    if (testBlockRemove) {
      require(numPartitions === numPartitionsInWAL, "All partitions must be in WAL for this test")
      require(numPartitionsInBM > 0, "Some partitions must be in BlockManager for this test")
      rdd.removeBlocks()
      assert(rdd.collect() === data.flatten)
    }

    if (testStoreInBM) {
      val rdd2 = new WriteAheadLogBackedBlockRDD[String](sparkContext, blockIds.toArray,
        recordHandles.toArray, storeInBlockManager = true, storageLevel = StorageLevel.MEMORY_ONLY)
      assert(rdd2.collect() === data.flatten)
      assert(
        blockIds.forall(blockManager.get(_).nonEmpty),
        "All blocks not found in block manager"
      )
    }
  }

  private def generateWALRecordHandles(
      blockData: Seq[Seq[String]],
      blockIds: Seq[BlockId]
    ): Seq[FileBasedWriteAheadLogSegment] = {
    require(blockData.size === blockIds.size)
    val writer = new FileBasedWriteAheadLogWriter(new File(dir, "logFile").toString, hadoopConf)
    val segments = blockData.zip(blockIds).map { case (data, id) =>
      writer.write(blockManager.dataSerialize(id, data.iterator))
    }
    writer.close()
    segments
  }

  private def generateFakeRecordHandles(count: Int): Seq[FileBasedWriteAheadLogSegment] = {
    Array.fill(count)(new FileBasedWriteAheadLogSegment("random", 0L, 0))
  }
}
