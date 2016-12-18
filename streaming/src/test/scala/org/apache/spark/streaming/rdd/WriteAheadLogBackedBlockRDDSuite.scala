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
 * 预写式日志支持块RDD测试套件
 * WAL(Write-Ahead-Log):在处理数据插入和删除的过程中用来记录操作内容的一种日志
 * WriteAheadLog BackedBlock 预写式日志支持块
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
    //复制LocalSparkContext,将测试依赖项引入核心测试更为简单
    // Copied from LocalSparkContext, simpler than to introduced test dependencies to core tests.
    sparkContext.stop()
    System.clearProperty("spark.driver.port")
  }
 //在两个块管理器中读取数据并预写日志
  test("Read data available in both block manager and write ahead log") {
    testRDD(numPartitions = 5, numPartitionsInBM = 5, numPartitionsInWAL = 5)
  }
 //读取数据只在块管理器,而不是预写日志
  test("Read data available only in block manager, not in write ahead log") {
    testRDD(numPartitions = 5, numPartitionsInBM = 5, numPartitionsInWAL = 0)
  }
 //读取数据只在预写日志,而不是在块管理器读取数据
  test("Read data available only in write ahead log, not in block manager") {
    testRDD(numPartitions = 5, numPartitionsInBM = 0, numPartitionsInWAL = 5)
  }
  //读取部分可用的块管理器的数据,剩余部分预写日志
  test("Read data with partially available in block manager, and rest in write ahead log") {
    testRDD(numPartitions = 5, numPartitionsInBM = 3, numPartitionsInWAL = 2)
  }
  //测试从blockmanager读取isblockvalid跳过无效的块
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
   * 												    块写的分区数,分区0到(numpartitionsinbm-1)将被写入块管理器中
   *                          Partitions 0 to (numPartitionsInBM-1) will be written to BlockManager
   * @param numPartitionsInWAL Number of partitions to write to the Write Ahead Log.
   * 													  写入前写入日志的分区数,分区数(分区数-1-numPartitionsInWAL)到(numPartitions - 1)
   * 													 将写到预写日志
   *                           Partitions (numPartitions - 1 - numPartitionsInWAL) to
   *                           (numPartitions - 1) will be written to WAL
   * @param testIsBlockValid Test whether setting isBlockValid to false skips block fetching
   * 												 测试是否设置isblockvalid假跳过块的读取
   * @param testBlockRemove Test whether calling rdd.removeBlock() makes the RDD still usable with
   *                        reads falling back to the WAL WAL(预写式日志)
   *                        测试是否调用RDD.removeblock()使得RDD还可从日志系统读回来
   * @param testStoreInBM   Test whether blocks read from log are stored back into block manager
   *												测试从日志中读取的块是否被存储到块管理器中
   * Example with numPartitions = 5, numPartitionsInBM = 3, and numPartitionsInWAL = 4
   * numPartitions总分区数,numPartitionsInBM块写入的分区 数,numPartitionsInWAL WAL(预写式日志)分区
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
    //由于出现乱码scala.util.Random.nextString(50),即改成scala.util.Random.nextInt(50).toString()
      /**
       *===List(41, 18, 1, 9, 8, 12, 42, 15, 12, 40)
       *===List(13, 2, 3, 35, 37, 43, 7, 19, 24, 13)
       *===List(6, 48, 44, 27, 5, 11, 22, 35, 28, 29)
       *===List(44, 42, 8, 18, 8, 38, 36, 41, 49, 39)
       *===List(15, 45, 44, 13, 15, 24, 48, 5, 3, 6)
       */
    val data = Seq.fill(numPartitions, 10)("\""+scala.util.Random.nextInt(50).toString()+"\"")
    data.foreach { x =>  println("==="+x) }
    
    /**
     * Random.nextInt()产生数据时有负数 res1: Int = -1874109309
     *===input-2137828776--551564096
     *===input--143001680-1841074061
     *===input--528107763-1366668151
     *===input--1136078048--1968524650
     *===input--684369970--1445572881
     */
    val blockIds = Array.fill(numPartitions)(StreamBlockId(Random.nextInt(), Random.nextInt()))
    blockIds.foreach { x => println("==="+x)}
    /**
     * res2 = List(
     * (List(0, 20, 0, 0, 48, 5, 0, 48, 28, 24),input-300100004-1533713222), 
     * (List(31, 36, 1, 37, 33, 47, 36, 38, 38, 31),input-432502173--1035939876), 
     * (List(22, 46, 45, 17, 49, 16, 33, 30, 2, 35),input--646589726--26132234), 
     * (List(45, 10, 14, 8, 17, 4, 28, 11, 45, 27),input--1836656533--1370908660), 
     * (List(45, 34, 26, 40, 20, 37, 22, 18, 12, 32),input-1454315495-1814109189))
     */
    data.zip(blockIds).take(numPartitionsInBM).foreach { case(block, blockId) =>
     // Put the necessary blocks in the block manager
     //把必要的块存放到块管理器中
      println("["+blockId+"]\t["+block.mkString(",")+"]")
      blockManager.putIterator(blockId, block.iterator, StorageLevel.MEMORY_ONLY_SER)
    }

    //产生虚拟的记录
    val fakeRecordHandles=generateFakeRecordHandles(numPartitions - numPartitionsInWAL)
    //takeRight 返回最后n个元素
    val blockData=data.takeRight(numPartitionsInWAL)
    //takeRight 返回最后n个元素
    val blckIds=blockIds.takeRight(numPartitionsInWAL)
     // Generate write ahead log record handles
    //生成预写式日志记录句柄,WAL(预写式日志)
    val WALRecordHandles=generateWALRecordHandles(blockData,blckIds)
    /**ArrayBuffer(
     * 	FileBasedWriteAheadLogSegment(C:\logFile,0,74), 
     *  FileBasedWriteAheadLogSegment(C:\logFile,78,71), 
     *  FileBasedWriteAheadLogSegment(C:\logFile,153,74), 
     *  FileBasedWriteAheadLogSegment(C:\logFile,231,72), 
     *  FileBasedWriteAheadLogSegment(C:\logFile,307,72))
     */
    val recordHandles = fakeRecordHandles ++WALRecordHandles
        

    // Make sure that the left `numPartitionsInBM` blocks are in block manager, and others are not
    //确保前面的'numpartitionsinbm'块在块管理中
    require(
      blockIds.take(numPartitionsInBM).forall(blockManager.get(_).nonEmpty),
      "Expected blocks not in BlockManager"
    )
    //确保后边的blockIds数据在块管理器中
    require(
      blockIds.takeRight(numPartitions - numPartitionsInBM).forall(blockManager.get(_).isEmpty),
      "Unexpected blocks in BlockManager"
    )

    // Make sure that the right 'numPartitionsInWAL' blocks are in WALs, and other are not
    //确保正确后边的'numpartitionsinwal'块在预写日志系统 
    require(
      recordHandles.takeRight(numPartitionsInWAL).forall(s =>
        new File(s.path.stripPrefix("file://")).exists()),
        //预期块不在预写日志
      "Expected blocks not in write ahead log"
    )
    require(
      recordHandles.take(numPartitions - numPartitionsInWAL).forall(s =>
        !new File(s.path.stripPrefix("file://")).exists()),
        //在预写日志中的异常的块
      "Unexpected blocks in write ahead log"
    )

    // Create the RDD and verify whether the returned data is correct
    //创建RDD和验证是否返回的正确数据
    val rdd = new WriteAheadLogBackedBlockRDD[String](sparkContext, blockIds.toArray,
      recordHandles.toArray, storeInBlockManager = false)
    println("collect:"+rdd.collect()+"\t data:"+data.flatten)
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
    //验证了RDD是无效后的块删除,还可以读取数据预写日志
    if (testBlockRemove) {
      require(numPartitions === numPartitionsInWAL, "All partitions must be in WAL for this test")
      require(numPartitionsInBM > 0, "Some partitions must be in BlockManager for this test")
      //rdd.collect().foreach { x => println("befor:"+x) }
      rdd.removeBlocks()
      //rdd.collect().foreach { x => println("after:"+x) }
      println("testBlockRemove:"+data.flatten)
      assert(rdd.collect() === data.flatten)
    }

    if (testStoreInBM) {
      val rdd2 = new WriteAheadLogBackedBlockRDD[String](sparkContext, blockIds.toArray,
        recordHandles.toArray, storeInBlockManager = true, storageLevel = StorageLevel.MEMORY_ONLY)
      assert(rdd2.collect() === data.flatten)
      assert(
        //数据存储到内存管理器中  
        blockIds.forall(blockManager.get(_).nonEmpty),
        //块管理器中没有找到的所有块
        "All blocks not found in block manager"
      )
    }
  }
  //产生WAL(预写式日志)句柄
  private def generateWALRecordHandles(
      //由于出现乱码scala.util.Random.nextString(50),即改成scala.util.Random.nextInt(50).toString()
      //随机生成的数据字符改成数据
      blockData: Seq[Seq[String]],
     // blockData: Seq[Seq[Int]],
      blockIds: Seq[BlockId]
    ): Seq[FileBasedWriteAheadLogSegment] = {
    require(blockData.size === blockIds.size)
    //生成日志logFile文件
    val writer = new FileBasedWriteAheadLogWriter(new File(dir, "logFile").toString, hadoopConf)
    //blockData=List(List(49, 34, 36, 13, 23, 16, 31, 38, 10, 26), 
    //List(24, 6, 22, 10, 33, 14, 40, 35, 7, 20), 
    //List(22, 5, 10, 32, 44, 24, 39, 16, 27, 9), 
    //List(1, 47, 18, 15, 22, 25, 43, 3, 29, 10), 
    //List(44, 1, 33, 9, 15, 18, 30, 48, 19, 8))
    //blockIds=WrappedArray(input-300100004-1533713222, input-432502173--1035939876, input--646589726--26132234, input--1836656533--1370908660, input-1454315495-1814109189)
    /**res0: List[(List[Int], String)] = List(
     * (List(49, 34, 36, 13, 23, 16, 31, 38, 10,26),input-300100004-1533713222), 
     * (List(24, 6, 22, 10, 33, 14, 40, 35, 7, 20),input-432502173--1035939876),
     * (List(22, 5, 10, 32, 44, 24, 39, 16, 27, 9),input--646589726--26132234),
     * (List(1, 47, 18, 15, 22, 25, 43, 3, 29, 10),input--1836656533--1370908660),
     * (List(44, 1, 33, 9, 15, 18, 30, 48, 19, 8),input-1454315495-1814109189))**/
    val segments = blockData.zip(blockIds).map { case (data, id) =>
      println(id+"||"+data.mkString(","))
      val dataSerialize=blockManager.dataSerialize(id, data.iterator)
      //每次写入一条数据
      writer.write(dataSerialize)
    }
    writer.close()
    segments
  }
  //产生一个虚拟记录处理
  private def generateFakeRecordHandles(count: Int): Seq[FileBasedWriteAheadLogSegment] = {
    Array.fill(count)(new FileBasedWriteAheadLogSegment("random", 0L, 0))
  }
}
