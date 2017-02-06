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

package org.apache.spark.rdd

import org.apache.spark.{SparkException, SparkContext, LocalSparkContext, SparkFunSuite}

import org.mockito.Mockito.spy
import org.apache.spark.storage.{RDDBlockId, StorageLevel}

/**
 * Fine-grained tests for local checkpointing.
 * For end-to-end tests, see CheckpointSuite.
 */
class LocalCheckpointSuite extends SparkFunSuite with LocalSparkContext {

  override def beforeEach(): Unit = {
    sc = new SparkContext("local[2]", "test")
  }

  test("transform storage level") {//转换存储级别
    //转换检查点
    val transform = LocalRDDCheckpointData.transformStorageLevel _
    assert(transform(StorageLevel.NONE) === StorageLevel.DISK_ONLY)
    assert(transform(StorageLevel.MEMORY_ONLY) === StorageLevel.MEMORY_AND_DISK)
    assert(transform(StorageLevel.MEMORY_ONLY_SER) === StorageLevel.MEMORY_AND_DISK_SER)
    assert(transform(StorageLevel.MEMORY_ONLY_2) === StorageLevel.MEMORY_AND_DISK_2)
    assert(transform(StorageLevel.MEMORY_ONLY_SER_2) === StorageLevel.MEMORY_AND_DISK_SER_2)
    assert(transform(StorageLevel.DISK_ONLY) === StorageLevel.DISK_ONLY)
    assert(transform(StorageLevel.DISK_ONLY_2) === StorageLevel.DISK_ONLY_2)
    assert(transform(StorageLevel.MEMORY_AND_DISK) === StorageLevel.MEMORY_AND_DISK)
    assert(transform(StorageLevel.MEMORY_AND_DISK_SER) === StorageLevel.MEMORY_AND_DISK_SER)
    assert(transform(StorageLevel.MEMORY_AND_DISK_2) === StorageLevel.MEMORY_AND_DISK_2)
    assert(transform(StorageLevel.MEMORY_AND_DISK_SER_2) === StorageLevel.MEMORY_AND_DISK_SER_2)
    // Off-heap is not supported and Spark should fail fast
    intercept[SparkException] {
      transform(StorageLevel.OFF_HEAP)
    }
  }

  test("basic lineage truncation") {//基本血缘截取
    val numPartitions = 4
    val parallelRdd = sc.parallelize(1 to 100, numPartitions)
    val mappedRdd = parallelRdd.map { i => i + 1 }
    val filteredRdd = mappedRdd.filter { i => i % 2 == 0 }
    val expectedPartitionIndices = (0 until numPartitions).toArray
    assert(filteredRdd.checkpointData.isEmpty)
    assert(filteredRdd.getStorageLevel === StorageLevel.NONE)
    assert(filteredRdd.partitions.map(_.index) === expectedPartitionIndices)
    assert(filteredRdd.dependencies.size === 1)
    assert(filteredRdd.dependencies.head.rdd === mappedRdd)
    assert(mappedRdd.dependencies.size === 1)
    assert(mappedRdd.dependencies.head.rdd === parallelRdd)
    assert(parallelRdd.dependencies.size === 0)

    // Mark the RDD for local checkpointing
    //标记RDD本地检查点
    filteredRdd.localCheckpoint()
    assert(filteredRdd.checkpointData.isDefined)
    assert(!filteredRdd.checkpointData.get.isCheckpointed)
    assert(!filteredRdd.checkpointData.get.checkpointRDD.isDefined)
    assert(filteredRdd.getStorageLevel === LocalRDDCheckpointData.DEFAULT_STORAGE_LEVEL)

    // After an action, the lineage is truncated
    //一个动作后,血缘被截断
    val result = filteredRdd.collect()
    assert(filteredRdd.checkpointData.get.isCheckpointed)
    assert(filteredRdd.checkpointData.get.checkpointRDD.isDefined)
    val checkpointRdd = filteredRdd.checkpointData.flatMap(_.checkpointRDD).get
    assert(filteredRdd.dependencies.size === 1)
    assert(filteredRdd.dependencies.head.rdd === checkpointRdd)
    assert(filteredRdd.partitions.map(_.index) === expectedPartitionIndices)
    assert(checkpointRdd.partitions.map(_.index) === expectedPartitionIndices)

    // Recomputation should yield the same result
    //重新计算应产生相同的结果
    assert(filteredRdd.collect() === result)
    assert(filteredRdd.collect() === result)
  }
  //基本血缘截断-缓存之前检查点
  test("basic lineage truncation - caching before checkpointing") {
    testBasicLineageTruncationWithCaching(
      newRdd.persist(StorageLevel.MEMORY_ONLY).localCheckpoint(),
      StorageLevel.MEMORY_AND_DISK)
  }
 //基本血缘截断-缓存之后检查点
  test("basic lineage truncation - caching after checkpointing") {
    testBasicLineageTruncationWithCaching(
      newRdd.localCheckpoint().persist(StorageLevel.MEMORY_ONLY),
      StorageLevel.MEMORY_AND_DISK)
  }
//间接的血统截断
  test("indirect lineage truncation") {
    testIndirectLineageTruncation(
      newRdd.localCheckpoint(),
      LocalRDDCheckpointData.DEFAULT_STORAGE_LEVEL)
  }
//间接的血统截断,缓存之前检查点
  test("indirect lineage truncation - caching before checkpointing") {
    testIndirectLineageTruncation(
      newRdd.persist(StorageLevel.MEMORY_ONLY).localCheckpoint(),
      StorageLevel.MEMORY_AND_DISK)
  }
//间接的血统截断,缓存之后检查点
  test("indirect lineage truncation - caching after checkpointing") {
    testIndirectLineageTruncation(
      newRdd.localCheckpoint().persist(StorageLevel.MEMORY_ONLY),
      StorageLevel.MEMORY_AND_DISK)
  }
  //没有迭代器的检查点
  test("checkpoint without draining iterator") {
    testWithoutDrainingIterator(
      newSortedRdd.localCheckpoint(),
      LocalRDDCheckpointData.DEFAULT_STORAGE_LEVEL,
      50)
  }

  test("checkpoint without draining iterator - caching before checkpointing") {
    testWithoutDrainingIterator(
      newSortedRdd.persist(StorageLevel.MEMORY_ONLY).localCheckpoint(),
      StorageLevel.MEMORY_AND_DISK,
      50)
  }

  test("checkpoint without draining iterator - caching after checkpointing") {
    testWithoutDrainingIterator(
      newSortedRdd.localCheckpoint().persist(StorageLevel.MEMORY_ONLY),
      StorageLevel.MEMORY_AND_DISK,
      50)
  }
  //块存在检查点
  test("checkpoint blocks exist") {
    testCheckpointBlocksExist(
      newRdd.localCheckpoint(),
      LocalRDDCheckpointData.DEFAULT_STORAGE_LEVEL)
  }
//块存在检查点-缓存之前检查点
  test("checkpoint blocks exist - caching before checkpointing") {
    testCheckpointBlocksExist(
      newRdd.persist(StorageLevel.MEMORY_ONLY).localCheckpoint(),
      StorageLevel.MEMORY_AND_DISK)
  }
//块存在检查点-缓存之后检查点
  test("checkpoint blocks exist - caching after checkpointing") {
    testCheckpointBlocksExist(
      newRdd.localCheckpoint().persist(StorageLevel.MEMORY_ONLY),
      StorageLevel.MEMORY_AND_DISK)
  }
//丢失的检查点块失败与信息的消息
  test("missing checkpoint block fails with informative message") {
    val rdd = newRdd.localCheckpoint()
    val numPartitions = rdd.partitions.size
    val partitionIndices = rdd.partitions.map(_.index)
    val bmm = sc.env.blockManager.master

    // After an action, the blocks should be found somewhere in the cache
    //一个动作后,块应该在缓存中找到
    rdd.collect()
    partitionIndices.foreach { i =>
      assert(bmm.contains(RDDBlockId(rdd.id, i)))
    }

    // Remove one of the blocks to simulate executor failure
    //删除一个块来模拟执行器故障
    // Collecting the RDD should now fail with an informative exception
    //收集RDD现在应该失败的异常信息
    val blockId = RDDBlockId(rdd.id, numPartitions - 1)
    bmm.removeBlock(blockId)
    try {
      rdd.collect()
      fail("Collect should have failed if local checkpoint block is removed...")
    } catch {
      case se: SparkException =>
        assert(se.getMessage.contains(s"Checkpoint block $blockId not found"))
        assert(se.getMessage.contains("rdd.checkpoint()")) // suggest an alternative
        assert(se.getMessage.contains("fault-tolerant")) // justify the alternative
    }
  }

  /**
   * Helper method to create a simple RDD.
   * 辅助方法来创建一个简单的RDD
   */
  private def newRdd: RDD[Int] = {
    sc.parallelize(1 to 100, 4)
      .map { i => i + 1 }
      .filter { i => i % 2 == 0 }
  }

  /**
   * Helper method to create a simple sorted RDD.
   * 辅助方法来创建一个简单的排序法。
   */
  private def newSortedRdd: RDD[Int] = newRdd.sortBy(identity)

  /**
   * Helper method to test basic lineage truncation with caching.
   *
   * @param rdd an RDD that is both marked for caching and local checkpointing
   */
  private def testBasicLineageTruncationWithCaching[T](
      rdd: RDD[T],
      targetStorageLevel: StorageLevel): Unit = {
    require(targetStorageLevel !== StorageLevel.NONE)
    require(rdd.getStorageLevel !== StorageLevel.NONE)
    require(rdd.isLocallyCheckpointed)
    val result = rdd.collect()
    assert(rdd.getStorageLevel === targetStorageLevel)
    assert(rdd.checkpointData.isDefined)
    assert(rdd.checkpointData.get.isCheckpointed)
    assert(rdd.checkpointData.get.checkpointRDD.isDefined)
    assert(rdd.dependencies.head.rdd === rdd.checkpointData.get.checkpointRDD.get)
    assert(rdd.collect() === result)
    assert(rdd.collect() === result)
  }

  /**
   * Helper method to test indirect lineage truncation.间接
   *
   * Indirect lineage truncation here means the action is called on one of the
   * checkpointed RDD's descendants, but not on the checkpointed RDD itself.
   *
   * @param rdd a locally checkpointed RDD
   */
  private def testIndirectLineageTruncation[T](
      rdd: RDD[T],
      targetStorageLevel: StorageLevel): Unit = {
    require(targetStorageLevel !== StorageLevel.NONE)
    require(rdd.isLocallyCheckpointed)
    val rdd1 = rdd.map { i => i + "1" }
    val rdd2 = rdd1.map { i => i + "2" }
    val rdd3 = rdd2.map { i => i + "3" }
    val rddDependencies = rdd.dependencies
    val rdd1Dependencies = rdd1.dependencies
    val rdd2Dependencies = rdd2.dependencies
    val rdd3Dependencies = rdd3.dependencies
    assert(rdd1Dependencies.size === 1)
    assert(rdd1Dependencies.head.rdd === rdd)
    assert(rdd2Dependencies.size === 1)
    assert(rdd2Dependencies.head.rdd === rdd1)
    assert(rdd3Dependencies.size === 1)
    assert(rdd3Dependencies.head.rdd === rdd2)

    // Only the locally checkpointed RDD should have special storage level
    //只有局部检查点RDD应具有特殊的存储级别
    assert(rdd.getStorageLevel === targetStorageLevel)
    assert(rdd1.getStorageLevel === StorageLevel.NONE)
    assert(rdd2.getStorageLevel === StorageLevel.NONE)
    assert(rdd3.getStorageLevel === StorageLevel.NONE)

    // After an action, only the dependencies of the checkpointed RDD changes
    //后一个动作,只有检查RDD变化的相关性
    val result = rdd3.collect()
    assert(rdd.dependencies !== rddDependencies)
    assert(rdd1.dependencies === rdd1Dependencies)
    assert(rdd2.dependencies === rdd2Dependencies)
    assert(rdd3.dependencies === rdd3Dependencies)
    assert(rdd3.collect() === result)
    assert(rdd3.collect() === result)
  }

  /**
   * Helper method to test checkpointing without fully draining the iterator.
   * 辅助方法来测试检查点没有充分引流的迭代器。
   * Not all RDD actions fully consume the iterator. As a result, a subset of the partitions
   * may not be cached. However, since we want to truncate the lineage safely, we explicitly
   * ensure that *all* partitions are fully cached. This method asserts this behavior.
   *
   * @param rdd a locally checkpointed RDD
   */
  private def testWithoutDrainingIterator[T](
      rdd: RDD[T],
      targetStorageLevel: StorageLevel,
      targetCount: Int): Unit = {
    require(targetCount > 0)
    require(targetStorageLevel !== StorageLevel.NONE)
    require(rdd.isLocallyCheckpointed)

    // This does not drain the iterator, but checkpointing should still work
    //这不漏的迭代器,但检查点仍然可以正常工作
    val first = rdd.first()
    assert(rdd.count() === targetCount)
    assert(rdd.count() === targetCount)
    assert(rdd.first() === first)
    assert(rdd.first() === first)

    // Test the same thing by calling actions on a descendant instead
    //通过调用一个后代而不是相同的动作来测试同样的事情
    val rdd1 = rdd.repartition(10)
    val rdd2 = rdd1.repartition(100)
    val rdd3 = rdd2.repartition(1000)
    val first2 = rdd3.first()
    assert(rdd3.count() === targetCount)
    assert(rdd3.count() === targetCount)
    assert(rdd3.first() === first2)
    assert(rdd3.first() === first2)
    assert(rdd.getStorageLevel === targetStorageLevel)
    assert(rdd1.getStorageLevel === StorageLevel.NONE)
    assert(rdd2.getStorageLevel === StorageLevel.NONE)
    assert(rdd3.getStorageLevel === StorageLevel.NONE)
  }

  /**
   * Helper method to test whether the checkpoint blocks are found in the cache.
   *
   * @param rdd a locally checkpointed RDD
   */
  private def testCheckpointBlocksExist[T](
      rdd: RDD[T],
      targetStorageLevel: StorageLevel): Unit = {
    val bmm = sc.env.blockManager.master
    val partitionIndices = rdd.partitions.map(_.index)

    // The blocks should not exist before the action
    //块不应该在动作之前存在
    partitionIndices.foreach { i =>
      assert(!bmm.contains(RDDBlockId(rdd.id, i)))
    }

    // After an action, the blocks should be found in the cache with the expected level
    //一个动作后,块应该被发现在缓存中与预期的水平
    rdd.collect()
    partitionIndices.foreach { i =>
      val blockId = RDDBlockId(rdd.id, i)
      val status = bmm.getBlockStatus(blockId)
      assert(status.nonEmpty)
      assert(status.values.head.storageLevel === targetStorageLevel)
    }
  }

}
