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

package org.apache.spark.sql.execution

import scala.util.control.NonFatal
import scala.collection.mutable
import scala.util.{Try, Random}

import org.scalatest.Matchers

import org.apache.spark.{TaskContextImpl, TaskContext, SparkFunSuite}
import org.apache.spark.shuffle.ShuffleMemoryManager
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{UnsafeRow, UnsafeProjection}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.memory.{ExecutorMemoryManager, MemoryAllocator, TaskMemoryManager}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Test suite for [[UnsafeFixedWidthAggregationMap]].
 *
 * Use [[testWithMemoryLeakDetection]] rather than [[test]] to construct test cases.
 * 不安全固定宽度聚合映射测试套件
 */
class UnsafeFixedWidthAggregationMapSuite
  extends SparkFunSuite
  with Matchers
  with SharedSQLContext {

  import UnsafeFixedWidthAggregationMap._
//StructType代表一张表,StructField代表一个字段
  private val groupKeySchema = StructType(StructField("product", StringType) :: Nil)
  private val aggBufferSchema = StructType(StructField("salePrice", IntegerType) :: Nil)
  private def emptyAggregationBuffer: InternalRow = InternalRow(0)
  private val PAGE_SIZE_BYTES: Long = 1L << 26; // 64 megabytes

  private var taskMemoryManager: TaskMemoryManager = null
  private var shuffleMemoryManager: TestShuffleMemoryManager = null

  def testWithMemoryLeakDetection(name: String)(f: => Unit) {
    def cleanup(): Unit = {
      if (taskMemoryManager != null) {
        val leakedShuffleMemory = shuffleMemoryManager.getMemoryConsumptionForThisTask()
        assert(taskMemoryManager.cleanUpAllAllocatedMemory() === 0)
        assert(leakedShuffleMemory === 0)
        taskMemoryManager = null
      }
      TaskContext.unset()
    }

    test(name) {
      taskMemoryManager = new TaskMemoryManager(new ExecutorMemoryManager(MemoryAllocator.HEAP))
      shuffleMemoryManager = new TestShuffleMemoryManager

      TaskContext.setTaskContext(new TaskContextImpl(
        stageId = 0,
        partitionId = 0,
        taskAttemptId = Random.nextInt(10000),
        attemptNumber = 0,
        taskMemoryManager = taskMemoryManager,
        metricsSystem = null,
        internalAccumulators = Seq.empty))

      try {
        f
      } catch {
        case NonFatal(e) =>
          Try(cleanup())
          throw e
      }
      cleanup()
    }
  }

  private def randomStrings(n: Int): Seq[String] = {
    val rand = new Random(42)
    Seq.fill(512) {
      Seq.fill(rand.nextInt(100))(rand.nextPrintableChar()).mkString
    }.distinct
  }
  //测试内存泄漏检测
  testWithMemoryLeakDetection("supported schemas") {
  //StructType代表一张表,StructField代表一个字段
    assert(supportsAggregationBufferSchema(
      StructType(StructField("x", DecimalType.USER_DEFAULT) :: Nil)))
    assert(supportsAggregationBufferSchema(
      StructType(StructField("x", DecimalType.SYSTEM_DEFAULT) :: Nil)))
    assert(!supportsAggregationBufferSchema(StructType(StructField("x", StringType) :: Nil)))
    assert(
      !supportsAggregationBufferSchema(StructType(StructField("x", ArrayType(IntegerType)) :: Nil)))
  }
  //内存泄漏检测
  testWithMemoryLeakDetection("empty map") {
    val map = new UnsafeFixedWidthAggregationMap(
      emptyAggregationBuffer,
      aggBufferSchema,
      groupKeySchema,
      taskMemoryManager,
      shuffleMemoryManager,
      1024, // initial capacity,初始化容量
      PAGE_SIZE_BYTES,
      false // disable perf metrics 禁用的性能指标
    )
    assert(!map.iterator().next())
    map.free()
  }
  //更新一个键的值
  testWithMemoryLeakDetection("updating values for a single key") {
    val map = new UnsafeFixedWidthAggregationMap(
      emptyAggregationBuffer,
      aggBufferSchema,
      groupKeySchema,
      taskMemoryManager,
      shuffleMemoryManager,
      1024, // initial capacity 初始化容量
      PAGE_SIZE_BYTES,
      false // disable perf metrics 禁用的性能指标
    )
    val groupKey = InternalRow(UTF8String.fromString("cats"))

    // Looking up a key stores a zero-entry in the map (like Python Counters or DefaultDicts)
    //在Map上查找一个键存储一零个条目
    assert(map.getAggregationBuffer(groupKey) != null)
    val iter = map.iterator()
    assert(iter.next())
    iter.getKey.getString(0) should be ("cats")
    iter.getValue.getInt(0) should be (0)
    assert(!iter.next())

    // Modifications to rows retrieved from the map should update the values in the map
    //从Map任务中检索的行的修改应更新地图中的值
    iter.getValue.setInt(0, 42)
    map.getAggregationBuffer(groupKey).getInt(0) should be (42)

    map.free()
  }
  //内存泄漏检测
  testWithMemoryLeakDetection("inserting large random keys") {//插入大随机键
    val map = new UnsafeFixedWidthAggregationMap(
      emptyAggregationBuffer,
      aggBufferSchema,
      groupKeySchema,
      taskMemoryManager,
      shuffleMemoryManager,
      128, // initial capacity 初始容量
      PAGE_SIZE_BYTES,
      false // disable perf metrics 禁用perf指标
    )
    val rand = new Random(42)
    val groupKeys: Set[String] = Seq.fill(512)(rand.nextString(1024)).toSet
    groupKeys.foreach { keyString =>
      assert(map.getAggregationBuffer(InternalRow(UTF8String.fromString(keyString))) != null)
    }

    val seenKeys = new mutable.HashSet[String]
    val iter = map.iterator()
    while (iter.next()) {
      seenKeys += iter.getKey.getString(0)
    }
    assert(seenKeys.size === groupKeys.size)
    assert(seenKeys === groupKeys)
    map.free()
  }
  //内存泄漏检测
  testWithMemoryLeakDetection("test external sorting") {//测试外部排序
    // Memory consumption in the beginning of the task.
    //任务开始时的内存消耗
    val initialMemoryConsumption = shuffleMemoryManager.getMemoryConsumptionForThisTask()

    val map = new UnsafeFixedWidthAggregationMap(
      emptyAggregationBuffer,
      aggBufferSchema,
      groupKeySchema,
      taskMemoryManager,
      shuffleMemoryManager,
      128, // initial capacity 初始容量
      PAGE_SIZE_BYTES,
      false // disable perf metrics 禁用的性能指标
    )

    val keys = randomStrings(1024).take(512)
    keys.foreach { keyString =>
      val buf = map.getAggregationBuffer(InternalRow(UTF8String.fromString(keyString)))
      buf.setInt(0, keyString.length)
      assert(buf != null)
    }

    // Convert the map into a sorter
    //转换Map分类
    val sorter = map.destructAndCreateExternalSorter()

    withClue(s"destructAndCreateExternalSorter should release memory used by the map") {
      assert(shuffleMemoryManager.getMemoryConsumptionForThisTask() === initialMemoryConsumption)
    }

    // Add more keys to the sorter and make sure the results come out sorted.
    //添加更多的键的排序和确定出来的结果排序
    val additionalKeys = randomStrings(1024)
    val keyConverter = UnsafeProjection.create(groupKeySchema)
    val valueConverter = UnsafeProjection.create(aggBufferSchema)

    additionalKeys.zipWithIndex.foreach { case (str, i) =>
      val k = InternalRow(UTF8String.fromString(str))
      val v = InternalRow(str.length)
      sorter.insertKV(keyConverter.apply(k), valueConverter.apply(v))

      if ((i % 100) == 0) {
        shuffleMemoryManager.markAsOutOfMemory()
        sorter.closeCurrentPage()
      }
    }

    val out = new scala.collection.mutable.ArrayBuffer[String]
    val iter = sorter.sortedIterator()
    while (iter.next()) {
      assert(iter.getKey.getString(0).length === iter.getValue.getInt(0))
      out += iter.getKey.getString(0)
    }

    assert(out === (keys ++ additionalKeys).sorted)

    map.free()
  }
  //用空映射进行外部排序的测试
  testWithMemoryLeakDetection("test external sorting with an empty map") {

    val map = new UnsafeFixedWidthAggregationMap(
      emptyAggregationBuffer,
      aggBufferSchema,
      groupKeySchema,
      taskMemoryManager,
      shuffleMemoryManager,
      128, // initial capacity 初始容量
      PAGE_SIZE_BYTES,
      false // disable perf metrics 禁用的性能指标
    )

    // Convert the map into a sorter
    //把map转换成分类
    val sorter = map.destructAndCreateExternalSorter()

    // Add more keys to the sorter and make sure the results come out sorted.
    //添加更多的键的分拣和确定出来的结果排序
    val additionalKeys = randomStrings(1024)
    val keyConverter = UnsafeProjection.create(groupKeySchema)
    val valueConverter = UnsafeProjection.create(aggBufferSchema)

    additionalKeys.zipWithIndex.foreach { case (str, i) =>
      val k = InternalRow(UTF8String.fromString(str))
      val v = InternalRow(str.length)
      sorter.insertKV(keyConverter.apply(k), valueConverter.apply(v))

      if ((i % 100) == 0) {
        shuffleMemoryManager.markAsOutOfMemory()
        sorter.closeCurrentPage()
      }
    }

    val out = new scala.collection.mutable.ArrayBuffer[String]
    val iter = sorter.sortedIterator()
    while (iter.next()) {
      // At here, we also test if copy is correct.
      //在这里,我们也测试如果复制是正确
      val key = iter.getKey.copy()
      val value = iter.getValue.copy()
      assert(key.getString(0).length === value.getInt(0))
      out += key.getString(0)
    }

    assert(out === (additionalKeys).sorted)

    map.free()
  }
  //用空记录进行外部排序的测试
  testWithMemoryLeakDetection("test external sorting with empty records") {

    // Memory consumption in the beginning of the task.
    //任务开始时的内存消耗
    val initialMemoryConsumption = shuffleMemoryManager.getMemoryConsumptionForThisTask()

    val map = new UnsafeFixedWidthAggregationMap(
      emptyAggregationBuffer,
      StructType(Nil),
      StructType(Nil),
      taskMemoryManager,
      shuffleMemoryManager,
      128, // initial capacity 初始容量
      PAGE_SIZE_BYTES,
      false // disable perf metrics 禁用的性能指标
    )

    (1 to 10).foreach { i =>
      val buf = map.getAggregationBuffer(UnsafeRow.createFromByteArray(0, 0))
      assert(buf != null)
    }

    // Convert the map into a sorter. Right now, it contains one record.
    ///将map到分类,现在它包含一个记录。
    val sorter = map.destructAndCreateExternalSorter()

    withClue(s"destructAndCreateExternalSorter should release memory used by the map") {
      assert(shuffleMemoryManager.getMemoryConsumptionForThisTask() === initialMemoryConsumption)
    }

    // Add more keys to the sorter and make sure the results come out sorted.
    //添加更多的键的分类和确定出来的结果排序
    (1 to 4096).foreach { i =>
      sorter.insertKV(UnsafeRow.createFromByteArray(0, 0), UnsafeRow.createFromByteArray(0, 0))

      if ((i % 100) == 0) {
        shuffleMemoryManager.markAsOutOfMemory()
        sorter.closeCurrentPage()
      }
    }

    var count = 0
    val iter = sorter.sortedIterator()
    while (iter.next()) {
      // At here, we also test if copy is correct.
      //在这里,我们也测试,如果正确复制
      iter.getKey.copy()
      iter.getValue.copy()
      count += 1
    }

    // 1 record was from the map and 4096 records were explicitly inserted.
    //1记录是从Map和4096个记录显式插入
    assert(count === 4097)

    map.free()
  }
  //在内存压力下在外部排序
  testWithMemoryLeakDetection("convert to external sorter under memory pressure (SPARK-10474)") {
    val smm = ShuffleMemoryManager.createForTesting(65536)
    val pageSize = 4096
    val map = new UnsafeFixedWidthAggregationMap(
      emptyAggregationBuffer,
      aggBufferSchema,
      groupKeySchema,
      taskMemoryManager,
      smm,
      128, // initial capacity 初始容量
      pageSize,
      false // disable perf metrics 禁用的性能指标
    )

    // Insert into the map until we've run out of space
    //插入到Map上,直到我们用完了空间
    val rand = new Random(42)
    var hasSpace = true
    while (hasSpace) {
      val str = rand.nextString(1024)
      val buf = map.getAggregationBuffer(InternalRow(UTF8String.fromString(str)))
      if (buf == null) {
        hasSpace = false
      } else {
        buf.setInt(0, str.length)
      }
    }

    // Ensure we're actually maxed out by asserting that we can't acquire even just 1 byte
    //确保我们实际上透支的说,我们无法获得哪怕只有1字节
    assert(smm.tryToAcquire(1) === 0)

    // Convert the map into a sorter. This used to fail before the fix for SPARK-10474
    // because we would try to acquire space for the in-memory sorter pointer array before
    // actually releasing the pages despite having spilled all of them.
    //将map转换成sorter,这在SPARK-10474的修复之前曾经失败因为我们将尝试为内存分配器指针数组获取空间
    //实际上释放页面,尽管已经溢出了所有这些页面
    var sorter: UnsafeKVExternalSorter = null
    try {
      sorter = map.destructAndCreateExternalSorter()
    } finally {
      if (sorter != null) {
        sorter.cleanupResources()
      }
    }
  }

}
