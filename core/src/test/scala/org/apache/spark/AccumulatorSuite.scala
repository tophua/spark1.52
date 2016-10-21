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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.ref.WeakReference

import org.scalatest.Matchers
import org.scalatest.exceptions.TestFailedException

import org.apache.spark.scheduler._

/***
  * 累加器测试用例
  */
class AccumulatorSuite extends SparkFunSuite with Matchers with LocalSparkContext {
  import InternalAccumulator._

  implicit def setAccum[A]: AccumulableParam[mutable.Set[A], A] =
    new AccumulableParam[mutable.Set[A], A] {
    //将两个累计值合并在一起,允许修改和回报效率第一值（避免配置对象）
      def addInPlace(t1: mutable.Set[A], t2: mutable.Set[A]) : mutable.Set[A] = {
        t1 ++= t2
        t1
      }
      //向累加器添加数据,允许修改和返回R的效率(以避免分配对象)
      def addAccumulator(t1: mutable.Set[A], t2: A) : mutable.Set[A] = {
        t1 += t2
        t1
      }
      //返回一个累加器类型的"零"(标识)值,给定它的初始值
      def zero(t: mutable.Set[A]) : mutable.Set[A] = {
        new mutable.HashSet[A]()
      }
    }

  test ("basic accumulation(累加器)"){
    sc = new SparkContext("local", "test")
    //声明累加器
    val acc : Accumulator[Int] = sc.accumulator(0)
    val d = sc.parallelize(1 to 20)
    d.foreach{x => 
      acc += x
      //打印累加器的值
      println(x+":"+acc)
      }
    //累加器的值是否相等210
    acc.value should be (210)

    val longAcc = sc.accumulator(0L)
    val maxInt = Integer.MAX_VALUE.toLong//
    d.foreach{x => longAcc += maxInt + x}
    longAcc.value should be (210L + maxInt * 20)
  }

  test ("value not assignable from tasks") {//任务不能分配的值
    sc = new SparkContext("local", "test")
    //声明累加器
    val acc : Accumulator[Int] = sc.accumulator(0)

    val d = sc.parallelize(1 to 20)
    an [Exception] should be thrownBy {d.foreach{x => acc.value = x}}
  }

  test ("add value to collection accumulators") {//累加器集合增加值
    val maxI = 1000
    //测试单和多线程
    for (nThreads <- List(1, 10)) { // test single & multi-threaded
      sc = new SparkContext("local[" + nThreads + "]", "test")
      val acc: Accumulable[mutable.Set[Any], Any] = sc.accumulable(new mutable.HashSet[Any]())
      val d = sc.parallelize(1 to maxI)
      d.foreach {
        x => acc += x
        //println(acc)
      }
      val v = acc.value.asInstanceOf[mutable.Set[Int]]//强制类型转换Set类型
      for (i <- 1 to maxI) {
        v should contain(i)//
      }
      resetSparkContext()
    }
  }
  
  test ("value not readable in tasks") {//任务中不可读的值
    val maxI = 1000
    //测试单和多线程
    for (nThreads <- List(1, 10)) { // test single & multi-threaded
      sc = new SparkContext("local[" + nThreads + "]", "test")
      val acc: Accumulable[mutable.Set[Any], Any] = sc.accumulable(new mutable.HashSet[Any]())
      val d = sc.parallelize(1 to maxI)
      an [SparkException] should be thrownBy {
        d.foreach {//acc.value任务中不可读值
          x => acc.value += x
        }
      }
      resetSparkContext()
    }
  }

  test ("collection accumulators") {//集合累加器
    val maxI = 1000
    for (nThreads <- List(1, 10)) {
      // test single & multi-threaded 测试单和多线程
      sc = new SparkContext("local[" + nThreads + "]", "test")
      val setAcc = sc.accumulableCollection(mutable.HashSet[Int]())
      val bufferAcc = sc.accumulableCollection(mutable.ArrayBuffer[Int]())
      val mapAcc = sc.accumulableCollection(mutable.HashMap[Int, String]())
      val d = sc.parallelize((1 to maxI) ++ (1 to maxI))
      d.foreach {
        x => {setAcc += x; bufferAcc += x; mapAcc += (x -> x.toString)}
      }

      // Note that this is typed correctly -- no casts necessary
      //请注意,这是类型正确-没有必要转换
      setAcc.value.size should be (maxI)
      bufferAcc.value.size should be (2 * maxI)
      mapAcc.value.size should be (maxI)
      for (i <- 1 to maxI) {
        setAcc.value should contain(i)
        bufferAcc.value should contain(i)
        mapAcc.value should contain (i -> i.toString)
      }
      resetSparkContext()
    }
  }

  test ("localValue readable in tasks") {//任务中的本地值可读性
    val maxI = 1000
    for (nThreads <- List(1, 10)) { // test single & multi-threaded
      sc = new SparkContext("local[" + nThreads + "]", "test")
      val acc: Accumulable[mutable.Set[Any], Any] = sc.accumulable(new mutable.HashSet[Any]())
      val groupedInts = (1 to (maxI/20)).map {x => (20 * (x - 1) to 20 * x).toSet}
      val d = sc.parallelize(groupedInts)
      d.foreach {
        x => acc.localValue ++= x
      }
      acc.value should be ( (0 to maxI).toSet)
      resetSparkContext()
    }
  }

  test ("garbage collection") {//垃圾回收
    // Create an accumulator and let it go out of scope to test that it's properly garbage collected
    //创建一个累加器,超出的范围,以测试它的垃圾回收
    sc = new SparkContext("local", "test")
    var acc: Accumulable[mutable.Set[Any], Any] = sc.accumulable(new mutable.HashSet[Any]())
    val accId = acc.id
    val ref = WeakReference(acc)

    // Ensure the accumulator is present
    //确保累加器的存在
    assert(ref.get.isDefined)

    // Remove the explicit reference to it and allow weak reference to get garbage collected
    //删除明确的引用,并允许弱引用获取垃圾收集
    acc = null
    System.gc()
    assert(ref.get.isEmpty)

    Accumulators.remove(accId)
    assert(!Accumulators.originals.get(accId).isDefined)
  }

  test("internal accumulators in TaskContext") {//在任务的上下文内的累加器
    sc = new SparkContext("local", "test")
    val accums = InternalAccumulator.create(sc)
    val taskContext = new TaskContextImpl(0, 0, 0, 0, null, null, accums)
    val internalMetricsToAccums = taskContext.internalMetricsToAccumulators //测量内部累加器
    val collectedInternalAccums = taskContext.collectInternalAccumulators()//内部累加器
    val collectedAccums = taskContext.collectAccumulators()
    assert(internalMetricsToAccums.size > 0)
    assert(internalMetricsToAccums.values.forall(_.isInternal))
    assert(internalMetricsToAccums.contains(TEST_ACCUMULATOR))
    val testAccum = internalMetricsToAccums(TEST_ACCUMULATOR)
    assert(collectedInternalAccums.size === internalMetricsToAccums.size)
    assert(collectedInternalAccums.size === collectedAccums.size)
    assert(collectedInternalAccums.contains(testAccum.id))
    assert(collectedAccums.contains(testAccum.id))
  }

  test("internal accumulators in a stage") {//在阶段内的累加器
    val listener = new SaveInfoListener
    val numPartitions = 10
    sc = new SparkContext("local", "test")
    sc.addSparkListener(listener)//添加一个SaveInfoListener监听器
    // Have each task add 1 to the internal accumulator    
    val rdd = sc.parallelize(1 to 100, numPartitions).mapPartitions { iter =>
      //获得每个任务的testAccumulator内部累加器并加1
      TaskContext.get().internalMetricsToAccumulators(TEST_ACCUMULATOR) += 1
      iter
    }
    // Register asserts in job completion callback to avoid flakiness
    //注册一个Job完成回调断点
    listener.registerJobCompletionCallback { _ =>
      
      val stageInfos = listener.getCompletedStageInfos //获得Stage完成
      val taskInfos = listener.getCompletedTaskInfos//获得任务完成
      assert(stageInfos.size === 1)
      assert(taskInfos.size === numPartitions)//任务完成数==分区数
      // The accumulator values should be merged in the stage
      //返回给定名称testAccumulator的累加器
      val stageAccum = findAccumulableInfo(stageInfos.head.accumulables.values, TEST_ACCUMULATOR)
      assert(stageAccum.value.toLong === numPartitions)
      // The accumulator should be updated locally on each task
      //在每个任务上,累加器应本地更新
      val taskAccumValues = taskInfos.map { taskInfo =>
        val taskAccum = findAccumulableInfo(taskInfo.accumulables, TEST_ACCUMULATOR)
        assert(taskAccum.update.isDefined)
        assert(taskAccum.update.get.toLong === 1)
        taskAccum.value.toLong
      }
      // Each task should keep track of the partial value on the way, i.e. 1, 2, ... numPartitions
      //每个任务保持跟踪都部分值1,2--分区数(10)
      println("taskAccumValues:"+taskAccumValues.sorted )
      assert(taskAccumValues.sorted === (1L to numPartitions).toSeq)
    }
    rdd.count()
  }

  test("internal accumulators in multiple stages") {//在多个阶段的内部累加器
    val listener = new SaveInfoListener
    val numPartitions = 10
    sc = new SparkContext("local", "test")
    sc.addSparkListener(listener)//注册监听器
    // Each stage creates its own set of internal accumulators so the    
    // values for the same metric should not be mixed up across stages
    //每个阶段创建其自己的一套内部累加器,因此,相同的度量的值不应该被混合跨阶段
    val rdd = sc.parallelize(1 to 100, numPartitions)
      .map { i => (i, i) }
      .mapPartitions { iter =>
        TaskContext.get().internalMetricsToAccumulators(TEST_ACCUMULATOR) += 1
        iter
      }//第一个Stage
      .reduceByKey { case (x, y) => x + y }
      .mapPartitions { iter =>
        TaskContext.get().internalMetricsToAccumulators(TEST_ACCUMULATOR) += 10
        iter
      }//第二个Stage
      .repartition(numPartitions * 2)
      .mapPartitions { iter =>//第三个Stage
        TaskContext.get().internalMetricsToAccumulators(TEST_ACCUMULATOR) += 100
        iter
      }
    // Register asserts in job completion callback to avoid flakiness
     //注册一个Job完成回调断点
    listener.registerJobCompletionCallback { _ =>
      // We ran 3 stages, and the accumulator values should be distinct
      //我们运行3个阶段,累加器的值应该是不同的
      val stageInfos = listener.getCompletedStageInfos
      assert(stageInfos.size === 3)//3个阶段
      val (firstStageAccum, secondStageAccum, thirdStageAccum) =
        (findAccumulableInfo(stageInfos(0).accumulables.values, TEST_ACCUMULATOR),
        findAccumulableInfo(stageInfos(1).accumulables.values, TEST_ACCUMULATOR),
        findAccumulableInfo(stageInfos(2).accumulables.values, TEST_ACCUMULATOR))
      assert(firstStageAccum.value.toLong === numPartitions)//第一个阶段的累加值
      assert(secondStageAccum.value.toLong === numPartitions * 10)//第二个阶段的累加值
      assert(thirdStageAccum.value.toLong === numPartitions * 2 * 100)//第三个阶段的累加值
    }
    rdd.count()
  }

  test("internal accumulators in fully resubmitted stages") {//在完全提交阶段内部累加器
    testInternalAccumulatorsWithFailedTasks((i: Int) => true) // fail all tasks 失败的所有任务
  }

  test("internal accumulators in partially resubmitted stages") {//在部分提交阶段内部累加器
    testInternalAccumulatorsWithFailedTasks((i: Int) => i % 2 == 0) // fail a subset 失败的一个子集
  }

  /**
   * Return the accumulable info that matches the specified name.
   * 返回查找给名称的累加器
   */
  private def findAccumulableInfo(
      accums: Iterable[AccumulableInfo],
      name: String): AccumulableInfo = {
    accums.find { a => a.name == name }.getOrElse {
      throw new TestFailedException(s"internal accumulator '$name' not found", 0)
    }
  }

  /**
   * Test whether internal accumulators are merged properly if some tasks fail.
   * 测试是否内部累加器合并正确如果任务失败
   */
  private def testInternalAccumulatorsWithFailedTasks(failCondition: (Int => Boolean)): Unit = {
    val listener = new SaveInfoListener
    val numPartitions = 10
    val numFailedPartitions = (0 until numPartitions).count(failCondition)
    // This says use 1 core and retry tasks up to 2 times
    //这是1个内核重试任务2次
    sc = new SparkContext("local[1, 2]", "test")
    sc.addSparkListener(listener)
    val rdd = sc.parallelize(1 to 100, numPartitions).mapPartitionsWithIndex { case (i, iter) =>
      val taskContext = TaskContext.get()
      taskContext.internalMetricsToAccumulators(TEST_ACCUMULATOR) += 1
      // Fail the first attempts of a subset of the tasks
      //失败的第一次尝试的一个子集的任务
      if (failCondition(i) && taskContext.attemptNumber() == 0) {
        throw new Exception("Failing a task intentionally.")
      }
      iter
    }
    // Register asserts in job completion callback to avoid flakiness
    //注册Job完成的回调断点
    listener.registerJobCompletionCallback { _ =>
      val stageInfos = listener.getCompletedStageInfos
      val taskInfos = listener.getCompletedTaskInfos
      assert(stageInfos.size === 1)
      assert(taskInfos.size === numPartitions + numFailedPartitions)
      val stageAccum = findAccumulableInfo(stageInfos.head.accumulables.values, TEST_ACCUMULATOR)
      // We should not double count values in the merged accumulator
      //不应该在合并后的累加器中的双计数值
      assert(stageAccum.value.toLong === numPartitions)
      val taskAccumValues = taskInfos.flatMap { taskInfo =>
        if (!taskInfo.failed) {
          // If a task succeeded, its update value should always be 1
          //如果一个任务成功了,它的更新值应该始终是1
          val taskAccum = findAccumulableInfo(taskInfo.accumulables, TEST_ACCUMULATOR)
          assert(taskAccum.update.isDefined)
          assert(taskAccum.update.get.toLong === 1)
          Some(taskAccum.value.toLong)
        } else {
          // If a task failed, we should not get its accumulator values
          //如果一个任务失败,我们不应该得到它的累加值
          assert(taskInfo.accumulables.isEmpty)
          None
        }
      }
      assert(taskAccumValues.sorted === (1L to numPartitions).toSeq)
    }
    rdd.count()
  }

}

private[spark] object AccumulatorSuite {

  /**
   * Run one or more Spark jobs and verify that the peak execution memory accumulator
   * is updated afterwards.
   * 行一个或多个Spark作业,并验证执行内存储器的事后更新
   */
  def verifyPeakExecutionMemorySet(
      sc: SparkContext,
      testName: String)(testBody: => Unit): Unit = {
    val listener = new SaveInfoListener
    sc.addSparkListener(listener)
    // Register asserts in job completion callback to avoid flakiness
    //注册job完成回调
    listener.registerJobCompletionCallback { jobId =>
      if (jobId == 0) {
        // The first job is a dummy one to verify that the accumulator does not already exist
        //第一个作业是一个虚拟的,验证是否已经存在了
        val accums = listener.getCompletedStageInfos.flatMap(_.accumulables.values)
        assert(!accums.exists(_.name == InternalAccumulator.PEAK_EXECUTION_MEMORY))
      } else {
        // In the subsequent jobs, verify that peak execution memory is updated
        val accum = listener.getCompletedStageInfos
          .flatMap(_.accumulables.values)
          .find(_.name == InternalAccumulator.PEAK_EXECUTION_MEMORY)
          .getOrElse {
          throw new TestFailedException(
            s"peak execution memory accumulator not set in '$testName'", 0)
        }
        assert(accum.value.toLong > 0)
      }
    }
    // Run the jobs 运行工作
    sc.parallelize(1 to 10).count()
    testBody
  }
}

/**
 * A simple listener that keeps track of the TaskInfos and StageInfos of all completed jobs.
 * 一个简单的侦听器,跟踪的taskinfos和所有已完成的工作stageinfos
 */
private class SaveInfoListener extends SparkListener {
  private val completedStageInfos: ArrayBuffer[StageInfo] = new ArrayBuffer[StageInfo]
  private val completedTaskInfos: ArrayBuffer[TaskInfo] = new ArrayBuffer[TaskInfo]
  //参数是作业标识
  private var jobCompletionCallback: (Int => Unit) = null // parameter is job ID

  def getCompletedStageInfos: Seq[StageInfo] = completedStageInfos.toArray.toSeq
  def getCompletedTaskInfos: Seq[TaskInfo] = completedTaskInfos.toArray.toSeq

  /** 
   *  Register a callback to be called on job end.
   *  注册一个在作业结束时调用的回调函数
   *   */
  def registerJobCompletionCallback(callback: (Int => Unit)): Unit = {
    jobCompletionCallback = callback //匿名方法
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    if (jobCompletionCallback != null) {
      jobCompletionCallback(jobEnd.jobId)
    }
  }
  //Stag完成
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    completedStageInfos += stageCompleted.stageInfo
  }
  //任务结结束时
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    completedTaskInfos += taskEnd.taskInfo
  }
}
