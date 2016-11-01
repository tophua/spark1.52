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

package org.apache.spark.scheduler

import java.util.Properties

import scala.collection.mutable.{ArrayBuffer, HashSet, HashMap, Map}
import scala.language.reflectiveCalls
import scala.util.control.NonFatal

import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.Timeouts
import org.scalatest.time.SpanSugar._

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.storage.{BlockId, BlockManagerId, BlockManagerMaster}
import org.apache.spark.util.CallSite
import org.apache.spark.executor.TaskMetrics

class DAGSchedulerEventProcessLoopTester(dagScheduler: DAGScheduler)
  extends DAGSchedulerEventProcessLoop(dagScheduler) {

  override def post(event: DAGSchedulerEvent): Unit = {
    try {
      // Forward event to `onReceive` directly to avoid processing event asynchronously.
      //直接跳转onReceive事件,避免异步进程事件
      onReceive(event)
    } catch {
      case NonFatal(e) => onError(e)
    }
  }
}

/**
 * An RDD for passing to DAGScheduler. These RDDs will use the dependencies and
 * RDD通过DAGScheduler,这些RDDS将使用依赖性和preferredlocations(如果有)传递给他们
 * preferredLocations (if any) that are passed to them. They are deliberately not executable
 * so we can test that DAGScheduler does not try to execute RDDs locally.
 * 他们是故意不可执行,所以我们可以测试DAGScheduler不想执行本地的RDDS
 */
class MyRDD(
    sc: SparkContext,
    numPartitions: Int,
    dependencies: List[Dependency[_]],//依赖
    locations: Seq[Seq[String]] = Nil) extends RDD[(Int, Int)](sc, dependencies) with Serializable {
  override def compute(split: Partition, context: TaskContext): Iterator[(Int, Int)] =
    throw new RuntimeException("should not be reached")
  override def getPartitions: Array[Partition] = (0 until numPartitions).map(i => new Partition {
    override def index: Int = i
  }).toArray
  override def getPreferredLocations(split: Partition): Seq[String] =
    if (locations.isDefinedAt(split.index)) locations(split.index) else Nil
  override def toString: String = "DAGSchedulerSuiteRDD " + id
}
/**
 * DAGScheduler决定了运行Task的理想位置，并把这些信息传递给下层的TaskScheduler
 * DAGScheduler还处理由于Shuffle数据丢失导致的失败，
 * 这有可能需要重新提交运行之前的Stage（非Shuffle数据丢失导致的Task失败由TaskScheduler处理）
 */

class DAGSchedulerSuiteDummyException extends Exception
//DAGSchedulerDAGScheduler的主要任务是基于Stage构建DAG,
//决定每个任务的最佳位置 记录哪个RDD或者Stage输出被物化 面向stage的调度层
//为job生成以stage组成的DAG，提交TaskSet给TaskScheduler执行
//重新提交shuffle输出丢失的stage
class DAGSchedulerSuite
  extends SparkFunSuite with BeforeAndAfter with LocalSparkContext with Timeouts {

  val conf = new SparkConf
  /** 
   *  Set of TaskSets the DAGScheduler has requested executed.
   *  请求执行器设置DAGScheduler的TaskSets
   *   
   *  */
  val taskSets = scala.collection.mutable.Buffer[TaskSet]()

  /** Stages for which the DAGScheduler has called TaskScheduler.cancelTasks(). */
  //调用TaskScheduler.cancelTasks()任务
  val cancelledStages = new HashSet[Int]()

  val taskScheduler = new TaskScheduler() {
    override def rootPool: Pool = null
    override def schedulingMode: SchedulingMode = SchedulingMode.NONE
    override def start() = {}
    override def stop() = {}
    override def executorHeartbeatReceived(execId: String, taskMetrics: Array[(Long, TaskMetrics)],
      blockManagerId: BlockManagerId): Boolean = true
    override def submitTasks(taskSet: TaskSet) = {
      // normally done by TaskSetManager
      // TaskSetManager正常完成
      taskSet.tasks.foreach(_.epoch = mapOutputTracker.getEpoch)
      taskSets += taskSet
    }
    //取消任务
    override def cancelTasks(stageId: Int, interruptThread: Boolean) {
      cancelledStages += stageId
    }
    override def setDAGScheduler(dagScheduler: DAGScheduler) = {}
    override def defaultParallelism() = 2 //默认并发数
    override def executorLost(executorId: String, reason: ExecutorLossReason): Unit = {}
    override def applicationAttemptId(): Option[String] = None
  }

  /** 
   *  Length of time to wait while draining listener events.
   *  侦听事件等待的超时时间
   *  */
  val WAIT_TIMEOUT_MILLIS = 10000//毫秒
  val sparkListener = new SparkListener() {
    val submittedStageInfos = new HashSet[StageInfo]
    val successfulStages = new HashSet[Int]
    val failedStages = new ArrayBuffer[Int]
    val stageByOrderOfExecution = new ArrayBuffer[Int]

    override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) {
      submittedStageInfos += stageSubmitted.stageInfo
    }

    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) {
      //StageInfo
      val stageInfo = stageCompleted.stageInfo
      //阶段排序的执行器
      stageByOrderOfExecution += stageInfo.stageId
      if (stageInfo.failureReason.isEmpty) {
        //完成的Stage
        successfulStages += stageInfo.stageId
      } else {
        //失败的Stage
        failedStages += stageInfo.stageId
      }
    }
  }

  var mapOutputTracker: MapOutputTrackerMaster = null
  var scheduler: DAGScheduler = null
  var dagEventProcessLoopTester: DAGSchedulerEventProcessLoop = null

  /**
   * Set of cache locations to return from our mock BlockManagerMaster.
   * 设置缓存的位置,返回我们模拟blockmanagermaster
   * Keys are (rdd ID, partition ID). Anything not present will return an empty
   * 键(RDD ID,分区ID),任何不存在的东西都返回一个缓存位置的空列表
   * list of cache locations silently.
   */
  val cacheLocations = new HashMap[(Int, Int), Seq[BlockManagerId]]
  // stub out BlockManagerMaster.getLocations to use our cacheLocations
  //我们使用cacheLocations获得位置
  val blockManagerMaster = new BlockManagerMaster(null, conf, true) {
      override def getLocations(blockIds: Array[BlockId]): IndexedSeq[Seq[BlockManagerId]] = {
      var test=null
        blockIds.map {
          _.asRDDId.map(id =>
            (id.rddId -> id.splitIndex)          
            ).flatMap(key => cacheLocations.get(key)).
            getOrElse(Seq())
        }.toIndexedSeq
      }
      override def removeExecutor(execId: String) {
        // don't need to propagate to the driver, which we don't have
        //不需要传播到驱动程序,我们没有
      }
    }

  /** 
   *  The list of results that DAGScheduler has collected.
   *  DAGScheduler集合结果的列表 
   *  */
  val results = new HashMap[Int, Any]()
  var failure: Exception = _
  val jobListener = new JobListener() {
    override def taskSucceeded(index: Int, result: Any) = results.put(index, result)
    override def jobFailed(exception: Exception) = { failure = exception }
  }

  before {
    sc = new SparkContext("local", "DAGSchedulerSuite")
    sparkListener.submittedStageInfos.clear()
    sparkListener.successfulStages.clear()
    sparkListener.failedStages.clear()
    failure = null
    sc.addSparkListener(sparkListener)
    taskSets.clear()
    cancelledStages.clear()
    cacheLocations.clear()
    results.clear()
    mapOutputTracker = new MapOutputTrackerMaster(conf)
    scheduler = new DAGScheduler(
        sc,
        taskScheduler,
        sc.listenerBus,
        mapOutputTracker,
        blockManagerMaster,
        sc.env)
    dagEventProcessLoopTester = new DAGSchedulerEventProcessLoopTester(scheduler)
  }

  after {
    scheduler.stop()
  }

  override def afterAll() {
    super.afterAll()
  }

  /**
   * Type of RDD we use for testing. Note that we should never call the real RDD compute methods.
   * 我们用于测试RDD类型,注意:我们不应该调用真正的RDD的计算方法
   * This is a pair RDD type so it can always be used in ShuffleDependencies.
   * 这是一个RDD类型对,所以它总是在ShuffleDependencies调用
   */
  type PairOfIntsRDD = RDD[(Int, Int)]

  /**
   * Process the supplied event as if it were the top of the DAGScheduler event queue, expecting
   * the scheduler not to exit.
   * 处理所支持的事件,如果是dagscheduler顶部队列事件,期望调度不退出
   *
   * After processing the event, submit waiting stages as is done on most iterations of the   
   * DAGScheduler event loop.
   * 处理事件后,提交等待阶段对dagscheduler事件循环迭代完成
   */
  private def runEvent(event: DAGSchedulerEvent) {
    dagEventProcessLoopTester.post(event)
  }

  /**
   * When we submit dummy Jobs, this is the compute function we supply. Except in a local test
   * 当我们提交虚拟Job,这是我们提供的计算功能,除了在下面的一个本地测试
   * below, we do not expect this function to ever be executed; instead, we will return results
   * 我们不希望这个函数被执行,相反,我们将返回的结果直接通过completionevents
   * directly through CompletionEvents.
   */
  private val jobComputeFunc = (context: TaskContext, it: Iterator[(_)]) =>
     it.next.asInstanceOf[Tuple2[_, _]]._1

  /** 
   *  Send the given CompletionEvent messages for the tasks in the TaskSet. 
   *  在taskset的任务发送消息给completionevent
   *  */
  private def complete(taskSet: TaskSet, results: Seq[(TaskEndReason, Any)]) {
    assert(taskSet.tasks.size >= results.size)
    for ((result, i) <- results.zipWithIndex) {
      if (i < taskSet.tasks.size) {
        runEvent(CompletionEvent(//result._1返馈,._2结果
          taskSet.tasks(i), result._1, result._2, null, createFakeTaskInfo(), null))
      }
    }
  }

  private def completeWithAccumulator(accumId: Long, taskSet: TaskSet,
                                      results: Seq[(TaskEndReason, Any)]) {
    assert(taskSet.tasks.size >= results.size)
    for ((result, i) <- results.zipWithIndex) {
      if (i < taskSet.tasks.size) {
        runEvent(CompletionEvent(taskSet.tasks(i), result._1, result._2,
          Map[Long, Any]((accumId, 1)), createFakeTaskInfo(), null))
      }
    }
  }

  /** 
   *  Sends the rdd to the scheduler for scheduling and returns the job id. 
   *  发送RDD调度的调度器和返回工作的ID
   *  */
  private def submit(
      rdd: RDD[_],
      partitions: Array[Int],
      func: (TaskContext, Iterator[_]) => _ = jobComputeFunc,
      listener: JobListener = jobListener,
      properties: Properties = null): Int = {
    val jobId = scheduler.nextJobId.getAndIncrement()
    runEvent(JobSubmitted(jobId, rdd, func, partitions, CallSite("", ""), listener, properties))
    jobId
  }

  /** 
   *  Sends TaskSetFailed to the scheduler. 
   *  发送tasksetfailed的调度
   *  */
  private def failed(taskSet: TaskSet, message: String) {
    runEvent(TaskSetFailed(taskSet, message, None))
  }

  /** 
   *  Sends JobCancelled to the DAG scheduler. 
   *  发送jobcancelled的DAG调度
   *  */
  private def cancel(jobId: Int) {
    runEvent(JobCancelled(jobId))
  }
  //父阶段应该有较低的阶段标识
  test("[SPARK-3353] parent stage should have lower stage id") {
    sparkListener.stageByOrderOfExecution.clear()
    sc.parallelize(1 to 10).map(x => (x, x)).reduceByKey(_ + _, 4).count()
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(sparkListener.stageByOrderOfExecution.length === 2)
    assert(sparkListener.stageByOrderOfExecution(0) < sparkListener.stageByOrderOfExecution(1))
  }

  test("zero split job") {//RDD零分隔的Job
    var numResults = 0
    val fakeListener = new JobListener() {//Job监听
      override def taskSucceeded(partition: Int, value: Any) = numResults += 1
      override def jobFailed(exception: Exception) = throw exception
    }
    val jobId = submit(new MyRDD(sc, 0, Nil), Array(), listener = fakeListener)
    assert(numResults === 0)
    cancel(jobId)
  }

  test("run trivial job") {//运行无价值的工作
    submit(new MyRDD(sc, 1, Nil), Array(0))
    complete(taskSets(0), List((Success, 42)))
    //JobListener监听完成
    assert(results === Map(0 -> 42))
    //清空数据结构
    assertDataStructuresEmpty()
  }

  test("run trivial job w/ dependency") {//运行无价值的Job依赖
    val baseRdd = new MyRDD(sc, 1, Nil)
    val finalRdd = new MyRDD(sc, 1, List(new OneToOneDependency(baseRdd)))
    submit(finalRdd, Array(0))
    complete(taskSets(0), Seq((Success, 42)))
    assert(results === Map(0 -> 42))
    //清空数据结构
    assertDataStructuresEmpty()
  }

  test("cache location preferences w/ dependency") {//缓存位置偏好依赖
    val baseRdd = new MyRDD(sc, 1, Nil).cache()
    val finalRdd = new MyRDD(sc, 1, List(new OneToOneDependency(baseRdd)))
    cacheLocations(baseRdd.id -> 0) =//数组的赋值
      Seq(makeBlockManagerId("hostA"), makeBlockManagerId("hostB"))
    submit(finalRdd, Array(0))
    val taskSet = taskSets(0)
    assertLocations(taskSet, Seq(Seq("hostA", "hostB")))
    complete(taskSet, Seq((Success, 42)))
    assert(results === Map(0 -> 42))
    assertDataStructuresEmpty()
  }

  test("regression test for getCacheLocs") {//对于getcachelocs回归测试
    val rdd = new MyRDD(sc, 3, Nil).cache()//三个分区
    cacheLocations(rdd.id -> 0) =//赋值
      Seq(makeBlockManagerId("hostA"), makeBlockManagerId("hostB"))
    cacheLocations(rdd.id -> 1) =
      Seq(makeBlockManagerId("hostB"), makeBlockManagerId("hostC"))
    cacheLocations(rdd.id -> 2) =
      Seq(makeBlockManagerId("hostC"), makeBlockManagerId("hostD"))
      //获得缓存的位置
    val locs = scheduler.getCacheLocs(rdd).map(_.map(_.host))
    assert(locs === Seq(Seq("hostA", "hostB"), Seq("hostB", "hostC"), Seq("hostC", "hostD")))
  }

  /**
   * This test ensures that if a particular RDD is cached, RDDs earlier in the dependency chain   
   * are not computed. It constructs the following chain of dependencies:
   * 这个测试以确保如果某RDD缓存,较早的RDD在依赖链中没有计算,它构建了下面的依赖链：
   * +---+ shuffle +---+    +---+    +---+
   * | A |<--------| B |<---| C |<---| D |
   * +---+         +---+    +---+    +---+
   * Here, B is derived from A by performing a shuffle, C has a one-to-one dependency on B,   
   * and D similarly has a one-to-one dependency on C. If none of the RDDs were cached, this
   * set of RDDs would result in a two stage job: one ShuffleMapStage, and a ResultStage that
   * reads the shuffled data from RDD A. This test ensures that if C is cached, the scheduler
   * doesn't perform a shuffle, and instead computes the result using a single ResultStage
   * that reads C's cached data.
   */
  //应该考虑所有父RDDS”缓存状态
  test("getMissingParentStages should consider all ancestor RDDs' cache statuses") {
    val rddA = new MyRDD(sc, 1, Nil)
    val rddB = new MyRDD(sc, 1, List(new ShuffleDependency(rddA, null)))
    val rddC = new MyRDD(sc, 1, List(new OneToOneDependency(rddB))).cache()
    val rddD = new MyRDD(sc, 1, List(new OneToOneDependency(rddC)))
    cacheLocations(rddC.id -> 0) =
      Seq(makeBlockManagerId("hostA"), makeBlockManagerId("hostB"))
    submit(rddD, Array(0))
    assert(scheduler.runningStages.size === 1)
    // Make sure that the scheduler is running the final result stage.
    //确保调度程序运行的是最终结果阶段
    // Because C is cached, the shuffle map stage to compute A does not need to be run.
    //因为C是缓存,Shuffle的Map任务阶段来计算A不需要运行
    assert(scheduler.runningStages.head.isInstanceOf[ResultStage])
  }
  //避免指数爆破时优先位置列表
  test("avoid exponential blowup when getting preferred locs list") {
    // Build up a complex dependency graph with repeated zip operations, without preferred locations
    //建立一个重复的压缩操作的复杂的依赖关系图,不喜欢的位置
    var rdd: RDD[_] = new MyRDD(sc, 1, Nil)
    (1 to 30).foreach(_ => rdd = rdd.zip(rdd))
    // getPreferredLocs runs quickly, indicating that exponential graph traversal is avoided.
    //getPreferredLocs运行快速,避免索引遍历
    failAfter(10 seconds) {
      //返回每一个数据数据块所在的机器名或者IP地址，
      //如果每一块数据是多份存储的，那么就会返回多个机器地址
      val preferredLocs = scheduler.getPreferredLocs(rdd, 0) 
      // No preferred locations are returned.
      //返回没有首选的位置
      assert(preferredLocs.length === 0)
    }
  }

  test("unserializable task") {//未序列任务
    val unserializableRdd = new MyRDD(sc, 1, Nil) {
      class UnserializableClass
      val unserializable = new UnserializableClass
    }
    submit(unserializableRdd, Array(0))
    assert(failure.getMessage.startsWith(//工作阶段失败而终止
      "Job aborted due to stage failure: Task not serializable:"))
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(sparkListener.failedStages.contains(0))
    assert(sparkListener.failedStages.size === 1)
    assertDataStructuresEmpty()
  }

  test("trivial job failure") {//无价值的Job失败
    submit(new MyRDD(sc, 1, Nil), Array(0))
    failed(taskSets(0), "some failure")
    //工作阶段失败而终止:一些失败
    assert(failure.getMessage === "Job aborted due to stage failure: some failure")
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(sparkListener.failedStages.contains(0))
    assert(sparkListener.failedStages.size === 1)
    assertDataStructuresEmpty()
  }

  test("trivial job cancellation") {//无价值的工作取消
    val rdd = new MyRDD(sc, 1, Nil)
    val jobId = submit(rdd, Array(0))
    cancel(jobId)
    assert(failure.getMessage === s"Job $jobId cancelled ")
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(sparkListener.failedStages.contains(0))
    assert(sparkListener.failedStages.size === 1)
    assertDataStructuresEmpty()
  }

  test("job cancellation no-kill backend") {//作业取消不杀死后端
    // make sure that the DAGScheduler doesn't crash when the TaskScheduler
    //确保DAGScheduler不崩溃时，任务调度器不执行killtask()
    // doesn't implement killTask()
    val noKillTaskScheduler = new TaskScheduler() {
      override def rootPool: Pool = null
      override def schedulingMode: SchedulingMode = SchedulingMode.NONE
      override def start(): Unit = {}
      override def stop(): Unit = {}
      override def submitTasks(taskSet: TaskSet): Unit = {
        taskSets += taskSet
      }
      override def cancelTasks(stageId: Int, interruptThread: Boolean) {
        throw new UnsupportedOperationException
      }
      override def setDAGScheduler(dagScheduler: DAGScheduler): Unit = {}
      override def defaultParallelism(): Int = 2 //默认并发数
      override def executorHeartbeatReceived(
          execId: String,
          taskMetrics: Array[(Long, TaskMetrics)],
          blockManagerId: BlockManagerId): Boolean = true
      override def executorLost(executorId: String, reason: ExecutorLossReason): Unit = {}
      override def applicationAttemptId(): Option[String] = None
    }
    val noKillScheduler = new DAGScheduler(
      sc,
      noKillTaskScheduler,
      sc.listenerBus,
      mapOutputTracker,
      blockManagerMaster,
      sc.env)
    dagEventProcessLoopTester = new DAGSchedulerEventProcessLoopTester(noKillScheduler)
    val jobId = submit(new MyRDD(sc, 1, Nil), Array(0))
    cancel(jobId)
    // Because the job wasn't actually cancelled, we shouldn't have received a failure message.
    //因为job并没有被取消,我们不应该收到一个失败的消息
    assert(failure === null)

    // When the task set completes normally, state should be correctly updated.
    //当任务集合正常完成时,状态应该正确更新
    complete(taskSets(0), Seq((Success, 42)))
    assert(results === Map(0 -> 42))
    assertDataStructuresEmpty()

    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(sparkListener.failedStages.isEmpty)
    assert(sparkListener.successfulStages.contains(0))
  }

  test("run trivial shuffle") {//运行无价值的shuffle
    val shuffleMapRdd = new MyRDD(sc, 2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, null)
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 1, List(shuffleDep))
    submit(reduceRdd, Array(0))
    complete(taskSets(0), Seq(
        (Success, makeMapStatus("hostA", 1)),
        (Success, makeMapStatus("hostB", 1))))
    assert(mapOutputTracker.getMapSizesByExecutorId(shuffleId, 0).map(_._1).toSet ===
      HashSet(makeBlockManagerId("hostA"), makeBlockManagerId("hostB")))
    complete(taskSets(1), Seq((Success, 42)))
    assert(results === Map(0 -> 42))
    assertDataStructuresEmpty()
  }
  //运行无价值的Shuffle与获取失败
  test("run trivial shuffle with fetch failure") {
    val shuffleMapRdd = new MyRDD(sc, 2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, null)
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 2, List(shuffleDep))
    submit(reduceRdd, Array(0, 1))
    complete(taskSets(0), Seq(
        (Success, makeMapStatus("hostA", reduceRdd.partitions.size)),
        (Success, makeMapStatus("hostB", reduceRdd.partitions.size))))
    //the 2nd ResultTask failed
    //第二个resulttask失败
    complete(taskSets(1), Seq(
        (Success, 42),
        (FetchFailed(makeBlockManagerId("hostA"), shuffleId, 0, 0, "ignored"), null)))
    // this will get called 这将被称为
    // blockManagerMaster.removeExecutor("exec-hostA")
    // ask the scheduler to try it again 请求一次调度
    scheduler.resubmitFailedStages()
    // have the 2nd attempt pass 第二次尝试通过
    complete(taskSets(2), Seq((Success, makeMapStatus("hostA", reduceRdd.partitions.size))))
    // we can see both result blocks now
    //我们现在可以看到两个结果块
    assert(mapOutputTracker.getMapSizesByExecutorId(shuffleId, 0).map(_._1.host).toSet ===
      HashSet("hostA", "hostB"))
    complete(taskSets(3), Seq((Success, 43)))
    assert(results === Map(0 -> 42, 1 -> 43))
    assertDataStructuresEmpty()
  }
  //多个获取失败的无价值的Shuffle
  test("trivial shuffle with multiple fetch failures") {
    val shuffleMapRdd = new MyRDD(sc, 2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, null)
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 2, List(shuffleDep))
    submit(reduceRdd, Array(0, 1))
    complete(taskSets(0), Seq(
      (Success, makeMapStatus("hostA", reduceRdd.partitions.size)),
      (Success, makeMapStatus("hostB", reduceRdd.partitions.size))))
    // The MapOutputTracker should know about both map output locations.
    //MapOutputTracker应该知道Map任务的输出位置
    assert(mapOutputTracker.getMapSizesByExecutorId(shuffleId, 0).map(_._1.host).toSet ===
      HashSet("hostA", "hostB"))

    // The first result task fails, with a fetch failure for the output from the first mapper.
    // 第一个结果任务失败,获取一个输出第一个Map的输出故障
    runEvent(CompletionEvent(
      taskSets(1).tasks(0),
      FetchFailed(makeBlockManagerId("hostA"), shuffleId, 0, 0, "ignored"),
      null,
      Map[Long, Any](),
      createFakeTaskInfo(),
      null))
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(sparkListener.failedStages.contains(1))

    // The second ResultTask fails, with a fetch failure for the output from the second mapper.
    //第二次resulttask失败,获取失败第二个Map的输出故障
    runEvent(CompletionEvent(
      taskSets(1).tasks(0),
      FetchFailed(makeBlockManagerId("hostA"), shuffleId, 1, 1, "ignored"),
      null,
      Map[Long, Any](),
      createFakeTaskInfo(),
      null))
    // The SparkListener should not receive redundant failure events.
    // SparkListener不应该得到冗余故障事件
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(sparkListener.failedStages.size == 1)
  }

  /**
   * This tests the case where another FetchFailed comes in while the map stage is getting
   * re-run.
   * 这个测试的情况下,另一个fetchfailed进来而Map任务阶段开始重新运行  
   */
  //后期获取失败不会导致同一个映射阶段的多个并发尝试
  test("late fetch failures don't cause multiple concurrent attempts for the same map stage") {
    val shuffleMapRdd = new MyRDD(sc, 2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, null)
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 2, List(shuffleDep))
    submit(reduceRdd, Array(0, 1))

    val mapStageId = 0
    def countSubmittedMapStageAttempts(): Int = {
      sparkListener.submittedStageInfos.count(_.stageId == mapStageId)
    }

    // The map stage should have been submitted.
    //Map阶段应该已经提交
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(countSubmittedMapStageAttempts() === 1)

    complete(taskSets(0), Seq(
      (Success, makeMapStatus("hostA", 2)),
      (Success, makeMapStatus("hostB", 2))))
    // The MapOutputTracker should know about both map output locations.
    //MapOutputTracker应该知道Map输出位置
    assert(mapOutputTracker.getMapSizesByExecutorId(shuffleId, 0).map(_._1.host).toSet ===
      HashSet("hostA", "hostB"))
    assert(mapOutputTracker.getMapSizesByExecutorId(shuffleId, 1).map(_._1.host).toSet ===
      HashSet("hostA", "hostB"))

    // The first result task fails, with a fetch failure for the output from the first mapper.
    //第一个结果任务失败,获取第一个从Map输出故障。
    runEvent(CompletionEvent(
      taskSets(1).tasks(0),
      FetchFailed(makeBlockManagerId("hostA"), shuffleId, 0, 0, "ignored"),
      null,
      Map[Long, Any](),
      createFakeTaskInfo(),
      null))
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(sparkListener.failedStages.contains(1))

    // Trigger resubmission of the failed map stage.
    //触发失败的Map阶段提交
    runEvent(ResubmitFailedStages)
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)

    // Another attempt for the map stage should have been submitted, resulting in 2 total attempts.
    //Map阶段的另一个尝试应该已经提交,尝试2次结果
    assert(countSubmittedMapStageAttempts() === 2)

    // The second ResultTask fails, with a fetch failure for the output from the second mapper.
    //第二次resulttask失败,一个获取失败在第二个Map任务的输出故障
    runEvent(CompletionEvent(
      taskSets(1).tasks(1),
      FetchFailed(makeBlockManagerId("hostB"), shuffleId, 1, 1, "ignored"),
      null,
      Map[Long, Any](),
      createFakeTaskInfo(),
      null))

    // Another ResubmitFailedStages event should not result in another attempt for the map
    // stage being run concurrently.
    //另一个resubmitfailedstages事件不应导致对地图的另一个尝试
    //兼运行阶段
    // NOTE: the actual ResubmitFailedStages may get called at any time during this, but it
    // shouldn't effect anything -- our calling it just makes *SURE* it gets called between the
    // desired event and our check.
    runEvent(ResubmitFailedStages)
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(countSubmittedMapStageAttempts() === 2)

  }

  /**
    * This tests the case where a late FetchFailed comes in after the map stage has finished getting
    * retried and a new reduce stage starts running.
    * 这个测试的情况下,后期fetchfailed进来之后的Map阶段完成复审和一个新的阶段开始减少
    */
  test("extremely late fetch failures don't cause multiple concurrent attempts for " +
      "the same stage") {
    val shuffleMapRdd = new MyRDD(sc, 2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, null)
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 2, List(shuffleDep))
    submit(reduceRdd, Array(0, 1))

    def countSubmittedReduceStageAttempts(): Int = {
      sparkListener.submittedStageInfos.count(_.stageId == 1)
    }
    def countSubmittedMapStageAttempts(): Int = {
      sparkListener.submittedStageInfos.count(_.stageId == 0)
    }

    // The map stage should have been submitted.
    //Map阶段应该已经提交
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(countSubmittedMapStageAttempts() === 1)

    // Complete the map stage.
    //完成Map阶段
    complete(taskSets(0), Seq(
      (Success, makeMapStatus("hostA", 2)),
      (Success, makeMapStatus("hostB", 2))))

    // The reduce stage should have been submitted.
    //reduce阶段应提交
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(countSubmittedReduceStageAttempts() === 1)

    // The first result task fails, with a fetch failure for the output from the first mapper.
    //第一个结果任务失败,一个获取失败Map任务输出故障
    runEvent(CompletionEvent(
      taskSets(1).tasks(0),
      FetchFailed(makeBlockManagerId("hostA"), shuffleId, 0, 0, "ignored"),
      null,
      Map[Long, Any](),
      createFakeTaskInfo(),
      null))

    // Trigger resubmission of the failed map stage and finish the re-started map task.
    //触发失败的Map阶段提交和完成重新启动Map任务
    runEvent(ResubmitFailedStages)
    complete(taskSets(2), Seq((Success, makeMapStatus("hostA", 1))))

    // Because the map stage finished, another attempt for the reduce stage should have been
    //因为Map阶段结束了,reduce阶段的另一次尝试应该被提交
    // submitted, resulting in 2 total attempts for each the map and the reduce stage.
    //造成2次总尝试的每一个Map和reduce阶段
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(countSubmittedMapStageAttempts() === 2)
    assert(countSubmittedReduceStageAttempts() === 2)

    // A late FetchFailed arrives from the second task in the original reduce stage.
    //最后一个fetchfailed到达第二任务原来在reduce阶段
    runEvent(CompletionEvent(
      taskSets(1).tasks(1),
      FetchFailed(makeBlockManagerId("hostB"), shuffleId, 1, 1, "ignored"),
      null,
      Map[Long, Any](),
      createFakeTaskInfo(),
      null))

    // Running ResubmitFailedStages shouldn't result in any more attempts for the map stage, because   
    // the FetchFailed should have been ignored
    //运行resubmitfailedstages不应该再为Map阶段带来更多的尝试,因为fetchfailed应该被忽略
    runEvent(ResubmitFailedStages)

    // The FetchFailed from the original reduce stage should be ignored.
    //从原始的fetchfailed减少阶段应该被忽略
    assert(countSubmittedMapStageAttempts() === 2)
  }
  //忽视最后的Map任务完成
  test("ignore late map task completions") {
    val shuffleMapRdd = new MyRDD(sc, 2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, null)
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 2, List(shuffleDep))
    submit(reduceRdd, Array(0, 1))
    // pretend we were told hostA went away
    //假装我们是告诉hostA走了
    val oldEpoch = mapOutputTracker.getEpoch
    runEvent(ExecutorLost("exec-hostA"))//丢失
    val newEpoch = mapOutputTracker.getEpoch
    assert(newEpoch > oldEpoch)
    val taskSet = taskSets(0)
    // should be ignored for being too old
    //应该被忽视因为太老
    runEvent(CompletionEvent(taskSet.tasks(0), Success, makeMapStatus("hostA",
      reduceRdd.partitions.size), null, createFakeTaskInfo(), null))
    // should work because it's a non-failed host
    //应该工作,因为它是一个非失败的主机
    runEvent(CompletionEvent(taskSet.tasks(0), Success, makeMapStatus("hostB",
      reduceRdd.partitions.size), null, createFakeTaskInfo(), null))
    // should be ignored for being too old
    //应该被忽视因为太老
    runEvent(CompletionEvent(taskSet.tasks(0), Success, makeMapStatus("hostA",
      reduceRdd.partitions.size), null, createFakeTaskInfo(), null))
    // should work because it's a new epoch
    //应该工作,因为它是一个新的时代
    taskSet.tasks(1).epoch = newEpoch
    runEvent(CompletionEvent(taskSet.tasks(1), Success, makeMapStatus("hostA",
      reduceRdd.partitions.size), null, createFakeTaskInfo(), null))
    assert(mapOutputTracker.getMapSizesByExecutorId(shuffleId, 0).map(_._1).toSet ===
           HashSet(makeBlockManagerId("hostB"), makeBlockManagerId("hostA")))
    complete(taskSets(1), Seq((Success, 42), (Success, 43)))
    assert(results === Map(0 -> 42, 1 -> 43))
    assertDataStructuresEmpty()
  }
  //运行Shuffle具有Map阶段故障
  test("run shuffle with map stage failure") {
    val shuffleMapRdd = new MyRDD(sc, 2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, null)
    val reduceRdd = new MyRDD(sc, 2, List(shuffleDep))
    submit(reduceRdd, Array(0, 1))

    // Fail the map stage.  This should cause the entire job to fail.
    //失败的Map阶段,这应该会导致整个Job失败
    val stageFailureMessage = "Exception failure in map stage"
    failed(taskSets(0), stageFailureMessage)
    assert(failure.getMessage === s"Job aborted due to stage failure: $stageFailureMessage")

    // Listener bus should get told about the map stage failing, but not the reduce stage
    //监听总线应该被告知Map阶段的失败,但不是减少阶段(因为减少阶段还没有开始)
    // (since the reduce stage hasn't been started yet).
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(sparkListener.failedStages.toSet === Set(0))

    assertDataStructuresEmpty()
  }

  /**
   * Makes sure that failures of stage used by multiple jobs are correctly handled.
   * 确保多个作业所使用的阶段失败是否正确处理
   *
   * This test creates the following dependency graph:
   * 测试创建以下依赖关系图：
   * shuffleMapRdd1     shuffleMapRDD2
   *        |     \        |
   *        |      \       |
   *        |       \      |
   *        |        \     |
   *   reduceRdd1    reduceRdd2
   *
   * We start both shuffleMapRdds and then fail shuffleMapRdd1.  As a result, the job listeners for
   * 我们开始shufflemaprdds然后失败shufflemaprdd1,
   * reduceRdd1 and reduceRdd2 should both be informed that the job failed.  shuffleMapRDD2 should
   * 因此一个结果,对于reducerdd1和reducerdd2工作作业失败都应告知的监听,shufflemaprdd2也应取消
   * also be cancelled, because it is only used by reduceRdd2 and reduceRdd2 cannot complete
   * 因为只有reducerdd2和reducerdd2不能完成没有shufflemaprdd1
   * without shuffleMapRdd1.
   */
  test("failure of stage used by two jobs") {//两个工作阶段的失败
    val shuffleMapRdd1 = new MyRDD(sc, 2, Nil)
    val shuffleDep1 = new ShuffleDependency(shuffleMapRdd1, null)
    val shuffleMapRdd2 = new MyRDD(sc, 2, Nil)
    val shuffleDep2 = new ShuffleDependency(shuffleMapRdd2, null)

    val reduceRdd1 = new MyRDD(sc, 2, List(shuffleDep1))
    val reduceRdd2 = new MyRDD(sc, 2, List(shuffleDep1, shuffleDep2))

    // We need to make our own listeners for this test, since by default submit uses the same
    //我们需要监听做测试,由于默认情况下,提交使用相同的侦听器进行所有作业
    // listener for all jobs, and here we want to capture the failure for each job separately.
    //在这里我们要捕捉每一个工作的失败
    class FailureRecordingJobListener() extends JobListener {
      var failureMessage: String = _
      override def taskSucceeded(index: Int, result: Any) {}
      override def jobFailed(exception: Exception): Unit = { failureMessage = exception.getMessage }
    }
    val listener1 = new FailureRecordingJobListener()
    val listener2 = new FailureRecordingJobListener()

    submit(reduceRdd1, Array(0, 1), listener = listener1)
    submit(reduceRdd2, Array(0, 1), listener = listener2)

    val stageFailureMessage = "Exception failure in map stage"
    failed(taskSets(0), stageFailureMessage)

    assert(cancelledStages.toSet === Set(0, 2))

    // Make sure the listeners got told about both failed stages.
    //确保两个失败的阶段告知这监听
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    assert(sparkListener.successfulStages.isEmpty)
    assert(sparkListener.failedStages.toSet === Set(0, 2))

    assert(listener1.failureMessage === s"Job aborted due to stage failure: $stageFailureMessage")
    assert(listener2.failureMessage === s"Job aborted due to stage failure: $stageFailureMessage")
    assertDataStructuresEmpty()
  }

  def checkJobPropertiesAndPriority(taskSet: TaskSet, expected: String, priority: Int): Unit = {
    assert(taskSet.properties != null)
    assert(taskSet.properties.getProperty("testProperty") === expected)
    assert(taskSet.priority === priority)
  }

  def launchJobsThatShareStageAndCancelFirst(): ShuffleDependency[Int, Int, Nothing] = {
    val baseRdd = new MyRDD(sc, 1, Nil)
    val shuffleDep1 = new ShuffleDependency(baseRdd, new HashPartitioner(1))
    val intermediateRdd = new MyRDD(sc, 1, List(shuffleDep1))
    val shuffleDep2 = new ShuffleDependency(intermediateRdd, new HashPartitioner(1))
    val finalRdd1 = new MyRDD(sc, 1, List(shuffleDep2))
    val finalRdd2 = new MyRDD(sc, 1, List(shuffleDep2))
    val job1Properties = new Properties()
    val job2Properties = new Properties()
    job1Properties.setProperty("testProperty", "job1")
    job2Properties.setProperty("testProperty", "job2")

    // Run jobs 1 & 2, both referencing the same stage, then cancel job1.
    //运行作业1和2,都引用同一个阶段,然后取消job1
    // Note that we have to submit job2 before we cancel job1 to have them actually share
    //注意,我们必须在我们那儿有他们真正共离取消提交的作业1
    // *Stages*, and not just shuffle dependencies, due to skipped stages (at least until
    // we address SPARK-10193.)
    val jobId1 = submit(finalRdd1, Array(0), properties = job1Properties)
    val jobId2 = submit(finalRdd2, Array(0), properties = job2Properties)
    assert(scheduler.activeJobs.nonEmpty)
    val testProperty1 = scheduler.jobIdToActiveJob(jobId1).properties.getProperty("testProperty")

    // remove job1 as an ActiveJob
    //移除Job1在活动Job(ActiveJob)
    cancel(jobId1)

    // job2 should still be running
    //job2应该仍在运行
    assert(scheduler.activeJobs.nonEmpty)
    val testProperty2 = scheduler.jobIdToActiveJob(jobId2).properties.getProperty("testProperty")
    assert(testProperty1 != testProperty2)
    // NB: This next assert isn't necessarily the "desired" behavior; it's just to document
    // the current behavior.  We've already submitted the TaskSet for stage 0 based on job1, but
    // even though we have cancelled that job and are now running it because of job2, we haven't
    // updated the TaskSet's properties.  Changing the properties to "job2" is likely the more
    // correct behavior.
    //taskset优先运行阶段,以“job1”为activejob
    val job1Id = 0  // TaskSet priority for Stages run with "job1" as the ActiveJob
    checkJobPropertiesAndPriority(taskSets(0), "job1", job1Id)
    complete(taskSets(0), Seq((Success, makeMapStatus("hostA", 1))))

    shuffleDep1
  }

  /**
   * Makes sure that tasks for a stage used by multiple jobs are submitted with the properties of a
   * later, active job if they were previously run under a job that is no longer active
   * 确保多个作业所使用的阶段的任务被提交给一个以后的属性,激活的工作,如果他们以前是在一个Job不再活跃
   */
  //阶段所使用的两个工作,第一个不再活跃
  test("stage used by two jobs, the first no longer active (SPARK-6880)") {
    launchJobsThatShareStageAndCancelFirst()

    // The next check is the key for SPARK-6880.  For the stage which was shared by both job1 and
    // job2 but never had any tasks submitted for job1, the properties of job2 are now used to run
    // the stage.
    checkJobPropertiesAndPriority(taskSets(1), "job2", 1)

    complete(taskSets(1), Seq((Success, makeMapStatus("hostA", 1))))
    assert(taskSets(2).properties != null)
    complete(taskSets(2), Seq((Success, 42)))
    assert(results === Map(0 -> 42))
    assert(scheduler.activeJobs.isEmpty)

    assertDataStructuresEmpty()
  }

  /**
   * Makes sure that tasks for a stage used by multiple jobs are submitted with the properties of a
   * later, active job if they were previously run under a job that is no longer active, even when
   * there are fetch failures
   */
  //两个工作阶段使用的阶段，一些获取失败,第一个Job不再活跃
  test("stage used by two jobs, some fetch failures, and the first job no longer active " +
    "(SPARK-6880)") {
    val shuffleDep1 = launchJobsThatShareStageAndCancelFirst()
    //askset优先运行阶段，以"job2"为activejob
    val job2Id = 1  // TaskSet priority for Stages run with "job2" as the ActiveJob

    // lets say there is a fetch failure in this task set, which makes us go back and   
    // run stage 0, attempt 1
    //可以说在这个任务集里有一个获取失败的,这使我们回到和运行阶段0，尝试1
    complete(taskSets(1), Seq(
      (FetchFailed(makeBlockManagerId("hostA"), shuffleDep1.shuffleId, 0, 0, "ignored"), null)))
    scheduler.resubmitFailedStages()

    // stage 0, attempt 1 should have the properties of job2
    //0阶段，尝试1应具有的job2特性
    assert(taskSets(2).stageId === 0)
    assert(taskSets(2).stageAttemptId === 1)
    checkJobPropertiesAndPriority(taskSets(2), "job2", job2Id)

    // run the rest of the stages normally, checking that they have the correct properties
    //正常运行阶段的其余部分,检查它们有正确的属性
    complete(taskSets(2), Seq((Success, makeMapStatus("hostA", 1))))
    checkJobPropertiesAndPriority(taskSets(3), "job2", job2Id)
    complete(taskSets(3), Seq((Success, makeMapStatus("hostA", 1))))
    checkJobPropertiesAndPriority(taskSets(4), "job2", job2Id)
    complete(taskSets(4), Seq((Success, 42)))
    assert(results === Map(0 -> 42))
    assert(scheduler.activeJobs.isEmpty)

    assertDataStructuresEmpty()
  }
  //超出范围失败和重试的无价值的Shuffle
  test("run trivial shuffle with out-of-band failure and retry") {
    val shuffleMapRdd = new MyRDD(sc, 2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, null)
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 1, List(shuffleDep))
    submit(reduceRdd, Array(0))
    // blockManagerMaster.removeExecutor("exec-hostA")
    // pretend we were told hostA went away
    //假装我们是告诉hostA走了
    runEvent(ExecutorLost("exec-hostA"))
    // DAGScheduler will immediately resubmit the stage after it appears to have no pending tasks
    // rather than marking it is as failed and waiting.
    complete(taskSets(0), Seq(
        (Success, makeMapStatus("hostA", 1)),
       (Success, makeMapStatus("hostB", 1))))
    // have hostC complete the resubmitted task
    //有hostc完成提交任务
    complete(taskSets(1), Seq((Success, makeMapStatus("hostC", 1))))
    assert(mapOutputTracker.getMapSizesByExecutorId(shuffleId, 0).map(_._1).toSet ===
           HashSet(makeBlockManagerId("hostC"), makeBlockManagerId("hostB")))
    complete(taskSets(2), Seq((Success, 42)))
    assert(results === Map(0 -> 42))
    assertDataStructuresEmpty()
  }

  test("recursive shuffle failures") {//递归Shuffle失败
    val shuffleOneRdd = new MyRDD(sc, 2, Nil)
    val shuffleDepOne = new ShuffleDependency(shuffleOneRdd, null)
    val shuffleTwoRdd = new MyRDD(sc, 2, List(shuffleDepOne))
    val shuffleDepTwo = new ShuffleDependency(shuffleTwoRdd, null)
    val finalRdd = new MyRDD(sc, 1, List(shuffleDepTwo))
    submit(finalRdd, Array(0))
    // have the first stage complete normally
    //有第一个阶段完成正常
    complete(taskSets(0), Seq(
        (Success, makeMapStatus("hostA", 2)),
        (Success, makeMapStatus("hostB", 2))))
    // have the second stage complete normally
    // 有第二个阶段完成正常
    complete(taskSets(1), Seq(
        (Success, makeMapStatus("hostA", 1)),
        (Success, makeMapStatus("hostC", 1))))
    // fail the third stage because hostA went down
    //失败的第三阶段因为hostA宕机了
    complete(taskSets(2), Seq(
        (FetchFailed(makeBlockManagerId("hostA"), shuffleDepTwo.shuffleId, 0, 0, "ignored"), null)))
    // TODO assert this:
    // blockManagerMaster.removeExecutor("exec-hostA")
    // have DAGScheduler try again
    //有dagscheduler再试一次
    scheduler.resubmitFailedStages()
    complete(taskSets(3), Seq((Success, makeMapStatus("hostA", 2))))
    complete(taskSets(4), Seq((Success, makeMapStatus("hostA", 1))))
    complete(taskSets(5), Seq((Success, 42)))
    assert(results === Map(0 -> 42))
    assertDataStructuresEmpty()
  }

  test("cached post-shuffle") {//缓存提交shuffle
    val shuffleOneRdd = new MyRDD(sc, 2, Nil).cache()
    val shuffleDepOne = new ShuffleDependency(shuffleOneRdd, null)
    val shuffleTwoRdd = new MyRDD(sc, 2, List(shuffleDepOne)).cache()
    val shuffleDepTwo = new ShuffleDependency(shuffleTwoRdd, null)
    val finalRdd = new MyRDD(sc, 1, List(shuffleDepTwo))
    submit(finalRdd, Array(0))
    cacheLocations(shuffleTwoRdd.id -> 0) = Seq(makeBlockManagerId("hostD"))
    cacheLocations(shuffleTwoRdd.id -> 1) = Seq(makeBlockManagerId("hostC"))
    // complete stage 2
    //完成阶段2
    complete(taskSets(0), Seq(
        (Success, makeMapStatus("hostA", 2)),
        (Success, makeMapStatus("hostB", 2))))
    // complete stage 1
    //完成阶段1
    complete(taskSets(1), Seq(
        (Success, makeMapStatus("hostA", 1)),
        (Success, makeMapStatus("hostB", 1))))
    // pretend stage 0 failed because hostA went down
    //假装阶段0失败 ,因为hostA宕机了
    complete(taskSets(2), Seq(
        (FetchFailed(makeBlockManagerId("hostA"), shuffleDepTwo.shuffleId, 0, 0, "ignored"), null)))
    // TODO assert this:
    // blockManagerMaster.removeExecutor("exec-hostA")
    // DAGScheduler should notice the cached copy of the second shuffle and try to get it rerun.
    //DAGScheduler应该注意的缓存副本,第二个洗牌试着重新洗牌
    scheduler.resubmitFailedStages()
    assertLocations(taskSets(3), Seq(Seq("hostD")))
    // allow hostD to recover
    //允许hostD恢复
    complete(taskSets(3), Seq((Success, makeMapStatus("hostD", 1))))
    complete(taskSets(4), Seq((Success, 42)))
    assert(results === Map(0 -> 42))
    assertDataStructuresEmpty()
  }

  test("misbehaved accumulator should not crash DAGScheduler and SparkContext") {
    val acc = new Accumulator[Int](0, new AccumulatorParam[Int] {
      override def addAccumulator(t1: Int, t2: Int): Int = t1 + t2
      override def zero(initialValue: Int): Int = 0
      override def addInPlace(r1: Int, r2: Int): Int = {
        throw new DAGSchedulerSuiteDummyException
      }
    })

    // Run this on executors
    //运行在执行器
    sc.parallelize(1 to 10, 2).foreach { item => acc.add(1) }

    // Make sure we can still run commands
    //确保我们仍然可以运行命令
    assert(sc.parallelize(1 to 10, 2).count() === 10)
  }

  /**
   * The job will be failed on first task throwing a DAGSchedulerSuiteDummyException.
   * 这项工作将在第一个任务扔dagschedulersuitedummyexception失败
   *  Any subsequent task WILL throw a legitimate java.lang.UnsupportedOperationException.
   *  任何后续任务都将抛出一个合法的UnsupportedOperationException
   *  If multiple tasks, there exists a race condition between the SparkDriverExecutionExceptions
   *  如果多个任务,存在一个竞争条件SparkDriverExecutionExceptions和他们的不同的原因，这将代表工作的结果…
   *  and their differing causes as to which will represent result for job...
   */
  test("misbehaved resultHandler should not crash DAGScheduler and SparkContext") {
    val e = intercept[SparkDriverExecutionException] {
      // Number of parallelized partitions implies number of tasks of job
      //并行分区数目意味着任务的工作数量
      val rdd = sc.parallelize(1 to 10, 2)
      sc.runJob[Int, Int](
        rdd,
        (context: TaskContext, iter: Iterator[Int]) => iter.size,
        // For a robust test assertion, limit number of job tasks to 1; that is,
        //对于一个健壮的测试断言,将任务任务的数量限制为1,这是
        // if multiple RDD partitions, use id of any one partition, say, first partition id=0
        //如果多个RDD的分区,使用ID的任何一个分区,第一个分区ID =0
        Seq(0),
        (part: Int, result: Int) => throw new DAGSchedulerSuiteDummyException)
    }
    assert(e.getCause.isInstanceOf[DAGSchedulerSuiteDummyException])

    // Make sure we can still run commands on our SparkContext
    //确保我们仍然可以运行在我们的sparkcontext命令
    assert(sc.parallelize(1 to 10, 2).count() === 10)
  }
  //异常不应该碰撞dagscheduler和sparkcontext
  test("getPartitions exceptions should not crash DAGScheduler and SparkContext (SPARK-8606)") {
    val e1 = intercept[DAGSchedulerSuiteDummyException] {
      val rdd = new MyRDD(sc, 2, Nil) {
        override def getPartitions: Array[Partition] = {
          throw new DAGSchedulerSuiteDummyException
        }
      }
      rdd.reduceByKey(_ + _, 1).count()
    }

    // Make sure we can still run commands    
    //确保我们仍然可以运行命令
    assert(sc.parallelize(1 to 10, 2).count() === 10)
  }
  //错误不应该碰撞dagscheduler和sparkcontext
  test("getPreferredLocations errors should not crash DAGScheduler and SparkContext (SPARK-8606)") {
    val e1 = intercept[SparkException] {
      val rdd = new MyRDD(sc, 2, Nil) {
        override def getPreferredLocations(split: Partition): Seq[String] = {
          throw new DAGSchedulerSuiteDummyException
        }
      }
      rdd.count()
    }
    assert(e1.getMessage.contains(classOf[DAGSchedulerSuiteDummyException].getName))

    // Make sure we can still run commands
    //确保我们仍然可以运行命令
    assert(sc.parallelize(1 to 10, 2).count() === 10)
  }
  //累加器不计算提交结果阶段
  test("accumulator not calculated for resubmitted result stage") {
    // just for register
    val accum = new Accumulator[Int](0, AccumulatorParam.IntAccumulatorParam)
    val finalRdd = new MyRDD(sc, 1, Nil)
    submit(finalRdd, Array(0))
    completeWithAccumulator(accum.id, taskSets(0), Seq((Success, 42)))
    completeWithAccumulator(accum.id, taskSets(0), Seq((Success, 42)))
    assert(results === Map(0 -> 42))

    val accVal = Accumulators.originals(accum.id).get.get.value

    assert(accVal === 1)

    assertDataStructuresEmpty()
  }
  //reduce任务应放在本地与Map输出
  ignore("reduce tasks should be placed locally with map output") {
    // Create an shuffleMapRdd with 1 partition
    val shuffleMapRdd = new MyRDD(sc, 1, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, null)
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 1, List(shuffleDep))
    submit(reduceRdd, Array(0))
    complete(taskSets(0), Seq(
        (Success, makeMapStatus("hostA", 1))))
    assert(mapOutputTracker.getMapSizesByExecutorId(shuffleId, 0).map(_._1).toSet ===
           HashSet(makeBlockManagerId("hostA")))

    // Reducer should run on the same host that map task ran
    //Reducer应在同一台主机上运行,Map任务
    val reduceTaskSet = taskSets(1)
    assertLocations(reduceTaskSet, Seq(Seq("hostA")))
    complete(reduceTaskSet, Seq((Success, 42)))
    assert(results === Map(0 -> 42))
    assertDataStructuresEmpty()
  }
  //reduce任务局部性的喜好,只应包括机器与最大的map输出
  ignore("reduce task locality preferences should only include machines with largest map outputs") {
    val numMapTasks = 4
    // Create an shuffleMapRdd with more partitions
    val shuffleMapRdd = new MyRDD(sc, numMapTasks, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, null)
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 1, List(shuffleDep))
    submit(reduceRdd, Array(0))

    val statuses = (1 to numMapTasks).map { i =>
      (Success, makeMapStatus("host" + i, 1, (10*i).toByte))
    }
    complete(taskSets(0), statuses)

    // Reducer should prefer the last 3 hosts as they have 20%, 30% and 40% of data
    //Reducer应该更喜欢最后的3台主机,因为他们有20%,30%和40%的数据
    val hosts = (1 to numMapTasks).map(i => "host" + i).reverse.take(numMapTasks - 1)

    val reduceTaskSet = taskSets(1)
    assertLocations(reduceTaskSet, Seq(hosts))
    complete(reduceTaskSet, Seq((Success, 42)))
    assert(results === Map(0 -> 42))
    assertDataStructuresEmpty()
  }
  //窄和Shuffle的依赖关系的阶段使用窄的地方
  test("stages with both narrow and shuffle dependencies use narrow ones for locality") {
    // Create an RDD that has both a shuffle dependency and a narrow dependency (e.g. for a join)
    //创建一个RDD具有Shuffle窄依赖
    val rdd1 = new MyRDD(sc, 1, Nil)
    val rdd2 = new MyRDD(sc, 1, Nil, locations = Seq(Seq("hostB")))
    val shuffleDep = new ShuffleDependency(rdd1, null)
    val narrowDep = new OneToOneDependency(rdd2)
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 1, List(shuffleDep, narrowDep))
    submit(reduceRdd, Array(0))
    complete(taskSets(0), Seq(
      (Success, makeMapStatus("hostA", 1))))
    assert(mapOutputTracker.getMapSizesByExecutorId(shuffleId, 0).map(_._1).toSet ===
      HashSet(makeBlockManagerId("hostA")))

    // Reducer should run where RDD 2 has preferences, even though though it also has a shuffle dep
    val reduceTaskSet = taskSets(1)
    assertLocations(reduceTaskSet, Seq(Seq("hostB")))
    complete(reduceTaskSet, Seq((Success, 42)))
    assert(results === Map(0 -> 42))
    assertDataStructuresEmpty()
  }
  //Spark异常应包括堆栈跟踪中的调用站点
  test("Spark exceptions should include call site in stack trace") {
    val e = intercept[SparkException] {
      sc.parallelize(1 to 10, 2).map { _ => throw new RuntimeException("uh-oh!") }.count()
    }

    // Does not include message, ONLY stack trace.
    //不包括消息,只有堆栈跟踪
    val stackTraceString = e.getStackTraceString

    // should actually include the RDD operation that invoked the method:
    //实际上应该包括RDD操作调用的方法
    assert(stackTraceString.contains("org.apache.spark.rdd.RDD.count"))

    // should include the FunSuite setup:
    //应包括funsuite设置
    assert(stackTraceString.contains("org.scalatest.FunSuite"))
  }

  /**
   * Assert that the supplied TaskSet has exactly the given hosts as its preferred locations.
   * 断言提供完全给予taskset主机作为其首选地点
   * Note that this checks only the host and not the executor ID.
   * 注意这只检查主机而不是执行人的身份
   */
  private def assertLocations(taskSet: TaskSet, hosts: Seq[Seq[String]]) {
    assert(hosts.size === taskSet.tasks.size)
    // preferredLocations对于data partition的位置偏好
    val exp=taskSet.tasks.map(_.preferredLocations).zip(hosts)
    for ((taskLocs, expectedLocs) <- taskSet.tasks.map(_.preferredLocations).zip(hosts)) {
      /**
       * taskLocs:Set(hostA, hostB)
			 * expectedLocs:Set(hostA, hostB)
       */
      println("taskLocs:"+taskLocs.map(_.host).toSet)
       println("expectedLocs:"+expectedLocs.toSet)
      assert(taskLocs.map(_.host).toSet === expectedLocs.toSet)
    }
  }

  private def makeMapStatus(host: String, reduces: Int, sizes: Byte = 2): MapStatus =
    MapStatus(makeBlockManagerId(host), Array.fill[Long](reduces)(sizes))

  private def makeBlockManagerId(host: String): BlockManagerId =
    BlockManagerId("exec-" + host, host, 12345)
/**
 * 清空数据结构
 */
  private def assertDataStructuresEmpty(): Unit = {
    assert(scheduler.activeJobs.isEmpty)
    assert(scheduler.failedStages.isEmpty)
    assert(scheduler.jobIdToActiveJob.isEmpty)
    assert(scheduler.jobIdToStageIds.isEmpty)
    assert(scheduler.stageIdToStage.isEmpty)
    assert(scheduler.runningStages.isEmpty)
    assert(scheduler.shuffleToMapStage.isEmpty)
    assert(scheduler.waitingStages.isEmpty)
    assert(scheduler.outputCommitCoordinator.isEmpty)
  }

  // Nothing in this test should break if the task info's fields are null, but
  // OutputCommitCoordinator requires the task info itself to not be null.
  private def createFakeTaskInfo(): TaskInfo = {
    val info = new TaskInfo(0, 0, 0, 0L, "", "", TaskLocality.ANY, false)
    info.finishTime = 1  // to prevent spurious errors in JobProgressListener
    info
  }

}

