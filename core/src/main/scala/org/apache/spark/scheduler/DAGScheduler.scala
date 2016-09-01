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

import java.io.NotSerializableException
import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.Map
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, Stack}
import scala.concurrent.duration._
import scala.language.existentials
import scala.language.postfixOps
import scala.util.control.NonFatal

import org.apache.commons.lang3.SerializationUtils

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.partial.{ApproximateActionListener, ApproximateEvaluator, PartialResult}
import org.apache.spark.rdd.RDD
import org.apache.spark.rpc.RpcTimeout
import org.apache.spark.storage._
import org.apache.spark.util._
import org.apache.spark.storage.BlockManagerMessages.BlockManagerHeartbeat

/**
 * The high-level scheduling layer that implements stage-oriented scheduling. It computes a DAG of
 * stages for each job, keeps track of which RDDs and stage outputs are materialized, and finds a
 * minimal schedule to run the job. It then submits stages as TaskSets to an underlying
 * TaskScheduler implementation that runs them on the cluster.
 *
 * In addition to coming up with a DAG of stages, this class also determines the preferred
 * locations to run each task on, based on the current cache status, and passes these to the
 * low-level TaskScheduler. Furthermore, it handles failures due to shuffle output files being
 * lost, in which case old stages may need to be resubmitted. Failures *within* a stage that are
 * not caused by shuffle file loss are handled by the TaskScheduler, which will retry each task
 * a small number of times before cancelling the whole stage.
 *
 * Here's a checklist to use when making or reviewing changes to this class:
 *
 *  - When adding a new data structure, update `DAGSchedulerSuite.assertDataStructuresEmpty` to
 *    include the new structure. This will help to catch memory leaks.
 */
private[spark]
/**
 * DAGScheduler主要用于在任务正式交给TaskSchedulerImpl提交之前做一些准备工作
 * 包括创建Job,将DAG中的RDD划分到不同的Stage,提交Stage等    
 * 主要维护jobId和stageId的关系,stage,ActiveJob以及缓存的RDD的partitions的位置信息
 */
class DAGScheduler(
    private[scheduler] val sc: SparkContext,
    private[scheduler] val taskScheduler: TaskScheduler,
    listenerBus: LiveListenerBus,
    mapOutputTracker: MapOutputTrackerMaster,
    blockManagerMaster: BlockManagerMaster,
    env: SparkEnv,
    clock: Clock = new SystemClock())
  extends Logging {

  def this(sc: SparkContext, taskScheduler: TaskScheduler) = {
    this(
      sc,
      taskScheduler,
      sc.listenerBus,
      sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster],
      sc.env.blockManager.master,
      sc.env)
  }

  def this(sc: SparkContext) = this(sc, sc.taskScheduler)

  private[scheduler] val metricsSource: DAGSchedulerSource = new DAGSchedulerSource(this)

  private[scheduler] val nextJobId = new AtomicInteger(0)
  private[scheduler] def numTotalJobs: Int = nextJobId.get()
  private val nextStageId = new AtomicInteger(0)
  //
  private[scheduler] val jobIdToStageIds = new HashMap[Int, HashSet[Int]]
  private[scheduler] val stageIdToStage = new HashMap[Int, Stage]
  private[scheduler] val shuffleToMapStage = new HashMap[Int, ShuffleMapStage]
  private[scheduler] val jobIdToActiveJob = new HashMap[Int, ActiveJob]
  //等待运行的调度Stage列表,防止过早执行
  // Stages we need to run whose parents aren't done
  private[scheduler] val waitingStages = new HashSet[Stage]

  // Stages we are running right now
  //正在运行的调度Stage列表,防止重复执行
  private[scheduler] val runningStages = new HashSet[Stage]
//运行失败的调度Stage列表,需要重新执行，这里的设计是出于容错的考虑
  // Stages that must be resubmitted due to fetch failures
  private[scheduler] val failedStages = new HashSet[Stage]

  private[scheduler] val activeJobs = new HashSet[ActiveJob]

  /**
   * Contains the locations that each RDD's partitions are cached on.  This map's keys are RDD ids
   * and its values are arrays indexed by partition numbers. Each array value is the set of
   * locations where that RDD partition is cached.
   * 缓存的RDD的Partitions的位置信息
   * All accesses to this map should be guarded by synchronizing on it (see SPARK-4454).
   */
  private val cacheLocs = new HashMap[Int, IndexedSeq[Seq[TaskLocation]]]

  // For tracking failed nodes, we use the MapOutputTracker's epoch number, which is sent with
  // every task. When we detect a node failing, we note the current epoch number and failed
  // executor, increment it for new tasks, and use this to ignore stray ShuffleMapTask results.
  //
  // TODO: Garbage collect information about failure epochs when we know there are no more
  //       stray messages to detect.
//失败跟踪每个节点,
  private val failedEpoch = new HashMap[String, Long]

  private [scheduler] val outputCommitCoordinator = env.outputCommitCoordinator

  // A closure(关闭,终止) serializer that we reuse.
  // This is only safe because DAGScheduler runs in a single thread.
  private val closureSerializer = SparkEnv.get.closureSerializer.newInstance()

  /** If enabled, FetchFailed will not cause stage retry, in order to surface the problem. */
  private val disallowStageRetryForTest = sc.getConf.getBoolean("spark.test.noStageRetry", false)
    //线程池
  private val messageScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("dag-scheduler-message")
   //主要职责处理DAGScheduler发各给它的各种消息
  private[scheduler] val eventProcessLoop = new DAGSchedulerEventProcessLoop(this)
  taskScheduler.setDAGScheduler(this)

  // Flag to control if reduce tasks are assigned preferred locations
  private val shuffleLocalityEnabled =
    sc.getConf.getBoolean("spark.shuffle.reduceLocality.enabled", false)
  // Number of map, reduce tasks above which we do not assign preferred locations
  // based on map output sizes. We limit the size of jobs for which assign preferred locations
  // as computing the top locations by size becomes expensive.
  private[this] val SHUFFLE_PREF_MAP_THRESHOLD = 1000
  // NOTE: This should be less than 2000 as we use HighlyCompressedMapStatus beyond that
  private[this] val SHUFFLE_PREF_REDUCE_THRESHOLD = 1000

  // Fraction of total map output that must be at a location for it to considered as a preferred
  // location for a reduce task.
  // Making this larger will focus on fewer locations where most data can be read locally, but
  // may lead to more delay in scheduling if those locations are busy.
  private[scheduler] val REDUCER_PREF_LOCS_FRACTION = 0.2

  /**
   * Called by the TaskSetManager to report task's starting.
   */
  def taskStarted(task: Task[_], taskInfo: TaskInfo) {
    eventProcessLoop.post(BeginEvent(task, taskInfo))
  }

  /**
   * Called by the TaskSetManager to report that a task has completed
   * and results are being fetched remotely.
   */
  def taskGettingResult(taskInfo: TaskInfo) {
    eventProcessLoop.post(GettingResultEvent(taskInfo))
  }

  /**
   * Called by the TaskSetManager to report task completions or failures.
   */
  def taskEnded(
      task: Task[_],
      reason: TaskEndReason,
      result: Any,
      accumUpdates: Map[Long, Any],
      taskInfo: TaskInfo,
      taskMetrics: TaskMetrics): Unit = {
    //DAGSchedulerEventProcessLoop接收CompletionEvent消息,将处理交给
    eventProcessLoop.post(
      CompletionEvent(task, reason, result, accumUpdates, taskInfo, taskMetrics))
  }

  /**
   * Update metrics for in-progress tasks and let the master know that the BlockManager is still
   * alive. Return true if the driver knows about the given block manager. Otherwise, return false,
   * indicating that the block manager should re-register.
   * 将execId,taskMetrics封装为SparkListenerExecutorMetricsUpdate事件中并post到listenerBus中
   * 此事件用于更新Stage的各种测量数据.
   * 
   */
  def executorHeartbeatReceived(
      execId: String,
      taskMetrics: Array[(Long, Int, Int, TaskMetrics)], // (taskId, stageId, stateAttempt, metrics)
      blockManagerId: BlockManagerId): Boolean = {
    //更新Stage的各中测量数据
    listenerBus.post(SparkListenerExecutorMetricsUpdate(execId, taskMetrics))
    //blockManagerMaster持有blockManagerMasterActor发送BlockManagerHeartBeat消息
    //Executor启动的时候向Drive发送BlockManagerHeartbeat心跳
    blockManagerMaster.driverEndpoint.askWithRetry[Boolean](
      BlockManagerHeartbeat(blockManagerId), new RpcTimeout(600 seconds, "BlockManagerHeartbeat"))
  }

  /**
   * Called by TaskScheduler implementation when an executor fails.  
   */
  def executorLost(execId: String): Unit = {
    eventProcessLoop.post(ExecutorLost(execId))
  }

  /**
   * Called by TaskScheduler implementation when a host is added.
   */
  def executorAdded(execId: String, host: String): Unit = {
    eventProcessLoop.post(ExecutorAdded(execId, host))
  }

  /**
   * Called by the TaskSetManager to cancel an entire TaskSet due to either repeated failures or
   * cancellation of the job itself.
   */
  def taskSetFailed(taskSet: TaskSet, reason: String, exception: Option[Throwable]): Unit = {
    eventProcessLoop.post(TaskSetFailed(taskSet, reason, exception))
  }

  private[scheduler]
  def getCacheLocs(rdd: RDD[_]): IndexedSeq[Seq[TaskLocation]] = cacheLocs.synchronized {
    // Note: this doesn't use `getOrElse()` because this method is called O(num tasks) times
    if (!cacheLocs.contains(rdd.id)) {
      // Note: if the storage level is NONE, we don't need to get locations from block manager.
      val locs: IndexedSeq[Seq[TaskLocation]] = if (rdd.getStorageLevel == StorageLevel.NONE) {
        IndexedSeq.fill(rdd.partitions.length)(Nil)
      } else {
        val blockIds =
          rdd.partitions.indices.map(index => RDDBlockId(rdd.id, index)).toArray[BlockId]
        blockManagerMaster.getLocations(blockIds).map { bms =>
          bms.map(bm => TaskLocation(bm.host, bm.executorId))
        }
      }
      cacheLocs(rdd.id) = locs
    }
    cacheLocs(rdd.id)
  }

  private def clearCacheLocs(): Unit = cacheLocs.synchronized {
    cacheLocs.clear()
  }

  /**
   * Get or create a shuffle map stage for the given shuffle dependency's map side.
   * 用于获取ShuffleDependenc所依赖的Stage,如果没有则新建
   */
  private def getShuffleMapStage(
      shuffleDep: ShuffleDependency[_, _, _],
      firstJobId: Int): ShuffleMapStage = {
    //根据shuffleId查找对应的Stage,则直接返回Stage
    shuffleToMapStage.get(shuffleDep.shuffleId) match {
      case Some(stage) => stage
      case None =>
        //否则调用registerShuffleDependencies方法已经生成,如果没有,则生成它们,        
        // We are going to register ancestor shuffle dependencies
        registerShuffleDependencies(shuffleDep, firstJobId)
        // Then register current shuffleDep
        //生成当前RDD所在的Stage
        val stage = newOrUsedShuffleStage(shuffleDep, firstJobId)
        shuffleToMapStage(shuffleDep.shuffleId) = stage
        stage
    }
  }

  /**
   * Helper function to eliminate some code re-use when creating new stages.
   */
  private def getParentStagesAndId(rdd: RDD[_], firstJobId: Int): (List[Stage], Int) = {
    //获取所有的父Stage的列表
    val parentStages = getParentStages(rdd, firstJobId)
    //生成Stage的Id
    val id = nextStageId.getAndIncrement()
    (parentStages, id)
  }

  /**
   * Create a ShuffleMapStage as part of the (re)-creation of a shuffle map stage in
   * newOrUsedShuffleStage.  The stage will be associated with the provided firstJobId.
   * Production of shuffle map stages should always use newOrUsedShuffleStage, not
   * newShuffleMapStage directly.
   */
  private def newShuffleMapStage(
      rdd: RDD[_],
      numTasks: Int,
      shuffleDep: ShuffleDependency[_, _, _],
      firstJobId: Int,
      callSite: CallSite): ShuffleMapStage = {
    val (parentStages: List[Stage], id: Int) = getParentStagesAndId(rdd, firstJobId)
    val stage: ShuffleMapStage = new ShuffleMapStage(id, rdd, numTasks, parentStages,
      firstJobId, callSite, shuffleDep)

    stageIdToStage(id) = stage
    updateJobIdStageIdMaps(firstJobId, stage)
    stage
  }

  /**
   * Create a ResultStage associated with the provided jobId.
   */
  private def newResultStage(
      rdd: RDD[_],
      numTasks: Int,
      jobId: Int,
      callSite: CallSite): ResultStage = {
    //获取所有的父Stage列表,父Stage主要是宽依赖对应的Stage,id是StageID
    val (parentStages: List[Stage], id: Int) = getParentStagesAndId(rdd, jobId)
    //创建Stege
    val stage: ResultStage = new ResultStage(id, rdd, numTasks, parentStages, jobId, callSite)
   //将stage注册到stageIdToStage
    stageIdToStage(id) = stage
    //Stage及其祖先Stage与JobId的对应关系
    updateJobIdStageIdMaps(jobId, stage)
    stage
  }

  /**
   * Create a shuffle map Stage for the given RDD.  The stage will also be associated with the
   * provided firstJobId.  If a stage for the shuffleId existed previously so that the shuffleId is
   * present in the MapOutputTracker, then the number and location of available outputs are
   * recovered from the MapOutputTracker
   * 根据ShuffleDependenc来生成Stage,如果Stage已经存在,那么恢复这个Stage的结果,从而避免了重复计算
   */
  private def newOrUsedShuffleStage(
      shuffleDep: ShuffleDependency[_, _, _],
      firstJobId: Int): ShuffleMapStage = {
    val rdd = shuffleDep.rdd
    val numTasks = rdd.partitions.length
    val stage = newShuffleMapStage(rdd, numTasks, shuffleDep, firstJobId, rdd.creationSite)
    if (mapOutputTracker.containsShuffle(shuffleDep.shuffleId)) {
      //stage已经被计算过,从mapOutputTracker中获取计算结果
      val serLocs = mapOutputTracker.getSerializedMapOutputStatuses(shuffleDep.shuffleId)
      val locs = MapOutputTracker.deserializeMapStatuses(serLocs)
      for (i <- 0 until locs.length) {
        //计算结果复制到Stage
        stage.outputLocs(i) = Option(locs(i)).toList // locs(i) will be null if missing
      }
      //保存Stage可用结果的数,对于不可用的部分,会被重新计算
      stage.numAvailableOutputs = locs.count(_ != null)
    } else {
      // Kind of ugly: need to register RDDs with the cache and map output tracker here
      // since we can't do it in the RDD constructor because # of partitions is unknown
      //向mapOutputTracker中注册该stage
      //shuffleMapTask的计算结果都会传递给Driver端的mapOutputTracker,其他的Task可以通过查询
      //它来获取这些结果
      logInfo("Registering RDD " + rdd.id + " (" + rdd.getCreationSite + ")")
      //mapOutputTracker实现了存储元数据的占位,ShuffleMapTask的结果通过调用registerMapOutPuts来保存计算结果
      //这个结果不是真实的数据,而是这些数据的位置,大小等元数据信息,这些下游的task就可以通过这些元数据信息获取其他
      //需要处理的数据
      mapOutputTracker.registerShuffle(shuffleDep.shuffleId, rdd.partitions.length)
    }
    stage
  }

  /**
   * Get or create the list of parent stages for a given RDD.  The new Stages will be created with
   * the provided firstJobId.
   * 获取所有的父Stage的列表,父Stage主要是宽依赖对应的Stage
   * Spark中Job会被划分为一到多个Stage,这些Stage的划分是从finalStage开始,从后往前边划分创建的
   * 
   * getParentStages用于获取或者创建给定RDD的所有父Stage,这些Stage将被分配给job对应的job
   */
  private def getParentStages(rdd: RDD[_], firstJobId: Int): List[Stage] = {
    val parents = new HashSet[Stage] //存储parent stage,HashSet为了防止里面元素重复 
    val visited = new HashSet[RDD[_]] //存储已经被访问到的RDD
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    //存储需要被处理的RDD,Stack中的RDD都需要被处理
    val waitingForVisit = new Stack[RDD[_]]//堆,后进先出的原则存储数据
    def visit(r: RDD[_]) {//广度优先遍历Rdd生成的依赖树
      if (!visited(r)) {//如果不存在时
        visited += r
        // Kind of ugly: need to register RDDs with the cache here since
        // we can't do it in its constructor because # of partitions is unknown
        for (dep <- r.dependencies) {//逐个处理当前RDD依赖的Parent RDD,通过调用dependencies获取RDD所有的dependencies序列
          dep match {//逐个访问每个RDD及其依赖的非Shuffle的RDD,遍历每个RDD的ShuffleDependency依赖
            
            case shufDep: ShuffleDependency[_, _, _] =>
              //在依赖是ShuffleDependency里需要生成新的Stage
              parents += getShuffleMapStage(shufDep, firstJobId)//并调用getShuffleMapStage获取或者创建Stage,并将这些返回的Stage都放入parents
            case _ =>
              //不是ShuffleDependency,那么属于同一个Stage
              waitingForVisit.push(dep.rdd)
          }
        }
      }
    }
    //以输入的RDD作为第一个需要处理的RDD,然后从该RDD开始,顺序处理其Parent Rdd
    waitingForVisit.push(rdd)//堆,后进先出的原则存储数据
    while (waitingForVisit.nonEmpty) {//只要Stack不为空,则一直处理
      //每次visit如果遇到了ShuffleDependency,那么就会形成一个Stage,否则这些RDD属于同个Stage      
      visit(waitingForVisit.pop())
    }
    parents.toList
  }

  /** 
   *  Find ancestor missing shuffle dependencies and register into shuffleToMapStage 
   *  负责确认该Stage的parent Stage是否已经生成,如果没有则生成它们
   *  */
  private def registerShuffleDependencies(shuffleDep: ShuffleDependency[_, _, _], firstJobId: Int) {
    //首先获取没有生成Stage的ShuffleDependency
    val parentsWithNoMapStage = getAncestorShuffleDependencies(shuffleDep.rdd)
    while (parentsWithNoMapStage.nonEmpty) {
      val currentShufDep = parentsWithNoMapStage.pop()
      //根据ShuffleDependenc来生成Stage,如果Stage已经存在,那么恢复这个Stage的结果,从而避免了重复计算
      val stage = newOrUsedShuffleStage(currentShufDep, firstJobId)
      shuffleToMapStage(currentShufDep.shuffleId) = stage
    }
  }

  /** Find ancestor shuffle dependencies that are not registered in shuffleToMapStage yet */
  //实现和getParentStage差不多,只不过它遇到ShuffleDependency时首先会判断Stage是否已经存,不存在则把这个
  //依赖作为返回值的一个元素,由调用者来完成Stage的创建
  private def getAncestorShuffleDependencies(rdd: RDD[_]): Stack[ShuffleDependency[_, _, _]] = {
    val parents = new Stack[ShuffleDependency[_, _, _]]
    val visited = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new Stack[RDD[_]]
    def visit(r: RDD[_]) {
      if (!visited(r)) {
        visited += r
        for (dep <- r.dependencies) {
          dep match {
            case shufDep: ShuffleDependency[_, _, _] =>
              if (!shuffleToMapStage.contains(shufDep.shuffleId)) {
                parents.push(shufDep)
              }

              waitingForVisit.push(shufDep.rdd)
            case _ =>
              waitingForVisit.push(dep.rdd)
          }
        }
      }
    }

    waitingForVisit.push(rdd)
    while (waitingForVisit.nonEmpty) {
      visit(waitingForVisit.pop())
    }
    parents
  }
/**
 * Missing 用来查到Stage的所有不可用的祖先Stage
 */
  private def getMissingParentStages(stage: Stage): List[Stage] = {
    val missing = new HashSet[Stage]
    val visited = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new Stack[RDD[_]]
    def visit(rdd: RDD[_]) {
      if (!visited(rdd)) {
        visited += rdd
        val rddHasUncachedPartitions = getCacheLocs(rdd).contains(Nil)
        if (rddHasUncachedPartitions) {
          for (dep <- rdd.dependencies) {
            dep match {
              case shufDep: ShuffleDependency[_, _, _] =>
                val mapStage = getShuffleMapStage(shufDep, stage.firstJobId)
                if (!mapStage.isAvailable) {
                  missing += mapStage
                }
              case narrowDep: NarrowDependency[_] =>
                waitingForVisit.push(narrowDep.rdd)
            }
          }
        }
      }
    }
    waitingForVisit.push(stage.rdd)
    while (waitingForVisit.nonEmpty) {
      visit(waitingForVisit.pop())
    }
    missing.toList
  }

  /**
   * Registers the given jobId among the jobs that need the given stage and
   * all of that stage's ancestors.
   * 最终将jobId添加到Stage及它的所有祖先Stage的映射,将job和Stage及它的所有祖先Stage的 ID
   * 更新到jobIdToStageids中
   *
   */
  private def updateJobIdStageIdMaps(jobId: Int, stage: Stage): Unit = {
    def updateJobIdStageIdMapsList(stages: List[Stage]) {
      if (stages.nonEmpty) {
        val s = stages.head
        s.jobIds += jobId
        jobIdToStageIds.getOrElseUpdate(jobId, new HashSet[Int]()) += s.id
        val parents: List[Stage] = getParentStages(s.rdd, jobId)
        val parentsWithoutThisJobId = parents.filter { ! _.jobIds.contains(jobId) }
        updateJobIdStageIdMapsList(parentsWithoutThisJobId ++ stages.tail)
      }
    }
    updateJobIdStageIdMapsList(List(stage))
  }

  /**
   * Removes state for job and any stages that are not needed by any other job.  Does not
   * handle cancelling tasks or notifying the SparkListener about finished jobs/stages/tasks.
   *
   * @param job The job whose state to cleanup.
   */
  private def cleanupStateForJobAndIndependentStages(job: ActiveJob): Unit = {
    val registeredStages = jobIdToStageIds.get(job.jobId)
    if (registeredStages.isEmpty || registeredStages.get.isEmpty) {
      logError("No stages registered for job " + job.jobId)
    } else {
      stageIdToStage.filterKeys(stageId => registeredStages.get.contains(stageId)).foreach {
        case (stageId, stage) =>
          val jobSet = stage.jobIds
          if (!jobSet.contains(job.jobId)) {
            logError(
              "Job %d not registered for stage %d even though that stage was registered for the job"
              .format(job.jobId, stageId))
          } else {
            def removeStage(stageId: Int) {
              // data structures based on Stage
              for (stage <- stageIdToStage.get(stageId)) {
                if (runningStages.contains(stage)) {
                  logDebug("Removing running stage %d".format(stageId))
                  runningStages -= stage
                }
                for ((k, v) <- shuffleToMapStage.find(_._2 == stage)) {
                  shuffleToMapStage.remove(k)
                }
                if (waitingStages.contains(stage)) {
                  logDebug("Removing stage %d from waiting set.".format(stageId))
                  waitingStages -= stage
                }
                if (failedStages.contains(stage)) {
                  logDebug("Removing stage %d from failed set.".format(stageId))
                  failedStages -= stage
                }
              }
              // data structures based on StageId
              stageIdToStage -= stageId
              logDebug("After removal of stage %d, remaining stages = %d"
                .format(stageId, stageIdToStage.size))
            }

            jobSet -= job.jobId
            if (jobSet.isEmpty) { // no other job needs this stage
              removeStage(stageId)
            }
          }
      }
    }
    jobIdToStageIds -= job.jobId
    jobIdToActiveJob -= job.jobId
    activeJobs -= job
    job.finalStage.resultOfJob = None
  }

  /**
   * Submit a job to the job scheduler and get a JobWaiter object back. The JobWaiter object
   * can be used to block until the the job finishes executing or can be used to cancel the job.
   * 将作业提交到作业调度器和得到一个jobwaiter对象,JobWaiter对象可以用来堵塞直到Job执行结束或可取消Job
   */
  def submitJob[T, U](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      callSite: CallSite,
      resultHandler: (Int, U) => Unit,
      properties: Properties): JobWaiter[U] = {
    // Check to make sure we are not launching a task on a partition that does not exist.
    //调用RDD.partitions函数获取 当前Job最大分区数,即maxPartitions
    val maxPartitions = rdd.partitions.length
    //根据maxPartitions确认我们没有在一个不存在的partition上运行任务
    partitions.find(p => p >= maxPartitions || p < 0).foreach { p =>
      throw new IllegalArgumentException(
        "Attempting to access a non-existent partition: " + p + ". " +
          "Total number of partitions: " + maxPartitions)
    }
    //生成当前Job的jobId
    val jobId = nextJobId.getAndIncrement()
    if (partitions.size == 0) {//创建JobWaiter,被阻塞,直到job完成或者被取消
      return new JobWaiter[U](this, jobId, 0, resultHandler)
    }

    assert(partitions.size > 0)
    //强制类型转换匿名方法
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
    //生成JobWaiter实例用来监听Job执行情况
    val waiter = new JobWaiter(this, jobId, partitions.size, resultHandler)
    //eventProcessLoop发送jobSubmittied事件
    eventProcessLoop.post(JobSubmitted(
      jobId, rdd, func2, partitions.toArray, callSite, waiter,
      SerializationUtils.clone(properties)))
      //返回waiter
    waiter
  }
//JOb的提交
  def runJob[T, U](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      callSite: CallSite,
      resultHandler: (Int, U) => Unit,
      properties: Properties): Unit = {
    val start = System.nanoTime //开始时间
    val waiter = submitJob(rdd, func, partitions, callSite, resultHandler, properties)
    //waiter.awaitResult()说明任务的运行是异步
    waiter.awaitResult() match {
      case JobSucceeded =>
        logInfo("Job %d finished: %s, took %f s".format//1e9就为1*(10的九次方),也就是十亿
          (waiter.jobId, callSite.shortForm, (System.nanoTime - start) / 1e9))
      case JobFailed(exception: Exception) =>
        logInfo("Job %d failed: %s, took %f s".format
          (waiter.jobId, callSite.shortForm, (System.nanoTime - start) / 1e9))
        // SPARK-8644: Include user stack trace in exceptions coming from DAGScheduler.
        val callerStackTrace = Thread.currentThread().getStackTrace.tail
        exception.setStackTrace(exception.getStackTrace ++ callerStackTrace)
        throw exception
    }
  }
//近似估计
  def runApproximateJob[T, U, R](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      evaluator: ApproximateEvaluator[U, R],
      callSite: CallSite,
      timeout: Long,
      properties: Properties): PartialResult[R] = {
    val listener = new ApproximateActionListener(rdd, func, evaluator, timeout)
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
    val partitions = (0 until rdd.partitions.length).toArray
    val jobId = nextJobId.getAndIncrement()
    eventProcessLoop.post(JobSubmitted(
      jobId, rdd, func2, partitions, callSite, listener, SerializationUtils.clone(properties)))
    listener.awaitResult()    // Will throw an exception if the job fails
  }

  /**
   * Cancel a job that is running or waiting in the queue.
   */
  def cancelJob(jobId: Int): Unit = {
    logInfo("Asked to cancel job " + jobId)
    eventProcessLoop.post(JobCancelled(jobId))
  }

  def cancelJobGroup(groupId: String): Unit = {
    logInfo("Asked to cancel job group " + groupId)
    eventProcessLoop.post(JobGroupCancelled(groupId))
  }

  /**
   * Cancel all jobs that are running or waiting in the queue.
   */
  def cancelAllJobs(): Unit = {
    eventProcessLoop.post(AllJobsCancelled)
  }

  private[scheduler] def doCancelAllJobs() {
    // Cancel all running jobs.
    runningStages.map(_.firstJobId).foreach(handleJobCancellation(_,
      reason = "as part of cancellation of all jobs"))
    activeJobs.clear() // These should already be empty by this point,
    jobIdToActiveJob.clear() // but just in case we lost track of some jobs...
    submitWaitingStages()
  }

  /**
   * Cancel all jobs associated with a running or scheduled stage.
   */
  def cancelStage(stageId: Int) {
    eventProcessLoop.post(StageCancelled(stageId))
  }

  /**
   * Resubmit any failed stages. Ordinarily called after a small amount of time has passed since
   * the last fetch failure.
   */
  private[scheduler] def resubmitFailedStages() {
    if (failedStages.size > 0) {
      // Failed stages may be removed by job cancellation, so failed might be empty even if
      // the ResubmitFailedStages event has been scheduled.
      logInfo("Resubmitting failed stages")
      clearCacheLocs()
      val failedStagesCopy = failedStages.toArray
      failedStages.clear()
      for (stage <- failedStagesCopy.sortBy(_.firstJobId)) {
        submitStage(stage)
      }
  }
    submitWaitingStages()
  }

  /**
   * Check for waiting or failed stages which are now eligible for resubmission.
   * Ordinarily run on every iteration of the event loop.
   * 用于将跟踪失败的节点重新恢复正常和提交等待中的Stage
   */
  private def submitWaitingStages() {
    // TODO: We might want to run this less often, when we are sure that something has become
    // runnable that wasn't before.
    logTrace("Checking for newly runnable parent stages")
    logTrace("running: " + runningStages)
    logTrace("waiting: " + waitingStages)
    logTrace("failed: " + failedStages)
    val waitingStagesCopy = waitingStages.toArray
    waitingStages.clear()//实际上循环waitingStages中的Stage并调用submitStage
    for (stage <- waitingStagesCopy.sortBy(_.firstJobId)) {
      submitStage(stage)
    }
  }

  /** Finds the earliest-created active job that needs the stage */
  // TODO: Probably should actually find among the active jobs that need this
  // stage the one with the highest priority (highest-priority pool, earliest created).
  // That should take care of at least part of the priority inversion problem with
  // cross-job dependencies.
  private def activeJobForStage(stage: Stage): Option[Int] = {
    val jobsThatUseStage: Array[Int] = stage.jobIds.toArray.sorted
    jobsThatUseStage.find(jobIdToActiveJob.contains)
  }

  private[scheduler] def handleJobGroupCancelled(groupId: String) {
    // Cancel all jobs belonging to this job group.
    // First finds all active jobs with this group id, and then kill stages for them.
    val activeInGroup = activeJobs.filter { activeJob =>
      Option(activeJob.properties).exists {
        _.getProperty(SparkContext.SPARK_JOB_GROUP_ID) == groupId
      }
    }
    val jobIds = activeInGroup.map(_.jobId)
    jobIds.foreach(handleJobCancellation(_, "part of cancelled job group %s".format(groupId)))
    submitWaitingStages()
  }

  private[scheduler] def handleBeginEvent(task: Task[_], taskInfo: TaskInfo) {
    // Note that there is a chance that this task is launched after the stage is cancelled.
    // In that case, we wouldn't have the stage anymore in stageIdToStage.
    val stageAttemptId = stageIdToStage.get(task.stageId).map(_.latestInfo.attemptId).getOrElse(-1)
    listenerBus.post(SparkListenerTaskStart(task.stageId, stageAttemptId, taskInfo))
    submitWaitingStages()
  }

  private[scheduler] def handleTaskSetFailed(
      taskSet: TaskSet,
      reason: String,
      exception: Option[Throwable]): Unit = {
    stageIdToStage.get(taskSet.stageId).foreach { abortStage(_, reason, exception) }
    submitWaitingStages()
  }

  private[scheduler] def cleanUpAfterSchedulerStop() {
    for (job <- activeJobs) {
      val error = new SparkException("Job cancelled because SparkContext was shut down")
      job.listener.jobFailed(error)
      // Tell the listeners that all of the running stages have ended.  Don't bother
      // cancelling the stages because if the DAG scheduler is stopped, the entire application
      // is in the process of getting stopped.
      val stageFailedMessage = "Stage cancelled because SparkContext was shut down"
      // The `toArray` here is necessary so that we don't iterate over `runningStages` while
      // mutating it.
      runningStages.toArray.foreach { stage =>
        markStageAsFinished(stage, Some(stageFailedMessage))
      }
      listenerBus.post(SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobFailed(error)))
    }
  }

  private[scheduler] def handleGetTaskResult(taskInfo: TaskInfo) {
    listenerBus.post(SparkListenerTaskGettingResult(taskInfo))
    submitWaitingStages()
  }
/**
 * 处理提交的Job,调用划分Stage方法
 */
  private[scheduler] def handleJobSubmitted(jobId: Int,
      finalRDD: RDD[_],
      func: (TaskContext, Iterator[_]) => _,
      partitions: Array[Int],
      callSite: CallSite,
      listener: JobListener,
      properties: Properties) {
    var finalStage: ResultStage = null
    try {
      // New stage creation may throw an exception if, for example, jobs are run on a
      // HadoopRDD whose underlying HDFS files have been deleted.
      //根据RDD创建finalStage,
      finalStage = newResultStage(finalRDD, partitions.length, jobId, callSite)
    } catch {
      case e: Exception =>
        //在创建的时候可能会出现异常：HDFS文件被修改，或者被删除了
        logWarning("Creating new stage failed due to exception - job: " + jobId, e)
        listener.jobFailed(e)
        return
    }
    //创建ActiveJob,准备计算这个finalStage
    if (finalStage != null) {
      //创建ActiveJob后提交计算任务
      val job = new ActiveJob(jobId, finalStage, func, partitions, callSite, listener, properties)
      clearCacheLocs()
      logInfo("Got job %s (%s) with %d output partitions".format(
        job.jobId, callSite.shortForm, partitions.length))
      logInfo("Final stage: " + finalStage + "(" + finalStage.name + ")")
      logInfo("Parents of final stage: " + finalStage.parents)
      logInfo("Missing parents: " + getMissingParentStages(finalStage))
      val jobSubmissionTime = clock.getTimeMillis()
      //并更新jobIdToActiveJob,activeJobs,resultOfJob
      jobIdToActiveJob(jobId) = job
      activeJobs += job
      finalStage.resultOfJob = Some(job)
      val stageIds = jobIdToStageIds(jobId).toArray
      val stageInfos = stageIds.flatMap(id => stageIdToStage.get(id).map(_.latestInfo))
      //向listenerBus发送SparkListernJobStart事件
      listenerBus.post(
        SparkListenerJobStart(job.jobId, jobSubmissionTime, stageInfos, properties))
      //提交finalStage
        submitStage(finalStage)
    }
    //提交等待中的Stage
    submitWaitingStages()
  }

  /** 
   *  stage划分
   *  Submits stage, but first recursively(递归) submits any missing parents. 
   *  */
  // 提交Stage，如果有parent Stage没有提交，那么递归提交它。
  //每个Stage提交之前,如果存在没有提交的祖先Stage,都会先提交祖先Stage,并且将子Stage放入waitingStages中等待
  //如果不存在没有提交的祖先Stage,则提交所有的Task
  private def submitStage(stage: Stage) {
    val jobId = activeJobForStage(stage)
    if (jobId.isDefined) {
      logDebug("submitStage(" + stage + ")")
      // 如果当前stage不在等待其parent stage的返回，并且 不在运行的状态， 并且 没有已经失败（失败会有重试机制，不会通过这里再次提交）
      if (!waitingStages(stage) && !runningStages(stage) && !failedStages(stage)) {
        val missing = getMissingParentStages(stage).sortBy(_.id)//用来找到Stage的所有不可用的祖先Stage
        logDebug("missing: " + missing)
        if (missing.isEmpty) {
          //如果所有的parent Stage都已经完成,那么提交该stage所包含的Task
          logInfo("Submitting " + stage + " (" + stage.rdd + "), which has no missing parents")
          submitMissingTasks(stage, jobId.get)
        } else {
          for (parent <- missing) {
            //有parent Stage未完成,则递归 提交它
            submitStage(parent)
          }
          waitingStages += stage
        }
      }
    } else {//无效的Stage,直接停止它
      abortStage(stage, "No active job for stage " + stage.id, None)
    }
  }

  /** Called when stage's parents are available and we can now do its task. */
  //用来找到Stage的所有不可用的祖先Stage
  private def submitMissingTasks(stage: Stage, jobId: Int) {
    logDebug("submitMissingTasks(" + stage + ")")
    // Get our pending tasks and remember them in our pendingTasks entry
    //pendingTasks存储待处理的Task,清空pendingTasks,由于当前Stage的任务刚开始提交,
    //所以需要清空,便于记录需要计算任务
    stage.pendingTasks.clear()
    //MapStatus包括执行Task的BlockManager的地址和要传给Reduce任务的Blok的估算大小
    //outputLocs如果Stage是Map任务,则outputLocs记录每个Partition的MapStatus
    // First figure out the indexes of partition ids to compute.
    //找出还未计算的partition(,
    val (allPartitions: Seq[Int], partitionsToCompute: Seq[Int]) = {
      stage match {
        case stage: ShuffleMapStage =>
          //如果Stage是Map任务,那么outputLocs中partition对应的List为isEmpty,说明此partition还未计算
          val allPartitions = 0 until stage.numPartitions
          val filteredPartitions = allPartitions.filter { id => stage.outputLocs(id).isEmpty }
          (allPartitions, filteredPartitions)
        case stage: ResultStage =>
          //如果stage不是map任务,那么需要获取Stage的finalJob,并调用finished方法判断每个Partition的任务是否完成
          val job = stage.resultOfJob.get
          val allPartitions = 0 until job.numPartitions
          val filteredPartitions = allPartitions.filter { id => !job.finished(id) }
          (allPartitions, filteredPartitions)
      }
    }

    // Create internal accumulators if the stage has no accumulators initialized.
    // Reset internal accumulators only if this stage is not partially submitted
    // Otherwise, we may override existing accumulator values from some tasks
   
    if (stage.internalAccumulators.isEmpty || allPartitions == partitionsToCompute) {
      stage.resetInternalAccumulators()
    }

    // Use the scheduling pool, job group, description, etc. from an ActiveJob associated
    // with this Stage
    val properties = jobIdToActiveJob(jobId).properties
   //将当前Stage加入运行中的Stage集合
    runningStages += stage
    // SparkListenerStageSubmitted should be posted before testing whether tasks are
    // serializable. If tasks are not serializable, a SparkListenerStageCompleted event
    // will be posted, which should always come after a corresponding SparkListenerStageSubmitted
    // event.
    
    outputCommitCoordinator.stageStart(stage.id)
    val taskIdToLocations = try {
      stage match {
        case s: ShuffleMapStage =>
          //getPreferredLocs获取任务的本地性
          partitionsToCompute.map { id => (id, getPreferredLocs(stage.rdd, id))}.toMap
        case s: ResultStage =>
          val job = s.resultOfJob.get
          partitionsToCompute.map { id =>
            val p = job.partitions(id)
            (id, getPreferredLocs(stage.rdd, p))
          }.toMap
      }
    } catch {
      case NonFatal(e) =>
        stage.makeNewStageAttempt(partitionsToCompute.size)
        listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo, properties))
        abortStage(stage, s"Task creation failed: $e\n${e.getStackTraceString}", Some(e))
        runningStages -= stage
        return
    }

    stage.makeNewStageAttempt(partitionsToCompute.size, taskIdToLocations.values.toSeq)
    //listenerBus发送SparkListenerStageSubmitted事件
    listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo, properties))

    // TODO: Maybe we can keep the taskBinary in Stage to avoid serializing it multiple times.
    // Broadcasted binary for the task, used to dispatch tasks to executors. Note that we broadcast
    // the serialized copy of the RDD and for each task we will deserialize it, which means each
    // task gets a different copy of the RDD. This provides stronger isolation between tasks that
    // might modify state of objects referenced in their closures. This is necessary in Hadoop
    // where the JobConf/Configuration object is not thread-safe.
    
    var taskBinary: Broadcast[Array[Byte]] = null
    try {
      // For ShuffleMapTask, serialize and broadcast (rdd, shuffleDep).
      // For ResultTask, serialize and broadcast (rdd, func).
      val taskBinaryBytes: Array[Byte] = stage match {
        case stage: ShuffleMapStage =>//如果Stage是map任务,那么序列化Stage的RDD及ShuffleDependency
          closureSerializer.serialize((stage.rdd, stage.shuffleDep): AnyRef).array()
        case stage: ResultStage =>
          //如果不是map任务,那么序列化Stege及resultOfJob的处理函数
          closureSerializer.serialize((stage.rdd, stage.resultOfJob.get.func): AnyRef).array()
      }
      //这些序列化得到的字节数组最后需要使用broadcast广播
      taskBinary = sc.broadcast(taskBinaryBytes)
    } catch {
      // In the case of a failure during serialization, abort the stage.
      case e: NotSerializableException =>
        abortStage(stage, "Task not serializable: " + e.toString, Some(e))
        runningStages -= stage

        // Abort execution
        return
      case NonFatal(e) =>
        abortStage(stage, s"Task serialization failed: $e\n${e.getStackTraceString}", Some(e))
        runningStages -= stage
        return
    }

    val tasks: Seq[Task[_]] = try {
      stage match {
        case stage: ShuffleMapStage =>//如果Stage是Map任务,则创建ShuffleMapTask
          partitionsToCompute.map { id =>
            val locs = taskIdToLocations(id)
            val part = stage.rdd.partitions(id)
            new ShuffleMapTask(stage.id, stage.latestInfo.attemptId,
              taskBinary, part, locs, stage.internalAccumulators)
          }

        case stage: ResultStage =>//创建ResultTask
          val job = stage.resultOfJob.get
          partitionsToCompute.map { id =>
            val p: Int = job.partitions(id)
            val part = stage.rdd.partitions(p)
            val locs = taskIdToLocations(id)
            new ResultTask(stage.id, stage.latestInfo.attemptId,
              taskBinary, part, locs, id, stage.internalAccumulators)
          }
      }
    } catch {
      case NonFatal(e) =>
        abortStage(stage, s"Task creation failed: $e\n${e.getStackTraceString}", Some(e))
        runningStages -= stage
        return
    }

    if (tasks.size > 0) {
      logInfo("Submitting " + tasks.size + " missing tasks from " + stage + " (" + stage.rdd + ")")
     //将创建的所有Task都添加到stage.pendingTasks中
      stage.pendingTasks ++= tasks
      logDebug("New pending tasks: " + stage.pendingTasks)
      //利用创建 所有Task,当前Stage的Id,jobId等信息创建TaskSet,并调用taskScheduler的submitTasks,
      //批理提交Stage及其所有Task
      taskScheduler.submitTasks(new TaskSet(
        tasks.toArray, stage.id, stage.latestInfo.attemptId, jobId, properties))
      stage.latestInfo.submissionTime = Some(clock.getTimeMillis())
    } else {
      // Because we posted SparkListenerStageSubmitted earlier, we should mark
      // the stage as completed here in case there are no tasks to run
      markStageAsFinished(stage, None)

      val debugString = stage match {
        case stage: ShuffleMapStage =>
          s"Stage ${stage} is actually done; " +
            s"(available: ${stage.isAvailable}," +
            s"available outputs: ${stage.numAvailableOutputs}," +
            s"partitions: ${stage.numPartitions})"
        case stage : ResultStage =>
          s"Stage ${stage} is actually done; (partitions: ${stage.numPartitions})"
      }
      logDebug(debugString)
    }
  }

  /** Merge updates from a task to our local accumulator values */
  private def updateAccumulators(event: CompletionEvent): Unit = {
    val task = event.task
    val stage = stageIdToStage(task.stageId)
    if (event.accumUpdates != null) {
      try {
        Accumulators.add(event.accumUpdates)

        event.accumUpdates.foreach { case (id, partialValue) =>
          // In this instance, although the reference in Accumulators.originals is a WeakRef,
          // it's guaranteed to exist since the event.accumUpdates Map exists

          val acc = Accumulators.originals(id).get match {
            case Some(accum) => accum.asInstanceOf[Accumulable[Any, Any]]
            case None => throw new NullPointerException("Non-existent reference to Accumulator")
          }

          // To avoid UI cruft, ignore cases where value wasn't updated
          if (acc.name.isDefined && partialValue != acc.zero) {
            val name = acc.name.get
            val value = s"${acc.value}"
            stage.latestInfo.accumulables(id) =
              new AccumulableInfo(id, name, None, value, acc.isInternal)
            event.taskInfo.accumulables +=
              new AccumulableInfo(id, name, Some(s"$partialValue"), value, acc.isInternal)
          }
        }
      } catch {
        // If we see an exception during accumulator update, just log the
        // error and move on.
        case e: Exception =>
          logError(s"Failed to update accumulators for $task", e)
      }
    }
  }

  /**
   * Responds to a task finishing. This is called inside the event loop so it assumes that it can
   * modify the scheduler's internal state. Use taskEnded() to post a task end event from outside.
   * 负责处理获取到的计算结果
   */
  private[scheduler] def handleTaskCompletion(event: CompletionEvent) {
    val task = event.task
    val stageId = task.stageId
    val taskType = Utils.getFormattedClassName(task)

    outputCommitCoordinator.taskCompleted(
      stageId,
      task.partitionId,
      event.taskInfo.attemptNumber, // this is a task attempt number
      event.reason)

    // The success case is dealt with separately below, since we need to compute accumulator
    // updates before posting.
     
    if (event.reason != Success) {
      val attemptId = task.stageAttemptId
      listenerBus.post(SparkListenerTaskEnd(stageId, attemptId, taskType, event.reason,
        event.taskInfo, event.taskMetrics))
    }

    if (!stageIdToStage.contains(task.stageId)) {
      // Skip all the actions if the stage has been cancelled.
      return
    }

    val stage = stageIdToStage(task.stageId)
    event.reason match {
      case Success =>
        //向listenerBus发送SparkListenerTaskEnd
        listenerBus.post(SparkListenerTaskEnd(stageId, stage.latestInfo.attemptId, taskType,
          event.reason, event.taskInfo, event.taskMetrics))
        stage.pendingTasks -= task
        task match {
          case rt: ResultTask[_, _] =>
            //ResultTask 任务处理结果
            //1)标识ActiveJob的finished里对应分区的任务完成状态,并且将已完成的任务数numFinished加1
            //2)如果ActiveJob的所有任务都完成,则标记当前Stage完成并SparkListenerJobEnd发送事件
            //3)调用JobWaiter的taskSucceeded方法,以便通知JobWaiter有任务成功
            // Cast to ResultStage here because it's part of the ResultTask
            // TODO Refactor this out to a function that accepts a ResultStage
            val resultStage = stage.asInstanceOf[ResultStage]
            resultStage.resultOfJob match {
              case Some(job) =>
                if (!job.finished(rt.outputId)) {
                  //1)标识ActiveJob的finished里对应分区的任务完成状态,并且将已完成的任务数numFinished加1
                  updateAccumulators(event)
                  job.finished(rt.outputId) = true
                  job.numFinished += 1
                  // If the whole job has finished, remove it
                  //2)如果ActiveJob的所有任务都完成,则标记当前Stage完成并SparkListenerJobEnd发送事件
                  if (job.numFinished == job.numPartitions) {
                    markStageAsFinished(resultStage)
                    cleanupStateForJobAndIndependentStages(job)
                    listenerBus.post(
                      SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobSucceeded))
                  }

                  // taskSucceeded runs some user code that might throw an exception. Make sure
                  // we are resilient against that.
                  //对于taskSucceeded,会运行用户自定义的结果处理函数,因此可能会抛出异常
                  try {
                    //3)调用JobWaiter的taskSucceeded方法,以便通知JobWaiter有任务成功
                    job.listener.taskSucceeded(rt.outputId, event.result)
                  } catch {
                    case e: Exception =>
                      // TODO: Perhaps we want to mark the resultStage as failed?
                      //直接标示失败
                      job.listener.jobFailed(new SparkDriverExecutionException(e))
                  }
                }
              case None =>
                //应该有任务的推测执行,因此一个Task可能会运行多次
                logInfo("Ignoring result from " + rt + " because its job has finished")
            }

          case smt: ShuffleMapTask =>
            /**
             * 1)将Task的partitionId和MapStatus追加到Stage的outputLocs中
             * 2)将当前Stage标记为完成,然后将前当Stage的ShuffleId和OutputLocs中的MapStatus注册到MapOutTracker
             *   这里注册的Map任务状态将最终被Reduce任务作用
             * 3)如果Stage的OutputLocs中某个分区的输出为Nil,那么说明有任务失败,这时候需要再次提交此Stage
             * 4)如果不存在Stage的outputLocs中某个分区的输出为Nil,那么说明所有任务执行成功了,这时需要遍历WaitingStage
             *   中的Stage并将它们放入runningStage,最后调用submitMissgTasks方法逐个提交这些准备运行的Stage任务
             */
            val shuffleStage = stage.asInstanceOf[ShuffleMapStage]
            updateAccumulators(event)
            val status = event.result.asInstanceOf[MapStatus]
            val execId = status.location.executorId
            logDebug("ShuffleMapTask finished on " + execId)
            if (failedEpoch.contains(execId) && smt.epoch <= failedEpoch(execId)) {
              logInfo(s"Ignoring possibly bogus $smt completion from executor $execId")
            } else {
              
              //将Task的partitionId和MapStatus追加到Stage的outputLocs中
              shuffleStage.addOutputLoc(smt.partitionId, status)
            }

            if (runningStages.contains(shuffleStage) && shuffleStage.pendingTasks.isEmpty) {
              //将当前Stage标记为完成
              markStageAsFinished(shuffleStage)
              logInfo("looking for newly runnable stages")
              logInfo("running: " + runningStages)
              logInfo("waiting: " + waitingStages)
              logInfo("failed: " + failedStages)

              // We supply true to increment the epoch number here in case this is a
              // recomputation of the map outputs. In that case, some nodes may have cached
              // locations with holes (from when we detected the error) and will need the
              // epoch incremented to refetch them.
              // TODO: Only increment the epoch number if this is not the first time
              //       we registered these map outputs.         
              //将前当Stage的ShuffleId和OutputLocs中的MapStatus注册到MapOutTracker,如果该Stage的所有Task都结束了,
              //那么需要将整体结束注册到MapOutputTrackMaster
              //这样下一个Stage的Task就可以通过它来获取Shuffle结果的元数据信息,进而从Shuffle数据所在的节点获取数据了
              mapOutputTracker.registerMapOutputs(
                shuffleStage.shuffleDep.shuffleId,
                shuffleStage.outputLocs.map(list => if (list.isEmpty) null else list.head),
                changeEpoch = true)

              clearCacheLocs()
              //如果Stage的OutputLocs中某个分区的输出为Nil,那么说明有任务失败,这时候需要再次提交此Stage
              if (shuffleStage.outputLocs.contains(Nil)) {
                // Some tasks had failed; let's resubmit this shuffleStage
                // TODO: Lower-level scheduler should also deal with this
                logInfo("Resubmitting " + shuffleStage + " (" + shuffleStage.name +
                  ") because some of its tasks had failed: " +
                  shuffleStage.outputLocs.zipWithIndex.filter(_._1.isEmpty)
                      .map(_._2).mkString(", "))
                submitStage(shuffleStage)
              } else {
                //如果不存在Stage的outputLocs中某个分区的输出为Nil,那么说明所有任务执行成功了,这时需要遍历WaitingStage
                //中的Stage并将它们放入runningStage,最后调用submitMissgTasks方法逐个提交这些准备运行的Stage任务
                val newlyRunnable = new ArrayBuffer[Stage]
                for (shuffleStage <- waitingStages) {
                  logInfo("Missing parents for " + shuffleStage + ": " +
                      //部分失败重新提交
                    getMissingParentStages(shuffleStage))
                }
                for (shuffleStage <- waitingStages if getMissingParentStages(shuffleStage).isEmpty)
                {
                  newlyRunnable += shuffleStage//可以提交Stage
                }
                waitingStages --= newlyRunnable
                runningStages ++= newlyRunnable
                for {
                  shuffleStage <- newlyRunnable.sortBy(_.id)
                  jobId <- activeJobForStage(shuffleStage)
                } {
                  logInfo("Submitting " + shuffleStage + " (" +
                    shuffleStage.rdd + "), which is now runnable")
                    //提交Stage的Task
                  submitMissingTasks(shuffleStage, jobId)
                }
              }
            }
          }

      case Resubmitted =>
        logInfo("Resubmitted " + task + ", so marking it as still running")
        stage.pendingTasks += task

      case FetchFailed(bmAddress, shuffleId, mapId, reduceId, failureMessage) =>
        val failedStage = stageIdToStage(task.stageId)
        val mapStage = shuffleToMapStage(shuffleId)

        if (failedStage.latestInfo.attemptId != task.stageAttemptId) {
          logInfo(s"Ignoring fetch failure from $task as it's from $failedStage attempt" +
            s" ${task.stageAttemptId} and there is a more recent attempt for that stage " +
            s"(attempt ID ${failedStage.latestInfo.attemptId}) running")
        } else {

          // It is likely that we receive multiple FetchFailed for a single stage (because we have
          // multiple tasks running concurrently on different executors). In that case, it is
          // possible the fetch failure has already been handled by the scheduler.
          if (runningStages.contains(failedStage)) {
            logInfo(s"Marking $failedStage (${failedStage.name}) as failed " +
              s"due to a fetch failure from $mapStage (${mapStage.name})")
            markStageAsFinished(failedStage, Some(failureMessage))
          } else {
            logDebug(s"Received fetch failure from $task, but its from $failedStage which is no " +
              s"longer running")
          }

          if (disallowStageRetryForTest) {
            abortStage(failedStage, "Fetch failure will not retry stage due to testing config",
              None)
          } else if (failedStages.isEmpty) {
            // Don't schedule an event to resubmit failed stages if failed isn't empty, because
            // in that case the event will already have been scheduled.
            // TODO: Cancel running tasks in the stage
            logInfo(s"Resubmitting $mapStage (${mapStage.name}) and " +
              s"$failedStage (${failedStage.name}) due to fetch failure")
            messageScheduler.schedule(new Runnable {
              override def run(): Unit = eventProcessLoop.post(ResubmitFailedStages)
            }, DAGScheduler.RESUBMIT_TIMEOUT, TimeUnit.MILLISECONDS)
          }
          failedStages += failedStage
          failedStages += mapStage
          // Mark the map whose fetch failed as broken in the map stage
          if (mapId != -1) {
            mapStage.removeOutputLoc(mapId, bmAddress)
            mapOutputTracker.unregisterMapOutput(shuffleId, mapId, bmAddress)
          }

          // TODO: mark the executor as failed only if there were lots of fetch failures on it
          if (bmAddress != null) {
            handleExecutorLost(bmAddress.executorId, fetchFailed = true, Some(task.epoch))
          }
        }

      case commitDenied: TaskCommitDenied =>
        // Do nothing here, left up to the TaskScheduler to decide how to handle denied commits

      case exceptionFailure: ExceptionFailure =>
        // Do nothing here, left up to the TaskScheduler to decide how to handle user failures

      case TaskResultLost =>
        // Do nothing here; the TaskScheduler handles these failures and resubmits the task.

      case other =>
        // Unrecognized failure - also do nothing. If the task fails repeatedly, the TaskScheduler
        // will abort the job.
    }
    submitWaitingStages()
  }

  /**
   * Responds to an executor being lost. This is called inside the event loop, so it assumes it can
   * modify the scheduler's internal state. Use executorLost() to post a loss event from outside.
   *
   * We will also assume that we've lost all shuffle blocks associated with the executor if the
   * executor serves its own blocks (i.e., we're not using external shuffle) OR a FetchFailed
   * occurred, in which case we presume all shuffle data related to this executor to be lost.
   *
   * Optionally the epoch during which the failure was caught can be passed to avoid allowing
   * stray fetch failures from possibly retriggering the detection of a node as lost.
   */
  private[scheduler] def handleExecutorLost(
      execId: String,
      fetchFailed: Boolean,
      maybeEpoch: Option[Long] = None) {
    val currentEpoch = maybeEpoch.getOrElse(mapOutputTracker.getEpoch)
    if (!failedEpoch.contains(execId) || failedEpoch(execId) < currentEpoch) {
      failedEpoch(execId) = currentEpoch
      logInfo("Executor lost: %s (epoch %d)".format(execId, currentEpoch))
      blockManagerMaster.removeExecutor(execId)

      if (!env.blockManager.externalShuffleServiceEnabled || fetchFailed) {
        // TODO: This will be really slow if we keep accumulating shuffle map stages
        for ((shuffleId, stage) <- shuffleToMapStage) {
          stage.removeOutputsOnExecutor(execId)
          val locs = stage.outputLocs.map(list => if (list.isEmpty) null else list.head)
          mapOutputTracker.registerMapOutputs(shuffleId, locs, changeEpoch = true)
        }
        if (shuffleToMapStage.isEmpty) {
          mapOutputTracker.incrementEpoch()
        }
        clearCacheLocs()
      }
    } else {
      logDebug("Additional executor lost message for " + execId +
               "(epoch " + currentEpoch + ")")
    }
    submitWaitingStages()
  }

  private[scheduler] def handleExecutorAdded(execId: String, host: String) {
    // remove from failedEpoch(execId) ?
    if (failedEpoch.contains(execId)) {
      logInfo("Host added was in lost list earlier: " + host)
      failedEpoch -= execId
    }
    //用于将跟踪失败的节点重新恢复正常和提交等待中的Stage
    submitWaitingStages()
  }

  private[scheduler] def handleStageCancellation(stageId: Int) {
    stageIdToStage.get(stageId) match {
      case Some(stage) =>
        val jobsThatUseStage: Array[Int] = stage.jobIds.toArray
        jobsThatUseStage.foreach { jobId =>
          handleJobCancellation(jobId, s"because Stage $stageId was cancelled")
        }
      case None =>
        logInfo("No active jobs to kill for Stage " + stageId)
    }
    submitWaitingStages()
  }

  private[scheduler] def handleJobCancellation(jobId: Int, reason: String = "") {
    if (!jobIdToStageIds.contains(jobId)) {
      logDebug("Trying to cancel unregistered job " + jobId)
    } else {
      failJobAndIndependentStages(
        jobIdToActiveJob(jobId), "Job %d cancelled %s".format(jobId, reason))
    }
    submitWaitingStages()
  }

  /**
   * Marks a stage as finished and removes it from the list of running stages.
   */
  private def markStageAsFinished(stage: Stage, errorMessage: Option[String] = None): Unit = {
    val serviceTime = stage.latestInfo.submissionTime match {
      case Some(t) => "%.03f".format((clock.getTimeMillis() - t) / 1000.0)
      case _ => "Unknown"
    }
    if (errorMessage.isEmpty) {
      logInfo("%s (%s) finished in %s s".format(stage, stage.name, serviceTime))
      stage.latestInfo.completionTime = Some(clock.getTimeMillis())
    } else {
      stage.latestInfo.stageFailed(errorMessage.get)
      logInfo("%s (%s) failed in %s s".format(stage, stage.name, serviceTime))
    }
    outputCommitCoordinator.stageEnd(stage.id)
    listenerBus.post(SparkListenerStageCompleted(stage.latestInfo))
    runningStages -= stage
  }

  /**
   * Aborts all jobs depending on a particular Stage. This is called in response to a task set
   * being canceled by the TaskScheduler. Use taskSetFailed() to inject this event from outside.
   */
  private[scheduler] def abortStage(
      failedStage: Stage,
      reason: String,
      exception: Option[Throwable]): Unit = {
    if (!stageIdToStage.contains(failedStage.id)) {
      // Skip all the actions if the stage has been removed.
      return
    }
    val dependentJobs: Seq[ActiveJob] =
      activeJobs.filter(job => stageDependsOn(job.finalStage, failedStage)).toSeq
    failedStage.latestInfo.completionTime = Some(clock.getTimeMillis())
    for (job <- dependentJobs) {
      failJobAndIndependentStages(job, s"Job aborted due to stage failure: $reason", exception)
    }
    if (dependentJobs.isEmpty) {
      logInfo("Ignoring failure of " + failedStage + " because all jobs depending on it are done")
    }
  }

  /** Fails a job and all stages that are only used by that job, and cleans up relevant state. */
  private def failJobAndIndependentStages(
      job: ActiveJob,
      failureReason: String,
      exception: Option[Throwable] = None): Unit = {
    val error = new SparkException(failureReason, exception.getOrElse(null))
    var ableToCancelStages = true

    val shouldInterruptThread =
      if (job.properties == null) false
      else job.properties.getProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, "false").toBoolean

    // Cancel all independent, running stages.
    val stages = jobIdToStageIds(job.jobId)
    if (stages.isEmpty) {
      logError("No stages registered for job " + job.jobId)
    }
    stages.foreach { stageId =>
      val jobsForStage: Option[HashSet[Int]] = stageIdToStage.get(stageId).map(_.jobIds)
      if (jobsForStage.isEmpty || !jobsForStage.get.contains(job.jobId)) {
        logError(
          "Job %d not registered for stage %d even though that stage was registered for the job"
            .format(job.jobId, stageId))
      } else if (jobsForStage.get.size == 1) {
        if (!stageIdToStage.contains(stageId)) {
          logError(s"Missing Stage for stage with id $stageId")
        } else {
          // This is the only job that uses this stage, so fail the stage if it is running.
          val stage = stageIdToStage(stageId)
          if (runningStages.contains(stage)) {
            try { // cancelTasks will fail if a SchedulerBackend does not implement killTask
              taskScheduler.cancelTasks(stageId, shouldInterruptThread)
              markStageAsFinished(stage, Some(failureReason))
            } catch {
              case e: UnsupportedOperationException =>
                logInfo(s"Could not cancel tasks for stage $stageId", e)
              ableToCancelStages = false
            }
          }
        }
      }
    }

    if (ableToCancelStages) {
      job.listener.jobFailed(error)
      cleanupStateForJobAndIndependentStages(job)
      listenerBus.post(SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobFailed(error)))
    }
  }

  /** Return true if one of stage's ancestors is target. */
  private def stageDependsOn(stage: Stage, target: Stage): Boolean = {
    if (stage == target) {
      return true
    }
    val visitedRdds = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new Stack[RDD[_]]
    def visit(rdd: RDD[_]) {
      if (!visitedRdds(rdd)) {
        visitedRdds += rdd
        for (dep <- rdd.dependencies) {
          dep match {
            case shufDep: ShuffleDependency[_, _, _] =>
              val mapStage = getShuffleMapStage(shufDep, stage.firstJobId)
              if (!mapStage.isAvailable) {
                waitingForVisit.push(mapStage.rdd)
              }  // Otherwise there's no need to follow the dependency back
            case narrowDep: NarrowDependency[_] =>
              waitingForVisit.push(narrowDep.rdd)
          }
        }
      }
    }
    waitingForVisit.push(stage.rdd)
    while (waitingForVisit.nonEmpty) {
      visit(waitingForVisit.pop())
    }
    visitedRdds.contains(target.rdd)
  }

  /**
   * Gets the locality information associated with a partition of a particular RDD.
   *
   * This method is thread-safe and is called from both DAGScheduler and SparkContext.
   *
   * @param rdd whose partitions are to be looked at
   * @param partition to lookup locality information for
   * @return list of machines that are preferred by the partition
   */
  private[spark]
  def getPreferredLocs(rdd: RDD[_], partition: Int): Seq[TaskLocation] = {
    getPreferredLocsInternal(rdd, partition, new HashSet)
  }

  /**
   * Recursive implementation for getPreferredLocs.
   * 首先查询DAGScheduler的内存数据结构中是否存在当前Paritition的数据本地性的信息，如果有的话直接返回，
   * 如果没有首先会调用rdd.getPreferedLocations
   * This method is thread-safe because it only accesses DAGScheduler state through thread-safe
   * methods (getCacheLocs()); please be careful when modifying this method, because any new
   * DAGScheduler state accessed by it may require additional synchronization.
   */
  private def getPreferredLocsInternal(
      rdd: RDD[_],
      partition: Int,
      visited: HashSet[(RDD[_], Int)]): Seq[TaskLocation] = {
    // If the partition has already been visited, no need to re-visit.
    // This avoids exponential path exploration.  SPARK-695
    if (!visited.add((rdd, partition))) {
      // Nil has already been returned for previously visited partitions.
      return Nil
    }
    // If the partition is cached, return the cache locations
    val cached = getCacheLocs(rdd)(partition)
    if (cached.nonEmpty) {
      return cached
    }
    // If the RDD has some placement preferences (as is the case for input RDDs), get those
    val rddPrefs = rdd.preferredLocations(rdd.partitions(partition)).toList
    if (rddPrefs.nonEmpty) {
      return rddPrefs.map(TaskLocation(_))
    }

    // If the RDD has narrow dependencies, pick the first partition of the first narrow dependency
    // that has any placement preferences. Ideally we would choose based on transfer sizes,
    // but this will do for now.
    rdd.dependencies.foreach {
      case n: NarrowDependency[_] =>
        for (inPart <- n.getParents(partition)) {
          val locs = getPreferredLocsInternal(n.rdd, inPart, visited)
          if (locs != Nil) {
            return locs
          }
        }
      case _ =>
    }

    // If the RDD has shuffle dependencies and shuffle locality is enabled, pick locations that
    // have at least REDUCER_PREF_LOCS_FRACTION of data as preferred locations
    if (shuffleLocalityEnabled && rdd.partitions.length < SHUFFLE_PREF_REDUCE_THRESHOLD) {
      rdd.dependencies.foreach {
        case s: ShuffleDependency[_, _, _] =>
          if (s.rdd.partitions.length < SHUFFLE_PREF_MAP_THRESHOLD) {
            // Get the preferred map output locations for this reducer
            val topLocsForReducer = mapOutputTracker.getLocationsWithLargestOutputs(s.shuffleId,
              partition, rdd.partitions.length, REDUCER_PREF_LOCS_FRACTION)
            if (topLocsForReducer.nonEmpty) {
              return topLocsForReducer.get.map(loc => TaskLocation(loc.host, loc.executorId))
            }
          }
        case _ =>
      }
    }
    Nil
  }

  def stop() {
    logInfo("Stopping DAGScheduler")
    messageScheduler.shutdownNow()
    eventProcessLoop.stop()
    taskScheduler.stop()
  }

  // Start the event thread and register the metrics source at the end of the constructor
  env.metricsSystem.registerSource(metricsSource)
  eventProcessLoop.start()
}
/**
 * 主要职责是调用DAGScheduler相应的方法来处理DAGScheduler发送给它的各种消息
 */
private[scheduler] class DAGSchedulerEventProcessLoop(dagScheduler: DAGScheduler)
  extends EventLoop[DAGSchedulerEvent]("dag-scheduler-event-loop") with Logging {

  private[this] val timer = dagScheduler.metricsSource.messageProcessingTimer

  /**
   * The main event loop of the DAG scheduler.
   */
  override def onReceive(event: DAGSchedulerEvent): Unit = {
    val timerContext = timer.time()
    try {
      doOnReceive(event)// 调用doOnReceive
    } finally {
      timerContext.stop()
    }
  }
  //模式匹配，是用过post的方式。
  private def doOnReceive(event: DAGSchedulerEvent): Unit = event match {
    //接收提交job，来自与RDD->SparkContext->DAGScheduler.submit方法。需要在这里中转一下，是为了模块功能的一致性。
    case JobSubmitted(jobId, rdd, func, partitions, callSite, listener, properties) =>
      dagScheduler.handleJobSubmitted(jobId, rdd, func, partitions, callSite, listener, properties)
    //消息源org.apache.spark.ui.jobs.JobProgressTab，在GUI上显示一个SparkContext的Job的执行状态。    
    //用户可以cancel一个Stage，会通过SparkContext->DAGScheduler 传递到这里。      
    case StageCancelled(stageId) =>
      dagScheduler.handleStageCancellation(stageId)
    //来自于org.apache.spark.scheduler.JobWaiter的消息。取消一个Job
    case JobCancelled(jobId) =>
      dagScheduler.handleJobCancellation(jobId)

    case JobGroupCancelled(groupId) =>// 取消整个Job Group
      dagScheduler.handleJobGroupCancelled(groupId)
    case AllJobsCancelled =>//取消所有Job
      dagScheduler.doCancelAllJobs()
    case ExecutorAdded(execId, host) =>
     // TaskScheduler得到一个Executor被添加的消息。具体来自org.apache.spark.scheduler.TaskSchedulerImpl.resourceOffers
      dagScheduler.handleExecutorAdded(execId, host)
  
    case ExecutorLost(execId) =>//来自TaskScheduler
      dagScheduler.handleExecutorLost(execId, fetchFailed = false)

    case BeginEvent(task, taskInfo) => //来自TaskScheduler
      dagScheduler.handleBeginEvent(task, taskInfo)

    case GettingResultEvent(taskInfo) =>//处理获得TaskResult信息的消息
      dagScheduler.handleGetTaskResult(taskInfo)

    case completion @ CompletionEvent(task, reason, _, _, taskInfo, taskMetrics) =>
      //来自TaskScheduler，报告task是完成或者失败
      dagScheduler.handleTaskCompletion(completion)

    case TaskSetFailed(taskSet, reason, exception) =>
      //来自TaskScheduler，要么TaskSet失败次数超过阈值或者由于Job Cancel
      dagScheduler.handleTaskSetFailed(taskSet, reason, exception)

    case ResubmitFailedStages =>
      //当一个Stage处理失败时，重试。来自org.apache.spark.scheduler.DAGScheduler.handleTaskCompletion
      dagScheduler.resubmitFailedStages()
  }

  override def onError(e: Throwable): Unit = {
    logError("DAGSchedulerEventProcessLoop failed; shutting down SparkContext", e)
    try {
      dagScheduler.doCancelAllJobs()
    } catch {
      case t: Throwable => logError("DAGScheduler failed to cancel all jobs.", t)
    }
    dagScheduler.sc.stop()
  }

  override def onStop(): Unit = {
    // Cancel any active jobs in postStop hook
    dagScheduler.cleanUpAfterSchedulerStop()
  }
}

private[spark] object DAGScheduler {
  // The time, in millis, to wait for fetch failure events to stop coming in after one is detected;
  // this is a simplistic way to avoid resubmitting tasks in the non-fetchable map stage one by one
  // as more failure events come in
  //
  val RESUBMIT_TIMEOUT = 200
}
