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

import java.util.concurrent.TimeUnit

import scala.collection.mutable
import scala.util.control.ControlThrowable

import com.codahale.metrics.{Gauge, MetricRegistry}

import org.apache.spark.scheduler._
import org.apache.spark.metrics.source.Source
import org.apache.spark.util.{ThreadUtils, Clock, SystemClock, Utils}

/**
 * 用于对已分配的Executor进行管理,创建和启动ExecutorAllocationManager,默认情况下不会创建ExecutorAllocationManager
 * 可以修改spark.dynamicAllocation.enabled为true来创建,
 * spark.dynamicAllocation.minExecutors设置动态分配最小Executor数量
 * spark.dynamicAllocation.maxExecutors设置动态分配最大Executor数量
 * spark.dynamicAllocation.initialExecutors每个Executor可以运行的Task数量等配置信息
 * 代理动态分配和删除executors
 * An agent that dynamically allocates and removes executors based on the workload.
 *
 * The ExecutorAllocationManager maintains a moving target number of executors which is periodically
 * synced to the cluster manager. The target starts at a configured initial value and changes with
 * the number of pending and running tasks.
 *
 * Decreasing the target number of executors happens when the current target is more than needed to
 * handle the current load. The target number of executors is always truncated to the number of
 * executors that could run all current running and pending tasks at once.
 *
 * Increasing the target number of executors happens in response to backlogged tasks waiting to be
 * scheduled. If the scheduler queue is not drained in N seconds, then new executors are added. If
 * the queue persists for another M seconds, then more executors are added and so on. The number
 * added in each round increases exponentially from the previous round until an upper bound has been
 * reached. The upper bound is based both on a configured property and on the current number of
 * running and pending tasks, as described above.
 *
 * The rationale for the exponential increase is twofold: (1) Executors should be added slowly
 * in the beginning in case the number of extra executors needed turns out to be small. Otherwise,
 * we may add more executors than we need just to remove them later. (2) Executors should be added
 * quickly over time in case the maximum number of executors is very high. Otherwise, it will take
 * a long time to ramp up under heavy workloads.
 *
 * The remove policy is simpler: If an executor has been idle for K seconds, meaning it has not
 * been scheduled to run any tasks, then it is removed.
 *
 * There is no retry logic in either case because we make the assumption that the cluster manager
 * will eventually fulfill all requests it receives asynchronously.
 *
 * The relevant Spark properties include the following:
 *
 *   spark.dynamicAllocation.enabled - Whether this feature is enabled
 *   spark.dynamicAllocation.minExecutors - Lower bound on the number of executors
 *   spark.dynamicAllocation.maxExecutors - Upper bound on the number of executors
 *   spark.dynamicAllocation.initialExecutors - Number of executors to start with
 *
 *   spark.dynamicAllocation.schedulerBacklogTimeout (M) -
 *     If there are backlogged tasks for this duration, add new executors
 *
 *   spark.dynamicAllocation.sustainedSchedulerBacklogTimeout (N) -
 *     If the backlog is sustained for this duration, add more executors
 *     This is used only after the initial backlog timeout is exceeded
 *
 *   spark.dynamicAllocation.executorIdleTimeout (K) -
 *     If an executor has been idle for this duration, remove it
 */
private[spark] class ExecutorAllocationManager(
    client: ExecutorAllocationClient,
    listenerBus: LiveListenerBus,
    conf: SparkConf)
  extends Logging {

  allocationManager =>

  import ExecutorAllocationManager._

  // Lower and upper bounds on the number of executors.执行数下限和上限
  private val minNumExecutors = conf.getInt("spark.dynamicAllocation.minExecutors", 0)
  private val maxNumExecutors = conf.getInt("spark.dynamicAllocation.maxExecutors",
    Integer.MAX_VALUE)

  // How long there must be backlogged tasks for before an addition is triggered (seconds)
  //触发添加之前必须有多长时间的任务（秒）
  private val schedulerBacklogTimeoutS = conf.getTimeAsSeconds(
    "spark.dynamicAllocation.schedulerBacklogTimeout", "1s")

  // Same as above, but used only after `schedulerBacklogTimeoutS` is exceeded
  //与上述相同，但仅在“schedulerBacklogTimeoutS”超出后才使用
  private val sustainedSchedulerBacklogTimeoutS = conf.getTimeAsSeconds(
    "spark.dynamicAllocation.sustainedSchedulerBacklogTimeout", s"${schedulerBacklogTimeoutS}s")

  // How long an executor must be idle for before it is removed (seconds)
  //执行者在删除之前必须空闲多久（秒）
  private val executorIdleTimeoutS = conf.getTimeAsSeconds(
    "spark.dynamicAllocation.executorIdleTimeout", "60s")

  private val cachedExecutorIdleTimeoutS = conf.getTimeAsSeconds(
    "spark.dynamicAllocation.cachedExecutorIdleTimeout", s"${Integer.MAX_VALUE}s")

  // During testing, the methods to actually kill and add executors are mocked out
  //在测试期间,实际上杀死和添加执行器的方法被
  private val testing = conf.getBoolean("spark.dynamicAllocation.testing", false)

  // TODO: The default value of 1 for spark.executor.cores works right now because dynamic
  // allocation is only supported for YARN and the default number of cores per executor in YARN is
  // 1, but it might need to be attained differently for different cluster managers
  //YARN仅支持分配，YARN中每个执行者的默认核心数量为1,但是对于不同的集群管理器,可能需要不同的方式实现
  //为每个任务分配的内核数
  private val tasksPerExecutor =
    conf.getInt("spark.executor.cores", 1) / conf.getInt("spark.task.cpus", 1)

  validateSettings()

  // Number of executors to add in the next round
  //在下一轮增加的执行数
  private var numExecutorsToAdd = 1

  // The desired number of executors at this moment in time. If all our executors were to die, this
  // is the number of executors we would immediately want from the cluster manager.
  //在此时刻所需的执行数,如果我们的所有执行者都要死,这个是我们将立即从集群管理器获得的执行者的数量。
  private var numExecutorsTarget =
    conf.getInt("spark.dynamicAllocation.initialExecutors", minNumExecutors)

  // Executors that have been requested to be removed but have not been killed yet
  //已要求将其删除,但没有死亡的Executors
  private val executorsPendingToRemove = new mutable.HashSet[String]

  // All known executors
  //所有已知executors
  private val executorIds = new mutable.HashSet[String]

  // A timestamp of when an addition should be triggered, or NOT_SET if it is not set
  // This is set when pending tasks are added but not scheduled yet
  //何时触发添加的时间戳，如果未设置，则为NOT_SET当待添加但尚未安排的任务已设置
  //应该触发加法的时间戳，如果未设置则为NOT_SET,这是在添加但未安排的挂起任务时设置的
  private var addTime: Long = NOT_SET

  // A timestamp for each executor of when the executor should be removed, indexed by the ID
  // This is set when an executor is no longer running a task, or when it first registers
  //每个执行者的执行者应该被删除的时间戳,由ID进行索引当这个执行者不再运行任务时,或者当它首次注册时
  //时间戳的每个executor的执行时应删除
  private val removeTimes = new mutable.HashMap[String, Long]

  // Polling loop interval (ms)
  //循环间隔
  private val intervalMillis: Long = 100

  // Clock used to schedule when executors should be added and removed
  // 时钟用于调度执行时应添加和删除
  private var clock: Clock = new SystemClock()

  // Listener for Spark events that impact the allocation policy
  // 侦听Spark事件引发的侦听器
  private val listener = new ExecutorAllocationListener

  // Executor that handles the scheduling task.
  // 处理调度任务的执行器线程池
  private val executor =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("spark-dynamic-executor-allocation")

  // Metric source for ExecutorAllocationManager to expose internal status to MetricsSystem.
  //ExecutorAllocationManager的Metric源显示MetricsSystem的内部状态。
  val executorAllocationManagerSource = new ExecutorAllocationManagerSource

  // Whether we are still waiting for the initial set of executors to be allocated.
  //我们是否仍在等待初始执行者被分配。虽然这是真的,我们不会取消未完成的执行的要求, 在以下情况下设置为false
  // While this is true, we will not cancel outstanding executor requests. This is
  // set to false when:
  //   (1) a stage is submitted, or 提交一个stage,或
  //   (2) an executor idle timeout has elapsed. 执行者空闲超时已过
  //是否仍在等待执行被分配的初始设置,
  @volatile private var initializing: Boolean = true

  // Number of locality aware tasks, used for executor placement.
  //用于执行最佳位置任务的数量
  private var localityAwareTasks = 0

  // Host to possible task running on it, used for executor placement.
  //可能运行主机上的任务
  private var hostToLocalTaskCount: Map[String, Int] = Map.empty

  /**
   * Verify that the settings specified through the config are valid.
   * If not, throw an appropriate exception.
   *验证通过配置指定的设置是否有效,如果无有效,抛出适当的异常。
   */
  private def validateSettings(): Unit = {
    if (minNumExecutors < 0 || maxNumExecutors < 0) {
      throw new SparkException("spark.dynamicAllocation.{min/max}Executors must be positive!")
    }
    if (maxNumExecutors == 0) {
      throw new SparkException("spark.dynamicAllocation.maxExecutors cannot be 0!")
    }
    if (minNumExecutors > maxNumExecutors) {
      throw new SparkException(s"spark.dynamicAllocation.minExecutors ($minNumExecutors) must " +
        s"be less than or equal to spark.dynamicAllocation.maxExecutors ($maxNumExecutors)!")
    }
    if (schedulerBacklogTimeoutS <= 0) {
      throw new SparkException("spark.dynamicAllocation.schedulerBacklogTimeout must be > 0!")
    }
    if (sustainedSchedulerBacklogTimeoutS <= 0) {
      throw new SparkException(
        "spark.dynamicAllocation.sustainedSchedulerBacklogTimeout must be > 0!")
    }
    if (executorIdleTimeoutS <= 0) {
      throw new SparkException("spark.dynamicAllocation.executorIdleTimeout must be > 0!")
    }
    // Require external shuffle service for dynamic allocation
    // Otherwise, we may lose shuffle files when killing executors
    //需要外部随机服务进行动态分配 否则,我们可能在杀死执行者时丢失随机文件
    if (!conf.getBoolean("spark.shuffle.service.enabled", false) && !testing) {
      throw new SparkException("Dynamic allocation of executors requires the external " +
        "shuffle service. You may enable this through spark.shuffle.service.enabled.")
    }
    if (tasksPerExecutor == 0) {
      throw new SparkException("spark.executor.cores must not be less than spark.task.cpus.")
    }
  }

  /**
   * Use a different clock for this allocation manager. This is mainly used for testing.
    * 为此分配管理器使用不同的时钟
   */
  def setClock(newClock: Clock): Unit = {
    clock = newClock
  }

  /**
   * Register for scheduler callbacks to decide when to add and remove executors, and start
   * the scheduling task.
   * 启动调度任务,注册调度回调方法来决定添加和删除的Executor
   * 
   */
  def start(): Unit = {
    //将ExecutorAllocationListener添加LiveListenerBus中    
    listenerBus.addListener(listener)
    val scheduleTask = new Runnable() {
      override def run(): Unit = {
        try {
          schedule()
        } catch {
          case ct: ControlThrowable =>
            throw ct
          case t: Throwable =>
            logWarning(s"Uncaught exception in thread ${Thread.currentThread().getName}", t)
        }
      }
    }
    //通过线程不断添加exector,遍历exector,将超进的Eexecutor杀掉并移除
    executor.scheduleAtFixedRate(scheduleTask, 0, intervalMillis, TimeUnit.MILLISECONDS)
  }

  /**
   * Stop the allocation manager. 停止分配manager
   */
  def stop(): Unit = {
    executor.shutdown()
    executor.awaitTermination(10, TimeUnit.SECONDS)
  }

  /**
   * 最大Executros数
   * The maximum number of executors we would need under the current load to satisfy all running
   * and pending tasks, rounded up.
    * 在目前的负载下,我们需要的最大执行数达到所有正在运行的和待处理的任务的数量。
   */
  private def maxNumExecutorsNeeded(): Int = {
    val numRunningOrPendingTasks = listener.totalPendingTasks + listener.totalRunningTasks
    (numRunningOrPendingTasks + tasksPerExecutor - 1) / tasksPerExecutor
  }

  /**
   * This is called at a fixed interval to regulate the number of pending executor requests
   * and number of executors running.
   * 这是在一个固定的时间间隔,调整未执行和运行executor数。
   * First, adjust our requested executors based on the add time and our current needs.
   * Then, if the remove time for an existing executor has expired, kill the executor.
   * 首先,根据增加的时间和我们当前的需求调整我们要求的执行者,然后,如果现有执行程序的删除时间已过期,则将执行程序删除。
   * This is factored out into its own method for testing.
    * 这是考虑到自己的测试方法
   * 
   */
  private def schedule(): Unit = synchronized {
    val now = clock.getTimeMillis //获得当前时间

    updateAndSyncNumExecutorsTarget(now)
    //retain保留
    removeTimes.retain { case (executorId, expireTime) =>
      //executorId,expireTime到期时间
      val expired = now >= expireTime
      if (expired) {
        initializing = false
        removeExecutor(executorId)
      }
      !expired
    }
  }

  /**
   * Updates our target number of executors and syncs the result with the cluster manager.
   * 同步更新集群管理器执行目标数
   * Check to see whether our existing allocation and the requests we've made previously exceed our
   * current needs. If so, truncate our target and let the cluster manager know so that it can
   * cancel pending requests that are unneeded.
    * 检查我们现有的配置和我们以前提出的请求是否超出了我们当前的需求。
    * 如果是这样,请截断我们的目标,让集群管理员知道,以便它可以取消不需要的待处理请求。
   *
   * If not, and the add time has expired, see if we can request new executors and refresh the add
   * time.
   * 如果没有,并且添加时间已过,请查看我们是否可以请求新的执行者并刷新添加时间。
   * @return the delta in the target number of executors.
   */
  private def updateAndSyncNumExecutorsTarget(now: Long): Int = synchronized {
    val maxNeeded = maxNumExecutorsNeeded

    if (initializing) {
      // Do not change our target while we are still initializing,
      //当我们还在初始化时,不要改变我们的目标
      // Otherwise the first job may have to ramp up unnecessarily
      //否则第一份工作可能不必要地增加
      0
    } else if (maxNeeded < numExecutorsTarget) {
      // The target number exceeds the number we actually need, so stop adding new
      // executors and inform the cluster manager to cancel the extra pending requests
      //目标数量超过我们实际需要的数量,因此停止添加新的执行程序,并通知集群管理器取消额外的待处理请求
      val oldNumExecutorsTarget = numExecutorsTarget
      numExecutorsTarget = math.max(maxNeeded, minNumExecutors)
      numExecutorsToAdd = 1

      // If the new target has not changed, avoid sending a message to the cluster manager
      //如果新目标未更改,请避免向群集管理器发送消息
      if (numExecutorsTarget < oldNumExecutorsTarget) {
        client.requestTotalExecutors(numExecutorsTarget, localityAwareTasks, hostToLocalTaskCount)
        logDebug(s"Lowering target number of executors to $numExecutorsTarget (previously " +
          s"$oldNumExecutorsTarget) because not all requested executors are actually needed")
      }
      numExecutorsTarget - oldNumExecutorsTarget
    } else if (addTime != NOT_SET && now >= addTime) {
      val delta = addExecutors(maxNeeded)
      logDebug(s"Starting timer to add more executors (to " +
        s"expire in $sustainedSchedulerBacklogTimeoutS seconds)")
      addTime += sustainedSchedulerBacklogTimeoutS * 1000
      delta
    } else {
      0
    }
  }

  /**
   * Request a number of executors from the cluster manager.
   * If the cap on the number of executors is reached, give up and reset the
   * number of executors to add next round instead of continuing to double it.
   * 从集群管理器请求的执行者,如果在执行者的数量达到上限,放弃执行数复位而不是继续添加下一轮双
   * @param maxNumExecutorsNeeded the maximum number of executors all currently running or pending
   *                              tasks could fill
   * @return the number of additional executors actually requested.
   */
  private def addExecutors(maxNumExecutorsNeeded: Int): Int = {
    // Do not request more executors if it would put our target over the upper bound
    //不要求更多的执行者,如果它将我们的目标超过上限
    if (numExecutorsTarget >= maxNumExecutors) {
      logDebug(s"Not adding executors because our current target total " +
        s"is already $numExecutorsTarget (limit $maxNumExecutors)")
      numExecutorsToAdd = 1
      return 0
    }

    val oldNumExecutorsTarget = numExecutorsTarget
    // There's no point in wasting time ramping up to the number of executors we already have, so
    // make sure our target is at least as much as our current allocation:
    //确保我们的目标至少与我们目前的配置一样多：
    numExecutorsTarget = math.max(numExecutorsTarget, executorIds.size)
    // Boost our target with the number to add for this round:
    //提高我们的目标,增加这一轮的数字：
    numExecutorsTarget += numExecutorsToAdd
    // Ensure that our target doesn't exceed what we need at the present moment:
    //确保我们的目标不会超过我们现在需要的目标：
    numExecutorsTarget = math.min(numExecutorsTarget, maxNumExecutorsNeeded)
    // Ensure that our target fits within configured bounds:
    //确保我们的目标适合配置的边界：
    numExecutorsTarget = math.max(math.min(numExecutorsTarget, maxNumExecutors), minNumExecutors)

    val delta = numExecutorsTarget - oldNumExecutorsTarget

    // If our target has not changed, do not send a message
    // to the cluster manager and reset our exponential growth
    //如果我们的目标没有改变,不要发送消息到集群管理器并重置我们的指数增长
    if (delta == 0) {
      numExecutorsToAdd = 1
      return 0
    }

    val addRequestAcknowledged = testing ||
      client.requestTotalExecutors(numExecutorsTarget, localityAwareTasks, hostToLocalTaskCount)
    if (addRequestAcknowledged) {
      val executorsString = "executor" + { if (delta > 1) "s" else "" }
      logInfo(s"Requesting $delta new $executorsString because tasks are backlogged" +
        s" (new desired total will be $numExecutorsTarget)")
      numExecutorsToAdd = if (delta == numExecutorsToAdd) {
        numExecutorsToAdd * 2
      } else {
        1
      }
      delta
    } else {
      logWarning(
        s"Unable to reach the cluster manager to request $numExecutorsTarget total executors!")
      0
    }
  }

  /**
   * Request the cluster manager to remove the given executor.
   * Return whether the request is received.
    * 请求集群管理器删除给定的执行器,返回是否收到请求。
   */
  private def removeExecutor(executorId: String): Boolean = synchronized {
    // Do not kill the executor if we are not aware of it (should never happen)
    //如果我们不知道(不应该发生),不要杀死执行者
    if (!executorIds.contains(executorId)) {
      logWarning(s"Attempted to remove unknown executor $executorId!")
      return false
    }

    // Do not kill the executor again if it is already pending to be killed (should never happen)
    //如果已经等待被杀死(不应该发生),不要再次杀死执行者
    if (executorsPendingToRemove.contains(executorId)) {
      logWarning(s"Attempted to remove executor $executorId " +
        s"when it is already pending to be removed!")
      return false
    }

    // Do not kill the executor if we have already reached the lower bound
    //如果我们已经达到了下限,不要杀死执行者
    val numExistingExecutors = executorIds.size - executorsPendingToRemove.size
    if (numExistingExecutors - 1 < minNumExecutors) {
      logDebug(s"Not removing idle executor $executorId because there are only " +
        s"$numExistingExecutors executor(s) left (limit $minNumExecutors)")
      return false
    }

    // Send a request to the backend to kill this executor
    //向后端发送请求以杀死这个执行者
    val removeRequestAcknowledged = testing || client.killExecutor(executorId)
    if (removeRequestAcknowledged) {
      logInfo(s"Removing executor $executorId because it has been idle for " +
        s"$executorIdleTimeoutS seconds (new desired total will be ${numExistingExecutors - 1})")
      executorsPendingToRemove.add(executorId)
      true
    } else {
      logWarning(s"Unable to reach the cluster manager to kill executor $executorId!")
      false
    }
  }

  /**
   * Callback invoked when the specified executor has been added.
    * 调用指定的执行程序时调用回调函数
   */
  private def onExecutorAdded(executorId: String): Unit = synchronized {
    if (!executorIds.contains(executorId)) {
      executorIds.add(executorId)
      // If an executor (call this executor X) is not removed because the lower bound
      // has been reached, it will no longer be marked as idle. When new executors join,
      // however, we are no longer at the lower bound, and so we must mark executor X
      // as idle again so as not to forget that it is a candidate for removal. (see SPARK-4951)
      executorIds.filter(listener.isExecutorIdle).foreach(onExecutorIdle)
      logInfo(s"New executor $executorId has registered (new total is ${executorIds.size})")
    } else {
      logWarning(s"Duplicate executor $executorId has registered")
    }
  }

  /**
   * Callback invoked when the specified executor has been removed.
   */
  private def onExecutorRemoved(executorId: String): Unit = synchronized {
    if (executorIds.contains(executorId)) {
      executorIds.remove(executorId)
      removeTimes.remove(executorId)
      logInfo(s"Existing executor $executorId has been removed (new total is ${executorIds.size})")
      if (executorsPendingToRemove.contains(executorId)) {
        executorsPendingToRemove.remove(executorId)
        logDebug(s"Executor $executorId is no longer pending to " +
          s"be removed (${executorsPendingToRemove.size} left)")
      }
    } else {
      logWarning(s"Unknown executor $executorId has been removed!")
    }
  }

  /**
   * Callback invoked when the scheduler receives new pending tasks.
   * This sets a time in the future that decides when executors should be added
   * if it is not already set.
   */
  private def onSchedulerBacklogged(): Unit = synchronized {
    if (addTime == NOT_SET) {
      logDebug(s"Starting timer to add executors because pending tasks " +
        s"are building up (to expire in $schedulerBacklogTimeoutS seconds)")
      addTime = clock.getTimeMillis + schedulerBacklogTimeoutS * 1000
    }
  }

  /**
   * Callback invoked when the scheduler queue is drained.
   * This resets all variables used for adding executors.
   */
  private def onSchedulerQueueEmpty(): Unit = synchronized {
    logDebug("Clearing timer to add executors because there are no more pending tasks")
    addTime = NOT_SET
    numExecutorsToAdd = 1
  }

  /**
   * Callback invoked when the specified executor is no longer running any tasks.
   * This sets a time in the future that decides when this executor should be removed if
   * the executor is not already marked as idle.
   */
  private def onExecutorIdle(executorId: String): Unit = synchronized {
    if (executorIds.contains(executorId)) {
      if (!removeTimes.contains(executorId) && !executorsPendingToRemove.contains(executorId)) {
        // Note that it is not necessary to query the executors since all the cached
        // blocks we are concerned with are reported to the driver. Note that this
        // does not include broadcast blocks.
        val hasCachedBlocks = SparkEnv.get.blockManager.master.hasCachedBlocks(executorId)
        val now = clock.getTimeMillis()
        val timeout = {
          if (hasCachedBlocks) {
            // Use a different timeout if the executor has cached blocks.
            //如果执行器具有缓存块,则使用不同的超时。
            now + cachedExecutorIdleTimeoutS * 1000
          } else {
            now + executorIdleTimeoutS * 1000
          }
        }
        val realTimeout = if (timeout <= 0) Long.MaxValue else timeout // overflow
        removeTimes(executorId) = realTimeout
        logDebug(s"Starting idle timer for $executorId because there are no more tasks " +
          s"scheduled to run on the executor (to expire in ${(realTimeout - now)/1000} seconds)")
      }
    } else {
      logWarning(s"Attempted to mark unknown executor $executorId idle")
    }
  }

  /**
   * Callback invoked when the specified executor is now running a task.
   * This resets all variables used for removing this executor.
    * 当指定的执行程序正在运行任务时调用回调,这将重置用于删除此执行程序的所有变量。
   */
  private def onExecutorBusy(executorId: String): Unit = synchronized {
    logDebug(s"Clearing idle timer for $executorId because it is now running a task")
    removeTimes.remove(executorId)
  }

  /**
   * A listener that notifies the given allocation manager of when to add and remove executors.
   * 通知给定分配管理器何时添加和删除执行程序的侦听器
   * This class is intentionally conservative in its assumptions about the relative ordering
   * and consistency of events returned by the listener. For simplicity, it does not account
   * for speculated tasks.
    * 该类在其关于侦听器返回的事件的相对排序和一致性的假设中有意保守,为了简单起见,它不考虑推测的任务。
   * 
   */
  private class ExecutorAllocationListener extends SparkListener {

    private val stageIdToNumTasks = new mutable.HashMap[Int, Int]
    private val stageIdToTaskIndices = new mutable.HashMap[Int, mutable.HashSet[Int]]
    private val executorIdToTaskIds = new mutable.HashMap[String, mutable.HashSet[Long]]
    // Number of tasks currently running on the cluster.  Should be 0 when no stages are active.
    //群集上当前运行的任务数,当没有阶段活跃时应为0。
    private var numRunningTasks: Int = _

    // stageId to tuple (the number of task with locality preferences, a map where each pair is a
    // node and the number of tasks that would like to be scheduled on that node) map,
    // maintain the executor placement hints for each stage Id used by resource framework to better
    // place the executors.
    private val stageIdToExecutorPlacementHints = new mutable.HashMap[Int, (Int, Map[String, Int])]

    override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
      initializing = false
      val stageId = stageSubmitted.stageInfo.stageId
      val numTasks = stageSubmitted.stageInfo.numTasks
      allocationManager.synchronized {
        stageIdToNumTasks(stageId) = numTasks
        allocationManager.onSchedulerBacklogged()

        // Compute the number of tasks requested by the stage on each host
        //计算每个主机上stage请求的任务数
        var numTasksPending = 0
        val hostToLocalTaskCountPerStage = new mutable.HashMap[String, Int]()
        stageSubmitted.stageInfo.taskLocalityPreferences.foreach { locality =>
          if (!locality.isEmpty) {
            numTasksPending += 1
            locality.foreach { location =>
              val count = hostToLocalTaskCountPerStage.getOrElse(location.host, 0) + 1
              hostToLocalTaskCountPerStage(location.host) = count
            }
          }
        }
        stageIdToExecutorPlacementHints.put(stageId,
          (numTasksPending, hostToLocalTaskCountPerStage.toMap))

        // Update the executor placement hints
        //更新执行者放置提示
        updateExecutorPlacementHints()
      }
    }

    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
      val stageId = stageCompleted.stageInfo.stageId
      allocationManager.synchronized {
        stageIdToNumTasks -= stageId
        stageIdToTaskIndices -= stageId
        stageIdToExecutorPlacementHints -= stageId

        // Update the executor placement hints
        //更新执行者放置提示
        updateExecutorPlacementHints()

        // If this is the last stage with pending tasks, mark the scheduler queue as empty
        // This is needed in case the stage is aborted for any reason
        //如果这是具有待处理任务的最后阶段,则将调度程序队列标记为空这是需要的,以防由于任何原因中止了该阶段
        if (stageIdToNumTasks.isEmpty) {
          allocationManager.onSchedulerQueueEmpty()
          if (numRunningTasks != 0) {
            logWarning("No stages are running, but numRunningTasks != 0")
            numRunningTasks = 0
          }
        }
      }
    }

    override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
      val stageId = taskStart.stageId
      val taskId = taskStart.taskInfo.taskId
      val taskIndex = taskStart.taskInfo.index
      val executorId = taskStart.taskInfo.executorId

      allocationManager.synchronized {
        numRunningTasks += 1
        // This guards against the race condition in which the `SparkListenerTaskStart`
        // event is posted before the `SparkListenerBlockManagerAdded` event, which is
        // possible because these events are posted in different threads. (see SPARK-4951)
        //这样可以防止在SparkListenerBaskManagerAdded事件之前发布“SparkListenerTaskStart”事件的竞争条件,
        //这可能是因为这些事件发布在不同的线程中。 （见SPARK-4951）
        if (!allocationManager.executorIds.contains(executorId)) {
          allocationManager.onExecutorAdded(executorId)
        }

        // If this is the last pending task, mark the scheduler queue as empty
        //如果这是最后待处理的任务,将调度程序队列标记为空
        stageIdToTaskIndices.getOrElseUpdate(stageId, new mutable.HashSet[Int]) += taskIndex
        if (totalPendingTasks() == 0) {
          allocationManager.onSchedulerQueueEmpty()
        }

        // Mark the executor on which this task is scheduled as busy
        //标记这个任务被安排为忙的执行者
        executorIdToTaskIds.getOrElseUpdate(executorId, new mutable.HashSet[Long]) += taskId
        allocationManager.onExecutorBusy(executorId)
      }
    }

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
      val executorId = taskEnd.taskInfo.executorId
      val taskId = taskEnd.taskInfo.taskId
      val taskIndex = taskEnd.taskInfo.index
      val stageId = taskEnd.stageId
      allocationManager.synchronized {
        numRunningTasks -= 1
        // If the executor is no longer running any scheduled tasks, mark it as idle
        //如果执行程序不再运行任何计划任务,请将其标记为空闲
        if (executorIdToTaskIds.contains(executorId)) {
          executorIdToTaskIds(executorId) -= taskId
          if (executorIdToTaskIds(executorId).isEmpty) {
            executorIdToTaskIds -= executorId
            allocationManager.onExecutorIdle(executorId)
          }
        }

        // If the task failed, we expect it to be resubmitted later. To ensure we have
        // enough resources to run the resubmitted task, we need to mark the scheduler
        // as backlogged again if it's not already marked as such (SPARK-8366)
        //如果任务失败,我们期望稍后重新提交, 为了确保我们有足够的资源来运行重新提交的任务,我们需要将调度程序再次标记为不再标记为（SPARK-8366）
        if (taskEnd.reason != Success) {
          if (totalPendingTasks() == 0) {
            allocationManager.onSchedulerBacklogged()
          }
          stageIdToTaskIndices.get(stageId).foreach { _.remove(taskIndex) }
        }
      }
    }

    override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
      val executorId = executorAdded.executorId
      if (executorId != SparkContext.DRIVER_IDENTIFIER) {
        // This guards against the race condition in which the `SparkListenerTaskStart`
        // event is posted before the `SparkListenerBlockManagerAdded` event, which is
        // possible because these events are posted in different threads. (see SPARK-4951)
        //这样可以防范“SparkListenerTaskStart”的比赛条件事件发布在“SparkListenerBlockManagerAdded”事件之前
        //可能因为这些事件发布在不同的线程中。 （见SPARK-4951）
        if (!allocationManager.executorIds.contains(executorId)) {
          allocationManager.onExecutorAdded(executorId)
        }
      }
    }

    override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
      allocationManager.onExecutorRemoved(executorRemoved.executorId)
    }

    /**
     * An estimate of the total number of pending tasks remaining for currently running stages. Does
     * not account for tasks which may have failed and been resubmitted.
      * 对当前运行阶段剩余的待处理任务总数的估计,不考虑可能失败并重新提交的任务。
     *
     * Note: This is not thread-safe without the caller owning the `allocationManager` lock.
      * 注意：如果调用者拥有“allocManager”锁，这不是线程安全的。
     */
    def totalPendingTasks(): Int = {
      stageIdToNumTasks.map { case (stageId, numTasks) =>
        numTasks - stageIdToTaskIndices.get(stageId).map(_.size).getOrElse(0)
      }.sum
    }

    /**
     * The number of tasks currently running across all stages.
      * 目前在所有阶段运行的任务数量
     */
    def totalRunningTasks(): Int = numRunningTasks

    /**
     * Return true if an executor is not currently running a task, and false otherwise.
     * 如果执行者当前没有运行任务,返回true,否则返回false。
     * Note: This is not thread-safe without the caller owning the `allocationManager` lock.
      * 注意：如果调用者拥有“allocManager”锁,这不是线程安全的。
     */
    def isExecutorIdle(executorId: String): Boolean = {
      !executorIdToTaskIds.contains(executorId)
    }

    /**
     * Update the Executor placement hints (the number of tasks with locality preferences,
     * a map where each pair is a node and the number of tasks that would like to be scheduled
     * on that node).
     * 更新Executor位置提示(具有本地偏好设置的任务数,每对是一个节点的映射以及该节点上要安排的任务数)。
     * These hints are updated when stages arrive and complete, so are not up-to-date at task
     * granularity within stages.
      * 这些提示在阶段到达和完成时更新,因此在阶段内的任务粒度不是最新的。
     */
    def updateExecutorPlacementHints(): Unit = {
      var localityAwareTasks = 0
      val localityToCount = new mutable.HashMap[String, Int]()
      stageIdToExecutorPlacementHints.values.foreach { case (numTasksPending, localities) =>
        localityAwareTasks += numTasksPending
        localities.foreach { case (hostname, count) =>
          val updatedCount = localityToCount.getOrElse(hostname, 0) + count
          localityToCount(hostname) = updatedCount
        }
      }

      allocationManager.localityAwareTasks = localityAwareTasks
      allocationManager.hostToLocalTaskCount = localityToCount.toMap
    }
  }

  /**
   * Metric source for ExecutorAllocationManager to expose its internal executor allocation
   * status to MetricsSystem.
    * ExecutorAllocationManager的Metric源公开其内部执行器分配状态到MetricsSystem。
   * Note: These metrics heavily rely on the internal implementation of
   * ExecutorAllocationManager, metrics or value of metrics will be changed when internal
   * implementation is changed, so these metrics are not stable across Spark version.
    * 注意：这些指标严重依赖于ExecutorAllocationManager的内部实现，度量或指标值将在内部更改实施更改,所以这些指标在Spark版本之间是不稳定的。
   */
  private[spark] class ExecutorAllocationManagerSource extends Source {
    val sourceName = "ExecutorAllocationManager"
    val metricRegistry = new MetricRegistry()

    private def registerGauge[T](name: String, value: => T, defaultValue: T): Unit = {
      metricRegistry.register(MetricRegistry.name("executors", name), new Gauge[T] {
        override def getValue: T = synchronized { Option(value).getOrElse(defaultValue) }
      })
    }

    registerGauge("numberExecutorsToAdd", numExecutorsToAdd, 0)
    registerGauge("numberExecutorsPendingToRemove", executorsPendingToRemove.size, 0)
    registerGauge("numberAllExecutors", executorIds.size, 0)
    registerGauge("numberTargetExecutors", numExecutorsTarget, 0)
    registerGauge("numberMaxNeededExecutors", maxNumExecutorsNeeded(), 0)
  }
}

private object ExecutorAllocationManager {
  val NOT_SET = Long.MaxValue
}
