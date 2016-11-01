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
import java.nio.ByteBuffer
import java.util.Arrays
import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.math.{min, max}
import scala.util.control.NonFatal

import org.apache.spark._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.SchedulingMode._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.util.{Clock, SystemClock, Utils}

/**
 * Schedules the tasks within a single TaskSet in the TaskSchedulerImpl. This class keeps track of
 * each task, retries tasks if they fail (up to a limited number of times), and
 * handles locality-aware scheduling for this TaskSet via delay scheduling. The main interfaces
 * to it are resourceOffer, which asks the TaskSet whether it wants to run a task on one node,
 * and statusUpdate, which tells it that one of its tasks changed state (e.g. finished).
 * TaskSetManager 会根据数据的就近原则为Task分配计算资源,监控Task的执行状态并采取必要的措施,如:
 * 失败重试,慢任务的推测性执行.
 * 
 * THREADING: This class is designed to only be called from code with a lock on the
 * TaskScheduler (e.g. its event handlers). It should not be called from other threads.
 *
 * @param sched           the TaskSchedulerImpl associated with the TaskSetManager
 * @param taskSet         the TaskSet to manage scheduling for
 * @param maxTaskFailures if any particular task fails this number of times, the entire
 *                        task set will be aborted
 */
private[spark] class TaskSetManager(//任务集管理器
    sched: TaskSchedulerImpl,
    val taskSet: TaskSet,//接收提交的任务的集合
    val maxTaskFailures: Int,//最大失败提交次数
    clock: Clock = new SystemClock())
  extends Schedulable with Logging {

  val conf = sched.sc.conf

  /*
   * Sometimes if an executor is dead or in an otherwise invalid state, the driver
   * does not realize right away leading to repeated task failures. If enabled,
   * this temporarily prevents a task from re-launching on an executor where
   * it just failed.
   * worker节点出现了故障，task执行失败后会在该 executor上不断重试，达到最大重试次数后会导致整个application执行失败，
   * 设置失败task在该节点运行失败后会换节点重试
   */
  private val EXECUTOR_TASK_BLACKLIST_TIMEOUT =
    conf.getLong("spark.scheduler.executorTaskBlacklistTime", 0L)

  // Quantile of tasks at which to start speculation
  //推测启动前，Stage必须要完成总Task的百分比
  val SPECULATION_QUANTILE = conf.getDouble("spark.speculation.quantile", 0.75)
  //比已完成Task的运行速度中位数慢多少倍才启用推测
  val SPECULATION_MULTIPLIER = conf.getDouble("spark.speculation.multiplier", 1.5)

  // Limit of bytes for total size of results (default is 1GB)
  //限制最大记录数
  val maxResultSize = Utils.getMaxResultSize(conf)

  // Serializer for closures and tasks.
  val env = SparkEnv.get
  val ser = env.closureSerializer.newInstance()

  val tasks = taskSet.tasks //获得所有Task任务
  val numTasks = tasks.length
  val copiesRunning = new Array[Int](numTasks)//运行任务ID
  val successful = new Array[Boolean](numTasks)//全部成功完成
  private val numFailures = new Array[Int](numTasks)//失败数
  // key is taskId, value is a Map of executor id to when it failed
  //执行失败的executor
  private val failedExecutors = new HashMap[Int, HashMap[String, Long]]()
  //任务重试数,即所有任务数
  val taskAttempts = Array.fill[List[TaskInfo]](numTasks)(Nil)
  var tasksSuccessful = 0 //全部成功

  var weight = 1
  var minShare = 0
  var priority = taskSet.priority//优先级
  var stageId = taskSet.stageId
  var name = "TaskSet_" + taskSet.stageId.toString
  var parent: Pool = null
  var totalResultSize = 0L//总记录数
  var calculatedTasks = 0 //计算任务数


  val runningTasksSet = new HashSet[Long]
  //正在运行的任务数
  override def runningTasks: Int = runningTasksSet.size
  //True表示任务集管理器应启动一次任务,TaskSetManagers至少一次任务成功完成
  // True once no more tasks should be launched for this task set manager. TaskSetManagers enter
  // the zombie(僵尸状态) state once at least one attempt of each task has completed successfully, or if the
  // task set is aborted (for example, because it was killed).  TaskSetManagers remain in the zombie
  // state until all tasks have finished running; we keep TaskSetManagers that are in the zombie
  // state in order to continue to track and account for the running tasks.
  // TODO: We should kill any running task attempts when the task set manager becomes a zombie.
  var isZombie = false

  // Set of pending(待执行) tasks for each executor. These collections are actually
  // treated(已处理过) as stacks, in which new tasks are added to the end of the
  // ArrayBuffer and removed from the end. This makes it faster to detect
  // tasks that repeatedly fail because whenever a task failed
  //每当一个任务失败,使它更快地检测到重复失败的任务 
  //it is put back at the head of the stack. They are also only cleaned up lazily;
  // when a task is launched, it remains in all the pending lists except
  // the one that it was launched from, but gets removed from them later. 
  //每个executor上即将被执行的tasks的映射集合,key 为executoroId，value 为task index 数组
  private val pendingTasksForExecutor = new HashMap[String, ArrayBuffer[Int]]

  // Set of pending(待执行) tasks for each host. Similar to pendingTasksForExecutor,
  // but at host level.
  //每个host上即将被执行的tasks的映射集合 ,key为 host,value为 host的tasks索引数组
  private val pendingTasksForHost = new HashMap[String, ArrayBuffer[Int]]

  // Set of pending tasks for each rack(机架) -- similar to the above.
  //每个rack上即将被执行的tasks的映射集合  key为 rack,value为优先位置所在的 host属于该机架的 tasks
  private val pendingTasksForRack = new HashMap[String, ArrayBuffer[Int]]

  // Set containing pending tasks with no locality preferences(没有最佳位置).
  // 存储所有没有位置信息的即将运行tasks的index索引的集合  
  var pendingTasksWithNoPrefs = new ArrayBuffer[Int]

  // Set containing all pending tasks(待执行任务) (also used as a stack, as above).
  // 存储所有即将运行tasks的index索引的集合  
  val allPendingTasks = new ArrayBuffer[Int]

  // Tasks that can be speculated(推测的任务). Since these will be a small fraction of total
  // tasks, we'll just hold them in a HashSet.
  val speculatableTasks = new HashSet[Int]

  // Task index, start and finish time for each task attempt (indexed by task ID)
  //存储任务索引即Task Id,及TaskInfo对象 [TaksID->TaskInfo]   
  val taskInfos = new HashMap[Long, TaskInfo]

  // How frequently to reprint duplicate exceptions in full, in milliseconds
  val EXCEPTION_PRINT_INTERVAL =
    conf.getLong("spark.logging.exceptionPrintInterval", 10000)

  // Map of recent exceptions (identified by string representation and top stack frame) to
  // duplicate count (how many times the same exception has appeared) and time the full exception
  // was printed. This should ideally be an LRU map that can drop old exceptions automatically.
  val recentExceptions = HashMap[String, (Int, Long)]()

  // Figure out the current map output tracker epoch and set it on all tasks
  //找出当前Map 输出跟踪，并将其设置在所有的任务
  val epoch = sched.mapOutputTracker.getEpoch
  logDebug("Epoch for " + taskSet + ": " + epoch)
  for (t <- tasks) {
    t.epoch = epoch
  }

  // Add all our tasks to the pending lists. We do this in reverse order
  // of task index so that tasks with low indices get launched first.
  //将所有的tasks添加到pending列表。我们用倒序的任务索引一遍较低索引的任务可以被优先加载 
  for (i <- (0 until numTasks).reverse) {
    addPendingTask(i)
  }

  // Figure out which locality levels we have in our TaskSet, so we can do delay scheduling
  //当前TaskManager充许使用的本地化级别
  var myLocalityLevels = computeValidLocalityLevels()
  //本地化级别等待时间
  var localityWaits = myLocalityLevels.map(getLocalityWait) // Time to wait at each level

  // Delay scheduling variables: we keep track of our current locality level and the time we
  // last launched a task at that level, and move up a level when localityWaits[curLevel] expires.
  // We then move down if we manage to launch a "more local" task.
  //本地化索引级别,获取此本地化级别的等待时长
  var currentLocalityIndex = 0    // Index of our current locality level in validLocalityLevels
  //运行本地化时间
  var lastLaunchTime = clock.getTimeMillis()  // Time we last launched a task at this level

  override def schedulableQueue: ConcurrentLinkedQueue[Schedulable] = null

  override def schedulingMode: SchedulingMode = SchedulingMode.NONE

  var emittedTaskSizeWarning = false

  /**
   * Add a task to all the pending-task lists that it should be on.
   * 添加一个任务到待执行任务列表 ,如果重新添加只包括它在每个列表中
   * If readding is set, we are
   * re-adding the task so only include it in each list if it's not already there.
   * addPendingTask 获取 task 的优先位置，即一组hosts；再获得这组 hosts 对应的 executors，
   * 从来反过来获得了 executor 对应 tasks 的关系，即pendingTasksForExecutor
   * 
   */
  private def addPendingTask(index: Int, readding: Boolean = false) {
    // Utility method that adds `index` to a list only if readding=false or it's not already there
    //定义了一个如果索引不存在添加索引至列表的工具方法  
    def addTo(list: ArrayBuffer[Int]) {
      if (!readding || !list.contains(index)) {
        list += index
      }
    }
    //遍历task的优先位置  
    for (loc <- tasks(index).preferredLocations) {//task最佳位置
      loc match {
        case e: ExecutorCacheTaskLocation =>//如果为ExecutorCacheTaskLocation  
          //如果HashMap中存在键k，则返回键k的值。否则向HashMap中新增映射关系k -> v并返回d
          //添加任务索引index至pendingTasksForExecutor列表 
          addTo(pendingTasksForExecutor.getOrElseUpdate(e.executorId, new ArrayBuffer))
        case e: HDFSCacheTaskLocation => {
          //调用sched（即TaskSchedulerImpl）的getExecutorsAliveOnHost()方法,获得指定Host上的Alive Executors  
          val exe = sched.getExecutorsAliveOnHost(loc.host)
          exe match {
            case Some(set) => {
              //循环host上的每个Alive Executor，添加任务索引index至pendingTasksForExecutor列表  
              for (e <- set) {
                //如果HashMap中存在键k，则返回键k的值。否则向HashMap中新增映射关系k -> v并返回d
                addTo(pendingTasksForExecutor.getOrElseUpdate(e, new ArrayBuffer))
              }
              logInfo(s"Pending task $index has a cached location at ${e.host} " +
                ", where there are executors " + set.mkString(","))
            }
            case None => logDebug(s"Pending task $index has a cached location at ${e.host} " +
                ", but there are no executors alive there.")
          }
        }
        case _ => Unit
      }
      //添加任务索引index至pendingTasksForHost列表 
      addTo(pendingTasksForHost.getOrElseUpdate(loc.host, new ArrayBuffer))
      //根据获得任务优先位置host获得机架rack，循环，添加任务索引index至pendingTasksForRack列表  
      for (rack <- sched.getRackForHost(loc.host)) {
        addTo(pendingTasksForRack.getOrElseUpdate(rack, new ArrayBuffer))
      }
    }
    //如果task没有位置属性，则将任务的索引index添加到pendingTasksWithNoPrefs,
    //pendingTasksWithNoPrefs为存储所有没有位置信息的即将运行tasks的index索引的集合
    if (tasks(index).preferredLocations == Nil) {
      addTo(pendingTasksWithNoPrefs)
    }
    //将任务的索引index加入到allPendingTasks，allPendingTasks为存储所有即将运行tasks的index索引的集合 
    if (!readding) {
      allPendingTasks += index  // No point scanning this whole list to find the old task there
    }
  }

  /**
   * Return the pending tasks list for a given executor ID, or an empty list if
   * there is no map entry for that host
   * 一个给定的executor ID 返回的待执行任务的任务列表
   */
  private def getPendingTasksForExecutor(executorId: String): ArrayBuffer[Int] = {
    pendingTasksForExecutor.getOrElse(executorId, ArrayBuffer())
  }

  /**
   * 一个给定的主机返回的待执行任务的任务列表
   * Return the pending tasks list for a given host, or an empty list if
   * there is no map entry for that host
   */
  private def getPendingTasksForHost(host: String): ArrayBuffer[Int] = {
    pendingTasksForHost.getOrElse(host, ArrayBuffer())
  }

  /**
   * Return the pending rack-local task list for a given rack, or an empty list if
   * there is no map entry for that rack
   */
  private def getPendingTasksForRack(rack: String): ArrayBuffer[Int] = {
    pendingTasksForRack.getOrElse(rack, ArrayBuffer())
  }

  /**
   * Dequeue a pending task from the given list and return its index.
   * Return None if the list is empty.
   * 将一个待运的任务从给定的列表并返回其索引
   * This method also cleans up any tasks in the list that have already
   * been launched, since we want that to happen lazily.
   */
  private def dequeueTaskFromList(execId: String, list: ArrayBuffer[Int]): Option[Int] = {
    var indexOffset = list.size
    while (indexOffset > 0) {
      indexOffset -= 1//自减1
      val index = list(indexOffset)
      if (!executorIsBlacklisted(execId, index)) {//判断是否有运行失败的任务
        // This should almost always be list.trimEnd(1) to remove tail        
        list.remove(indexOffset)//删除最后一个
        if (copiesRunning(index) == 0 && !successful(index)) {
          return Some(index)
        }
      }
    }
    None
  }

  /** Check whether a task is currently running an attempt on a given host */
  private def hasAttemptOnHost(taskIndex: Int, host: String): Boolean = {
    taskAttempts(taskIndex).exists(_.host == host)
  }

  /**
   * Is this re-execution of a failed task on an executor it already failed in before
   * 这是在重新执行一个失败的任务，它已经之前失败过
   * EXECUTOR_TASK_BLACKLIST_TIMEOUT has elapsed ?
   */
  private def executorIsBlacklisted(execId: String, taskId: Int): Boolean = {
    if (failedExecutors.contains(taskId)) {//是否包含一个失败Task
      val failed = failedExecutors.get(taskId).get
         //包含execId和系统当前时间-存入Map系统时间
      return failed.contains(execId) &&
        clock.getTimeMillis() - failed.get(execId).get < EXECUTOR_TASK_BLACKLIST_TIMEOUT
    }

    false
  }

  /**
   * Return a speculative task for a given executor if any are available(). The task should not have
   * an attempt running on this host, in case the host is slow. In addition, the task should meet
   * the given locality constraint.
   */
  // Labeled as protected to allow tests to override providing speculative tasks if necessary
  protected def dequeueSpeculativeTask(execId: String, host: String, locality: TaskLocality.Value)
    : Option[(Int, TaskLocality.Value)] =
  {
    speculatableTasks.retain(index => !successful(index)) // Remove finished tasks from set

    def canRunOnHost(index: Int): Boolean =
      !hasAttemptOnHost(index, host) && !executorIsBlacklisted(execId, index)

    if (!speculatableTasks.isEmpty) {
      // Check for process-local tasks; note that tasks can be process-local
      // on multiple nodes when we replicate cached blocks, as in Spark Streaming
      for (index <- speculatableTasks if canRunOnHost(index)) {
        val prefs = tasks(index).preferredLocations
        val executors = prefs.flatMap(_ match {
          case e: ExecutorCacheTaskLocation => Some(e.executorId)
          case _ => None
        });
        if (executors.contains(execId)) {
          speculatableTasks -= index
          return Some((index, TaskLocality.PROCESS_LOCAL))
        }
      }

      // Check for node-local tasks
      if (TaskLocality.isAllowed(locality, TaskLocality.NODE_LOCAL)) {
        for (index <- speculatableTasks if canRunOnHost(index)) {
          val locations = tasks(index).preferredLocations.map(_.host)
          if (locations.contains(host)) {
            speculatableTasks -= index
            return Some((index, TaskLocality.NODE_LOCAL))
          }
        }
      }

      // Check for no-preference tasks
      if (TaskLocality.isAllowed(locality, TaskLocality.NO_PREF)) {
        for (index <- speculatableTasks if canRunOnHost(index)) {
          val locations = tasks(index).preferredLocations
          if (locations.size == 0) {
            speculatableTasks -= index
            return Some((index, TaskLocality.PROCESS_LOCAL))
          }
        }
      }

      // Check for rack-local tasks
      if (TaskLocality.isAllowed(locality, TaskLocality.RACK_LOCAL)) {
        for (rack <- sched.getRackForHost(host)) {
          for (index <- speculatableTasks if canRunOnHost(index)) {
            val racks = tasks(index).preferredLocations.map(_.host).map(sched.getRackForHost)
            if (racks.contains(rack)) {
              speculatableTasks -= index
              return Some((index, TaskLocality.RACK_LOCAL))
            }
          }
        }
      }

      // Check for non-local tasks
      if (TaskLocality.isAllowed(locality, TaskLocality.ANY)) {
        for (index <- speculatableTasks if canRunOnHost(index)) {
          speculatableTasks -= index
          return Some((index, TaskLocality.ANY))
        }
      }
    }

    None
  }

  /**
   * Dequeue a pending task for a given node and return its index and locality level.
   * Only search for tasks matching the given locality constraint.
   * 只有搜索匹配给定任务的存储位置
   * 将一个待处理的任务，对于一个给定的节点，返回任务集内的任务索引及存储级别
   * @return An option containing (task index within the task set, locality, is speculative?)
   */
  private def dequeueTask(execId: String, host: String, maxLocality: TaskLocality.Value)
    : Option[(Int, TaskLocality.Value, Boolean)] =
  {
    //查找在同一进程中待运行的任务
    for (index <- dequeueTaskFromList(execId, getPendingTasksForExecutor(execId))) {
      return Some((index, TaskLocality.PROCESS_LOCAL, false))
    }
    //查找数据在同一个节点待运行的任务
    if (TaskLocality.isAllowed(maxLocality, TaskLocality.NODE_LOCAL)) {
      for (index <- dequeueTaskFromList(execId, getPendingTasksForHost(host))) {
        return Some((index, TaskLocality.NODE_LOCAL, false))
      }
    }
   //查找数据在哪里访问都一样快待运行的任务
    if (TaskLocality.isAllowed(maxLocality, TaskLocality.NO_PREF)) {
      // Look for noPref tasks after NODE_LOCAL for minimize cross-rack traffic
      for (index <- dequeueTaskFromList(execId, pendingTasksWithNoPrefs)) {
        return Some((index, TaskLocality.PROCESS_LOCAL, false))
      }
    }
    //查找数据在同一机架的不同节点上待运行的任务
    if (TaskLocality.isAllowed(maxLocality, TaskLocality.RACK_LOCAL)) {
      for {
        rack <- sched.getRackForHost(host)
        index <- dequeueTaskFromList(execId, getPendingTasksForRack(rack))
      } {
        return Some((index, TaskLocality.RACK_LOCAL, false))
      }
    }
    //查找数据在非同一机架的网络上待运行的任务
    if (TaskLocality.isAllowed(maxLocality, TaskLocality.ANY)) {
      for (index <- dequeueTaskFromList(execId, allPendingTasks)) {
        return Some((index, TaskLocality.ANY, false))
      }
    }

    // find a speculative task if all others tasks have been scheduled
    dequeueSpeculativeTask(execId, host, maxLocality).map {
      case (taskIndex, allowedLocality) => (taskIndex, allowedLocality, true)}
  }

  /**
   * Respond to an offer of a single executor from the scheduler by finding a task
   * 给Worker分配Task
   * NOTE: this function is either called with a maxLocality which
   * would be adjusted by delay scheduling algorithm or it will be with a special
   * NO_PREF locality which will be not modified
   *  为taskSet分配资源，校验是否满足的逻辑
   * @param execId the executor Id of the offered resource
   * @param host  the host Id of the offered resource
   * @param maxLocality the maximum locality we want to schedule the tasks at
   */
  @throws[TaskNotSerializableException]
  def resourceOffer(
      execId: String,
      host: String,
      maxLocality: TaskLocality.TaskLocality)
    : Option[TaskDescription] =
  {
    if (!isZombie) {
      //获得当前开始时间
      val curTime = clock.getTimeMillis()
      //获取当前任务充许使用的本地化级别
      var allowedLocality = maxLocality      
      if (maxLocality != TaskLocality.NO_PREF) {
        allowedLocality = getAllowedLocalityLevel(curTime)
        if (allowedLocality > maxLocality) {
          // We're not allowed to search for farther-away tasks
          //我们不被允许搜索更远的任务
          allowedLocality = maxLocality
        }
      }
      //查找execId,host,pendingTasksWithNoPrefs中有待运行的task
      dequeueTask(execId, host, allowedLocality) match {
        case Some((index, taskLocality, speculative)) => {//speculative是否使用推测执行
          // Found a task; do some bookkeeping and return a task description
          //找到一个任务索引,返回一个任务描述,任务的最佳位置
          val task = tasks(index)
          val taskId = sched.newTaskId()//获得任务ID
          // Do various bookkeeping
          //记录运行的任务
          copiesRunning(index) += 1
          val attemptNum = taskAttempts(index).size//任务的提交重试次数
          //创建TaskInfo,并对task,addedFiles,addedJars进行序列化
          val info = new TaskInfo(taskId, index, attemptNum, curTime,
            execId, host, taskLocality, speculative)
          //存储任务索引即Task Id,及TaskInfo对象 [TaksID->TaskInfo] 
          taskInfos(taskId) = info
          //存储[TaksID->TaskInfo]
          taskAttempts(index) = info :: taskAttempts(index)
          // Update our locality level for delay scheduling
          // NO_PREF will not affect the variables related to delay scheduling
          //获取当前任务充许使用的本地化级别
          if (maxLocality != TaskLocality.NO_PREF) {
            currentLocalityIndex = getLocalityIndex(taskLocality)
            lastLaunchTime = curTime
          }
          // Serialize and return the task
          
          val startTime = clock.getTimeMillis()
          //序列化Task
          val serializedTask: ByteBuffer = try {
            Task.serializeWithDependencies(task, sched.sc.addedFiles, sched.sc.addedJars, ser)
          } catch {
            // If the task cannot be serialized, then there's no point to re-attempt the task,
            // as it will always fail. So just abort the whole task-set.
            case NonFatal(e) =>
              val msg = s"Failed to serialize task $taskId, not attempting to retry it."
              logError(msg, e)
              abort(s"$msg Exception during serialization: $e")
              throw new TaskNotSerializableException(e)
          }
          if (serializedTask.limit > TaskSetManager.TASK_SIZE_TO_WARN_KB * 1024 &&
              !emittedTaskSizeWarning) {
            emittedTaskSizeWarning = true
            logWarning(s"Stage ${task.stageId} contains a task of very large size " +
              s"(${serializedTask.limit / 1024} KB). The maximum recommended task size is " +
              s"${TaskSetManager.TASK_SIZE_TO_WARN_KB} KB.")
          }
          //HashSet runningTasksSet添加taskId
          addRunningTask(taskId)

          // We used to log the time it takes to serialize the task, but task size is already
          // a good proxy to task serialization time.
          // val timeTaken = clock.getTime() - startTime
          val taskName = s"task ${info.id} in stage ${taskSet.id}"
          logInfo("Starting %s (TID %d, %s, %s, %d bytes)".format(
              taskName, taskId, host, taskLocality, serializedTask.limit))
           //taskStarted向DagSchedulerEventProcessLoop发送BeginEvent事件
           //开始运行task
          sched.dagScheduler.taskStarted(task, info)
          //封装TaskDescription对象返回
          return Some(new TaskDescription(taskId = taskId, attemptNumber = attemptNum, execId,
            taskName, index, serializedTask))
        }
        case _ =>
      }
    }
    None
  }
//标记Taskset已完成
  private def maybeFinishTaskSet() {
    if (isZombie && runningTasks == 0) {
      sched.taskSetFinished(this)//如果所有Task都已经成功完成,那么从taskSetsByStageIdAndAttempt删除
    }
  }

  /**
   * Get the level we can launch tasks according to delay scheduling, based on current wait time.
   * 获取任务集充许使用的本地化级别
   */
  private def getAllowedLocalityLevel(curTime: Long): TaskLocality.TaskLocality = {
    // Remove the scheduled or finished tasks lazily
    //判断task是否可以被调度  
    def tasksNeedToBeScheduledFrom(pendingTaskIds: ArrayBuffer[Int]): Boolean = {
      var indexOffset = pendingTaskIds.size
       // 循环  
      while (indexOffset > 0) {
         // 索引递减  
        indexOffset -= 1
        // 获得task索引  
        val index = pendingTaskIds(indexOffset)
         // 如果对应task不存在任何运行实例，且未执行成功，可以调度，返回true  
        if (copiesRunning(index) == 0 && !successful(index)) {
          return true
        } else {
          // 从pendingTaskIds中移除  
          pendingTaskIds.remove(indexOffset)
        }
      }
      false
    }
    // Walk through the list of tasks that can be scheduled at each location and returns true
    // if there are any tasks that still need to be scheduled. Lazily cleans up tasks that have
    // already been scheduled.
    //
    def moreTasksToRunIn(pendingTasks: HashMap[String, ArrayBuffer[Int]]): Boolean = {
      val emptyKeys = new ArrayBuffer[String]
       // 循环pendingTasks  
      val hasTasks = pendingTasks.exists {
        case (id: String, tasks: ArrayBuffer[Int]) =>
          // 判断task是否可以被调度  
          if (tasksNeedToBeScheduledFrom(tasks)) {
            true
          } else {
            emptyKeys += id
            false
          }
      }
      // The key could be executorId, host or rackId
       // 移除数据  
      emptyKeys.foreach(id => pendingTasks.remove(id))
      hasTasks
    }
   //根据当前本地化级别,获得此本地化的等待时间,从当前索引currentLocalityIndex开始，循环myLocalityLevels  
    while (currentLocalityIndex < myLocalityLevels.length - 1) {
      // 是否存在待调度task，根据不同的Locality Level，调用moreTasksToRunIn()方法从不同的数据结构中获取，  
      // NO_PREF直接看pendingTasksWithNoPrefs是否为空  
      val moreTasks = myLocalityLevels(currentLocalityIndex) match {
        case TaskLocality.PROCESS_LOCAL => moreTasksToRunIn(pendingTasksForExecutor)//即同一个 executor上
        case TaskLocality.NODE_LOCAL => moreTasksToRunIn(pendingTasksForHost)//数据在同一个节点上
        case TaskLocality.NO_PREF => pendingTasksWithNoPrefs.nonEmpty
        case TaskLocality.RACK_LOCAL => moreTasksToRunIn(pendingTasksForRack)//数据在同一机架的不同节点上
      }
      if (!moreTasks) {// 不存在可以被调度的task  
        // This is a performance optimization: if there are no more tasks that can
        // be scheduled at a particular locality level, there is no point in waiting
        // for the locality wait timeout (SPARK-4939).
        // 记录lastLaunchTime  
         lastLaunchTime = curTime
        logDebug(s"No tasks for locality level ${myLocalityLevels(currentLocalityIndex)}, " +
          s"so moving to locality level ${myLocalityLevels(currentLocalityIndex + 1)}")
         //位置策略索引加1 
        currentLocalityIndex += 1
      } else if (curTime - lastLaunchTime >= localityWaits(currentLocalityIndex)) {
        //如果当前时间与上次运行本地化时间之差大于等于上一步获得的时间
        // Jump to the next locality level, and reset lastLaunchTime so that the next locality
        // wait timer doesn't immediately expire
        //运行本地化时间增加获取本地化级别的等待时长
        lastLaunchTime += localityWaits(currentLocalityIndex)
        //将位置策略currentLocalityIndex索引加1
        currentLocalityIndex += 1
        logDebug(s"Moving to ${myLocalityLevels(currentLocalityIndex)} after waiting for " +
          s"${localityWaits(currentLocalityIndex)}ms")
      } else {
        return myLocalityLevels(currentLocalityIndex)
      }
    }
    myLocalityLevels(currentLocalityIndex)
  }

  /**
   * Find the index in myLocalityLevels for a given locality. This is also designed to work with
   * localities that are not in myLocalityLevels (in case we somehow get those) by returning the
   * next-biggest level we have. Uses the fact that the last value in myLocalityLevels is ANY.
   * 查找一个给定的数据本地性级别,返回一个最好存储级别索引
   */
  def getLocalityIndex(locality: TaskLocality.TaskLocality): Int = {
    var index = 0
    while (locality > myLocalityLevels(index)) {
      index += 1
    }
    index
  }

  /**
   * Marks the task as getting result and notifies the DAG Scheduler
   * 对TaskSet中的任务信息进行成功标记
   */
  def handleTaskGettingResult(tid: Long): Unit = {
    val info = taskInfos(tid)
    info.markGettingResult()
    sched.dagScheduler.taskGettingResult(info)
  }

  /**
   * Check whether has enough quota to fetch the result with `size` bytes
   * 检查是否有足够的配额来获取“大小”字节的结果
   */
  def canFetchMoreResults(size: Long): Boolean = sched.synchronized {
  // 如果结果的大小大于1GB，那么直接丢弃，
   // 可以在spark.driver.maxResultSize设置
    totalResultSize += size
    calculatedTasks += 1
    if (maxResultSize > 0 && totalResultSize > maxResultSize) {
      val msg = s"Total size of serialized results of ${calculatedTasks} tasks " +
        s"(${Utils.bytesToString(totalResultSize)}) is bigger than spark.driver.maxResultSize " +
        s"(${Utils.bytesToString(maxResultSize)})"
      logError(msg)
      abort(msg)
      false
    } else {
      true
    }
  }

  /**
   * Marks the task as successful and notifies the DAGScheduler that a task has ended.
   * 标记任务全部成功完成
   */
  def handleSuccessfulTask(tid: Long, result: DirectTaskResult[_]): Unit = {
    val info = taskInfos(tid)
    val index = info.index
    info.markSuccessful()//标记Task完成时间
    removeRunningTask(tid)//从正在运行集合中移除Task
    // This method is called by "TaskSchedulerImpl.handleSuccessfulTask" which holds the
    // "TaskSchedulerImpl" lock until exiting. To avoid the SPARK-7655 issue, we should not
    // "deserialize" the value when holding a lock to avoid blocking other threads. So we call
    // "result.value()" in "TaskResultGetter.enqueueSuccessfulTask" before reaching here.
    // Note: "result.value()" only deserializes the value when it's called at the first time, so
    // here "result.value()" just returns the value and won't block other threads.
    //DAGSchedulerEventProcessLoop接收CompletionEvent消息,将处理交给CompletionEvent
    sched.dagScheduler.taskEnded(
       //Success返回任务成功
      tasks(index), Success, result.value(), result.accumUpdates, info, result.metrics)
    if (!successful(index)) {
      tasksSuccessful += 1
      logInfo("Finished task %s in stage %s (TID %d) in %d ms on %s (%d/%d)".format(
        info.id, taskSet.id, info.taskId, info.duration, info.host, tasksSuccessful, numTasks))
      // Mark successful and stop if all the tasks have succeeded.
      successful(index) = true
      if (tasksSuccessful == numTasks) {//如果所有任务都成功完成
        isZombie = true
      }
    } else {
      logInfo("Ignoring task-finished event for " + info.id + " in stage " + taskSet.id +
        " because task " + index + " has already completed successfully")
    }
    //根据索引删除TaskInfo
    failedExecutors.remove(index)
    //标记全部任务完成
    maybeFinishTaskSet()
  }

  /**
   * Marks the task as failed, re-adds it to the list of pending tasks, and notifies the
   * DAG Scheduler.
   * 首先会调用taskSetManager来处理任务失败的情况,如果任务的失败数没有超过阈值,那么会重新提交任务
   */
  def handleFailedTask(tid: Long, state: TaskState, reason: TaskEndReason) {
    val info = taskInfos(tid)
    if (info.failed) {
      return
    }
    removeRunningTask(tid)//移除给定tid
    info.markFailed()//标记任务失败
    val index = info.index
    copiesRunning(index) -= 1
    var taskMetrics : TaskMetrics = null

    val failureReason = s"Lost task ${info.id} in stage ${taskSet.id} (TID $tid, ${info.host}): " +
      reason.asInstanceOf[TaskFailedReason].toErrorString
    val failureException: Option[Throwable] = reason match {
      case fetchFailed: FetchFailed =>
        logWarning(failureReason)
        if (!successful(index)) {
          successful(index) = true
          tasksSuccessful += 1
        }
        // Not adding to failed executors for FetchFailed.
        isZombie = true
        None

      case ef: ExceptionFailure =>
        taskMetrics = ef.metrics.orNull
        if (ef.className == classOf[NotSerializableException].getName) {
          // If the task result wasn't serializable, there's no point in trying to re-execute it.
          logError("Task %s in stage %s (TID %d) had a not serializable result: %s; not retrying"
            .format(info.id, taskSet.id, tid, ef.description))
          abort("Task %s in stage %s (TID %d) had a not serializable result: %s".format(
            info.id, taskSet.id, tid, ef.description))
          return
        }
        val key = ef.description
        val now = clock.getTimeMillis()
        val (printFull, dupCount) = {
          if (recentExceptions.contains(key)) {
            val (dupCount, printTime) = recentExceptions(key)
            if (now - printTime > EXCEPTION_PRINT_INTERVAL) {
              recentExceptions(key) = (0, now)
              (true, 0)
            } else {
              recentExceptions(key) = (dupCount + 1, printTime)
              (false, dupCount + 1)
            }
          } else {
            recentExceptions(key) = (0, now)
            (true, 0)
          }
        }
        if (printFull) {
          logWarning(failureReason)
        } else {
          logInfo(
            s"Lost task ${info.id} in stage ${taskSet.id} (TID $tid) on executor ${info.host}: " +
            s"${ef.className} (${ef.description}) [duplicate $dupCount]")
        }
        ef.exception

      case e: TaskFailedReason =>  // TaskResultLost, TaskKilled, and others
        logWarning(failureReason)
        None

      case e: TaskEndReason =>
        logError("Unknown TaskEndReason: " + e)
        None
    }
    //always add to failed executors
    //添加失败的执行者
    failedExecutors.getOrElseUpdate(index, new HashMap[String, Long]()).
      put(info.executorId, clock.getTimeMillis())
      //调用DAGScheduler级别的容错
    sched.dagScheduler.taskEnded(tasks(index), reason, null, null, info, taskMetrics)
    //标记为等待调度
    addPendingTask(index)
    if (!isZombie && state != TaskState.KILLED && !reason.isInstanceOf[TaskCommitDenied]) {
      
      // If a task failed because its attempt to commit was denied, do not count this failure
      // towards failing the stage. This is intended to prevent spurious stage failures in cases
      // where many speculative tasks are launched and denied to commit.
      assert (null != failureReason)
      numFailures(index) += 1
      if (numFailures(index) >= maxTaskFailures) {
        //如果失败次数已经超过阈值,那么标记该TaskSetManager为失败
        //阈值可以通过Spark.task.maxFailurse设置,默认值是4
        logError("Task %d in stage %s failed %d times; aborting job".format(
          index, taskSet.id, maxTaskFailures))
        abort("Task %d in stage %s failed %d times, most recent failure: %s\nDriver stacktrace:"
          .format(index, taskSet.id, maxTaskFailures, failureReason), failureException)
        return
      }
    }
    //设置TaskSet完成
    maybeFinishTaskSet()
  }

  def abort(message: String, exception: Option[Throwable] = None): Unit = sched.synchronized {
    // TODO: Kill running tasks if we were not terminated due to a Mesos error
    sched.dagScheduler.taskSetFailed(taskSet, message, exception)
    isZombie = true
    maybeFinishTaskSet()
  }

  /** 
   *  If the given task ID is not in the set of running tasks, adds it.
   *  如果给定的task ID不在运行任务的集合中，则添加它,用于跟踪运行任务的数量,执行调度策略
   * Used to keep track of the number of running tasks, for enforcing scheduling policies.
   */
  def addRunningTask(tid: Long) {
    if (runningTasksSet.add(tid) && parent != null) {
      parent.increaseRunningTasks(1)
    }
  }

  /** 
   *  If the given task ID is in the set of running tasks, removes it.
   *  如果给定的task ID在运行任务的集合中，则删除它
   *   */
  def removeRunningTask(tid: Long) {
    if (runningTasksSet.remove(tid) && parent != null) {
      parent.decreaseRunningTasks(1)
    }
  }

  override def getSchedulableByName(name: String): Schedulable = {
    null
  }

  override def addSchedulable(schedulable: Schedulable) {}

  override def removeSchedulable(schedulable: Schedulable) {}

  override def getSortedTaskSetQueue(): ArrayBuffer[TaskSetManager] = {
    var sortedTaskSetQueue = new ArrayBuffer[TaskSetManager]()
    sortedTaskSetQueue += this
    sortedTaskSetQueue
  }

  /** 
   *  Called by TaskScheduler when an executor is lost so we can re-enqueue our tasks
   *  调用TaskScheduler重新排列的任务
   *  */
  override def executorLost(execId: String, host: String) {
    logInfo("Re-queueing tasks for " + execId + " from TaskSet " + taskSet.id)

    // Re-enqueue pending tasks for this host based on the status of the cluster. Note
    // that it's okay if we add a task to the same queue twice (if it had multiple preferred
    // locations), because dequeueTaskFromList will skip already-running tasks.
    for (index <- getPendingTasksForExecutor(execId)) {
      addPendingTask(index, readding = true)
    }
    for (index <- getPendingTasksForHost(host)) {
      addPendingTask(index, readding = true)
    }

    // Re-enqueue any tasks that ran on the failed executor if this is a shuffle map stage,
    // and we are not using an external shuffle server which could serve the shuffle outputs.
    // The reason is the next stage wouldn't be able to fetch the data from this dead executor
    // so we would need to rerun these tasks on other executors.
    if (tasks(0).isInstanceOf[ShuffleMapTask] && !env.blockManager.externalShuffleServiceEnabled) {
      for ((tid, info) <- taskInfos if info.executorId == execId) {
        val index = taskInfos(tid).index
        if (successful(index)) {
          successful(index) = false
          copiesRunning(index) -= 1
          tasksSuccessful -= 1
          addPendingTask(index)
          // Tell the DAGScheduler that this task was resubmitted so that it doesn't think our
          // stage finishes when a total of tasks.size tasks finish.
          sched.dagScheduler.taskEnded(tasks(index), Resubmitted, null, null, info, null)
        }
      }
    }
    // Also re-enqueue any tasks that were running on the node
    for ((tid, info) <- taskInfos if info.running && info.executorId == execId) {
      handleFailedTask(tid, TaskState.FAILED, ExecutorLostFailure(execId))
    }
    // recalculate valid locality levels and waits when executor is lost
    recomputeLocality()
  }

  /**
   * Check for tasks to be speculated and return true if there are any. This is called periodically
   * by the TaskScheduler.
   *
   * TODO: To make this scale to large jobs, we need to maintain a list of running tasks, so that
   * we don't scan the whole task set. It might also help to make this sorted by launch time.
   */
  override def checkSpeculatableTasks(): Boolean = {
    // Can't speculate if we only have one task, and no need to speculate if the task set is a
    // zombie.
    if (isZombie || numTasks == 1) {
      return false
    }
    var foundTasks = false
    val minFinishedForSpeculation = (SPECULATION_QUANTILE * numTasks).floor.toInt
    logDebug("Checking for speculative tasks: minFinished = " + minFinishedForSpeculation)
    if (tasksSuccessful >= minFinishedForSpeculation && tasksSuccessful > 0) {
      val time = clock.getTimeMillis()
      val durations = taskInfos.values.filter(_.successful).map(_.duration).toArray
      Arrays.sort(durations)
      val medianDuration = durations(min((0.5 * tasksSuccessful).round.toInt, durations.size - 1))
      val threshold = max(SPECULATION_MULTIPLIER * medianDuration, 100)
      // TODO: Threshold should also look at standard deviation of task durations and have a lower
      // bound based on that.
      logDebug("Task length threshold for speculation: " + threshold)
      for ((tid, info) <- taskInfos) {
        val index = info.index
        if (!successful(index) && copiesRunning(index) == 1 && info.timeRunning(time) > threshold &&
          !speculatableTasks.contains(index)) {
          logInfo(
            "Marking task %d in stage %s (on %s) as speculatable because it ran more than %.0f ms"
              .format(index, taskSet.id, info.host, threshold))
          speculatableTasks += index
          foundTasks = true
        }
      }
    }
    foundTasks
  }
  /**
   *获取Locality级别TaskSetManager分配任务等待时间
   */
  private def getLocalityWait(level: TaskLocality.TaskLocality): Long = {
   //在执行一个本地数据任务时候,放弃并执行到一个非本地数据的地方前,需要等待的时间
    val defaultWait = conf.get("spark.locality.wait", "3s")//本地化级别的默认等待时间3秒
    val localityWaitKey = level match {
      case TaskLocality.PROCESS_LOCAL => "spark.locality.wait.process"//同一个进程的即内存等待时间
      case TaskLocality.NODE_LOCAL => "spark.locality.wait.node"//同一个节点的等待时间
      case TaskLocality.RACK_LOCAL => "spark.locality.wait.rack"//同一个机架的等待时间
      case _ => null//
    }

    if (localityWaitKey != null) {
      conf.getTimeAsMs(localityWaitKey, defaultWait)//默认等待3秒
    } else {
      0L
    }
  }

  /**
   * Compute the locality levels used in this TaskSet. Assumes that all tasks have already been
   * added to queues using addPendingTask.
   * 计算TaskSet使用的数据本地性位置策略(级别),假设所有的任务已经通过addPendingTask()被添加入队列 
   * PROCESS_LOCAL为例:如果存在Executor中有待执行的任务(pendingTasksForExecutor不为空)且PROCESS_LOCAL
   * 本地化的等待时间不为0(调用getLocalityWait方法获得)且存在Executor已经激活(pendingTasksForExecutor.isExecutorAlive)
   * 那么允许本地化级别里包含PROCESS_LOCAL
   */
  private def computeValidLocalityLevels(): Array[TaskLocality.TaskLocality] = {
    // 引入任务位置策略  
    import TaskLocality.{PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY}
     // 创建ArrayBuffer类型的levels，存储TaskLocality  
    val levels = new ArrayBuffer[TaskLocality.TaskLocality]
    //locality levels是否包含 PROCESS_LOCAL
    /**
     * 如果pendingTasksForExecutor不为空，且PROCESS_LOCAL级别中TaskSetManager等待分配下一个任务的时间不为零，且  
     * 如果pendingTasksForExecutor中每个executorId在sched的executorIdToTaskCount中存在 
     * executorIdToTaskCount为每个executor上运行的task的数目集合  
     */
    if (!pendingTasksForExecutor.isEmpty && getLocalityWait(PROCESS_LOCAL) != 0 &&
        //pendingTasksForExecutor存储 key 为executoroId，value 为task index 数组
        //isExecutorAlive判断参数中的 executor id 当前是否 active        
        pendingTasksForExecutor.keySet.exists(sched.isExecutorAlive(_))) {
      levels += PROCESS_LOCAL
    }
    //如果pendingTasksForHost不为空，且NODE_LOCAL级别中TaskSetManager等待分配下一个任务的时间不为零，且  
    //如果pendingTasksForHost中每个host在sched的executorsByHost中存在  
    //executorsByHost为每个host上executors的集合  
    if (!pendingTasksForHost.isEmpty && getLocalityWait(NODE_LOCAL) != 0 &&       
       //taskSetManager的所有 tasks对应的所有 hosts,是否有任一是 tasks的优先位置 hosts,若有返回 true,否则返回 fals
        pendingTasksForHost.keySet.exists(sched.hasExecutorsAliveOnHost(_))) {
      levels += NODE_LOCAL
    }
    //如果存在没有位置信息的task，则添加NO_PREF级别
    if (!pendingTasksWithNoPrefs.isEmpty) {
      levels += NO_PREF
    }
    //同样处理RACK_LOCAL级别
    if (!pendingTasksForRack.isEmpty && getLocalityWait(RACK_LOCAL) != 0 &&
        //pendingTasksForRack保存key为 rack，value 为优先位置所在的 host 属于该机架的 tasks
        /**
         * 判断 taskSetManager的locality levels是否包含RACK_LOCAL的规则为：
         * taskSetManager的所有tasks的优先位置 host所在的所有racks与当前 active executors所在的机架是否有交集，
         * 若有则返回 true，否则返回 false
         */
        pendingTasksForRack.keySet.exists(sched.hasHostAliveOnRack(_))) {
      levels += RACK_LOCAL
    }
    //最后加上一个ANY级别  
    levels += ANY
    logDebug("Valid locality levels for " + taskSet + ": " + levels.mkString(", "))
    //返回   
    levels.toArray
  }
  //重新计算位置  
  def recomputeLocality() {
    //它是有效位置策略级别中的索引,指示当前的位置信息。也就是我们上一个task被launched所使用的Locality Level
    //currentLocalityIndex为有效位置策略级别中的索引，默认为0  
    val previousLocalityLevel = myLocalityLevels(currentLocalityIndex)
     //确定在我们的任务集TaskSet中应该使用哪种位置Level,以便我们做延迟调度  
    myLocalityLevels = computeValidLocalityLevels()
    //获得位置策略级别的等待时间  
    localityWaits = myLocalityLevels.map(getLocalityWait)
    //设置当前使用的位置策略级别的索引 
    currentLocalityIndex = getLocalityIndex(previousLocalityLevel)
  }

  def executorAdded() {
    recomputeLocality()
  }
}

private[spark] object TaskSetManager {
  // The user will be warned if any stages contain a task that has a serialized size greater than
  // this.
  val TASK_SIZE_TO_WARN_KB = 100
}
