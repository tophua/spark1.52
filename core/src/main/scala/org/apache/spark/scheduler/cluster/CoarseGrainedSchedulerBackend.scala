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

package org.apache.spark.scheduler.cluster

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}

import org.apache.spark.rpc._
import org.apache.spark.{ExecutorAllocationClient, Logging, SparkEnv, SparkException, TaskState}
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.util.{ThreadUtils, SerializableBuffer, AkkaUtils, Utils}

/**
 * A scheduler backend that waits for coarse grained executors to connect to it through Akka.
 * 一个后台调度器等待粗粒执行器通过Akka连接,
 * This backend holds onto each executor for the duration of the Spark job rather than relinquishing
 * 这个后端保存到每个的执行器的持续时间,而不是Spark作业(Job)放弃执行器
 * executors whenever a task is done and asking the scheduler to launch a new executor for
 * 每当一个任务完成要求的调度,为每一个新任务启动一个新的执行器
 * each new task. Executors may be launched in a variety of ways, such as Mesos tasks for the
 * 执行器启动多种方式,如粗颗粒 Mesos模式或者Spark独立部署的独立进程模式
 * coarse-grained Mesos mode or standalone processes for Spark's standalone deploy mode
 * (spark.deploy.*).
 */
private[spark]
class CoarseGrainedSchedulerBackend(scheduler: TaskSchedulerImpl, val rpcEnv: RpcEnv)
  extends ExecutorAllocationClient with SchedulerBackend with Logging
{
  // Use an atomic variable to track total number of cores in the cluster for simplicity and speed
  //使用原子变量简单速度跟踪在群集中的内核的总数
  var totalCoreCount = new AtomicInteger(0)
  // Total number of executors that are currently registered
  //当前已注册的执行器(executors)数
  var totalRegisteredExecutors = new AtomicInteger(0)
  val conf = scheduler.sc.conf
  private val akkaFrameSize = AkkaUtils.maxFrameSizeBytes(conf) 
  // Submit tasks only after (registered resources / total expected resources)
   //提交任务之后(注册资源/预计总资源)
  // is equal to at least this value, that is double between 0 and 1.
  //至少这个值是相等的,在1和0之间
  var minRegisteredRatio =
    math.min(1, conf.getDouble("spark.scheduler.minRegisteredResourcesRatio", 0))
  // Submit tasks after maxRegisteredWaitingTime milliseconds
  // 提交任务后最大注册等待时间(maxRegisteredWaitingTime)毫秒
  // if minRegisteredRatio has not yet been reached
  //如果minregisteredratio尚未达到
  val maxRegisteredWaitingTimeMs =
    conf.getTimeAsMs("spark.scheduler.maxRegisteredResourcesWaitingTime", "30s")
  val createTime = System.currentTimeMillis()
  //集群中executor的数据集合,key为String类型的executorId,value为ExecutorData类型的executor详细信息
  private val executorDataMap = new HashMap[String, ExecutorData]

  // Number of executors requested from the cluster manager that have not registered yet
  //从集群管理器请求的尚未注册的执行程序数
  //请求的执行器数,还没有注册群集管理器
  private var numPendingExecutors = 0

  private val listenerBus = scheduler.sc.listenerBus

  // Executors we have requested the cluster manager to kill that have not died yet
  //我们请求集群管理器杀死执行器,并没有死亡的执行器
  private val executorsPendingToRemove = new HashSet[String]

  // A map to store hostname with its possible task number running on it
  //Map存储主机及可能运行的任务数  
  protected var hostToLocalTaskCount: Map[String, Int] = Map.empty

  // The number of pending tasks which is locality required
  //位置所需的待解决的任务数
  protected var localityAwareTasks = 0
/**
 * 
 * 整个程序运行时候的驱动器,例如接收CoarseGrainedExecutorBackend的注册,是CoarseGrainedExecutorBackend的内部成员
 * 并与Cluster Manager进行通信与调度
 */
  class DriverEndpoint(override val rpcEnv: RpcEnv, sparkProperties: Seq[(String, String)])
    extends ThreadSafeRpcEndpoint with Logging {

    // If this DriverEndpoint is changed to support multiple threads,
    // 如果 DriverEndpoint的改变支持多线程
    // then this may need to be changed so that we don't share the serializer
    //那么这可能需要改变,所以我们不能共享跨线程序列化实例
    // instance across threads
    private val ser = SparkEnv.get.closureSerializer.newInstance()

    override protected def log = CoarseGrainedSchedulerBackend.this.log

    private val addressToExecutorId = new HashMap[RpcAddress, String]

    private val reviveThread =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor("driver-revive-thread")

    override def onStart() {
      // Periodically revive offers to allow delay scheduling to work
      //提供定期恢复允许延迟调度工作节点,重新获取资源的Task的最长间隔(毫秒)时间
      val reviveIntervalMs = conf.getTimeAsMs("spark.scheduler.revive.interval", "1s")

      reviveThread.scheduleAtFixedRate(new Runnable {
        override def run(): Unit = Utils.tryLogNonFatalError {
          //启动向自己发送ReviveOffers消息
          Option(self).foreach(_.send(ReviveOffers))
        }
      }, 0, reviveIntervalMs, TimeUnit.MILLISECONDS)
    }
   //收到CoarseGrainedExecutorBackend发送StatusUpdate消息,
    override def receive: PartialFunction[Any, Unit] = {
      case StatusUpdate(executorId, taskId, state, data) =>
        //调用TaskSchedulerImpl.statusUpdate
        scheduler.statusUpdate(taskId, state, data.value)
        if (TaskState.isFinished(state)) {
          executorDataMap.get(executorId) match {
            case Some(executorInfo) =>
              executorInfo.freeCores += scheduler.CPUS_PER_TASK
              //并且根据更新后的Executor重新调度
              makeOffers(executorId)
            case None =>
              // Ignoring the update since we don't know about the executor.
              //忽略任务状态更新,因为我们不知道的执行器的Id
              logWarning(s"Ignored task status update ($taskId state $state) " +
                s"from unknown executor with ID $executorId")
          }
        }
    /**
     * CoarseGrainedSchedulerBackend的start方法调用发送ReviveOffers消息,向DriverEndpoint发送ReviveOffers消息
     * DriverEndpoint接收到ReviveOffers消息后调用makeOffers
     */
      case ReviveOffers =>
        makeOffers()

      case KillTask(taskId, executorId, interruptThread) =>
        executorDataMap.get(executorId) match {
          case Some(executorInfo) =>
            executorInfo.executorEndpoint.send(KillTask(taskId, executorId, interruptThread))
          case None =>
            // Ignoring the task kill since the executor is not registered.
            // 试图杀死未知的执行器的任务
            logWarning(s"Attempted to kill task $taskId for unknown executor $executorId.")
        }

    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
       /**
        * ask
       * 主要向DriverAction发送RegisterExecutor消息,DriverActor接到RegisterExecutor消息后处理步骤:
       * 1)向CoarseGrainedExecutorBackend发送RegisteredExecutor消息,CoarseGrainedExecutorBackend收到RegisteredExecutor消息后
       *   创建Executor
       * 2)更新Executor所在地址与Executor的映射关系(addressToExecutorId),
       *   Driver获取的共CPU核数(totalCoreCount),注册到Driver的Exceuctor总数(totalRegisteredExecutors)等信息
       * 3)创建ExecutorData并且注册到executorDataMap中
       * 4)调用makeOffers方法执行任务  
       * 
       */
      case RegisterExecutor(executorId, executorRef, hostPort, cores, logUrls) =>
        Utils.checkHostPort(hostPort, "Host port expected " + hostPort)
        if (executorDataMap.contains(executorId)) {
          context.reply(RegisterExecutorFailed("Duplicate executor ID: " + executorId))
        } else {
          logInfo("Registered executor: " + executorRef + " with ID " + executorId)
          addressToExecutorId(executorRef.address) = executorId
          totalCoreCount.addAndGet(cores)
          totalRegisteredExecutors.addAndGet(1)
          val (host, _) = Utils.parseHostPort(hostPort)
          val data = new ExecutorData(executorRef, executorRef.address, host, cores, cores, logUrls)
          // This must be synchronized because variables mutated
          //这必须是同步的,因为变量是可变的
          // in this block are read when requesting executors
          //在块的读取请求执行
          CoarseGrainedSchedulerBackend.this.synchronized {            
            //在Driver中通过ExecutorData封装并注册ExecutorBackend的信息到Driver的内存数据结构executorMapData中
            executorDataMap.put(executorId, data)
            if (numPendingExecutors > 0) {
              numPendingExecutors -= 1
              logDebug(s"Decremented number of pending executors ($numPendingExecutors left)")
            }
          }
          // Note: some tests expect the reply to come after we put the executor in the map
          //注意:一些测试期望的答复后,我们把执行器放在Map中
          context.reply(RegisteredExecutor)
          listenerBus.post(
            SparkListenerExecutorAdded(System.currentTimeMillis(), executorId, data))
          makeOffers()
        }

      case StopDriver =>
        context.reply(true)
        stop()

      case StopExecutors =>
        logInfo("Asking each executor to shut down")
        for ((_, executorData) <- executorDataMap) {
          executorData.executorEndpoint.send(StopExecutor)
        }
        context.reply(true)

      case RemoveExecutor(executorId, reason) =>
        removeExecutor(executorId, reason)
        context.reply(true)
/**
 * AppClient的计算资源物理分配过程步骤:
 * 1)调用Master的launchExecutor方法,向Worker发送LaunchExecutor消息
 * 2)Woker接收LaunchExecutor消息后,创建Executor的工作目录,创建Application的本地目录,创建并启动ExceutorRunner
 *   最后向Master发送ExecutorStateChanged消息
 * 3)ExecutorRunner创建并运行线程WorkerThread,workerThread在执行过程中调用fetchAndRunExecutor完成对CoarseGrainedExecutorBackend进程构造
 * 4)CoarseGrainedExecutorBackend进程向Driver发送RetrieveSparkProps消息
 * 5)Driver收到RetrieveSparkProps消息后向CoarseGrainedExecutorBackend进程发送sparkProperties消息,
 *   CoarseGrainedExecutorBackend进程最后创建自身需要的ActorSystem
 * 6)CoarseGrainedExecutorBackend进程向刚刚启动的ActorSystem注册CoarseGrainedExecutorBackend(实现Actor特质),所以触发start方法
 *   CoarseGrainedExecutorBackend的start方法向DriverActor发送RegisterExecutor消息
 * 7)Driver接收RegisterExecutor消息后,先向CoarseGrainedExecutorBackend发送RegisteredExecutor消息,然后更新Executor所在地址
 *   与Executor的映射关系(addressTo-ExecutorId),Deiver获取的总共CPU核数(totalCoreCount),注册到Driver的Executor的总数(totalRegisteredExecutors)等信息
 *   最后创建ExecturoData并注册到executorDataMap中
 * 8)CoarseGrainedExecutorBackend进程收到RegisteredExecutor消息后创建Executor
 * 9)CoarseGrainedExecutorBackend进程向刚刚启动的ActorSystem注册workerWatcher,注册workerWatcher的时候会触发start方法
 *   start方法会向worker发送SendHeartbeat消息初始化连接
 * 10)Worker收到SendHeartbeat消息后向Master发送Heartbeat消息,Master收到Heartbeat消息后如果发现Worker没有注册过,则向Worker发送ReconnectWorker消息
 *    要求重新向Master注册
 */
      case RetrieveSparkProps =>
        context.reply(sparkProperties)
    }

    // Make fake resource offers on all executors
    //在所有执行器提供假的资源
    /**
     * makeOffers方法处理逻辑
     * 1)将executorDataMap中ExecutorData都转为WorkerOffer
     * 2)调用TaskSchedulerImpl的resourceOffers方法给当前任务分配Executor
     * 3)然后调用launchTasks
     * 
     */
    private def makeOffers() {
      //Filter out executors under killing
      //过滤掉正在杀死的executors,得到activeExecutors
      val activeExecutors = executorDataMap.filterKeys(!executorsPendingToRemove.contains(_))
      //获取workOffers即资源 
      val workOffers = activeExecutors.map { case (id, executorData) =>
        //activeExecutors中executorData的executorHost、freeCores,获取workOffers,即资源
        new WorkerOffer(id, executorData.executorHost, executorData.freeCores)
      }.toSeq
      //调用scheduler的resourceOffers()方法,分配资源  
      //调用launchTasks()方法,启动tasks
      launchTasks(scheduler.resourceOffers(workOffers))
    }

    override def onDisconnected(remoteAddress: RpcAddress): Unit = {
      addressToExecutorId.get(remoteAddress).foreach(removeExecutor(_,
        "remote Rpc client disassociated"))
    }

    // Make fake resource offers on just one executor
    //根据executorId重新资源分配启动任务
    private def makeOffers(executorId: String) {
      // Filter out executors under killing
      //过虑出正在杀死的执行器
      if (!executorsPendingToRemove.contains(executorId)) {
        val executorData = executorDataMap(executorId)
        val workOffers = Seq(
          new WorkerOffer(executorId, executorData.executorHost, executorData.freeCores))
        launchTasks(scheduler.resourceOffers(workOffers))
      }
    }

    // Launch tasks returned by a set of resource offers
    //启动任务返回提供一组资源
    //
    /**
     * Executor在接到消息后,就会开始执行Task,
     * launchTasks处理步骤
     * 1)序列化TaskDescription
     * 2)取出TaskDescription所描述任务分配的ExecutorData信息,并且将ExecutorData描述的空闲CPU核数减去
     *   任务占用的核数
     * 3)向Executor所在的CoarseGrainedExecutorBackend进程中发送LaunchTask消息
     * 4)CoarseGrainedExecutorBackend收到LaunchTask消息后,反序列化TaskDescription,使用TaskDescription
     *   的taskId,name,serializedTask调用Executor的方法LaunchTask
     */
    private def launchTasks(tasks: Seq[Seq[TaskDescription]]) {
      for (task <- tasks.flatten) {
        val serializedTask = ser.serialize(task)
        if (serializedTask.limit >= akkaFrameSize - AkkaUtils.reservedSizeBytes) {
          scheduler.taskIdToTaskSetManager.get(task.taskId).foreach { taskSetMgr =>
            try {
              var msg = "Serialized task %s:%d was %d bytes, which exceeds max allowed: " +
                "spark.akka.frameSize (%d bytes) - reserved (%d bytes). Consider increasing " +
                "spark.akka.frameSize or using broadcast variables for large values."
              msg = msg.format(task.taskId, task.index, serializedTask.limit, akkaFrameSize,
                AkkaUtils.reservedSizeBytes)
              taskSetMgr.abort(msg)
            } catch {
              case e: Exception => logError("Exception in error callback", e)
            }
          }
        }
        else {
          val executorData = executorDataMap(task.executorId)
          executorData.freeCores -= scheduler.CPUS_PER_TASK
          executorData.executorEndpoint.send(LaunchTask(new SerializableBuffer(serializedTask)))
        }
      }
    }

    // Remove a disconnected slave from the cluster
    // 从集群中移除断开连接的从节点
    def removeExecutor(executorId: String, reason: String): Unit = {
      executorDataMap.get(executorId) match {
        case Some(executorInfo) =>
          // This must be synchronized because variables mutated
          //这必须同步因为变量可变的
          // in this block are read when requesting executors
          // 在这块读取请求执行时的可变
          CoarseGrainedSchedulerBackend.this.synchronized {
            addressToExecutorId -= executorInfo.executorAddress
            executorDataMap -= executorId
            executorsPendingToRemove -= executorId
          }
          totalCoreCount.addAndGet(-executorInfo.totalCores)
          totalRegisteredExecutors.addAndGet(-1)
          scheduler.executorLost(executorId, SlaveLost(reason))
          listenerBus.post(
            SparkListenerExecutorRemoved(System.currentTimeMillis(), executorId, reason))
        case None => logInfo(s"Asked to remove non-existent executor $executorId")
      }
    }

    override def onStop() {
      reviveThread.shutdownNow()
    }
  }

  var driverEndpoint: RpcEndpointRef = null
  val taskIdsOnSlave = new HashMap[String, HashSet[String]]
/**
 * 从sc.conf中复制Spark属性,然后注册并持有driverEndpoint引用
 */
  override def start() {
    
    val properties = new ArrayBuffer[(String, String)]
    for ((key, value) <- scheduler.sc.conf.getAll) {
      if (key.startsWith("spark.")) {
        properties += ((key, value))
      }
    }

    // TODO (prashant) send conf instead of properties
    driverEndpoint = rpcEnv.setupEndpoint(
      CoarseGrainedSchedulerBackend.ENDPOINT_NAME, new DriverEndpoint(rpcEnv, properties))
  }

  def stopExecutors() {
    try {
      if (driverEndpoint != null) {
        logInfo("Shutting down all executors")
        driverEndpoint.askWithRetry[Boolean](StopExecutors)
      }
    } catch {
      case e: Exception =>
        throw new SparkException("Error asking standalone scheduler to shut down executors", e)
    }
  }

  override def stop() {
    stopExecutors()
    try {
      if (driverEndpoint != null) {
        driverEndpoint.askWithRetry[Boolean](StopDriver)
      }
    } catch {
      case e: Exception =>
        throw new SparkException("Error stopping standalone scheduler's driver endpoint", e)
    }
  }
/**
 * reviveOffers用于向driverEndpoint发送ReviveOffers
 */
  override def reviveOffers() {
    //SchedulerBackend把自己手头上的可用资源交给TaskScheduler,TaskScheduler根据调度策略分配给排队的任务吗,
    // 返回一批可执行的任务描述,SchedulerBackend负责launchTask,
    // 即最终把task塞到了executor模型上,executor里的线程池会执行task的run
    driverEndpoint.send(ReviveOffers)
  }

  override def killTask(taskId: Long, executorId: String, interruptThread: Boolean) {
    driverEndpoint.send(KillTask(taskId, executorId, interruptThread))
  }
///如果用户不设置,系统使用集群中运行shuffle操作的默认任务数
  override def defaultParallelism(): Int = {
    conf.getInt("spark.default.parallelism", math.max(totalCoreCount.get(), 2))
  }

  // Called by subclasses when notified of a lost worker
  //当通知一个丢失的工作节点,由子类调用
  def removeExecutor(executorId: String, reason: String) {
    try {
      driverEndpoint.askWithRetry[Boolean](RemoveExecutor(executorId, reason))
    } catch {
      case e: Exception =>
        throw new SparkException("Error notifying standalone scheduler's driver endpoint", e)
    }
  }

  def sufficientResourcesRegistered(): Boolean = true

  override def isReady(): Boolean = {
    if (sufficientResourcesRegistered) {
      logInfo("SchedulerBackend is ready for scheduling beginning after " +
        s"reached minRegisteredResourcesRatio: $minRegisteredRatio")
      return true
    }
    if ((System.currentTimeMillis() - createTime) >= maxRegisteredWaitingTimeMs) {
      logInfo("SchedulerBackend is ready for scheduling beginning after waiting " +
        s"maxRegisteredResourcesWaitingTime: $maxRegisteredWaitingTimeMs(ms)")
      return true
    }
    false
  }

  /**
   * Return the number of executors currently registered with this backend.
   * 返回当前注册执行器数
   */
  def numExistingExecutors: Int = executorDataMap.size

  /**
   * Request an additional number of executors from the cluster manager.
   * 从群集管理器请求一个额外执行器数
   *
   * @return whether the request is acknowledged.还回请求是否被承认
   */
  final override def requestExecutors(numAdditionalExecutors: Int): Boolean = synchronized {
    if (numAdditionalExecutors < 0) {
      throw new IllegalArgumentException(
        "Attempted to request a negative number of additional executor(s) " +
        s"$numAdditionalExecutors from the cluster manager. Please specify a positive number!")
    }
    logInfo(s"Requesting $numAdditionalExecutors additional executor(s) from the cluster manager")
    logDebug(s"Number of pending executors is now $numPendingExecutors")
    //待执行器数
    numPendingExecutors += numAdditionalExecutors
    // Account for executors pending to be added or removed
    //考虑执行器待定被添加或移除
    val newTotal = numExistingExecutors + numPendingExecutors - executorsPendingToRemove.size
    doRequestTotalExecutors(newTotal)
  }

  /**
   * Update the cluster manager on our scheduling needs. Three bits of information are included
   * 更新我们的调度需求的集群管理器,
   * to help it make decisions.
   * @param numExecutors The total number of executors we'd like to have. The cluster manager
   * 										 我们需要执行器总数,集群管理器不应该杀死任何运行的执行器,以达到这个数字
   *                     shouldn't kill any running executor to reach this number, but,
   *                     if all existing executors were to die, this is the number of executors
   *                     如果所有存在的执行器死了,这些执行器数需要我们分配
   *                     we'd want to be allocated.
   * @param localityAwareTasks The number of tasks in all active stages that have a locality
   * 													 有一个最佳位置的所有活动阶段的任务数,这包括运行,挂起和完成任务
   *                           preferences. This includes running, pending, and completed tasks.
   * @param hostToLocalTaskCount A map of hosts to the number of tasks from all active stages
   * 														 一个Map的Key主机,value所有活动阶段的任务数量,就喜欢在那个主机上运行
   *                             that would like to like to run on that host.
   *                             This includes running, pending, and completed tasks.
   *                             包括运行、挂起和完成任务
   * @return whether the request is acknowledged by the cluster manager. 该请求被集群管理器确认
   */
  final override def requestTotalExecutors(
      numExecutors: Int,
      localityAwareTasks: Int,
      hostToLocalTaskCount: Map[String, Int]
    ): Boolean = synchronized {
    if (numExecutors < 0) {
      throw new IllegalArgumentException(
        "Attempted to request a negative number of executor(s) " +
          s"$numExecutors from the cluster manager. Please specify a positive number!")
    }

    this.localityAwareTasks = localityAwareTasks
    this.hostToLocalTaskCount = hostToLocalTaskCount

    numPendingExecutors =
      math.max(numExecutors - numExistingExecutors + executorsPendingToRemove.size, 0)
    doRequestTotalExecutors(numExecutors)
  }

  /**
   * Request executors from the cluster manager by specifying the total number desired,
   * 请求执行器从群集管理器通过指定所需的总数量,包括现有的正在运行的执行器
   * including existing pending and running executors.
   *
   * The semantics here guarantee that we do not over-allocate executors for this application,
   * 这里的语义保证我们不在该应用程序分配的执行器
   * since a later request overrides the value of any prior request. The alternative interface
   * of requesting a delta of executors risks double counting new executors when there are
   * insufficient resources to satisfy the first request. We make the assumption here that the
   * cluster manager will eventually fulfill all requests when resources free up.
   *
   * @return whether the request is acknowledged.
   */
  protected def doRequestTotalExecutors(requestedTotal: Int): Boolean = false

  /**
   * Request that the cluster manager kill the specified executors.
   * 请求集群管理器杀死执行器的列表
   * @return whether the kill request is acknowledged.杀死请求确认
   */
  final override def killExecutors(executorIds: Seq[String]): Boolean = synchronized {
    killExecutors(executorIds, replace = false)
  }

  /**
   * Request that the cluster manager kill the specified executors.
   * 请求的集群管理器杀死指定的executors
   * @param executorIds identifiers of executors to kill 杀死执行器唯一标示符
   * @param replace whether to replace the killed executors with new ones 是否杀死执行器更换新的执行器
   * @return whether the kill request is acknowledged.
   */
  final def killExecutors(executorIds: Seq[String], replace: Boolean): Boolean = synchronized {
    logInfo(s"Requesting to kill executor(s) ${executorIds.mkString(", ")}")
    val (knownExecutors, unknownExecutors) = executorIds.partition(executorDataMap.contains)
    unknownExecutors.foreach { id =>
      logWarning(s"Executor to kill $id does not exist!")
    }

    // If an executor is already pending to be removed, do not kill it again (SPARK-9795)
    //如果一个执行器已经等待被删除,不要再杀死它了
    val executorsToKill = knownExecutors.filter { id => !executorsPendingToRemove.contains(id) }
    executorsPendingToRemove ++= executorsToKill

    // If we do not wish to replace the executors we kill, sync the target number of executors
    //如果我们不希望代替我们杀死的执行器,使用群集管理器避免分配新的同步执行器目标数,
    // with the cluster manager to avoid allocating new ones. When computing the new target,
    //当计算新目标时,考虑到执行器都需要添加或删除
    // take into account executors that are pending to be added or removed.
    if (!replace) {
      doRequestTotalExecutors(
        numExistingExecutors + numPendingExecutors - executorsPendingToRemove.size)
    } else {
      numPendingExecutors += knownExecutors.size
    }

    doKillExecutors(executorsToKill)
  }

  /**
   * Kill the given list of executors through the cluster manager.
   * 通过群集管理器杀死给定列表的执行器
   * @return whether the kill request is acknowledged. 杀死请求是否被承认
   */
  protected def doKillExecutors(executorIds: Seq[String]): Boolean = false

}

private[spark] object CoarseGrainedSchedulerBackend {
  val ENDPOINT_NAME = "CoarseGrainedScheduler"
}
