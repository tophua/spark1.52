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

package org.apache.spark.deploy.yarn

import java.util.Collections
import java.util.concurrent._
import java.util.regex.Pattern

import scala.collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.util.RackResolver
import org.apache.log4j.{Level, Logger}

import org.apache.spark.{Logging, SecurityManager, SparkConf}
import org.apache.spark.deploy.yarn.YarnSparkHadoopUtil._
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.RemoveExecutor
import org.apache.spark.util.ThreadUtils

/**
 * YarnAllocator is charged with requesting containers from the YARN ResourceManager and deciding
 * what to do with containers when YARN fulfills these requests.
  * YarnAllocator负责从YARN ResourceManager请求容器,并决定在YARN满足这些请求时如何处理容器
 *
 * This class makes use of YARN's AMRMClient APIs. We interact with the AMRMClient in three ways:
  * 该类使用YARN的AMRMClient API。 我们通过三种方式与AMRMClient进行交互：
 * * Making our resource needs known, which updates local bookkeeping about containers requested.
 * * Calling "allocate", which syncs our local container requests with the RM, and returns any
 *   containers that YARN has granted to us.  This also functions as a heartbeat.
 * * Processing the containers granted to us to possibly launch executors inside of them.
 *
 * The public methods of this class are thread-safe.  All methods that mutate state are
 * synchronized.
 */
private[yarn] class YarnAllocator(
    driverUrl: String,
    driverRef: RpcEndpointRef,
    conf: Configuration,
    sparkConf: SparkConf,
    amClient: AMRMClient[ContainerRequest],
    appAttemptId: ApplicationAttemptId,
    args: ApplicationMasterArguments,
    securityMgr: SecurityManager)
  extends Logging {

  import YarnAllocator._

  // RackResolver logs an INFO message whenever it resolves a rack, which is way too often.
  //RackResolver在解析机架时会记录INFO消息,这种情况太常见了
  if (Logger.getLogger(classOf[RackResolver]).getLevel == null) {
    Logger.getLogger(classOf[RackResolver]).setLevel(Level.WARN)
  }

  // Visible for testing.
  //可见测试
  val allocatedHostToContainersMap = new HashMap[String, collection.mutable.Set[ContainerId]]
  val allocatedContainerToHostMap = new HashMap[ContainerId, String]

  // Containers that we no longer care about. We've either already told the RM to release them or
  // will on the next heartbeat. Containers get removed from this map after the RM tells us they've
  // completed.
  //我们不再关心的容器。 我们已经告诉RM释放它们,或者将在下一次心跳时释放它们,RM告诉我们他们已经完成后,容器会从此地图中删除。
  private val releasedContainers = Collections.newSetFromMap[ContainerId](
    new ConcurrentHashMap[ContainerId, java.lang.Boolean])

  @volatile private var numExecutorsRunning = 0
  // Used to generate a unique ID per executor
  //用于为每个执行程序生成唯一ID
  private var executorIdCounter = 0
  @volatile private var numExecutorsFailed = 0

  @volatile private var targetNumExecutors =
    YarnSparkHadoopUtil.getInitialTargetExecutorNumber(sparkConf)

  // Keep track of which container is running which executor to remove the executors later
  // Visible for testing.
  //跟踪哪个容器正在运行哪个执行程序以后删除执行程序可见以进行测试
  private[yarn] val executorIdToContainer = new HashMap[String, Container]

  private var numUnexpectedContainerRelease = 0L
  private val containerIdToExecutorId = new HashMap[ContainerId, String]

  // Executor memory in MB.执行程序内存以MB为单位
  protected val executorMemory = args.executorMemory
  // Additional memory overhead.额外的内存开销
  protected val memoryOverhead: Int = sparkConf.getInt("spark.yarn.executor.memoryOverhead",
    math.max((MEMORY_OVERHEAD_FACTOR * executorMemory).toInt, MEMORY_OVERHEAD_MIN))
  // Number of cores per executor.每个执行程序的核心数
  protected val executorCores = args.executorCores
  // Resource capability requested for each executors为每个执行者请求的资源能力
  private[yarn] val resource = Resource.newInstance(executorMemory + memoryOverhead, executorCores)

  private val launcherPool = ThreadUtils.newDaemonCachedThreadPool(
    "ContainerLauncher",
    sparkConf.getInt("spark.yarn.containerLauncherMaxThreads", 25))

  // For testing
  private val launchContainers = sparkConf.getBoolean("spark.yarn.launchContainers", true)

  private val labelExpression = sparkConf.getOption("spark.yarn.executor.nodeLabelExpression")

  // ContainerRequest constructor that can take a node label expression. We grab it through
  // reflection because it's only available in later versions of YARN.
  //ContainerRequest构造函数,可以采用节点标签表达式,我们通过反射获取它,
  // 因为它仅在YARN的更高版本中可用
  private val nodeLabelConstructor = labelExpression.flatMap { expr =>
    try {
      Some(classOf[ContainerRequest].getConstructor(classOf[Resource],
        classOf[Array[String]], classOf[Array[String]], classOf[Priority], classOf[Boolean],
        classOf[String]))
    } catch {
      case e: NoSuchMethodException => {
        logWarning(s"Node label expression $expr will be ignored because YARN version on" +
          " classpath does not support it.")
        None
      }
    }
  }

  // A map to store preferred hostname and possible task numbers running on it.
  //用于存储首选主机名和在其上运行的可能任务编号的映射
  private var hostToLocalTaskCounts: Map[String, Int] = Map.empty

  // Number of tasks that have locality preferences in active stages
  //在活动阶段具有位置首选项的任务数
  private var numLocalityAwareTasks: Int = 0

  // A container placement strategy based on pending tasks' locality preference
  //基于待定任务的位置偏好的容器放置策略
  private[yarn] val containerPlacementStrategy =
    new LocalityPreferredContainerPlacementStrategy(sparkConf, conf, resource)

  def getNumExecutorsRunning: Int = numExecutorsRunning

  def getNumExecutorsFailed: Int = numExecutorsFailed

  /**
   * Number of container requests that have not yet been fulfilled.
    * 尚未履行的容器请求数
   */
  def getNumPendingAllocate: Int = getNumPendingAtLocation(ANY_HOST)

  /**
   * Number of container requests at the given location that have not yet been fulfilled.
    * 在给定位置尚未完成的容器请求数
   */
  private def getNumPendingAtLocation(location: String): Int =
    amClient.getMatchingRequests(RM_REQUEST_PRIORITY, location, resource).map(_.size).sum

  /**
   * Request as many executors from the ResourceManager as needed to reach the desired total. If
   * the requested total is smaller than the current number of running executors, no executors will
   * be killed.
    * 根据需要从ResourceManager请求尽可能多的执行程序以达到所需的总数,
    * 如果请求的总数小于当前运行的执行程序数,则不会终止执行程序。
   * @param requestedTotal total number of containers requested 请求的容器总数
   * @param localityAwareTasks number of locality aware tasks to be used as container placement hint 要用作容器放置提示的位置感知任务的数量
   * @param hostToLocalTaskCount a map of preferred hostname to possible task counts to be used as
   *                             container placement hint.
    *                             可选任务计数的首选主机名映射,用作容器放置提示。
   * @return Whether the new requested total is different than the old value.
    *         新请求的总数是否与旧值不同
   */
  def requestTotalExecutorsWithPreferredLocalities(
      requestedTotal: Int,
      localityAwareTasks: Int,
      hostToLocalTaskCount: Map[String, Int]): Boolean = synchronized {
    this.numLocalityAwareTasks = localityAwareTasks
    this.hostToLocalTaskCounts = hostToLocalTaskCount

    if (requestedTotal != targetNumExecutors) {
      logInfo(s"Driver requested a total number of $requestedTotal executor(s).")
      targetNumExecutors = requestedTotal
      true
    } else {
      false
    }
  }

  /**
   * Request that the ResourceManager release the container running the specified executor.
    * 请求ResourceManager释放运行指定执行程序的容器
   */
  def killExecutor(executorId: String): Unit = synchronized {
    if (executorIdToContainer.contains(executorId)) {
      val container = executorIdToContainer.remove(executorId).get
      containerIdToExecutorId.remove(container.getId)
      internalReleaseContainer(container)
      numExecutorsRunning -= 1
    } else {
      logWarning(s"Attempted to kill unknown executor $executorId!")
    }
  }

  /**
   * Request resources such that, if YARN gives us all we ask for, we'll have a number of containers
   * equal to maxExecutors. 请求资源,如果YARN给我们所要求的全部,我们将有一些容器等于maxExecutors。
   *
   * Deal with any containers YARN has granted to us by possibly launching executors in them.
   * 处理YARN通过在其中启动执行程序而授予我们的任何容器
   * This must be synchronized because variables read in this method are mutated by other methods.这必须同步,因为此方法中读取的变量会被其他方法变异
   */
  def allocateResources(): Unit = synchronized {
    updateResourceRequests()

    val progressIndicator = 0.1f
    // Poll the ResourceManager. This doubles as a heartbeat if there are no pending container
    // requests. 轮询ResourceManager,如果没有待处理的容器请求,则会将其作为心跳加倍
    val allocateResponse = amClient.allocate(progressIndicator)

    val allocatedContainers = allocateResponse.getAllocatedContainers()

    if (allocatedContainers.size > 0) {
      logDebug("Allocated containers: %d. Current executor count: %d. Cluster resources: %s."
        .format(
          allocatedContainers.size,
          numExecutorsRunning,
          allocateResponse.getAvailableResources))

      handleAllocatedContainers(allocatedContainers)
    }

    val completedContainers = allocateResponse.getCompletedContainersStatuses()
    if (completedContainers.size > 0) {
      logDebug("Completed %d containers".format(completedContainers.size))

      processCompletedContainers(completedContainers)

      logDebug("Finished processing %d completed containers. Current running executor count: %d."
        .format(completedContainers.size, numExecutorsRunning))
    }
  }

  /**
   * Update the set of container requests that we will sync with the RM based on the number of
   * executors we have currently running and our target number of executors.
   * 根据我们当前运行的执行程序数和目标执行程序数更新我们将与RM同步的容器请求集。
   * Visible for testing.
   */
  def updateResourceRequests(): Unit = {
    val numPendingAllocate = getNumPendingAllocate
    val missing = targetNumExecutors - numPendingAllocate - numExecutorsRunning

    // TODO. Consider locality preferences of pending container requests.
    // Since the last time we made container requests, stages have completed and been submitted,
    // and that the localities at which we requested our pending executors
    // no longer apply to our current needs. We should consider to remove all outstanding
    // container requests and add requests anew each time to avoid this.
    //自上次我们提出容器请求以来,已经完成并提交了阶段,并且我们请求未决执行人的地点不再适用于我们当前的需求。  //我们应该考虑删除所有未完成的容器请求并每次重新添加请求以避免这种情况。
    if (missing > 0) {
      logInfo(s"Will request $missing executor containers, each with ${resource.getVirtualCores} " +
        s"cores and ${resource.getMemory} MB memory including $memoryOverhead MB overhead")

      val containerLocalityPreferences = containerPlacementStrategy.localityOfRequestedContainers(
        missing, numLocalityAwareTasks, hostToLocalTaskCounts, allocatedHostToContainersMap)

      for (locality <- containerLocalityPreferences) {
        val request = createContainerRequest(resource, locality.nodes, locality.racks)
        amClient.addContainerRequest(request)
        val nodes = request.getNodes
        val hostStr = if (nodes == null || nodes.isEmpty) "Any" else nodes.last
        logInfo(s"Container request (host: $hostStr, capability: $resource)")
      }
    } else if (missing < 0) {
      val numToCancel = math.min(numPendingAllocate, -missing)
      logInfo(s"Canceling requests for $numToCancel executor containers")

      val matchingRequests = amClient.getMatchingRequests(RM_REQUEST_PRIORITY, ANY_HOST, resource)
      if (!matchingRequests.isEmpty) {
        matchingRequests.head.take(numToCancel).foreach(amClient.removeContainerRequest)
      } else {
        logWarning("Expected to find pending requests, but found none.")
      }
    }
  }

  /**
   * Creates a container request, handling the reflection required to use YARN features that were
   * added in recent versions.
    * 创建容器请求,处理使用最近版本中添加的YARN功能所需的反射
   */
  protected def createContainerRequest(
      resource: Resource,
      nodes: Array[String],
      racks: Array[String]): ContainerRequest = {
    nodeLabelConstructor.map { constructor =>
      constructor.newInstance(resource, nodes, racks, RM_REQUEST_PRIORITY, true: java.lang.Boolean,
        labelExpression.orNull)
    }.getOrElse(new ContainerRequest(resource, nodes, racks, RM_REQUEST_PRIORITY))
  }

  /**
   * Handle containers granted by the RM by launching executors on them.
   * 通过在RM上启动执行程序来处理由RM授予的容器
   * Due to the way the YARN allocation protocol works, certain healthy race conditions can result
   * in YARN granting containers that we no longer need. In this case, we release them.
    *
   *由于YARN分配协议的工作方式,某些健康的竞争条件可能导致YARN授予我们不再需要的容器,在这种情况下,我们发布它们。
可见测试。
   * Visible for testing.
   */
  def handleAllocatedContainers(allocatedContainers: Seq[Container]): Unit = {
    val containersToUse = new ArrayBuffer[Container](allocatedContainers.size)

    // Match incoming requests by host 匹配主机的传入请求
    val remainingAfterHostMatches = new ArrayBuffer[Container]
    for (allocatedContainer <- allocatedContainers) {
      matchContainerToRequest(allocatedContainer, allocatedContainer.getNodeId.getHost,
        containersToUse, remainingAfterHostMatches)
    }

    // Match remaining by rack 匹配剩余的机架
    val remainingAfterRackMatches = new ArrayBuffer[Container]
    for (allocatedContainer <- remainingAfterHostMatches) {
      val rack = RackResolver.resolve(conf, allocatedContainer.getNodeId.getHost).getNetworkLocation
      matchContainerToRequest(allocatedContainer, rack, containersToUse,
        remainingAfterRackMatches)
    }

    // Assign remaining that are neither node-local nor rack-local
    //分配剩余的既不是节点本地也不是机架本地
    val remainingAfterOffRackMatches = new ArrayBuffer[Container]
    for (allocatedContainer <- remainingAfterRackMatches) {
      matchContainerToRequest(allocatedContainer, ANY_HOST, containersToUse,
        remainingAfterOffRackMatches)
    }

    if (!remainingAfterOffRackMatches.isEmpty) {
      logDebug(s"Releasing ${remainingAfterOffRackMatches.size} unneeded containers that were " +
        s"allocated to us")
      for (container <- remainingAfterOffRackMatches) {
        internalReleaseContainer(container)
      }
    }

    runAllocatedContainers(containersToUse)

    logInfo("Received %d containers from YARN, launching executors on %d of them."
      .format(allocatedContainers.size, containersToUse.size))
  }

  /**
   * Looks for requests for the given location that match the given container allocation. If it
   * finds one, removes the request so that it won't be submitted again. Places the container into
   * containersToUse or remaining.
    * 查找与给定容器分配匹配的给定位置的请求,如果找到一个,则删除该请求,以便不再提交,
    * 将容器放入容器中使用或保留。
   *
   * @param allocatedContainer container that was given to us by YARN YARN给我们的容器
   * @param location resource name, either a node, rack, or * 资源名称,节点,机架或*
   * @param containersToUse list of containers that will be used 将使用的容器列表
   * @param remaining list of containers that will not be used 不使用的容器列表
   */
  private def matchContainerToRequest(
      allocatedContainer: Container,
      location: String,
      containersToUse: ArrayBuffer[Container],
      remaining: ArrayBuffer[Container]): Unit = {
    // SPARK-6050: certain Yarn configurations return a virtual core count that doesn't match the
    // request; for example, capacity scheduler + DefaultResourceCalculator. So match on requested
    // memory, but use the asked vcore count for matching, effectively disabling matching on vcore
    // count.
    val matchingResource = Resource.newInstance(allocatedContainer.getResource.getMemory,
          resource.getVirtualCores)
    val matchingRequests = amClient.getMatchingRequests(allocatedContainer.getPriority, location,
      matchingResource)

    // Match the allocation to a request 将分配与请求匹配
    if (!matchingRequests.isEmpty) {
      val containerRequest = matchingRequests.get(0).iterator.next
      amClient.removeContainerRequest(containerRequest)
      containersToUse += allocatedContainer
    } else {
      remaining += allocatedContainer
    }
  }

  /**
   * Launches executors in the allocated containers.
    * 在已分配的容器中启动执行程序
   */
  private def runAllocatedContainers(containersToUse: ArrayBuffer[Container]): Unit = {
    for (container <- containersToUse) {
      numExecutorsRunning += 1
      assert(numExecutorsRunning <= targetNumExecutors)
      val executorHostname = container.getNodeId.getHost
      val containerId = container.getId
      executorIdCounter += 1
      val executorId = executorIdCounter.toString

      assert(container.getResource.getMemory >= resource.getMemory)

      logInfo("Launching container %s for on host %s".format(containerId, executorHostname))
      executorIdToContainer(executorId) = container
      containerIdToExecutorId(container.getId) = executorId

      val containerSet = allocatedHostToContainersMap.getOrElseUpdate(executorHostname,
        new HashSet[ContainerId])

      containerSet += containerId
      allocatedContainerToHostMap.put(containerId, executorHostname)

      val executorRunnable = new ExecutorRunnable(
        container,
        conf,
        sparkConf,
        driverUrl,
        executorId,
        executorHostname,
        executorMemory,
        executorCores,
        appAttemptId.getApplicationId.toString,
        securityMgr)
      if (launchContainers) {
        logInfo("Launching ExecutorRunnable. driverUrl: %s,  executorHostname: %s".format(
          driverUrl, executorHostname))
        launcherPool.execute(executorRunnable)
      }
    }
  }

  // Visible for testing. 处理已完成的容器
  private[yarn] def processCompletedContainers(completedContainers: Seq[ContainerStatus]): Unit = {
    for (completedContainer <- completedContainers) {
      val containerId = completedContainer.getContainerId
      val alreadyReleased = releasedContainers.remove(containerId)
      if (!alreadyReleased) {
        // Decrement the number of executors running. The next iteration of
        // the ApplicationMaster's reporting thread will take care of allocating.
        //减少运行的执行程序的数量,ApplicationMaster报告线程的下一次迭代将负责分配。
        numExecutorsRunning -= 1
        logInfo("Completed container %s (state: %s, exit status: %s)".format(
          containerId,
          completedContainer.getState,
          completedContainer.getExitStatus))
        // Hadoop 2.2.X added a ContainerExitStatus we should switch to use
        // there are some exit status' we shouldn't necessarily count against us, but for
        // now I think its ok as none of the containers are expected to exit
        if (completedContainer.getExitStatus == ContainerExitStatus.PREEMPTED) {
          logInfo("Container preempted: " + containerId)
        } else if (completedContainer.getExitStatus == -103) { // vmem limit exceeded
          logWarning(memLimitExceededLogMessage(
            completedContainer.getDiagnostics,
            VMEM_EXCEEDED_PATTERN))
        } else if (completedContainer.getExitStatus == -104) { // pmem limit exceeded
          logWarning(memLimitExceededLogMessage(
            completedContainer.getDiagnostics,
            PMEM_EXCEEDED_PATTERN))
        } else if (completedContainer.getExitStatus != 0) {
          logInfo("Container marked as failed: " + containerId +
            ". Exit status: " + completedContainer.getExitStatus +
            ". Diagnostics: " + completedContainer.getDiagnostics)
          numExecutorsFailed += 1
        }
      }

      if (allocatedContainerToHostMap.containsKey(containerId)) {
        val host = allocatedContainerToHostMap.get(containerId).get
        val containerSet = allocatedHostToContainersMap.get(host).get

        containerSet.remove(containerId)
        if (containerSet.isEmpty) {
          allocatedHostToContainersMap.remove(host)
        } else {
          allocatedHostToContainersMap.update(host, containerSet)
        }

        allocatedContainerToHostMap.remove(containerId)
      }

      containerIdToExecutorId.remove(containerId).foreach { eid =>
        executorIdToContainer.remove(eid)

        if (!alreadyReleased) {
          // The executor could have gone away (like no route to host, node failure, etc)
          //执行者可能已经离开(就像没有主机路由,节点故障等)
          // Notify backend about the failure of the executor
          //通知后端有关执行程序失败的信息
          numUnexpectedContainerRelease += 1
          driverRef.send(RemoveExecutor(eid,
            s"Yarn deallocated the executor $eid (container $containerId)"))
        }
      }
    }
  }

  private def internalReleaseContainer(container: Container): Unit = {
    releasedContainers.add(container.getId())
    amClient.releaseAssignedContainer(container.getId())
  }

  private[yarn] def getNumUnexpectedContainerRelease = numUnexpectedContainerRelease

}

private object YarnAllocator {
  val MEM_REGEX = "[0-9.]+ [KMG]B"
  val PMEM_EXCEEDED_PATTERN =
    Pattern.compile(s"$MEM_REGEX of $MEM_REGEX physical memory used")
  val VMEM_EXCEEDED_PATTERN =
    Pattern.compile(s"$MEM_REGEX of $MEM_REGEX virtual memory used")

  def memLimitExceededLogMessage(diagnostics: String, pattern: Pattern): String = {
    val matcher = pattern.matcher(diagnostics)
    val diag = if (matcher.find()) " " + matcher.group() + "." else ""
    ("Container killed by YARN for exceeding memory limits." + diag
      + " Consider boosting spark.yarn.executor.memoryOverhead.")
  }
}
