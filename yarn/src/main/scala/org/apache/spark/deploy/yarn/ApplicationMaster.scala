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

import scala.util.control.NonFatal

import java.io.{File, IOException}
import java.lang.reflect.InvocationTargetException
import java.net.{Socket, URL}
import java.util.concurrent.atomic.AtomicReference

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.yarn.api._
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.conf.YarnConfiguration

import org.apache.spark.rpc._
import org.apache.spark.{Logging, SecurityManager, SparkConf, SparkContext, SparkEnv,
  SparkException, SparkUserAppException}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.history.HistoryServer
import org.apache.spark.scheduler.cluster.{CoarseGrainedSchedulerBackend, YarnSchedulerBackend}
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.util._

/**
 * Common application master functionality for Spark on Yarn.
  * Spark on Yarn的常见应用程序主要功能
 */
private[spark] class ApplicationMaster(
    args: ApplicationMasterArguments,
    client: YarnRMClient)
  extends Logging {

  // Load the properties file with the Spark configuration and set entries as system properties,
  //使用Spark配置加载属性文件并将条目设置为系统属性
  // so that user code run inside the AM also has access to them.
  //以便在AM内部运行的用户代码也可以访问它们
  if (args.propertiesFile != null) {
    Utils.getPropertiesFromFile(args.propertiesFile).foreach { case (k, v) =>
      sys.props(k) = v
    }
  }

  // TODO: Currently, task to container is computed once (TaskSetManager) - which need not be
  // optimal as more containers are available. Might need to handle this better.
  //随着更多容器的可用性而最佳,可能需要更好地处理这个问题

  private val sparkConf = new SparkConf()
  private val yarnConf: YarnConfiguration = SparkHadoopUtil.get.newConfiguration(sparkConf)
    .asInstanceOf[YarnConfiguration]
  private val isClusterMode = args.userClass != null

  // Default to numExecutors * 2, with minimum of 3
  //默认为numExecutors * 2,最小值为3
  private val maxNumExecutorFailures = sparkConf.getInt("spark.yarn.max.executor.failures",
    sparkConf.getInt("spark.yarn.max.worker.failures",
      math.max(sparkConf.getInt("spark.executor.instances", 0) *  2, 3)))

  @volatile private var exitCode = 0
  @volatile private var unregistered = false
  @volatile private var finished = false
  @volatile private var finalStatus = getDefaultFinalStatus
  @volatile private var finalMsg: String = ""
  @volatile private var userClassThread: Thread = _

  @volatile private var reporterThread: Thread = _
  @volatile private var allocator: YarnAllocator = _
  private val allocatorLock = new Object()

  // Fields used in client mode.
  //客户端模式中使用的字段
  private var rpcEnv: RpcEnv = null
  private var amEndpoint: RpcEndpointRef = _

  // Fields used in cluster mode.群集模式中使用的字段
  private val sparkContextRef = new AtomicReference[SparkContext](null)

  private var delegationTokenRenewerOption: Option[AMDelegationTokenRenewer] = None

  final def run(): Int = {
    try {
      val appAttemptId = client.getAttemptId()

      if (isClusterMode) {
        // Set the web ui port to be ephemeral for yarn so we don't conflict with
        // other spark processes running on the same box
        //将web ui端口设置为纱线的短暂,这样我们就不会与在同一个盒子上运行的其他spark进程发生冲突
        System.setProperty("spark.ui.port", "0")

        // Set the master property to match the requested mode.
        //设置主属性以匹配请求的模式
        System.setProperty("spark.master", "yarn-cluster")

        // Propagate the application ID so that YarnClusterSchedulerBackend can pick it up.
        //传播应用程序ID,以便YarnClusterSchedulerBackend可以获取它
        System.setProperty("spark.yarn.app.id", appAttemptId.getApplicationId().toString())

        // Propagate the attempt if, so that in case of event logging,
        //如果在事件日志记录的情况下传播该尝试
        // different attempt's logs gets created in different directory
        //在不同目录中创建不同的尝试日志
        System.setProperty("spark.yarn.app.attemptId", appAttemptId.getAttemptId().toString())
      }

      logInfo("ApplicationAttemptId: " + appAttemptId)

      val fs = FileSystem.get(yarnConf)

      // This shutdown hook should run *after* the SparkContext is shut down.
      //关闭SpkCurror之后,这个关闭钩子应该运行*
      val priority = ShutdownHookManager.SPARK_CONTEXT_SHUTDOWN_PRIORITY - 1
      ShutdownHookManager.addShutdownHook(priority) { () =>
        val maxAppAttempts = client.getMaxRegAttempts(sparkConf, yarnConf)
        val isLastAttempt = client.getAttemptId().getAttemptId() >= maxAppAttempts

        if (!finished) {
          // This happens when the user application calls System.exit(). We have the choice
          // of either failing or succeeding at this point. We report success to avoid
          // retrying applications that have succeeded (System.exit(0)), which means that
          // applications that explicitly exit with a non-zero status will also show up as
          // succeeded in the RM UI.
          //当用户应用程序调用Stutial.Ext()时就会发生这种情况,在这一点上,我们要么选择失败要么成功。
          //我们报告成功以避免重试已成功的应用程序(System.Exchange()),
          //这意味着显式退出非零状态的应用程序也将在RM UI中成功显示。
          finish(finalStatus,
            ApplicationMaster.EXIT_SUCCESS,
            "Shutdown hook called before final status was reported.")
        }

        if (!unregistered) {
          // we only want to unregister if we don't want the RM to retry
          //如果我们不希望RM重试,我们只想取消注册
          if (finalStatus == FinalApplicationStatus.SUCCEEDED || isLastAttempt) {
            unregister(finalStatus, finalMsg)
            cleanupStagingDir(fs)
          }
        }
      }

      // Call this to force generation of secret so it gets populated into the
      // Hadoop UGI. This has to happen before the startUserApplication which does a
      // doAs in order for the credentials to be passed on to the executor containers.
      //调用这个来强制生成秘密,这样它就被填充到Hadoop UGI中,
      //这必须发生在StaseServer应用程序之前,该应用程序执行DOAS以将证书传递给执行器容器
      val securityMgr = new SecurityManager(sparkConf)

      // If the credentials file config is present, we must periodically renew tokens. So create
      // a new AMDelegationTokenRenewer
      //如果凭证文件配置存在,我们必须定期更新令牌,所以创造一个新的代表性的可再生能源
      if (sparkConf.contains("spark.yarn.credentials.file")) {
        delegationTokenRenewerOption = Some(new AMDelegationTokenRenewer(sparkConf, yarnConf))
        // If a principal and keytab have been set, use that to create new credentials for executors
        // periodically
        //如果已经设置了主体和键选项卡,则使用它来定期为执行器创建新凭据
        delegationTokenRenewerOption.foreach(_.scheduleLoginFromKeytab())
      }

      if (isClusterMode) {
        runDriver(securityMgr)
      } else {
        runExecutorLauncher(securityMgr)
      }
    } catch {
      case e: Exception =>
        // catch everything else if not specifically handled
        logError("Uncaught exception: ", e)
        finish(FinalApplicationStatus.FAILED,
          ApplicationMaster.EXIT_UNCAUGHT_EXCEPTION,
          "Uncaught exception: " + e)
    }
    exitCode
  }

  /**
   * Set the default final application status for client mode to UNDEFINED to handle
   * if YARN HA restarts the application so that it properly retries. Set the final
   * status to SUCCEEDED in cluster mode to handle if the user calls System.exit
   * from the application code.
    * 将客户端模式的默认最终应用程序状态设置为未定义,
    * 以处理如果纱线HA重新启动应用程序以使其正确重试。
    * 将最终状态设置为集群模式下的成功,以处理用户是否从应用程序代码中调用Syt.Exchange
   */
  final def getDefaultFinalStatus(): FinalApplicationStatus = {
    if (isClusterMode) {
      FinalApplicationStatus.SUCCEEDED
    } else {
      FinalApplicationStatus.UNDEFINED
    }
  }

  /**
   * unregister is used to completely unregister the application from the ResourceManager.
   * This means the ResourceManager will not retry the application attempt on your behalf if
   * a failure occurred.
    * 取消注册用于从ResourceManager中完全取消注册应用程序。
    * 这意味着如果发生故障,ResourceManager将不会代表您重试应用程序尝试
   */
  final def unregister(status: FinalApplicationStatus, diagnostics: String = null): Unit = {
    synchronized {
      if (!unregistered) {
        logInfo(s"Unregistering ApplicationMaster with $status" +
          Option(diagnostics).map(msg => s" (diag message: $msg)").getOrElse(""))
        unregistered = true
        client.unregister(status, Option(diagnostics).getOrElse(""))
      }
    }
  }

  final def finish(status: FinalApplicationStatus, code: Int, msg: String = null): Unit = {
    synchronized {
      if (!finished) {
        val inShutdown = ShutdownHookManager.inShutdown()
        logInfo(s"Final app status: $status, exitCode: $code" +
          Option(msg).map(msg => s", (reason: $msg)").getOrElse(""))
        exitCode = code
        finalStatus = status
        finalMsg = msg
        finished = true
        //Thread.currentThread().getContextClassLoader,可以获取当前线程的引用,getContextClassLoader用来获取线程的上下文类加载器
        if (!inShutdown && Thread.currentThread() != reporterThread && reporterThread != null) {
          logDebug("shutting down reporter thread")
          reporterThread.interrupt()
        }
        if (!inShutdown && Thread.currentThread() != userClassThread && userClassThread != null) {
          logDebug("shutting down user thread")
          userClassThread.interrupt()
        }
        if (!inShutdown) delegationTokenRenewerOption.foreach(_.stop())
      }
    }
  }

  private def sparkContextInitialized(sc: SparkContext) = {
    sparkContextRef.synchronized {
      sparkContextRef.compareAndSet(null, sc)
      sparkContextRef.notifyAll()
    }
  }

  private def sparkContextStopped(sc: SparkContext) = {
    sparkContextRef.compareAndSet(sc, null)
  }

  private def registerAM(
      _rpcEnv: RpcEnv,
      driverRef: RpcEndpointRef,
      uiAddress: String,
      securityMgr: SecurityManager) = {
    val sc = sparkContextRef.get()

    val appId = client.getAttemptId().getApplicationId().toString()
    val attemptId = client.getAttemptId().getAttemptId().toString()
    val historyAddress =
      sparkConf.getOption("spark.yarn.historyServer.address")
        .map { text => SparkHadoopUtil.get.substituteHadoopVariables(text, yarnConf) }
        .map { address => s"${address}${HistoryServer.UI_PATH_PREFIX}/${appId}/${attemptId}" }
        .getOrElse("")

    val _sparkConf = if (sc != null) sc.getConf else sparkConf
    val driverUrl = _rpcEnv.uriOf(
        SparkEnv.driverActorSystemName,
        RpcAddress(_sparkConf.get("spark.driver.host"), _sparkConf.get("spark.driver.port").toInt),
        CoarseGrainedSchedulerBackend.ENDPOINT_NAME)
    allocator = client.register(driverUrl,
      driverRef,
      yarnConf,
      _sparkConf,
      if (sc != null) sc.preferredNodeLocationData else Map(),
      uiAddress,
      historyAddress,
      securityMgr)

    allocator.allocateResources()
    reporterThread = launchReporterThread()
  }

  /**
   * Create an [[RpcEndpoint]] that communicates with the driver.
   * 创建与驱动程序通信的[[RpcEndpoint]]
   * In cluster mode, the AM and the driver belong to same process
   * so the AMEndpoint need not monitor lifecycle of the driver.
    *
   * 在群集模式下,AM和驱动程序属于同一进程,因此AMEndpoint无需监视驱动程序的生命周期
    *
   * @return A reference to the driver's RPC endpoint.
   */
  private def runAMEndpoint(
      host: String,
      port: String,
      isClusterMode: Boolean): RpcEndpointRef = {
    val driverEndpoint = rpcEnv.setupEndpointRef(
      SparkEnv.driverActorSystemName,
      RpcAddress(host, port.toInt),
      YarnSchedulerBackend.ENDPOINT_NAME)
    amEndpoint =
      rpcEnv.setupEndpoint("YarnAM", new AMEndpoint(rpcEnv, driverEndpoint, isClusterMode))
    driverEndpoint
  }

  private def runDriver(securityMgr: SecurityManager): Unit = {
    addAmIpFilter()
    userClassThread = startUserApplication()

    // This a bit hacky, but we need to wait until the spark.driver.port property has
    // been set by the Thread executing the user class.
    //这有点hacky,但我们需要等到执行用户类的Thread设置了spark.driver.port属性。
    val sc = waitForSparkContextInitialized()

    // If there is no SparkContext at this point, just fail the app.
    //如果此时没有SparkContext,则只需使应用程序失败
    if (sc == null) {
      finish(FinalApplicationStatus.FAILED,
        ApplicationMaster.EXIT_SC_NOT_INITED,
        "Timed out waiting for SparkContext.")
    } else {
      rpcEnv = sc.env.rpcEnv
      val driverRef = runAMEndpoint(
        sc.getConf.get("spark.driver.host"),
        sc.getConf.get("spark.driver.port"),
        isClusterMode = true)
      registerAM(rpcEnv, driverRef, sc.ui.map(_.appUIAddress).getOrElse(""), securityMgr)
      userClassThread.join()
    }
  }

  private def runExecutorLauncher(securityMgr: SecurityManager): Unit = {
    val port = sparkConf.getInt("spark.yarn.am.port", 0)
    rpcEnv = RpcEnv.create("sparkYarnAM", Utils.localHostName, port, sparkConf, securityMgr)
    val driverRef = waitForSparkDriver()
    addAmIpFilter()
    registerAM(rpcEnv, driverRef, sparkConf.get("spark.driver.appUIAddress", ""), securityMgr)

    // In client mode the actor will stop the reporter thread.
    //在客户端模式中,actor将停止报告者线程
    reporterThread.join()
  }

  private def launchReporterThread(): Thread = {
    // Ensure that progress is sent before YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS elapses.
    //确保在YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS过去之前发送进度
    val expiryInterval = yarnConf.getInt(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS, 120000)

    // we want to be reasonably responsive without causing too many requests to RM.
    //我们想要合理的响应,而不会导致太多的RM请求
    val heartbeatInterval = math.max(0, math.min(expiryInterval / 2,
      sparkConf.getTimeAsMs("spark.yarn.scheduler.heartbeat.interval-ms", "3s")))

    // we want to check more frequently for pending containers
    //我们想更频繁地检查待处理的容器
    val initialAllocationInterval = math.min(heartbeatInterval,
      sparkConf.getTimeAsMs("spark.yarn.scheduler.initial-allocation.interval", "200ms"))

    var nextAllocationInterval = initialAllocationInterval

    // The number of failures in a row until Reporter thread give up
    //在Reporter线程放弃之前连续失败的次数
    val reporterMaxFailures = sparkConf.getInt("spark.yarn.scheduler.reporterThread.maxFailures", 5)

    val t = new Thread {
      override def run() {
        var failureCount = 0
        while (!finished) {
          try {
            if (allocator.getNumExecutorsFailed >= maxNumExecutorFailures) {
              finish(FinalApplicationStatus.FAILED,
                ApplicationMaster.EXIT_MAX_EXECUTOR_FAILURES,
                s"Max number of executor failures ($maxNumExecutorFailures) reached")
            } else {
              logDebug("Sending progress")
              allocator.allocateResources()
            }
            failureCount = 0
          } catch {
            case i: InterruptedException =>
            case e: Throwable => {
              failureCount += 1
              if (!NonFatal(e) || failureCount >= reporterMaxFailures) {
                finish(FinalApplicationStatus.FAILED,
                  ApplicationMaster.EXIT_REPORTER_FAILURE, "Exception was thrown " +
                    s"$failureCount time(s) from Reporter thread.")
              } else {
                logWarning(s"Reporter thread fails $failureCount time(s) in a row.", e)
              }
            }
          }
          try {
            val numPendingAllocate = allocator.getNumPendingAllocate
            val sleepInterval =
              if (numPendingAllocate > 0) {
                val currentAllocationInterval =
                  math.min(heartbeatInterval, nextAllocationInterval)
                nextAllocationInterval = currentAllocationInterval * 2 // avoid overflow
                currentAllocationInterval
              } else {
                nextAllocationInterval = initialAllocationInterval
                heartbeatInterval
              }
            logDebug(s"Number of pending allocations is $numPendingAllocate. " +
                     s"Sleeping for $sleepInterval.")
            allocatorLock.synchronized {
              allocatorLock.wait(sleepInterval)
            }
          } catch {
            case e: InterruptedException =>
          }
        }
      }
    }
    // setting to daemon status, though this is usually not a good idea.
    //设置为守护程序状态,虽然这通常不是一个好主意
    t.setDaemon(true)
    t.setName("Reporter")
    t.start()
    logInfo(s"Started progress reporter thread with (heartbeat : $heartbeatInterval, " +
            s"initial allocation : $initialAllocationInterval) intervals")
    t
  }

  /**
   * Clean up the staging directory.
    * 清理暂存目录
   */
  private def cleanupStagingDir(fs: FileSystem) {
    var stagingDirPath: Path = null
    try {
      val preserveFiles = sparkConf.getBoolean("spark.yarn.preserve.staging.files", false)
      if (!preserveFiles) {
        stagingDirPath = new Path(System.getenv("SPARK_YARN_STAGING_DIR"))
        if (stagingDirPath == null) {
          logError("Staging directory is null")
          return
        }
        logInfo("Deleting staging directory " + stagingDirPath)
        fs.delete(stagingDirPath, true)
      }
    } catch {
      case ioe: IOException =>
        logError("Failed to cleanup staging dir " + stagingDirPath, ioe)
    }
  }

  private def waitForSparkContextInitialized(): SparkContext = {
    logInfo("Waiting for spark context initialization")
    sparkContextRef.synchronized {
      val totalWaitTime = sparkConf.getTimeAsMs("spark.yarn.am.waitTime", "100s")
      val deadline = System.currentTimeMillis() + totalWaitTime

      while (sparkContextRef.get() == null && System.currentTimeMillis < deadline && !finished) {
        logInfo("Waiting for spark context initialization ... ")
        sparkContextRef.wait(10000L)
      }

      val sparkContext = sparkContextRef.get()
      if (sparkContext == null) {
        logError(("SparkContext did not initialize after waiting for %d ms. Please check earlier"
          + " log output for errors. Failing the application.").format(totalWaitTime))
      }
      sparkContext
    }
  }

  private def waitForSparkDriver(): RpcEndpointRef = {
    logInfo("Waiting for Spark driver to be reachable.")
    var driverUp = false
    val hostport = args.userArgs(0)
    val (driverHost, driverPort) = Utils.parseHostPort(hostport)

    // Spark driver should already be up since it launched us, but we don't want to
    // wait forever, so wait 100 seconds max to match the cluster mode setting.
    //Spark驱动程序应该已经启动了,但是我们不想永远等待,所以请等待最多100秒以匹配群集模式设置。
    val totalWaitTimeMs = sparkConf.getTimeAsMs("spark.yarn.am.waitTime", "100s")
    val deadline = System.currentTimeMillis + totalWaitTimeMs

    while (!driverUp && !finished && System.currentTimeMillis < deadline) {
      try {
        val socket = new Socket(driverHost, driverPort)
        socket.close()
        logInfo("Driver now available: %s:%s".format(driverHost, driverPort))
        driverUp = true
      } catch {
        case e: Exception =>
          logError("Failed to connect to driver at %s:%s, retrying ...".
            format(driverHost, driverPort))
          Thread.sleep(100L)
      }
    }

    if (!driverUp) {
      throw new SparkException("Failed to connect to driver!")
    }

    sparkConf.set("spark.driver.host", driverHost)
    sparkConf.set("spark.driver.port", driverPort.toString)

    runAMEndpoint(driverHost, driverPort.toString, isClusterMode = false)
  }

  /** Add the Yarn IP filter that is required for properly securing the UI.
    * 添加正确保护UI所需的Yarn IP过滤器*/
  private def addAmIpFilter() = {
    val proxyBase = System.getenv(ApplicationConstants.APPLICATION_WEB_PROXY_BASE_ENV)
    val amFilter = "org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter"
    val params = client.getAmIpFilterParams(yarnConf, proxyBase)
    if (isClusterMode) {
      System.setProperty("spark.ui.filters", amFilter)
      params.foreach { case (k, v) => System.setProperty(s"spark.$amFilter.param.$k", v) }
    } else {
      amEndpoint.send(AddWebUIFilter(amFilter, params.toMap, proxyBase))
    }
  }

  /**
   * Start the user class, which contains the spark driver, in a separate Thread.
   * If the main routine exits cleanly or exits with System.exit(N) for any N
   * we assume it was successful, for all other cases we assume failure.
   *
    * 在单独的Thread中启动包含spark驱动程序的用户类,
    * 如果主程序干净地退出或退出System.exit(N)任何N我们认为它是成功的,对于所有其他情况我们假设失败。
   * Returns the user thread that was started.
    * 返回已启动的用户线程
   */
  private def startUserApplication(): Thread = {
    logInfo("Starting the user application in a separate Thread")

    val classpath = Client.getUserClasspath(sparkConf)
    val urls = classpath.map { entry =>
      new URL("file:" + new File(entry.getPath()).getAbsolutePath())
    }
    val userClassLoader =
      if (Client.isUserClassPathFirst(sparkConf, isDriver = true)) {
        new ChildFirstURLClassLoader(urls, Utils.getContextOrSparkClassLoader)
      } else {
        new MutableURLClassLoader(urls, Utils.getContextOrSparkClassLoader)
      }

    var userArgs = args.userArgs
    if (args.primaryPyFile != null && args.primaryPyFile.endsWith(".py")) {
      // When running pyspark, the app is run using PythonRunner. The second argument is the list
      // of files to add to PYTHONPATH, which Client.scala already handles, so it's empty.
      userArgs = Seq(args.primaryPyFile, "") ++ userArgs
    }
    if (args.primaryRFile != null && args.primaryRFile.endsWith(".R")) {
      // TODO(davies): add R dependencies here
    }
    val mainMethod = userClassLoader.loadClass(args.userClass)
      .getMethod("main", classOf[Array[String]])

    val userThread = new Thread {
      override def run() {
        try {
          mainMethod.invoke(null, userArgs.toArray)
          finish(FinalApplicationStatus.SUCCEEDED, ApplicationMaster.EXIT_SUCCESS)
          logDebug("Done running users class")
        } catch {
          case e: InvocationTargetException =>
            e.getCause match {
              case _: InterruptedException =>
                // Reporter thread can interrupt to stop user class
              case SparkUserAppException(exitCode) =>
                val msg = s"User application exited with status $exitCode"
                logError(msg)
                finish(FinalApplicationStatus.FAILED, exitCode, msg)
              case cause: Throwable =>
                logError("User class threw exception: " + cause, cause)
                finish(FinalApplicationStatus.FAILED,
                  ApplicationMaster.EXIT_EXCEPTION_USER_CLASS,
                  "User class threw exception: " + cause)
            }
        }
      }
    }
    userThread.setContextClassLoader(userClassLoader)
    userThread.setName("Driver")
    userThread.start()
    userThread
  }

  /**
   * An [[RpcEndpoint]] that communicates with the driver's scheduler backend.
    * 与驱动程序的调度程序后端通信的[[RpcEndpoint]]
   */
  private class AMEndpoint(
      override val rpcEnv: RpcEnv, driver: RpcEndpointRef, isClusterMode: Boolean)
    extends RpcEndpoint with Logging {

    override def onStart(): Unit = {
      driver.send(RegisterClusterManager(self))

    }

    override def receive: PartialFunction[Any, Unit] = {
      case x: AddWebUIFilter =>
        logInfo(s"Add WebUI Filter. $x")
        driver.send(x)
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case RequestExecutors(requestedTotal, localityAwareTasks, hostToLocalTaskCount) =>
        Option(allocator) match {
          case Some(a) =>
            allocatorLock.synchronized {
              if (a.requestTotalExecutorsWithPreferredLocalities(requestedTotal,
                localityAwareTasks, hostToLocalTaskCount)) {
                allocatorLock.notifyAll()
              }
            }

          case None =>
            logWarning("Container allocator is not ready to request executors yet.")
        }
        context.reply(true)

      case KillExecutors(executorIds) =>
        logInfo(s"Driver requested to kill executor(s) ${executorIds.mkString(", ")}.")
        Option(allocator) match {
          case Some(a) => executorIds.foreach(a.killExecutor)
          case None => logWarning("Container allocator is not ready to kill executors yet.")
        }
        context.reply(true)
    }

    override def onDisconnected(remoteAddress: RpcAddress): Unit = {
      logInfo(s"Driver terminated or disconnected! Shutting down. $remoteAddress")
      // In cluster mode, do not rely on the disassociated event to exit
      // This avoids potentially reporting incorrect exit codes if the driver fails
      if (!isClusterMode) {
        finish(FinalApplicationStatus.SUCCEEDED, ApplicationMaster.EXIT_SUCCESS)
      }
    }
  }

}

object ApplicationMaster extends Logging {

  // exit codes for different causes, no reason behind the values
  //退出代码的原因不同,价值背后没有理由
  private val EXIT_SUCCESS = 0
  private val EXIT_UNCAUGHT_EXCEPTION = 10
  private val EXIT_MAX_EXECUTOR_FAILURES = 11
  private val EXIT_REPORTER_FAILURE = 12
  private val EXIT_SC_NOT_INITED = 13
  private val EXIT_SECURITY = 14
  private val EXIT_EXCEPTION_USER_CLASS = 15

  private var master: ApplicationMaster = _

  def main(args: Array[String]): Unit = {
    SignalLogger.register(log)
    val amArgs = new ApplicationMasterArguments(args)
    SparkHadoopUtil.get.runAsSparkUser { () =>
      master = new ApplicationMaster(amArgs, new YarnRMClient(amArgs))
      System.exit(master.run())
    }
  }

  private[spark] def sparkContextInitialized(sc: SparkContext): Unit = {
    master.sparkContextInitialized(sc)
  }

  private[spark] def sparkContextStopped(sc: SparkContext): Boolean = {
    master.sparkContextStopped(sc)
  }

}

/**
 * This object does not provide any special functionality. It exists so that it's easy to tell
 * apart the client-mode AM from the cluster-mode AM when using tools such as ps or jps.
  * 此对象不提供任何特殊功能,它的存在使得在使用ps或jps等工具时,
  * 很容易将群集模式AM中的客户端模式AM分开
 */
object ExecutorLauncher {

  def main(args: Array[String]): Unit = {
    ApplicationMaster.main(args)
  }

}
