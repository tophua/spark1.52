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

package org.apache.spark.deploy.worker

import java.io.File
import java.io.IOException
import java.text.SimpleDateFormat
import java.util.{ UUID, Date }
import java.util.concurrent._
import java.util.concurrent.{ Future => JFuture, ScheduledFuture => JScheduledFuture }

import scala.collection.JavaConversions._
import scala.collection.mutable.{ HashMap, HashSet, LinkedHashMap }
import scala.concurrent.ExecutionContext
import scala.util.Random
import scala.util.control.NonFatal

import org.apache.spark.{ Logging, SecurityManager, SparkConf }
import org.apache.spark.deploy.{ Command, ExecutorDescription, ExecutorState }
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.deploy.ExternalShuffleService
import org.apache.spark.deploy.master.{ DriverState, Master }
import org.apache.spark.deploy.worker.ui.WorkerWebUI
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.rpc._
import org.apache.spark.util.{ ThreadUtils, SignalLogger, Utils }
/**
 * Spark工作节点,对Spark应用程序来说,由集群管理器分配得到资源的worker节点主要负责
 * 创建Executor,将资源和任务进一步分配给Executor,同步资源信息给Cluster Manager
 */
private[deploy] class Worker(
  override val rpcEnv: RpcEnv,//RpcEnv处理从RpcEndpointRef或远程节点发送过来的消息
  webUiPort: Int,
  //worker 节点当前可用的core个数
  cores: Int,
  //worker节点当前可用的memory大小
  memory: Int,
  //Master地址
  masterRpcAddresses: Array[RpcAddress],//Master RPC地址
  systemName: String,//系统名称
  endpointName: String,//终端名称
  workDirPath: String = null,
  val conf: SparkConf,
  val securityMgr: SecurityManager)
    extends ThreadSafeRpcEndpoint with Logging {

  private val host = rpcEnv.address.host
  private val port = rpcEnv.address.port

  Utils.checkHost(host, "Expected hostname")
  assert(port > 0)

  // A scheduled executor used to send messages at the specified time.
  //用于在指定时间发送消息的调度的执行器
  private val forwordMessageScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("worker-forward-message-scheduler")

  // A separated thread to clean up the workDir. Used to provide the implicit parameter of `Future`
  //一个分离的线程清理workDir目录,用于提供“Future”方法的隐式参数
  // methods.
  private val cleanupThreadExecutor = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonSingleThreadExecutor("worker-cleanup-thread"))

  // For worker and executor IDs
  private def createDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
  // Send a heartbeat every (heartbeat timeout) / 4 milliseconds
  //HEARTBEAT_MILLIS （默认是15秒（15000毫秒）
  //System.getProperty("spark.worker.timeout", "60").toLong * 1000 / 4）为时间间隔,定期向master发送心跳, 
  private val HEARTBEAT_MILLIS = conf.getLong("spark.worker.timeout", 60) * 1000 / 4

  // Model retries to connect to the master, after Hadoop's model.
  //模型重新尝试连接到主节点,Hadoop的模型后
  // The first six attempts to reconnect are in shorter intervals (between 5 and 15 seconds)
  //前六次尝试重新连接的时间间隔较短（在5到15秒之间）
  // Afterwards, the next 10 attempts are between 30 and 90 seconds.
  //之后,下一个10次尝试是在30到90秒之间
  // A bit of randomness is introduced so that not all of the workers attempt to reconnect at
  // the same time.
  //引入了一点随机性,以便不是所有的工作节占都尝试在同一时间重新连接
  private val INITIAL_REGISTRATION_RETRIES = 6
  private val TOTAL_REGISTRATION_RETRIES = INITIAL_REGISTRATION_RETRIES + 10
  private val FUZZ_MULTIPLIER_INTERVAL_LOWER_BOUND = 0.500
  private val REGISTRATION_RETRY_FUZZ_MULTIPLIER = {
    val randomNumberGenerator = new Random(UUID.randomUUID.getMostSignificantBits)
    randomNumberGenerator.nextDouble + FUZZ_MULTIPLIER_INTERVAL_LOWER_BOUND
  }
  private val INITIAL_REGISTRATION_RETRY_INTERVAL_SECONDS = (math.round(10 *
    REGISTRATION_RETRY_FUZZ_MULTIPLIER))
  private val PROLONGED_REGISTRATION_RETRY_INTERVAL_SECONDS = (math.round(60
    * REGISTRATION_RETRY_FUZZ_MULTIPLIER))
  //是否定期清理worker的应用程序工作目录
  private val CLEANUP_ENABLED = conf.getBoolean("spark.worker.cleanup.enabled", false)
  // How often worker will clean up old app folders
  //清理worker本地过期的应用程序工作目录的时间间隔（秒）
  private val CLEANUP_INTERVAL_MILLIS =
    conf.getLong("spark.worker.cleanup.interval", 60 * 30) * 1000
    //worker保留应用程序工作目录的有效时间。
  // TTL for app folders/data;  after TTL expires it will be cleaned up
  private val APP_DATA_RETENTION_SECONDS =
    conf.getLong("spark.worker.cleanup.appDataTtl", 7 * 24 * 3600)

  private val testing: Boolean = sys.props.contains("spark.testing")
  private var master: Option[RpcEndpointRef] = None
  private var activeMasterUrl: String = ""
  private[worker] var activeMasterWebUiUrl: String = ""
  private val workerUri = rpcEnv.uriOf(systemName, rpcEnv.address, endpointName)
  private var registered = false
  private var connected = false
  private val workerId = generateWorkerId()
  private val sparkHome =
    if (testing) {
      assert(sys.props.contains("spark.test.home"), "spark.test.home is not set!")
      new File(sys.props("spark.test.home"))
    } else {
      new File(sys.env.get("SPARK_HOME").getOrElse("."))
    }

  var workDir: File = null
  //完成运行Executor
  val finishedExecutors = new LinkedHashMap[String, ExecutorRunner]
  val drivers = new HashMap[String, DriverRunner]
  val executors = new HashMap[String, ExecutorRunner]
  val finishedDrivers = new LinkedHashMap[String, DriverRunner]
  val appDirectories = new HashMap[String, Seq[String]]
  val finishedApps = new HashSet[String]

  val retainedExecutors = conf.getInt("spark.worker.ui.retainedExecutors",
    WorkerWebUI.DEFAULT_RETAINED_EXECUTORS)
  val retainedDrivers = conf.getInt("spark.worker.ui.retainedDrivers",
    WorkerWebUI.DEFAULT_RETAINED_DRIVERS)

  // The shuffle service is not actually started unless configured.
    //Shuffle服务并没有真正开始,除非配置
  private val shuffleService = new ExternalShuffleService(conf, securityMgr)
 //Spark master和workers使用的公共DNS（默认空）
  private val publicAddress = {
    val envVar = conf.getenv("SPARK_PUBLIC_DNS")
    if (envVar != null) envVar else host
  }
  private var webUi: WorkerWebUI = null

  private var connectionAttemptCount = 0

  private val metricsSystem = MetricsSystem.createMetricsSystem("worker", conf, securityMgr)
  private val workerSource = new WorkerSource(this)

  private var registerMasterFutures: Array[JFuture[_]] = null
  private var registrationRetryTimer: Option[JScheduledFuture[_]] = None

  // A thread pool for registering with masters. Because registering with a master is a blocking
  // action, this thread pool must be able to create "masterRpcAddresses.size" threads at the same
  // time so that we can register with all masters.
  private val registerMasterThreadPool = ThreadUtils.newDaemonCachedThreadPool(
    "worker-register-master-threadpool",
    masterRpcAddresses.size // Make sure we can register with all masters at the same time
    )

  var coresUsed = 0
  var memoryUsed = 0

  def coresFree: Int = cores - coresUsed
  def memoryFree: Int = memory - memoryUsed

  private def createWorkDir() {
    workDir = Option(workDirPath).map(new File(_)).getOrElse(new File(sparkHome, "work"))
    try {
      // This sporadically fails - not sure why ... !workDir.exists() && !workDir.mkdirs()
      // So attempting to create and then check if directory was created or not.
      workDir.mkdirs()
      if (!workDir.exists() || !workDir.isDirectory) {
        logError("Failed to create work directory " + workDir)
        System.exit(1)
      }
      assert(workDir.isDirectory)
    } catch {
      case e: Exception =>
        logError("Failed to create work directory " + workDir, e)
        System.exit(1)
    }
  }

  override def onStart() {
    assert(!registered)
    logInfo("Starting Spark worker %s:%d with %d cores, %s RAM".format(
      host, port, cores, Utils.megabytesToString(memory)))
    logInfo(s"Running Spark version ${org.apache.spark.SPARK_VERSION}")
    logInfo("Spark home: " + sparkHome)
    //创建工作目录
    createWorkDir()
    //启动shuffleService,ShuffleClient一样Netty的异步网络
    shuffleService.startIfEnabled()
    //启动WorkerWebUI
    webUi = new WorkerWebUI(this, workDir, webUiPort)
    webUi.bind()

    //用于将Worker注册到Master
    registerWithMaster()
    //启动测量信息
    metricsSystem.registerSource(workerSource)
    metricsSystem.start()
    // Attach the worker metrics servlet handler to the web ui after the metrics system is started.
    metricsSystem.getServletHandlers.foreach(webUi.attachHandler)
  }
  /**
   * 调用changeMaster方法更新activeMasterUrl,activeMasterWebUIurl,master,masterAddress消息
   */
  private def changeMaster(masterRef: RpcEndpointRef, uiUrl: String) {
    // activeMasterUrl it's a valid Spark url since we receive it from master.
    activeMasterUrl = masterRef.address.toSparkURL
    activeMasterWebUiUrl = uiUrl
    master = Some(masterRef)
    connected = true
    // Cancel any outstanding re-registration attempts because we found a new master
    cancelLastRegistrationRetry()
  }
  /**
   * Worker启动后会向所有Master发起注册,在注册消息中说明本WorkerNode含有的Core数目及可用的内存大小
   */
  private def tryRegisterAllMasters(): Array[JFuture[_]] = {
    masterRpcAddresses.map { masterAddress =>
      registerMasterThreadPool.submit(new Runnable {
        override def run(): Unit = {
          try {
            logInfo("Connecting to master " + masterAddress + "...")
            val masterEndpoint =
              rpcEnv.setupEndpointRef(Master.SYSTEM_NAME, masterAddress, Master.ENDPOINT_NAME)
            //向Master发起注册Worker,Woker启动后,会向Master发送消息注册,Master注册完成会回复RegisteredWorker
            //Worker完成注册后就可以接受来自Master的调度请求
            masterEndpoint.send(RegisterWorker(
              workerId, host, port, self, cores, memory, webUi.boundPort, publicAddress))
          } catch {
            case ie: InterruptedException => // Cancelled
            case NonFatal(e)              => logWarning(s"Failed to connect to master $masterAddress", e)
          }
        }
      })
    }
  }

  /**
   * Re-register with the master because a network failure or a master failure has occurred.
   * 因为网络故障或者Master故障已发生需Worker重新注册Master
   * If the re-registration attempt threshold is exceeded, the worker exits with error.
   * 如果重新注册超过重试次数,worker异常退出
   * Note that for thread-safety this should only be called from the rpcEndpoint.
   */
  private def reregisterWithMaster(): Unit = {
    Utils.tryOrExit {
      connectionAttemptCount += 1
      if (registered) {
        cancelLastRegistrationRetry()
      } else if (connectionAttemptCount <= TOTAL_REGISTRATION_RETRIES) {
        //Worker在向Master注册的时候 有重试机制,即在指定时间如果收不到Master的响应
        //那么Worker会重新发送注册请求,目前重试的次数至多为16次,为了避免所有的Worker
        //都在同一个时刻向Master发送注册请求,每次重试的时间间隔是随机的,而且前6次的重试间隔在5-15秒
        //而后10次的重试间隔在30-90秒
        logInfo(s"Retrying connection to master (attempt # $connectionAttemptCount)")
        /**
         * Re-register with the active master this worker has been communicating with(重新注册活动Master与Worker一直在通信). If there
         * is none(如果没有), then it means this worker is still bootstrapping and hasn't established a
         * connection with a master yet(这意味着Worker自启动没有建立Master连接), in which case we should re-register with all masters.
         * (在这种情况下,应该重新注册Master)
         *
         * It is important to re-register only with the active master during failures(重新注册活动的Master只有在Master故障期间). Otherwise,
         * if the worker unconditionally(无条件) attempts to re-register with all masters, the following
         * race condition may arise and cause a "duplicate worker" error detailed in SPARK-4592:
         *
         *   (1) Master A fails and Worker attempts to reconnect to all masters
         *   (2) Master B takes over(接管) and notifies Worker
         *   (3) Worker responds(响应) by registering with Master B
         *   (4) Meanwhile(同时), Worker's previous reconnection attempt reaches Master B,
         *       causing the same Worker to register with Master B twice(导致Worker注册两次Master B)
         *
         * Instead(相反), if we only register with the known active master(如果注册一个未知活动Master), we can assume that the
         * old master must have died because another master has taken over(旧的Master必须已经死状态,因为别一个Master接管). Note that this is
         * still not safe if the old master recovers within this interval, but this is a much
         * less likely scenario.
         */
        master match {
          case Some(masterRef) =>
            // registered == false && master != None means we lost the connection to master, so
            // masterRef cannot be used and we need to recreate it again. Note: we must not set
            // master to None due to the above comments.
            if (registerMasterFutures != null) {
              registerMasterFutures.foreach(_.cancel(true))
            }
            val masterAddress = masterRef.address
            registerMasterFutures = Array(registerMasterThreadPool.submit(new Runnable {
              override def run(): Unit = {
                try {
                  logInfo("Connecting to master " + masterAddress + "...")
                  val masterEndpoint =
                    rpcEnv.setupEndpointRef(Master.SYSTEM_NAME, masterAddress, Master.ENDPOINT_NAME)
                  masterEndpoint.send(RegisterWorker(
                    workerId, host, port, self, cores, memory, webUi.boundPort, publicAddress))
                } catch {
                  case ie: InterruptedException => // Cancelled
                  case NonFatal(e)              => logWarning(s"Failed to connect to master $masterAddress", e)
                }
              }
            }))
          case None =>
            if (registerMasterFutures != null) {
              registerMasterFutures.foreach(_.cancel(true))
            }
            // We are retrying the initial registration
            //我们正在尝试的初始注册
            registerMasterFutures = tryRegisterAllMasters()
        }
        // We have exceeded the initial registration retry threshold,
        //我们已经超过了初始注册重试阈值
        // All retries from now on should use a higher interval
        //所有重试从现在开始应该使用较高的间隔
        //前6次的重试间隔在5-15秒,
        if (connectionAttemptCount == INITIAL_REGISTRATION_RETRIES) {
          registrationRetryTimer.foreach(_.cancel(true))
          registrationRetryTimer = Some(
            forwordMessageScheduler.scheduleAtFixedRate(new Runnable {
              override def run(): Unit = Utils.tryLogNonFatalError {
                self.send(ReregisterWithMaster)
              }
            }, PROLONGED_REGISTRATION_RETRY_INTERVAL_SECONDS,
              PROLONGED_REGISTRATION_RETRY_INTERVAL_SECONDS,
              TimeUnit.SECONDS))
        }
      } else {
        logError("All masters are unresponsive! Giving up.")
        System.exit(1)
      }
    }
  }

  /**
   * Cancel last registeration retry, or do nothing if no retry
   * 
   */
  private def cancelLastRegistrationRetry(): Unit = {
    if (registerMasterFutures != null) {
      registerMasterFutures.foreach(_.cancel(true))
      registerMasterFutures = null
    }
    registrationRetryTimer.foreach(_.cancel(true))
    registrationRetryTimer = None
  }

  private def registerWithMaster() {
    // onDisconnected may be triggered multiple times, so don't attempt registration
    // if there are outstanding registration attempts scheduled.
    registrationRetryTimer match {
      case None =>
        registered = false //标记未注册,为true代表注册成功
        //向所有Mastr发送RegisterWorker消息
        registerMasterFutures = tryRegisterAllMasters() //向所有的Master发送发出注册请求
        connectionAttemptCount = 0//记录重试次数
        registrationRetryTimer = Some(forwordMessageScheduler.scheduleAtFixedRate(
          new Runnable {//开启定时器,超时时间在5--15秒
            override def run(): Unit = Utils.tryLogNonFatalError {
              self.send(ReregisterWithMaster)
            }
          },
          INITIAL_REGISTRATION_RETRY_INTERVAL_SECONDS,
          INITIAL_REGISTRATION_RETRY_INTERVAL_SECONDS,
          TimeUnit.SECONDS))
      case Some(_) =>
        logInfo("Not spawning another attempt to register with the master, since there is an" +
          " attempt scheduled already.")
    }
  }

  override def receive: PartialFunction[Any, Unit] = {
    /**
     * Woker启动后,会向Master发送消息RegisterWorker注册,Master注册完成会回复RegisteredWorker
     * Worker完成注册后就可以接受来自Master的调度请求
     * Worker在收到Master确认注册成功的消息RegisteredWorker消息的处理逻辑
     * 1)标记注册成功
     * 2)调用changeMaster方法更新activeMasterUrl,activeMasterWebUIurl,master,masterAddress消息
     * 3)启动定时调用给自己发送SendHeartbeat
     */
    case RegisteredWorker(masterRef, masterWebUiUrl) =>
      logInfo("Successfully registered with master " + masterRef.address.toSparkURL)
      registered = true
      changeMaster(masterRef, masterWebUiUrl)
      forwordMessageScheduler.scheduleAtFixedRate(new Runnable {
        override def run(): Unit = Utils.tryLogNonFatalError {
          self.send(SendHeartbeat) //启动定时调用给自己发送SendHeartbeat
        }//默认是15秒,1 / 4分为时间间隔,定期向master发送心跳
      }, 0, HEARTBEAT_MILLIS, TimeUnit.MILLISECONDS)
      if (CLEANUP_ENABLED) {
        logInfo(s"Worker cleanup enabled; old application directories will be deleted in: $workDir")
        forwordMessageScheduler.scheduleAtFixedRate(new Runnable {
          override def run(): Unit = Utils.tryLogNonFatalError {
            self.send(WorkDirCleanup)
          }
        }, CLEANUP_INTERVAL_MILLIS, CLEANUP_INTERVAL_MILLIS, TimeUnit.MILLISECONDS)
      }

    case SendHeartbeat =>
      //Worker收到SendHeartbeat消息后,会向Master转发Heartbeat消息
      if (connected) { sendToMaster(Heartbeat(workerId, self)) }

    case WorkDirCleanup =>
      // Spin up a separate thread (in a future) to do the dir cleanup; don't tie up worker
      // rpcEndpoint.
      // Copy ids so that it can be used in the cleanup thread.
      val appIds = executors.values.map(_.appId).toSet
      val cleanupFuture = concurrent.future {
        val appDirs = workDir.listFiles()
        if (appDirs == null) {
          throw new IOException("ERROR: Failed to list files in " + appDirs)
        }
        appDirs.filter { dir =>
          // the directory is used by an application - check that the application is not running
          // when cleaning up
          val appIdFromDir = dir.getName
          val isAppStillRunning = appIds.contains(appIdFromDir)
          dir.isDirectory && !isAppStillRunning &&
            !Utils.doesDirectoryContainAnyNewFiles(dir, APP_DATA_RETENTION_SECONDS)
        }.foreach { dir =>
          logInfo(s"Removing directory: ${dir.getPath}")
          Utils.deleteRecursively(dir)
        }
      }(cleanupThreadExecutor)

      cleanupFuture.onFailure {
        case e: Throwable =>
          logError("App dir cleanup failed: " + e.getMessage, e)
      }(cleanupThreadExecutor)
/**
 * 恢复Worker的步骤:
 * 1)重新注册Worker（实际上是更新Master本地维护的数据结构）,置状态为UNKNOWN
 * 2)向Worker发送Master Changed的消息
 * 3)Worker收到消息后,向Master回复WorkerSchedulerStateResponse消息,并通过该消息上报executor和driver的信息
 */
    case MasterChanged(masterRef, masterWebUiUrl) =>
      logInfo("Master has changed, new master is at " + masterRef.address.toSparkURL)
      changeMaster(masterRef, masterWebUiUrl)

      val execs = executors.values.
        map(e => new ExecutorDescription(e.appId, e.execId, e.cores, e.state))
      masterRef.send(WorkerSchedulerStateResponse(workerId, execs.toList, drivers.keys.toSeq))
    /**
     * Worker注册失败的返回消息,Worker接到该消息后会直接退出
     */
    case RegisterWorkerFailed(message) =>
      if (!registered) {
        logError("Worker registration failed: " + message)
        System.exit(1)
      }
    /**
     * Master收到Heartbeat消息后的实现步骤
     * 1)更新workerInfo.lastHeartbeat,即最后一次接收到心跳的时间戳
     * 2)如果worker的id与Worker的映射关系(idToWorker)中找不到匹配的Worker,但是Worker的缓存(workers)的缓存
     *   中却存在此Id,那么向Worker发送ReconnectWorker消息
     */
    case ReconnectWorker(masterUrl) =>
      logInfo(s"Master with url $masterUrl requested this worker to reconnect.")
      registerWithMaster()
    /**
     * Worker接收Master发送LaunchExecutor消息后处理步骤如下:
     * 1)创建Executor的工作目录
     * 2)创建Application的本地目录,当Application完成时,此目录会被删除
     * 3)创建并启动ExecutorRunner
     * 4)向Master发送ExecutorStateChanged消息
     *
     */
    case LaunchExecutor(masterUrl, appId, execId, appDesc, cores_, memory_) =>
      //验证该命令是否发自一个合法的Master
      if (masterUrl != activeMasterUrl) {
        logWarning("Invalid Master (" + masterUrl + ") attempted to launch executor.")
      } else {
        try {
          logInfo("Asked to launch executor %s/%d for %s".format(appId, execId, appDesc.name))

          // Create the executor's working directory
          val executorDir = new File(workDir, appId + "/" + execId)
          if (!executorDir.mkdirs()) {
            throw new IOException("Failed to create directory " + executorDir)
          }

          // Create local dirs for the executor. These are passed to the executor via the
          // SPARK_EXECUTOR_DIRS environment variable, and deleted by the Worker when the
          // application finishes.
          val appLocalDirs = appDirectories.get(appId).getOrElse {
            Utils.getOrCreateLocalRootDirs(conf).map { dir =>
              val appDir = Utils.createDirectory(dir, namePrefix = "executor")
              Utils.chmod700(appDir)
              appDir.getAbsolutePath()
            }.toSeq
          }
          appDirectories(appId) = appLocalDirs
          val manager = new ExecutorRunner(
            appId,
            execId,
            appDesc.copy(command = Worker.maybeUpdateSSLSettings(appDesc.command, conf)),
            cores_,
            memory_,
            self,
            workerId,
            host,
            webUi.boundPort,
            publicAddress,
            sparkHome,
            executorDir,
            workerUri,
            conf,
            appLocalDirs, ExecutorState.LOADING)
          //将新建的executor放到上面提到的Hash Map中
          executors(appId + "/" + execId) = manager
          //启动这个Executor进程
          manager.start()
          //将现在已经使用的core和memory进行的统计
          coresUsed += cores_
          memoryUsed += memory_
          //向Master发送ExecutorStateChanged消息
          sendToMaster(ExecutorStateChanged(appId, execId, manager.state, None, None))
        } catch {
          /**
           * 如果在这过程中有异常抛出,那么需要check是否是executor已经加到Hash Map中,
           * 如果有则首先停止它,然后从Hash Map中删除它。并且向Master report Executor是FAILED的。
           * Master会重新启动新的Executor
           */
          case e: Exception => {
            logError(s"Failed to launch executor $appId/$execId for ${appDesc.name}.", e)
            if (executors.contains(appId + "/" + execId)) {
              executors(appId + "/" + execId).kill()
              executors -= appId + "/" + execId
            }
            sendToMaster(ExecutorStateChanged(appId, execId, ExecutorState.FAILED,
              Some(e.toString), None))
          }
        }
      }
    /**
     * 当Executor的状态有更新,向Master汇报,Worker接收LaunchExecutor的命令后,会向Master汇报其启动
     * 其启动Executor状态,然后Executor在被杀死或者退出时,ExecutorRunner会向Worker汇报该情况而Worker
     * 又会将这个状态汇报到Master
     */
    case executorStateChanged @ ExecutorStateChanged(appId, execId, state, message, exitStatus) =>
      handleExecutorStateChanged(executorStateChanged)
    /**
     * 在Application完成的时候,Master会告知Worker删除指定的Executor
     */
    case KillExecutor(masterUrl, appId, execId) =>
      if (masterUrl != activeMasterUrl) {
        logWarning("Invalid Master (" + masterUrl + ") attempted to launch executor " + execId)
      } else {
        val fullId = appId + "/" + execId
        executors.get(fullId) match {
          case Some(executor) =>
            logInfo("Asked to kill executor " + fullId)
            executor.kill()
          case None =>
            logInfo("Asked to kill unknown executor " + fullId)
        }
      }

    case LaunchDriver(driverId, driverDesc) => {
      logInfo(s"Asked to launch driver $driverId")
      val driver = new DriverRunner(
        conf,
        driverId,
        workDir,
        sparkHome,
        driverDesc.copy(command = Worker.maybeUpdateSSLSettings(driverDesc.command, conf)),
        self,
        workerUri,
        securityMgr)
      drivers(driverId) = driver
      driver.start()

      coresUsed += driverDesc.cores
      memoryUsed += driverDesc.mem
    }
    /**
     * 通知Driver所在的Worker的杀死该Driver
     */

    case KillDriver(driverId) => {
      logInfo(s"Asked to kill driver $driverId")
      drivers.get(driverId) match {
        case Some(runner) =>
          runner.kill()
        case None =>
          logError(s"Asked to kill unknown driver $driverId")
      }
    }
    /**
     * 在Dirver的状态变化时,DirverRunner会向Worker发送状态更新的消息,Worker会将这个消息转到Master
     * 在Master接收停止Driver的消息后,也会自身发送Driver状态更新消息
     */

    case driverStateChanged @ DriverStateChanged(driverId, state, exception) => {
      handleDriverStateChanged(driverStateChanged)
    }
    /**
     * 重新注册Master 
     */
    case ReregisterWithMaster =>
      reregisterWithMaster()

    case ApplicationFinished(id) =>
      finishedApps += id
      maybeCleanupApplication(id)
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RequestWorkerState =>
      context.reply(WorkerStateResponse(host, port, workerId, executors.values.toList,
        finishedExecutors.values.toList, drivers.values.toList,
        finishedDrivers.values.toList, activeMasterUrl, cores, memory,
        coresUsed, memoryUsed, activeMasterWebUiUrl))
  }

  override def onDisconnected(remoteAddress: RpcAddress): Unit = {
    if (master.exists(_.address == remoteAddress)) {
      logInfo(s"$remoteAddress Disassociated !")
      masterDisconnected()
    }
  }

  private def masterDisconnected() {
    logError("Connection to master failed! Waiting for master to reconnect...")
    connected = false
    registerWithMaster()
  }

  private def maybeCleanupApplication(id: String): Unit = {
    val shouldCleanup = finishedApps.contains(id) && !executors.values.exists(_.appId == id)
    if (shouldCleanup) {
      finishedApps -= id
      appDirectories.remove(id).foreach { dirList =>
        logInfo(s"Cleaning up local directories for application $id")
        dirList.foreach { dir =>
          Utils.deleteRecursively(new File(dir))
        }
      }
      shuffleService.applicationRemoved(id)
    }
  }

  /**
   * Send a message to the current master. If we have not yet registered successfully with any
   * master, the message will be dropped.
   * 发送一个消息到当前Master,如果我们还没有成功注册任何的Master,该消息将被删除。
   */
  private def sendToMaster(message: Any): Unit = {
    master match {
      case Some(masterRef) => masterRef.send(message)
      case None =>
        logWarning(
          s"Dropping $message because the connection to master has not yet been established")
    }
  }

  private def generateWorkerId(): String = {
    "worker-%s-%s-%d".format(createDateFormat.format(new Date), host, port)
  }

  override def onStop() {
    cleanupThreadExecutor.shutdownNow()
    metricsSystem.report()
    cancelLastRegistrationRetry()
    forwordMessageScheduler.shutdownNow()
    registerMasterThreadPool.shutdownNow()
    executors.values.foreach(_.kill())
    drivers.values.foreach(_.kill())
    shuffleService.stop()
    webUi.stop()
    metricsSystem.stop()
  }

  private def trimFinishedExecutorsIfNecessary(): Unit = {
    // do not need to protect with locks since both WorkerPage and Restful server get data through
    // thread-safe RpcEndPoint
    if (finishedExecutors.size > retainedExecutors) {
      finishedExecutors.take(math.max(finishedExecutors.size / 10, 1)).foreach {
        case (executorId, _) => finishedExecutors.remove(executorId)
      }
    }
  }

  private def trimFinishedDriversIfNecessary(): Unit = {
    // do not need to protect with locks since both WorkerPage and Restful server get data through
    // thread-safe RpcEndPoint
    if (finishedDrivers.size > retainedDrivers) {
      finishedDrivers.take(math.max(finishedDrivers.size / 10, 1)).foreach {
        case (driverId, _) => finishedDrivers.remove(driverId)
      }
    }
  }

  private[worker] def handleDriverStateChanged(driverStateChanged: DriverStateChanged): Unit = {
    val driverId = driverStateChanged.driverId
    val exception = driverStateChanged.exception
    val state = driverStateChanged.state
    state match {
      case DriverState.ERROR =>
        logWarning(s"Driver $driverId failed with unrecoverable exception: ${exception.get}")
      case DriverState.FAILED =>
        logWarning(s"Driver $driverId exited with failure")
      case DriverState.FINISHED =>
        logInfo(s"Driver $driverId exited successfully")
      case DriverState.KILLED =>
        logInfo(s"Driver $driverId was killed by user")
      case _ =>
        logDebug(s"Driver $driverId changed state to $state")
    }
    sendToMaster(driverStateChanged)
    val driver = drivers.remove(driverId).get
    finishedDrivers(driverId) = driver
    trimFinishedDriversIfNecessary()
    memoryUsed -= driver.driverDesc.mem
    coresUsed -= driver.driverDesc.cores
  }

  private[worker] def handleExecutorStateChanged(executorStateChanged: ExecutorStateChanged): Unit = {
    //向Master转发ExecutorStateChanged消息
    sendToMaster(executorStateChanged)
    val state = executorStateChanged.state
    if (ExecutorState.isFinished(state)) {
      val appId = executorStateChanged.appId
      val fullId = appId + "/" + executorStateChanged.execId
      val message = executorStateChanged.message
      val exitStatus = executorStateChanged.exitStatus
      executors.get(fullId) match {
        case Some(executor) =>
          logInfo("Executor " + fullId + " finished with state " + state +
            message.map(" message " + _).getOrElse("") +
            exitStatus.map(" exitStatus " + _).getOrElse(""))
          executors -= fullId
          finishedExecutors(fullId) = executor
          trimFinishedExecutorsIfNecessary()
          coresUsed -= executor.cores
          memoryUsed -= executor.memory
        case None =>
          logInfo("Unknown Executor " + fullId + " finished with state " + state +
            message.map(" message " + _).getOrElse("") +
            exitStatus.map(" exitStatus " + _).getOrElse(""))
      }
      maybeCleanupApplication(appId)
    }
  }
}

private[deploy] object Worker extends Logging {
  val SYSTEM_NAME = "sparkWorker"
  val ENDPOINT_NAME = "Worker"

  def main(argStrings: Array[String]) {
    SignalLogger.register(log)
    val conf = new SparkConf //创建Sparkconf
    //Work参数解析
    val args = new WorkerArguments(argStrings, conf)
    //创建,启动actorSystem,并向ActorSystem注册Worker
    val rpcEnv = startRpcEnvAndEndpoint(args.host, args.port, args.webUiPort, args.cores,
      args.memory, args.masters, args.workDir)
    rpcEnv.awaitTermination()
  }
  /**
   * Rpc Environment(RpcEnv)是一个RpcEndpoints用于处理消息的环境,
   * 它管理着整个RpcEndpoints的声明周期：(1)根据name或uri注册endpoints(2)管理各种消息的处理(3)停止endpoints。
   * RpcEnv必须通过工厂类RpcEnvFactory创建
   * 创建,启动Worker的ActorSystem,所有Worker的ActorSystem的Akka的访问地址以akka://sparkWorker加编号访问
   * 每个Worker的ActorSystem都需要注册自身的Worker,同时每个Worker的ActorSystem都需要注册到WorkerActorSystem缓存
   */
  def startRpcEnvAndEndpoint(
    host: String,
    port: Int,
    webUiPort: Int,
    cores: Int,
    memory: Int,
    masterUrls: Array[String],
    workDir: String,
    workerNumber: Option[Int] = None,
    conf: SparkConf = new SparkConf): RpcEnv = {

    // The LocalSparkCluster runs multiple local sparkWorkerX RPC Environments
    val systemName = SYSTEM_NAME + workerNumber.map(_.toString).getOrElse("")
    val securityMgr = new SecurityManager(conf)
    val rpcEnv = RpcEnv.create(systemName, host, port, conf, securityMgr)
    val masterAddresses = masterUrls.map(RpcAddress.fromSparkURL(_))
    rpcEnv.setupEndpoint(ENDPOINT_NAME, new Worker(rpcEnv, webUiPort, cores, memory,
      masterAddresses, systemName, ENDPOINT_NAME, workDir, conf, securityMgr))
    rpcEnv
  }

  def isUseLocalNodeSSLConfig(cmd: Command): Boolean = {
    val pattern = """\-Dspark\.ssl\.useNodeLocalConf\=(.+)""".r
    val result = cmd.javaOpts.collectFirst {
      case pattern(_result) => _result.toBoolean
    }
    result.getOrElse(false)
  }

  def maybeUpdateSSLSettings(cmd: Command, conf: SparkConf): Command = {
    val prefix = "spark.ssl."
    val useNLC = "spark.ssl.useNodeLocalConf"
    if (isUseLocalNodeSSLConfig(cmd)) {
      val newJavaOpts = cmd.javaOpts
        .filter(opt => !opt.startsWith(s"-D$prefix")) ++
        conf.getAll.collect { case (key, value) if key.startsWith(prefix) => s"-D$key=$value" } :+
        s"-D$useNLC=true"
      cmd.copy(javaOpts = newJavaOpts)
    } else {
      cmd
    }
  }
}
