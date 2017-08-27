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

package org.apache.spark.deploy.client

import java.util.concurrent._
import java.util.concurrent.{ Future => JFuture, ScheduledFuture => JScheduledFuture }

import scala.util.control.NonFatal

import org.apache.spark.{ Logging, SparkConf }
import org.apache.spark.deploy.{ ApplicationDescription, ExecutorState }
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.deploy.master.Master
import org.apache.spark.rpc._
import org.apache.spark.util.{ RpcUtils, ThreadUtils, Utils }

/**
 * Interface allowing applications to speak with a Spark deploy cluster. Takes a master URL,
 * 允许应用程序与Spark部署集群会话的接口,需要一个主节点URL
 * an app description, and a listener for cluster events, and calls back the listener when various
  * events occur.
 * 一个应用程序描述和一个集群事件的监听器,并在发生各种事件时回调监听器
 * 启动与调度
 * param masterUrls Each url should look like spark://host:port.
 * 每个URL都应该看起来像Spark:/主机:端口
 */

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
private[spark] class AppClient(
  rpcEnv: RpcEnv,
  masterUrls: Array[String],
  appDescription: ApplicationDescription,
  listener: AppClientListener,
  conf: SparkConf)
    extends Logging {

  private val masterRpcAddresses = masterUrls.map(RpcAddress.fromSparkURL(_))
  //注册超时20秒
  private val REGISTRATION_TIMEOUT_SECONDS = 20
  //注册重试次数3 
  private val REGISTRATION_RETRIES = 3
  //注册是否成功
  @volatile private var registered = false
  private var endpoint: RpcEndpointRef = null 
  private var appId: String = null
  /**
   * RpcEnv)是一个RpcEndpoints用于处理消息的环境onStart方法,所以ClientActor在正试启动前触发其onStart方法
   * ClientEndpoint 主要负责向Master注册当前的程序,是AppClient的内部成员
   */
  private class ClientEndpoint(override val rpcEnv: RpcEnv) extends ThreadSafeRpcEndpoint
      with Logging {

    private var master: Option[RpcEndpointRef] = None
    // To avoid calling listener.disconnected() multiple times
    //为了避免多次调用disconnected()
    private var alreadyDisconnected = false //已经断开连接
    //为了避免多次调用listener.dead()
    @volatile private var alreadyDead = false // To avoid calling listener.dead() multiple times
    @volatile private var registerMasterFutures: Array[JFuture[_]] = null
    @volatile private var registrationRetryTimer: JScheduledFuture[_] = null

    // A thread pool for registering with masters. Because registering with a master is a blocking
    //一个用于注册主节点的线程池,因为注册一个主节点是一个阻塞动作
    // action, this thread pool must be able to create "masterRpcAddresses.size" threads at the same
    //这个线程池必须能够创建“masterrpcaddresses.size的线程,
    // time so that we can register with all masters.
    //在同一时间,以便我们可以注册所有的主节点
    private val registerMasterThreadPool = ThreadUtils.newDaemonCachedThreadPool(
      "appclient-register-master-threadpool",
      //确保我们可以同时在同一时间注册所有的Master
      masterRpcAddresses.length // Make sure we can register with all masters at the same time
      )

    // A scheduled executor for scheduling the registration actions
    //一个计划的执行用于调度注册动作
    private val registrationRetryThread =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor("appclient-registration-retry-thread")

    override def onStart(): Unit = {
      try {
        //向所有的Master注册当前Application
        registerWithMaster(1)
      } catch {
        case e: Exception =>
          logWarning("Failed to connect to master", e)
          markDisconnected()
          stop()
      }
    }

    /**
     *  Register with all masters asynchronously and returns an array `Future`s for cancellation.
     *  向所有的Master注册当前Apllcation其中Master依然使用rpcEnv.setupEndpointRef方式获得
     */
    private def tryRegisterAllMasters(): Array[JFuture[_]] = {
      //yield 会把当前的元素记下来,保存在集合中,循环结束后将返回该集合
      //submit 返回Java Future[_]
      for (masterAddress <- masterRpcAddresses) yield {
        //submit有返回值
        registerMasterThreadPool.submit(new Runnable {
          override def run(): Unit = try {
            if (registered) {
              return
            }
            logInfo("Connecting to master " + masterAddress.toSparkURL + "...")
            //RpcEndpoint对于Actor,RpcEndpointRef对应ActorRef,RpcEnv即对应了ActorSystem
            //根据systemName、address、endpointName获取RpcEndpointRef
            val masterRef =
              rpcEnv.setupEndpointRef(Master.SYSTEM_NAME, masterAddress, Master.ENDPOINT_NAME)
            //向Master发送RegisterApplication消息,注册Application
            masterRef.send(RegisterApplication(appDescription, self))
          } catch {
            case ie: InterruptedException => // Cancelled
            case NonFatal(e)              => logWarning(s"Failed to connect to master $masterAddress", e)
          }
        })
      }

    }

    /**
     * Register with all masters asynchronously. It will call `registerWithMaster` every
     * 异步注册与所有的主节点,它将调用registerWithMaster,每REGISTRATION_TIMEOUT_SECONDS秒,直到超过REGISTRATION_RETRIES次
     * REGISTRATION_TIMEOUT_SECONDS seconds until exceeding REGISTRATION_RETRIES times.
     * Once we connect to a master successfully, all scheduling work and Futures will be cancelled.
     * 一旦我们成功地连接到一个主节点,所有的调度工作,未来将被取消.
     * 向所有的Master注册当前Application
     * nthRetry means this is the nth attempt to register with master.
     * 意味着尝试注册主节点次数
     */
    private def registerWithMaster(nthRetry: Int) {
      //向所有的Master注册当前Apllcation其中Master依然使用rpcEnv.setupEndpointRef方式获得
      registerMasterFutures = tryRegisterAllMasters()
      //schedule和scheduleAtFixedRate的区别在于,如果指定开始执行的时间在当前系统运行时间之前,
      // scheduleAtFixedRate会把已经过去的时间也作为周期执行,而schedule不会把过去的时间算上
      registrationRetryTimer = registrationRetryThread.scheduleAtFixedRate(new Runnable {
        override def run(): Unit = {
          Utils.tryOrExit {
            if (registered) { // 注册成功,那么取消所有的重试
              registerMasterFutures.foreach(_.cancel(true))
              registerMasterThreadPool.shutdownNow()

            } else if (nthRetry >= REGISTRATION_RETRIES) {
              //重试超过指定次数（3次）,则认为当前集群不可用,退出
              markDead("All masters are unresponsive! Giving up.")
            } else {
              registerMasterFutures.foreach(_.cancel(true))
              //递归 进行新一轮的重试
              registerWithMaster(nthRetry + 1)
            }
          }
        } //如果注册20秒内未收到成功的消息,那么再次重复注册
      }, REGISTRATION_TIMEOUT_SECONDS, REGISTRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)
    }

    /**
     * Send a message to the current master. If we have not yet registered successfully with any
     * master, the message will be dropped.
     * 发送一个消息到当前Master,如果不注册成功Master,则把消息删除.
     */
    private def sendToMaster(message: Any): Unit = {
      master match {
        case Some(masterRef) => masterRef.send(message)
        case None            => logWarning(s"Drop $message because has not yet connected to master")
      }
    }

    private def isPossibleMaster(remoteAddress: RpcAddress): Boolean = {
      masterRpcAddresses.contains(remoteAddress)
    }

    override def receive: PartialFunction[Any, Unit] = {
      //收到Master发送消息RegisteredAppliction中,
      /**
       * AppClient接收到Master发送RegisteredApplication消息后处理步骤
       * 1)更新appId,并标识当前Application已经注册到Master
       * 2)调用changeMaster更新ActiveMasterUrl,master,masterAddress等信息
       * 3)调用SparkDeploySchedulerBackend的Connected方法,
       *   更新appId,并且调用notifyContext方法标示Application注册完成
       */
      case RegisteredApplication(appId_, masterRef) =>
        // FIXME How to handle the following cases? 如何处理以下情况？
        // 1. A master receives multiple registrations and sends back multiple
        // RegisteredApplications due to an unstable network.
        //  由于不稳定的网络,主机接收多个注册并发回多个注册应用程序
        // 2. Receive multiple RegisteredApplication from different masters because the master is
        // changing.
        //由于Master主节点正在更改,从不同的Master接收多个RegisteredApplication
        appId = appId_
        registered = true
        master = Some(masterRef) //
        listener.connected(appId)
      /**
       * 在Application无论成功完成或者失败的情况下,都会通过这个消息通知AppClient停止
       */
      case ApplicationRemoved(message) =>
        markDead("Master removed our application: %s".format(message))
        stop()
      /**
       * 收到Master发送ExecutorAdded消息后,向Master发送ExecutorStateChanged消息
       * Master收到ExecutorStateChanged消息后将DriverEndpoint发送ExecutorUpdated消息,用于更新Driver上有关Executor
       */
      case ExecutorAdded(id: Int, workerId: String, hostPort: String, cores: Int, memory: Int) =>
        val fullId = appId + "/" + id
        logInfo("Executor added: %s on %s (%s) with %d cores".format(fullId, workerId, hostPort,
          cores))
        // FIXME if changing master and `ExecutorAdded` happen at the same time (the order is not
        // guaranteed), `ExecutorStateChanged` may be sent to a dead master.
        sendToMaster(ExecutorStateChanged(appId, id, ExecutorState.RUNNING, None, None))
        listener.executorAdded(fullId, workerId, hostPort, cores, memory)
      /**
       * 将Eexcutor的状态更新通知Appclient
       */
      case ExecutorUpdated(id, state, message, exitStatus) =>
        val fullId = appId + "/" + id
        val messageText = message.map(s => " (" + s + ")").getOrElse("")
        logInfo("Executor updated: %s is now %s%s".format(fullId, state, messageText))
        if (ExecutorState.isFinished(state)) {
          listener.executorRemoved(fullId, message.getOrElse(""), exitStatus)
        }
      /**
       * 在Master故障恢复后,它会通过该消息通知AppClient和worker当前Master已经更改
       */
      case MasterChanged(masterRef, masterWebUiUrl) =>
        logInfo("Master has changed, new master is at " + masterRef.address.toSparkURL)
        master = Some(masterRef)
        alreadyDisconnected = false
        masterRef.send(MasterChangeAcknowledged(appId))
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case StopAppClient =>
        markDead("Application has been stopped.")
        sendToMaster(UnregisterApplication(appId))
        context.reply(true)
        stop()

      case r: RequestExecutors =>
        master match {
          case Some(m) => context.reply(m.askWithRetry[Boolean](r))
          case None =>
            logWarning("Attempted to request executors before registering with Master.")
            context.reply(false)
        }

      case k: KillExecutors =>
        master match {
          case Some(m) => context.reply(m.askWithRetry[Boolean](k))
          case None =>
            logWarning("Attempted to kill executors before registering with Master.")
            context.reply(false)
        }
    }

    override def onDisconnected(address: RpcAddress): Unit = {
      if (master.exists(_.address == address)) {
        logWarning(s"Connection to $address failed; waiting for master to reconnect...")
        markDisconnected()
      }
    }

    override def onNetworkError(cause: Throwable, address: RpcAddress): Unit = {
      if (isPossibleMaster(address)) {
        logWarning(s"Could not connect to $address: $cause")
      }
    }

    /**
     * Notify the listener that we disconnected, if we hadn't already done so before.
     * 通知监听器断开连接,如果未断开连接.
     */
    def markDisconnected() {
      if (!alreadyDisconnected) {
        listener.disconnected()
        alreadyDisconnected = true
      }
    }

    def markDead(reason: String) {
      if (!alreadyDead) {
        listener.dead(reason)
        alreadyDead = true
      }
    }

    override def onStop(): Unit = {
      if (registrationRetryTimer != null) {
        registrationRetryTimer.cancel(true)
      }
      registrationRetryThread.shutdownNow()
      registerMasterFutures.foreach(_.cancel(true))
      registerMasterThreadPool.shutdownNow()
    }

  }
  /**
   * AppClient主要代表Application和Master通信,AppClient在启动时,会向Driver的ActorSystem注册ClientEndpoint
   * 由于向ActorSystem注册Actor时,ActorSystem会首先调用Actor的perStart方法,所以ClientEndpoint在正式启动前会
   * 触发其Start方法
   */
  def start() {
    // Just launch an rpcEndpoint; it will call back into the listener.
    //RpcEnv)是一个RpcEndpoints用于处理消息的环境onStart方法,所以ClientActor在正试启动前触发其onStart方法
    endpoint = rpcEnv.setupEndpoint("AppClient", new ClientEndpoint(rpcEnv))
  }

  def stop() {
    if (endpoint != null) {
      try {
        val timeout = RpcUtils.askRpcTimeout(conf)
        timeout.awaitResult(endpoint.ask[Boolean](StopAppClient))
      } catch {
        case e: TimeoutException =>
          logInfo("Stop request to Master timed out; it may already be shut down.")
      }
      endpoint = null
    }
  }

  /**
   * Request executors from the Master by specifying the total number desired,
   * 请求executors从Master通过指定期望的总数,包括现有的正在运行的执行者
   * including existing pending and running executors.
   *
   * @return whether the request is acknowledged. 是否被承认的请求
   */
  def requestTotalExecutors(requestedTotal: Int): Boolean = {
    if (endpoint != null && appId != null) {
      endpoint.askWithRetry[Boolean](RequestExecutors(appId, requestedTotal))
    } else {
      logWarning("Attempted to request executors before driver fully initialized.")
      false
    }
  }

  /**
   * Kill the given list of executors through the Master.
   * 通过Master杀死给定executor Id列表
   * @return whether the kill request is acknowledged.
   */
  def killExecutors(executorIds: Seq[String]): Boolean = {
    if (endpoint != null && appId != null) {
      endpoint.askWithRetry[Boolean](KillExecutors(appId, executorIds))
    } else {
      logWarning("Attempted to kill executors before driver fully initialized.")
      false
    }
  }

}
