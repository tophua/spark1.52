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

import java.util.concurrent.Semaphore

import org.apache.spark.rpc.RpcAddress
import org.apache.spark.{Logging, SparkConf, SparkContext, SparkEnv}
import org.apache.spark.deploy.{ApplicationDescription, Command}
import org.apache.spark.deploy.client.{AppClient, AppClientListener}
import org.apache.spark.scheduler.{ExecutorExited, ExecutorLossReason, SlaveLost, TaskSchedulerImpl}
import org.apache.spark.util.Utils
/**
 * 启动过程
 */
private[spark] class SparkDeploySchedulerBackend(
    scheduler: TaskSchedulerImpl,
    sc: SparkContext,
    masters: Array[String])
  extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv)
  with AppClientListener
  with Logging {

  private var client: AppClient = null
  private var stopping = false

  @volatile var shutdownCallback: SparkDeploySchedulerBackend => Unit = _
  @volatile private var appId: String = _

  private val registrationBarrier = new Semaphore(0)
//当运行在一个独立部署集群上或者是一个粗粒度共享模式的Mesos集群上的时候,最多可以请求多少个CPU核心。默认是所有的都能用
  private val maxCores = conf.getOption("spark.cores.max").map(_.toInt)
  private val totalExpectedCores = maxCores.getOrElse(0)
/**
 * 执行过程
 * 1)调用父类CoarseGrainedSchedulerBackend的start方法,注册DriverActor
 * 2)进行一些参数,java选项,类路径的设置,这些配置被封装为Command,然后由ApplicationDescription持由
 *   ApplicationDescription作为AppClient的构造参数,在AppClient启动时传递给Worker,
 *   worker最终利用ApplicationDescription里封装的Command启动Executor
 * 3)启动AppClient
 *   
 */
  override def start() {
    super.start()

    // The endpoint for executors to talk to us
    //执行者与我们对话的端点
    val driverUrl = rpcEnv.uriOf(SparkEnv.driverActorSystemName,
    //运行driver的主机名或 IP 地址
      RpcAddress(sc.conf.get("spark.driver.host"), sc.conf.get("spark.driver.port").toInt),
      CoarseGrainedSchedulerBackend.ENDPOINT_NAME)
    //现在executor还没有申请,因此关于executor的所有信息都是未知的。
    //这些参数将会在org.apache.spark.deploy.worker.ExecutorRunner启动ExecutorBackend的时候替换这些参数
    val args = Seq(
      "--driver-url", driverUrl,
      "--executor-id", "{{EXECUTOR_ID}}",
      "--hostname", "{{HOSTNAME}}",
      "--cores", "{{CORES}}",
      "--app-id", "{{APP_ID}}",
      "--worker-url", "{{WORKER_URL}}")
      //要传递给executor的额外JVM 选项,注意不能使用它来设置Spark属性或堆大小设置
    val extraJavaOpts = sc.conf.getOption("spark.executor.extraJavaOptions")
      .map(Utils.splitCommandString).getOrElse(Seq.empty)
      //追加到executor类路径中的附加类路径,兼容旧版本的Spark
    val classPathEntries = sc.conf.getOption("spark.executor.extraClassPath")
      .map(_.split(java.io.File.pathSeparator).toSeq).getOrElse(Nil)
      //启动executor JVM 时要用到的特殊库路径
    val libraryPathEntries = sc.conf.getOption("spark.executor.extraLibraryPath")
      .map(_.split(java.io.File.pathSeparator).toSeq).getOrElse(Nil)

    // When testing, expose the parent class path to the child. This is processed by
    // compute-classpath.{cmd,sh} and makes all needed jars available to child processes
    // when the assembly is built with the "*-provided" profiles enabled.
    //当测试时,将父类路径暴露给孩子,这是由compute-classpath {cmd，sh}处理的，并且在使用“*
    // -provided”配置文件启用时构建程序集时，使所有需要的jar可用于子进程
    val testingClassPath =
      if (sys.props.contains("spark.testing")) {
        //System.getenv()和System.getProperties()的区别
        //System.getenv() 返回系统环境变量值 设置系统环境变量：当前登录用户主目录下的".bashrc"文件中可以设置系统环境变量
        //System.getProperties() 返回Java进程变量值 通过命令行参数的"-D"选项
        sys.props("java.class.path").split(java.io.File.pathSeparator).toSeq
      } else {
        Nil
      }

    // Start executors with a few necessary configs for registering with the scheduler
    //启动执行程序与一些必要的配置注册调度程序
    val sparkJavaOpts = Utils.sparkJavaOpts(conf, SparkConf.isExecutorStartupConf)
    val javaOpts = sparkJavaOpts ++ extraJavaOpts
    val command = Command("org.apache.spark.executor.CoarseGrainedExecutorBackend",
      args, sc.executorEnvs, classPathEntries ++ testingClassPath, libraryPathEntries, javaOpts)
    val appUIAddress = sc.ui.map(_.appUIAddress).getOrElse("")
    //获得每个executor最多的CPU core数目
    val coresPerExecutor = conf.getOption("spark.executor.cores").map(_.toInt)
    val appDesc = new ApplicationDescription(sc.appName, maxCores, sc.executorMemory,
      command, appUIAddress, sc.eventLogDir, sc.eventLogCodec, coresPerExecutor)
    /**主要代表Application和Master通信,在AppClient启动时,会向Driver的**/
    client = new AppClient(sc.env.rpcEnv, masters, appDesc, this, conf)
    client.start()
    waitForRegistration()
  }

  override def stop() {
    stopping = true
    super.stop()
    client.stop()

    val callback = shutdownCallback
    if (callback != null) {
      callback(this)
    }
  }
/**
 * AppClient调用SparkDeploySchedulerBackend的Connected方法,
        更新appId,并且调用notifyContext方法标示Application注册完成
 */
  override def connected(appId: String) {
    logInfo("Connected to Spark cluster with app ID " + appId)
    this.appId = appId
    notifyContext()
  }

  override def disconnected() {
    notifyContext()
    if (!stopping) {
      logWarning("Disconnected from Spark cluster! Waiting for reconnection...")
    }
  }

  override def dead(reason: String) {
    notifyContext()
    if (!stopping) {
      logError("Application has been killed. Reason: " + reason)
      try {
        scheduler.error(reason)
      } finally {
        // Ensure the application terminates, as we can no longer run jobs.
        //确保应用程序终止,因为我们无法再运行作业
        sc.stop()
      }
    }
  }

  override def executorAdded(fullId: String, workerId: String, hostPort: String, cores: Int,
    memory: Int) {
    logInfo("Granted executor ID %s on hostPort %s with %d cores, %s RAM".format(
      fullId, hostPort, cores, Utils.megabytesToString(memory)))
  }

  override def executorRemoved(fullId: String, message: String, exitStatus: Option[Int]) {
    val reason: ExecutorLossReason = exitStatus match {
      case Some(code) => ExecutorExited(code)
      case None => SlaveLost(message)
    }
    logInfo("Executor %s removed: %s".format(fullId, message))
    removeExecutor(fullId.split("/")(1), reason.toString)
  }

  override def sufficientResourcesRegistered(): Boolean = {
    totalCoreCount.get() >= totalExpectedCores * minRegisteredRatio
  }

  override def applicationId(): String =
    Option(appId).getOrElse {
      logWarning("Application ID is not initialized yet.")
      super.applicationId
    }

  /**
   * Request executors from the Master by specifying the total number desired,
   * including existing pending and running executors.
    * 通过指定所需的总数来请求Master执行器,包括现有的正在运行和正在运行的执行器
   *
   * @return whether the request is acknowledged.请求是否被确认
   */
  protected override def doRequestTotalExecutors(requestedTotal: Int): Boolean = {
    Option(client) match {
      case Some(c) => c.requestTotalExecutors(requestedTotal)
      case None =>
        logWarning("Attempted to request executors before driver fully initialized.")
        false
    }
  }

  /**
   * Kill the given list of executors through the Master.
    * 通过Master杀死给定的执行者名单
   * @return whether the kill request is acknowledged.是否确认杀死请求
   */
  protected override def doKillExecutors(executorIds: Seq[String]): Boolean = {
    Option(client) match {
      case Some(c) => c.killExecutors(executorIds)
      case None =>
        logWarning("Attempted to kill executors before driver fully initialized.")
        false
    }
  }

  private def waitForRegistration() = {
    registrationBarrier.acquire()
  }

  private def notifyContext() = {
    registrationBarrier.release()
  }

}
