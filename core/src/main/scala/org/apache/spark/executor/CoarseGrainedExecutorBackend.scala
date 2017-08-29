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

package org.apache.spark.executor

import java.net.URL
import java.nio.ByteBuffer

import org.apache.hadoop.conf.Configuration

import scala.collection.mutable
import scala.util.{Failure, Success}

import org.apache.spark.rpc._
import org.apache.spark._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.worker.WorkerWatcher
import org.apache.spark.scheduler.TaskDescription
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.util.{ThreadUtils, SignalLogger, Utils}

/**
  * 粗颗粒
  * @param rpcEnv
  * @param driverUrl
  * @param executorId
  * @param hostPort
  * @param cores
  * @param userClassPath
  * @param env
  */
private[spark] class CoarseGrainedExecutorBackend(
    override val rpcEnv: RpcEnv,
    driverUrl: String,
    executorId: String,
    hostPort: String,
    cores: Int,
    userClassPath: Seq[URL],
    env: SparkEnv)
  extends ThreadSafeRpcEndpoint with ExecutorBackend with Logging {

  Utils.checkHostPort(hostPort, "Expected hostport")

  var executor: Executor = null
  @volatile var driver: Option[RpcEndpointRef] = None

  // If this CoarseGrainedExecutorBackend is changed to support multiple threads, then this may need
  // to be changed so that we don't share the serializer instance across threads
  //如果这个CoarseGrainedExecutorBackend更改为支持多个线程,那么这可能需要更改,这样我们不会跨线程共享序列化器实例
  private[this] val ser: SerializerInstance = env.closureSerializer.newInstance()

  override def onStart() {
    logInfo("Connecting to driver: " + driverUrl)
    rpcEnv.asyncSetupEndpointRefByURI(driverUrl).flatMap { ref =>
      // This is a very fast action so we can use "ThreadUtils.sameThread"
      //这是一个很快的动作,所以我们可以使用“ThreadUtils.sameThread”
      driver = Some(ref)
      /**
       * 主要向DriverAction发送RegisterExecutor消息,DriverActor接到RegisterExecutor消息后处理步骤:
       * 1)向CoarseGrainedExecutorBackend发送RegisteredExecutor消息,CoarseGrainedExecutorBackend收到RegisteredExecutor消息后
       *   创建Executor
       * 2)更新Executor所在地址与Executor的映射关系(addressToExecutorId),
       *   Driver获取的共CPU核数(totalCoreCount),注册到Driver的Exceuctor总数(totalRegisteredExecutors)等信息
       * 3)创建ExecutorData并且注册到executorDataMap中
       * 4)调用makeOffers方法执行任务  
       * 向DriverEndpoint发送RegisterExecutor消息,DriverEndpoint收到消息后,先向CoarseGrainedExecutorBackend发送
       * RegisteredExecutor消息,
       * ref也就相当于Driver
       */
      ref.ask[RegisteredExecutor.type](
        RegisterExecutor(executorId, self, hostPort, cores, extractLogUrls))
	  //onComplete,onSuccess,onFailure三个回调函数来异步执行Future任务
    }(ThreadUtils.sameThread).onComplete {
      // This is a very fast action so we can use "ThreadUtils.sameThread"
      //这是一个很快的动作，所以我们可以使用“ThreadUtils.sameThread”
      case Success(msg) => Utils.tryLogNonFatalError {
        Option(self).foreach(_.send(msg)) // msg must be RegisteredExecutor
      }
      case Failure(e) => {
        logError(s"Cannot register with driver: $driverUrl", e)
        System.exit(1)
      }
    }(ThreadUtils.sameThread)
  }

  def extractLogUrls: Map[String, String] = {
    val prefix = "SPARK_LOG_URL_"
    //System.getenv()和System.getProperties()的区别
    //System.getenv() 返回系统环境变量值 设置系统环境变量：当前登录用户主目录下的".bashrc"文件中可以设置系统环境变量
    //System.getProperties() 返回Java进程变量值 通过命令行参数的"-D"选项
    sys.env.filterKeys(_.startsWith(prefix))
      .map(e => (e._1.substring(prefix.length).toLowerCase, e._2))
  }
//定义偏函数是具有类型PartialFunction[-A,+B]的一种函数。A是其接受的函数类型,B是其返回的结果类型
  override def receive: PartialFunction[Any, Unit] = {
      /**
       * 主要向DriverAction发送RegisterExecutor消息,DriverActor接到RegisterExecutor消息后处理步骤:
       * 1)向CoarseGrainedExecutorBackend发送RegisteredExecutor消息,CoarseGrainedExecutorBackend收到RegisteredExecutor消息后
       *   创建Executor
       * 2)更新Executor所在地址与Executor的映射关系(addressToExecutorId),
       *   Driver获取的共CPU核数(totalCoreCount),注册到Driver的Exceuctor总数(totalRegisteredExecutors)等信息
       * 3)创建ExecutorData并且注册到executorDataMap中
       * 4)调用makeOffers方法执行任务   
       */
    case RegisteredExecutor =>
      logInfo("Successfully registered with driver")
      val (hostname, _) = Utils.parseHostPort(hostPort)
      executor = new Executor(executorId, hostname, env, userClassPath, isLocal = false)

    case RegisterExecutorFailed(message) =>
      logError("Slave registration failed: " + message)
      System.exit(1)
    /**
     * launchTasks处理步骤
     * 1)序列化TaskDescription
     * 2)取出TaskDescription所描述任务分配的ExecutorData信息,并且将ExecutorData描述的空闲CPU核数减去
     *   任务占用的核数
     * 3)向Executor所在的CoarseGrainedExecutorBackend进程中发送LaunchTask消息
     * 4)CoarseGrainedExecutorBackend收到LaunchTask消息后,反序列化TaskDescription,使用TaskDescription
     *   的taskId,name,serializedTask调用Executor的方法LaunchTask
     */
    case LaunchTask(data) =>
      if (executor == null) {
        //如果不存在Executor则会报错,退出系统  
        logError("Received LaunchTask command but executor was null")
        System.exit(1)
      } else {
        //反序列化Task,得到TaskDescription信息  
        val taskDesc = ser.deserialize[TaskDescription](data.value)
        logInfo("Got assigned task " + taskDesc.taskId)
        //调用executor#launchTask在executor上加载任务  
        executor.launchTask(this, taskId = taskDesc.taskId, attemptNumber = taskDesc.attemptNumber,
          taskDesc.name, taskDesc.serializedTask)
      }

    case KillTask(taskId, _, interruptThread) =>
      if (executor == null) {
        logError("Received KillTask command but executor was null")
        System.exit(1)
      } else {
        executor.killTask(taskId, interruptThread)
      }

    case StopExecutor =>
      logInfo("Driver commanded a shutdown")
      executor.stop()
      stop()
      rpcEnv.shutdown()
  }

      override def onDisconnected(remoteAddress: RpcAddress): Unit = {
        if (driver.exists(_.address == remoteAddress)) {
          logError(s"Driver $remoteAddress disassociated! Shutting down.")
      System.exit(1)
    } else {
      logWarning(s"An unknown ($remoteAddress) driver disconnected.")
    }
  }
    //通过AKKA向Driver汇报本次Task的状态
  override def statusUpdate(taskId: Long, state: TaskState, data: ByteBuffer) {
    val msg = StatusUpdate(executorId, taskId, state, data)
    driver match {
      case Some(driverRef) => driverRef.send(msg)
      case None => logWarning(s"Drop $msg because has not yet connected to driver")
    }
  }
}
/**
 * 粗颗粒
 */
private[spark] object CoarseGrainedExecutorBackend extends Logging {
  /**
   * 处理过程
   * 1)给Driver发送RetrieveSparkProps消息获取Spark属性
   * 2)使用获取的Spark属性创建自身需要的RpcEnv
   * 3)注册CoarseGrainedExecutorBackend到RpcEnv中
   * 4)注册WorkerWatcher中到RpcEnv,WorkerWatcher用于Worker向Master发送心跳以证明Worker运行正常
   */
  private def run(
      driverUrl: String,
      executorId: String,
      hostname: String,
      cores: Int,
      appId: String,
      workerUrl: Option[String],
      userClassPath: Seq[URL]) {

    SignalLogger.register(log)

    SparkHadoopUtil.get.runAsSparkUser { () =>
      // Debug code
      Utils.checkHost(hostname)

      // Bootstrap to fetch the driver's Spark properties.
      //Bootstrap来获取驱动的Spark属性
      val executorConf = new SparkConf
      val port = executorConf.getInt("spark.executor.port", 0)
      val fetcher = RpcEnv.create(
        "driverPropsFetcher",
        hostname,
        port,
        executorConf,
        new SecurityManager(executorConf))
      val driver = fetcher.setupEndpointRefByURI(driverUrl)
      //向driver发送RetrieveSparkProps消息 
      val props = driver.askWithRetry[Seq[(String, String)]](RetrieveSparkProps) ++
        Seq[(String, String)](("spark.app.id", appId))
      //CoarseGrainedExecutorBackend启动driverPropsFetcher注册,触发Start方法
      fetcher.shutdown()

      // Create SparkEnv using properties we fetched from the driver.
      //使用从驱动程序获取的属性创建SparkEnv
      val driverConf = new SparkConf()
      for ((key, value) <- props) {
        // this is required for SSL in standalone mode
        //这在独立模式下是必需的
        if (SparkConf.isExecutorStartupConf(key)) {
          driverConf.setIfMissing(key, value)
        } else {
          driverConf.set(key, value)
        }
      }
      if (driverConf.contains("spark.yarn.credentials.file")) {
        logInfo("Will periodically update credentials from: " +
          driverConf.get("spark.yarn.credentials.file"))
        SparkHadoopUtil.get.startExecutorDelegationTokenRenewer(driverConf)
      }

      val env = SparkEnv.createExecutorEnv(
        driverConf, executorId, hostname, port, cores, isLocal = false)

      // SparkEnv sets spark.driver.port so it shouldn't be 0 anymore.
      //0随机 driver侦听的端口
      val boundPort = env.conf.getInt("spark.executor.port", 0)
      assert(boundPort != 0)

      // Start the CoarseGrainedExecutorBackend endpoint.
      //启动CoarseGrainedExecutorBackend端点
      val sparkHostPort = hostname + ":" + boundPort
      env.rpcEnv.setupEndpoint("Executor", new CoarseGrainedExecutorBackend(
        env.rpcEnv, driverUrl, executorId, sparkHostPort, cores, userClassPath, env))
      workerUrl.foreach { url =>
        env.rpcEnv.setupEndpoint("WorkerWatcher", new WorkerWatcher(env.rpcEnv, url))
      }
      env.rpcEnv.awaitTermination()
      SparkHadoopUtil.get.stopExecutorDelegationTokenRenewer()
    }
  }

  def main(args: Array[String]) {
    var driverUrl: String = null
    var executorId: String = null
    var hostname: String = null
    var cores: Int = 0
    var appId: String = null
    var workerUrl: Option[String] = None
    val userClassPath = new mutable.ListBuffer[URL]()

    var argv = args.toList
    while (!argv.isEmpty) {
      argv match {
        case ("--driver-url") :: value :: tail =>
          driverUrl = value
          argv = tail
        case ("--executor-id") :: value :: tail =>
          executorId = value
          argv = tail
        case ("--hostname") :: value :: tail =>
          hostname = value
          argv = tail
        case ("--cores") :: value :: tail =>
          cores = value.toInt
          argv = tail
        case ("--app-id") :: value :: tail =>
          appId = value
          argv = tail
        case ("--worker-url") :: value :: tail =>
          // Worker url is used in spark standalone mode to enforce fate-sharing with worker
          //Worker URL用于spark独立模式,以强制与worker进行命运共享
          workerUrl = Some(value)
          argv = tail
        case ("--user-class-path") :: value :: tail =>
          userClassPath += new URL(value)
          argv = tail
        case Nil =>
        case tail =>
          // scalastyle:off println
          System.err.println(s"Unrecognized options: ${tail.mkString(" ")}")
          // scalastyle:on println
          printUsageAndExit()
      }
    }

    if (driverUrl == null || executorId == null || hostname == null || cores <= 0 ||
      appId == null) {
      printUsageAndExit()
    }
  /**
   * 处理过程
   * 1)给Driver发送RetrieveSparkProps消息获取Spark属性
   * 2)使用获取的Spark属性创建自身需要的RpcEnv
   * 3)注册CoarseGrainedExecutorBackend到RpcEnv中
   * 4)注册WorkerWatcher中到RpcEnv,WorkerWatcher用于Worker向Master发送心跳以证明Worker运行正常
   */

    run(driverUrl, executorId, hostname, cores, appId, workerUrl, userClassPath)
  }

  private def printUsageAndExit() = {
    // scalastyle:off println
    System.err.println(
      """
      |"Usage: CoarseGrainedExecutorBackend [options]
      |
      | Options are:
      |   --driver-url <driverUrl>
      |   --executor-id <executorId>
      |   --hostname <hostname>
      |   --cores <cores>
      |   --app-id <appid>
      |   --worker-url <workerUrl>
      |   --user-class-path <url>
      |""".stripMargin)
    // scalastyle:on println
    System.exit(1)
  }

}
