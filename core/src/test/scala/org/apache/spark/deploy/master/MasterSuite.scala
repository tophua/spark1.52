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

package org.apache.spark.deploy.master

import java.util.Date

import scala.concurrent.duration._
import scala.io.Source
import scala.language.postfixOps

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.scalatest.{Matchers, PrivateMethodTester}
import org.scalatest.concurrent.Eventually
import other.supplier.{CustomPersistenceEngine, CustomRecoveryModeFactory}

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.deploy._
import org.apache.spark.rpc.RpcEnv

class MasterSuite extends SparkFunSuite with Matchers with Eventually with PrivateMethodTester {
  //可以使用自定义的恢复模式工厂
  test("can use a custom recovery mode factory") {
    val conf = new SparkConf(loadDefaults = false)
    conf.set("spark.deploy.recoveryMode", "CUSTOM")//恢复模式为自定义
    conf.set("spark.deploy.recoveryMode.factory",
      classOf[CustomRecoveryModeFactory].getCanonicalName)//使用恢复工厂
    conf.set("spark.master.rest.enabled", "false")
    //实例化的尝试
    val instantiationAttempts = CustomRecoveryModeFactory.instantiationAttempts
    //命令到持久化
    val commandToPersist = new Command(
      mainClass = "", 
      arguments = Nil, //参数
      environment = Map.empty,//环境
      classPathEntries = Nil,//类路径条目
      libraryPathEntries = Nil,//库路径条目
      javaOpts = Nil
    )

    val appToPersist = new ApplicationInfo(
      startTime = 0, //开始运行时间
      id = "test_app",
      desc = new ApplicationDescription(
        name = "",
        maxCores = None,//最大CPU内核数
        memoryPerExecutorMB = 0,//每个Executor内存数
        command = commandToPersist,
        appUiUrl = "",
        eventLogDir = None,//事件日志目录
        eventLogCodec = None,//事件日志的编解码器
        coresPerExecutor = None),//每个执行者的内核
      submitDate = new Date(),//提交时间
      driver = null,
      defaultCores = 0 //默认CPU内核数
    )

    val driverToPersist = new DriverInfo(
      startTime = 0,//开始运行时间
      id = "test_driver",
      desc = new DriverDescription(
        jarUrl = "",
        mem = 0,//内存数
        cores = 0,//CPU内核数
        //监督,管理
        supervise = false,
        command = commandToPersist
      ),
      //提交时间
      submitDate = new Date()
    )
   
    val workerToPersist = new WorkerInfo(
      id = "test_worker",//
      host = "127.0.0.1",//IP
      port = 10000,//端口
      cores = 0,//CPU内核数
      memory = 0,//内存数
      endpoint = null,
      webUiPort = 0,
      publicAddress = ""//对外地址
    )

    val (rpcEnv, _, _) =
      //运行RPC环境
      Master.startRpcEnvAndEndpoint("127.0.0.1", 0, 0, conf)

    try {
      //根据url同步获取RpcEndpointRef
      rpcEnv.setupEndpointRef(Master.SYSTEM_NAME, rpcEnv.address, Master.ENDPOINT_NAME)

      CustomPersistenceEngine.lastInstance.isDefined shouldBe true
      val persistenceEngine = CustomPersistenceEngine.lastInstance.get

      persistenceEngine.addApplication(appToPersist)
      persistenceEngine.addDriver(driverToPersist)
      persistenceEngine.addWorker(workerToPersist)
      //读取持久化数据
      val (apps, drivers, workers) = persistenceEngine.readPersistedData(rpcEnv)

      apps.map(_.id) should contain(appToPersist.id)
      drivers.map(_.id) should contain(driverToPersist.id)
      workers.map(_.id) should contain(workerToPersist.id)

    } finally {
      rpcEnv.shutdown()
      rpcEnv.awaitTermination()
    }

    CustomRecoveryModeFactory.instantiationAttempts should be > instantiationAttempts
  }
  //主节点/工作节点网站界面可用
  test("master/worker web ui available") {
    implicit val formats = org.json4s.DefaultFormats
    val conf = new SparkConf()
    val localCluster = new LocalSparkCluster(2, 2, 512, conf)
    localCluster.start()
    try {
      eventually(timeout(5 seconds), interval(100 milliseconds)) {
        val json = Source.fromURL(s"http://localhost:${localCluster.masterWebUIPort}/json")
          .getLines().mkString("\n")
        val JArray(workers) = (parse(json) \ "workers")
        workers.size should be (2)
        workers.foreach { workerSummaryJson =>
          val JString(workerWebUi) = workerSummaryJson \ "webuiaddress"
          val workerResponse = parse(Source.fromURL(s"${workerWebUi}/json")
            .getLines().mkString("\n"))
          (workerResponse \ "cores").extract[Int] should be (2)
        }
      }
    } finally {
      localCluster.stop()
    }
  }

  test("basic scheduling - spread out") {//基本调度-扩展
    basicScheduling(spreadOut = true)//如果spreadOut为false,worker节点调度直到使用它的所有的内核资源
  }

  test("basic scheduling - no spread out") {//基本调度-不扩展
    basicScheduling(spreadOut = false)//如果spreadOut为false,worker节点调度直到使用它的所有的内核资源
  }

  test("basic scheduling with more memory - spread out") {//基本调度与更多内存-扩展
    basicSchedulingWithMoreMemory(spreadOut = true)//如果spreadOut为false,worker节点调度直到使用它的所有的内核资源
  }

  test("basic scheduling with more memory - no spread out") {//基本调度与更多内存-不扩展
    basicSchedulingWithMoreMemory(spreadOut = false)//如果spreadOut为false,worker节点调度直到使用它的所有的内核资源
  }

  test("scheduling with max cores - spread out") {//最大内核的调度
    schedulingWithMaxCores(spreadOut = true)//如果spreadOut为false,worker节点调度直到使用它的所有的内核资源
  }

  test("scheduling with max cores - no spread out") {//最大内核的调度-没有扩展
    schedulingWithMaxCores(spreadOut = false)//如果spreadOut为false,worker节点调度直到使用它的所有的内核资源
  }

  test("scheduling with cores per executor - spread out") {//每执行器核心的调度
    schedulingWithCoresPerExecutor(spreadOut = true)//如果spreadOut为false,worker节点调度直到使用它的所有的内核资源
  }

  test("scheduling with cores per executor - no spread out") {//每执行器核心的调度-没有扩展
    schedulingWithCoresPerExecutor(spreadOut = false)//如果spreadOut为false,worker节点调度直到使用它的所有的内核资
  }

  test("scheduling with cores per executor AND max cores - spread out") {//每执行器内核和最大内核的调度
    schedulingWithCoresPerExecutorAndMaxCores(spreadOut = true)//如果spreadOut为false,worker节点调度直到使用它的所有的内核资源
  }

  test("scheduling with cores per executor AND max cores - no spread out") {//每执行器内核和最大内核的调度-没有扩展
    schedulingWithCoresPerExecutorAndMaxCores(spreadOut = false)//如果spreadOut为false,worker节点调度直到使用它的所有的内核资源
  }

  test("scheduling with executor limit - spread out") {//执行者限制的调度-扩展
    schedulingWithExecutorLimit(spreadOut = true)//如果spreadOut为false,worker节点调度直到使用它的所有的内核资源
  }

  test("scheduling with executor limit - no spread out") {//执行者限制的调度-没有扩展
    schedulingWithExecutorLimit(spreadOut = false)//如果spreadOut为false,worker节点调度直到使用它的所有的内核资源
  }

  test("scheduling with executor limit AND max cores - spread out") {//执行限制和最大内核的调度-扩展
    schedulingWithExecutorLimitAndMaxCores(spreadOut = true)//如果spreadOut为false,worker节点调度直到使用它的所有的内核资源
  }

  test("scheduling with executor limit AND max cores - no spread out") {//执行限制和最大内核的调度-没有扩展
    schedulingWithExecutorLimitAndMaxCores(spreadOut = false)//如果spreadOut为false,worker节点调度直到使用它的所有的内核资源
  }

  test("scheduling with executor limit AND cores per executor - spread out") {//执行者限制的调度和执行者的核心-扩展
    schedulingWithExecutorLimitAndCoresPerExecutor(spreadOut = true)//如果spreadOut为false,worker节点调度直到使用它的所有的内核资源
  }

  test("scheduling with executor limit AND cores per executor - no spread out") {//调度与执行者限制和核心每执行人-没有蔓延
    schedulingWithExecutorLimitAndCoresPerExecutor(spreadOut = false)//如果spreadOut为false,worker节点调度直到使用它的所有的内核资源
  }
  //调度与执行者限制和核心每执行人和最大内核-扩展
  test("scheduling with executor limit AND cores per executor AND max cores - spread out") {
    schedulingWithEverything(spreadOut = true)
  }
  //调度与执行者限制和核心每执行人和最大内核-没有扩展
  test("scheduling with executor limit AND cores per executor AND max cores - no spread out") {
    schedulingWithEverything(spreadOut = false)
  }
  //基本调度,是否展开
  private def basicScheduling(spreadOut: Boolean): Unit = {
    val master = makeMaster()
    val appInfo = makeAppInfo(1024)//应用程序1024
    val scheduledCores = scheduleExecutorsOnWorkers(master, appInfo, workerInfos, spreadOut)
    //返回为每个Worker上分配内核的数组
    assert(scheduledCores === Array(10, 10, 10))
  }
 //基本调度具有更多的内存
  private def basicSchedulingWithMoreMemory(spreadOut: Boolean): Unit = {
    val master = makeMaster()
    val appInfo = makeAppInfo(3072)//应用程序3072
     //返回为每个Worker上分配内核的数组
    val scheduledCores = scheduleExecutorsOnWorkers(master, appInfo, workerInfos, spreadOut)
     //如果spreadOut(false),worker节点调度直到使用它的所有的内核资源,否则,就转移到下一个worker节点均匀分配内核
    assert(scheduledCores === Array(10, 10, 10))
  }
  //调度最大内核数,参数
  private def schedulingWithMaxCores(spreadOut: Boolean): Unit = {
    val master = makeMaster()
    //最大内核数
    val appInfo1 = makeAppInfo(1024, maxCores = Some(8))
    //最大内核数
    val appInfo2 = makeAppInfo(1024, maxCores = Some(16))
    //返回为每个Worker上分配内核的数组
    val scheduledCores1 = scheduleExecutorsOnWorkers(master, appInfo1, workerInfos, spreadOut)
    val scheduledCores2 = scheduleExecutorsOnWorkers(master, appInfo2, workerInfos, spreadOut)
    //如果spreadOut(false),worker节点调度直到使用它的所有的内核资源,否则,就转移到下一个worker节点均匀分配内核
    if (spreadOut) {
      //如果spreadOut为true,worker节点的内核均匀分配
      assert(scheduledCores1 === Array(3, 3, 2))
      assert(scheduledCores2 === Array(6, 5, 5))
    } else {
      //如果spreadOut为false,worker节点调度直到使用它的所有的内核资源
      assert(scheduledCores1 === Array(8, 0, 0))
      assert(scheduledCores2 === Array(10, 6, 0))
    }
  }
  //每执行器内核调度
  private def schedulingWithCoresPerExecutor(spreadOut: Boolean): Unit = {
    val master = makeMaster()
    //每个Executor内存数,coresPerExecutor每次分配Executor使用内核数,默认为1
    val appInfo1 = makeAppInfo(1024, coresPerExecutor = Some(2))//每个执行使用内核数
    val appInfo2 = makeAppInfo(256, coresPerExecutor = Some(2))//
    val appInfo3 = makeAppInfo(256, coresPerExecutor = Some(3))//3个Worker每执行者分配3个内核
    //返回为每个Worker上分配内核的数组
    val scheduledCores1 = scheduleExecutorsOnWorkers(master, appInfo1, workerInfos, spreadOut)
    val scheduledCores2 = scheduleExecutorsOnWorkers(master, appInfo2, workerInfos, spreadOut)
    val scheduledCores3 = scheduleExecutorsOnWorkers(master, appInfo3, workerInfos, spreadOut)
    //最大内存4096
    assert(scheduledCores1 === Array(8, 8, 8)) // 4 * 2 because of memory limits 由于内存限制
    assert(scheduledCores2 === Array(10, 10, 10)) // 5 * 2
    assert(scheduledCores3 === Array(9, 9, 9)) // 3 * 3
  }

  // Sorry for the long method name!
  //每执行器内核调度和最 大内核调度
  private def schedulingWithCoresPerExecutorAndMaxCores(spreadOut: Boolean): Unit = {
    val master = makeMaster()
    val appInfo1 = makeAppInfo(256, coresPerExecutor = Some(2), maxCores = Some(4))//应用最大内核总数
    val appInfo2 = makeAppInfo(256, coresPerExecutor = Some(2), maxCores = Some(20))//应用最大内核总数
    val appInfo3 = makeAppInfo(256, coresPerExecutor = Some(3), maxCores = Some(20))//应用最大内核总数
    //返回为每个Worker上分配内核的数组
    val scheduledCores1 = scheduleExecutorsOnWorkers(master, appInfo1, workerInfos, spreadOut)
    val scheduledCores2 = scheduleExecutorsOnWorkers(master, appInfo2, workerInfos, spreadOut)
    val scheduledCores3 = scheduleExecutorsOnWorkers(master, appInfo3, workerInfos, spreadOut)
    //如果spreadOut(false),worker节点调度直到使用它的所有的内核资源,否则,就转移到下一个worker节点均匀分配内核
    if (spreadOut) {
      assert(scheduledCores1 === Array(2, 2, 0))//应用最大内核总数
      assert(scheduledCores2 === Array(8, 6, 6))
      assert(scheduledCores3 === Array(6, 6, 6))
    } else {
      assert(scheduledCores1 === Array(4, 0, 0))//应用最大内核总数
      assert(scheduledCores2 === Array(10, 10, 0))//应用最大内核总数
      assert(scheduledCores3 === Array(9, 9, 0))//应用最大内核总数
    }
  }
  //调度限制Executor
  private def schedulingWithExecutorLimit(spreadOut: Boolean): Unit = {
    val master = makeMaster()
    val appInfo = makeAppInfo(256)
    appInfo.executorLimit = 0//限制executor数
   //返回为每个Worker上分配内核的数组
    val scheduledCores1 = scheduleExecutorsOnWorkers(master, appInfo, workerInfos, spreadOut)
    appInfo.executorLimit = 2//限制executor数
    val scheduledCores2 = scheduleExecutorsOnWorkers(master, appInfo, workerInfos, spreadOut)
    appInfo.executorLimit = 5//限制executor数
    val scheduledCores3 = scheduleExecutorsOnWorkers(master, appInfo, workerInfos, spreadOut)
    assert(scheduledCores1 === Array(0, 0, 0))
    assert(scheduledCores2 === Array(10, 10, 0))
    assert(scheduledCores3 === Array(10, 10, 10))
  }
  //调度限制Executor和最大内核数
  private def schedulingWithExecutorLimitAndMaxCores(spreadOut: Boolean): Unit = {
    val master = makeMaster()
    val appInfo = makeAppInfo(256, maxCores = Some(16))//最大内核数
    appInfo.executorLimit = 0
    val scheduledCores1 = scheduleExecutorsOnWorkers(master, appInfo, workerInfos, spreadOut)
    appInfo.executorLimit = 2//限制executor数
    val scheduledCores2 = scheduleExecutorsOnWorkers(master, appInfo, workerInfos, spreadOut)
    appInfo.executorLimit = 5//限制executor数
    val scheduledCores3 = scheduleExecutorsOnWorkers(master, appInfo, workerInfos, spreadOut)
    assert(scheduledCores1 === Array(0, 0, 0))
    //如果没有散开(false),我们应该保持对这个工作节点调度执行直到使用它的所有资源,否则,就转移到下一个工作节点
    if (spreadOut) {
      assert(scheduledCores2 === Array(8, 8, 0))
      assert(scheduledCores3 === Array(6, 5, 5))
    } else {
      assert(scheduledCores2 === Array(10, 6, 0))
      assert(scheduledCores3 === Array(10, 6, 0))
    }
  }
  //执行限制最大内核调度
  private def schedulingWithExecutorLimitAndCoresPerExecutor(spreadOut: Boolean): Unit = {
    val master = makeMaster()
    val appInfo = makeAppInfo(256, coresPerExecutor = Some(4))
    appInfo.executorLimit = 0
    val scheduledCores1 = scheduleExecutorsOnWorkers(master, appInfo, workerInfos, spreadOut)
    appInfo.executorLimit = 2
    val scheduledCores2 = scheduleExecutorsOnWorkers(master, appInfo, workerInfos, spreadOut)
    appInfo.executorLimit = 5
    val scheduledCores3 = scheduleExecutorsOnWorkers(master, appInfo, workerInfos, spreadOut)
    assert(scheduledCores1 === Array(0, 0, 0))
    //如果没有散开(false),我们应该保持对这个工作节点调度执行直到使用它的所有资源,否则,就转移到下一个工作节点
    if (spreadOut) { 
      assert(scheduledCores2 === Array(4, 4, 0))
    } else {
      assert(scheduledCores2 === Array(8, 0, 0))
    }
    assert(scheduledCores3 === Array(8, 8, 4))
  }

  // Everything being: executor limit + cores per executor + max cores
  //任务开始:执行者限制+每执行者内核数+最大内核
  private def schedulingWithEverything(spreadOut: Boolean): Unit = {
    val master = makeMaster()
    //每个Executors内存数
    val appInfo = makeAppInfo(256, coresPerExecutor = Some(4), maxCores = Some(18))
    appInfo.executorLimit = 0
    val scheduledCores1 = scheduleExecutorsOnWorkers(master, appInfo, workerInfos, spreadOut)
    appInfo.executorLimit = 2
    val scheduledCores2 = scheduleExecutorsOnWorkers(master, appInfo, workerInfos, spreadOut)
    appInfo.executorLimit = 5
    val scheduledCores3 = scheduleExecutorsOnWorkers(master, appInfo, workerInfos, spreadOut)
    assert(scheduledCores1 === Array(0, 0, 0))
    //如果没有散开(false),我们应该保持对这个工作节点调度执行直到使用它的所有资源,否则,就转移到下一个工作节点
    if (spreadOut) {
      assert(scheduledCores2 === Array(4, 4, 0))
      assert(scheduledCores3 === Array(8, 4, 4))
    } else {
      assert(scheduledCores2 === Array(8, 0, 0))
      assert(scheduledCores3 === Array(8, 8, 0))
    }
  }

  // ==========================================
  // | Utility methods and fields for testing |
  // | 用于测试的实用方法和字段                                            |
  // ==========================================
  //返回为每个Worker上分配的cores的数组
  private val _scheduleExecutorsOnWorkers = PrivateMethod[Array[Int]]('scheduleExecutorsOnWorkers)
  private val workerInfo = makeWorkerInfo(4096, 10)
  //三个Worker数组,每个Worker内存4096,内核10
  private val workerInfos = Array(workerInfo, workerInfo, workerInfo)
  //构造Master
  private def makeMaster(conf: SparkConf = new SparkConf): Master = {
    val securityMgr = new SecurityManager(conf)
    val rpcEnv = RpcEnv.create(Master.SYSTEM_NAME, "localhost", 0, conf, securityMgr)
    val master = new Master(rpcEnv, rpcEnv.address, 0, securityMgr, conf)
    master
  }
  //构造ApplicationInfo
  private def makeAppInfo(
      memoryPerExecutorMb: Int,//每执行者的内存(Mb)
      coresPerExecutor: Option[Int] = None,//每个执行者的内核
      maxCores: Option[Int] = None): ApplicationInfo = {//最大内核
    val desc = new ApplicationDescription(
      "test", maxCores, memoryPerExecutorMb, null, "", None, None, coresPerExecutor)
    val appId = System.currentTimeMillis.toString
    new ApplicationInfo(0, appId, desc, new Date, null, Int.MaxValue)
  }
  //构造WorkerInfo,memoryMb内存数,cores 内核数
  private def makeWorkerInfo(memoryMb: Int, cores: Int): WorkerInfo = {
    val workerId = System.currentTimeMillis.toString
    /**
     *参数:
     * workerId:Id标识    host:Worker的主机  100:Worker的端口,cores:Worker节点的CPU内核,
     * memoryMb:Worker节点的内存,101:webUi端口,publicAddress:发布地址名称
     */
    new WorkerInfo(workerId, "host", 100, cores, memoryMb, null, 101, "address")
  }
 //在工作节点调用执行者,返回为每个Worker上分配的cores的数组
  private def scheduleExecutorsOnWorkers(
      master: Master,
      appInfo: ApplicationInfo,
      workerInfos: Array[WorkerInfo],
      spreadOut: Boolean): Array[Int] = {
    //如果没有散开(false),我们应该保持对这个工作节点调度执行直到使用它的所有资源,否则,就转移到下一个工作节点
    master.invokePrivate(_scheduleExecutorsOnWorkers(appInfo, workerInfos, spreadOut))
  }

}
