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

package org.apache.spark.deploy

import scala.concurrent.duration._

import org.mockito.Mockito.{mock, when}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually._

import org.apache.spark._
import org.apache.spark.deploy.DeployMessages.{MasterStateResponse, RequestMasterState}
import org.apache.spark.deploy.master.ApplicationInfo
import org.apache.spark.deploy.master.Master
import org.apache.spark.deploy.worker.Worker
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef, RpcEnv}
import org.apache.spark.scheduler.cluster._
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.RegisterExecutor

/**
 * End-to-end tests for dynamic allocation in standalone mode.
 * 测试独立模式下端到端的动态分配
 */
class StandaloneDynamicAllocationSuite
  extends SparkFunSuite
  with LocalSparkContext
  with BeforeAndAfterAll {

  private val numWorkers = 2
  private val conf = new SparkConf()
  private val securityManager = new SecurityManager(conf)

  private var masterRpcEnv: RpcEnv = null
  private var workerRpcEnvs: Seq[RpcEnv] = null
  private var master: Master = null
  private var workers: Seq[Worker] = null

  /**
   * Start the local cluster.
   * 启动本地群集
   * Note: local-cluster mode is insufficient because we want a reference to the Master.
   * 注意:本地群集模式是不够的,因为我们引用主节点
   */
  override def beforeAll(): Unit = {
    super.beforeAll()
    masterRpcEnv = RpcEnv.create(Master.SYSTEM_NAME, "localhost", 0, conf, securityManager)
    workerRpcEnvs = (0 until numWorkers).map { i =>
      RpcEnv.create(Worker.SYSTEM_NAME + i, "localhost", 0, conf, securityManager)
    }
    master = makeMaster()
    workers = makeWorkers(10, 2048)
    // Wait until all workers register with master successfully
    //等到所有的Woker都成功注册到主节点
    eventually(timeout(60.seconds), interval(10.millis)) {
      assert(getMasterState.workers.size === numWorkers)
    }
  }

  override def afterAll(): Unit = {
    masterRpcEnv.shutdown()
    workerRpcEnvs.foreach(_.shutdown())
    master.stop()
    workers.foreach(_.stop())
    masterRpcEnv = null
    workerRpcEnvs = null
    master = null
    workers = null
    super.afterAll()
  }

  test("dynamic allocation default behavior") {//默认动态分配行为
    sc = new SparkContext(appConf)
    val appId = sc.applicationId
    eventually(timeout(10.seconds), interval(10.millis)) {
      val apps = getApplications()
      assert(apps.size === 1)
      assert(apps.head.id === appId)
      assert(apps.head.executors.size === 2)
      assert(apps.head.getExecutorLimit === Int.MaxValue)
    }
    // kill all executors
    //杀死所有的执行者
   // assert(killAllExecutors(sc))
    var apps = getApplications()
    assert(apps.head.executors.size === 0)
    assert(apps.head.getExecutorLimit === 0)
    // request 1
    // 请求1
    assert(sc.requestExecutors(1))
    apps = getApplications()
    assert(apps.head.executors.size === 1)
    assert(apps.head.getExecutorLimit === 1)
    // request 1 more
    // 请求1个以上
    assert(sc.requestExecutors(1))
    apps = getApplications()
    assert(apps.head.executors.size === 2)
    assert(apps.head.getExecutorLimit === 2)
    // request 1 more; this one won't go through
    //请求1个以上,这一个不会去通过
    assert(sc.requestExecutors(1))
    apps = getApplications()
    assert(apps.head.executors.size === 2)
    assert(apps.head.getExecutorLimit === 3)
    // kill all existing executors; we should end up with 3 - 2 = 1 executor
    //杀了所有现有的执行者,我们应该结束了3 - 2 = 1执行
    assert(killAllExecutors(sc))
    apps = getApplications()
    assert(apps.head.executors.size === 1)
    assert(apps.head.getExecutorLimit === 1)
    // kill all executors again; this time we'll have 1 - 1 = 0 executors left
    //杀死所有的执行者了,这个时候我们会有1 - 1 = 0者离开
    assert(killAllExecutors(sc))
    apps = getApplications()
    assert(apps.head.executors.size === 0)
    assert(apps.head.getExecutorLimit === 0)
    // request many more; this increases the limit well beyond the cluster capacity
    //要求更多,这增加了限制,远远超出了集群容量
    assert(sc.requestExecutors(1000))
    apps = getApplications()
    assert(apps.head.executors.size === 2)
    assert(apps.head.getExecutorLimit === 1000)
  }
  //动态分配最大内核小于等于每个Worker 内存数
  test("dynamic allocation with max cores <= cores per worker") {
    sc = new SparkContext(appConf.set("spark.cores.max", "8"))
    val appId = sc.applicationId
    eventually(timeout(10.seconds), interval(10.millis)) {
      val apps = getApplications()
      assert(apps.size === 1)
      assert(apps.head.id === appId)
      assert(apps.head.executors.size === 2)
      assert(apps.head.executors.values.map(_.cores).toArray === Array(4, 4))
      assert(apps.head.getExecutorLimit === Int.MaxValue)
    }
    // kill all executors
    //杀死所有执行者
    assert(killAllExecutors(sc))
    var apps = getApplications()
    assert(apps.head.executors.size === 0)
    assert(apps.head.getExecutorLimit === 0)
    // request 1
    //请求1
    assert(sc.requestExecutors(1))
    apps = getApplications()
    assert(apps.head.executors.size === 1)
    assert(apps.head.executors.values.head.cores === 8)
    assert(apps.head.getExecutorLimit === 1)
    // request 1 more; this one won't go through because we're already at max cores.
    // This highlights a limitation of using dynamic allocation with max cores WITHOUT
    // setting cores per executor: once an application scales down and then scales back
    // up, its executors may not be spread out anymore!
    //要求1以上,这一个不会去通过,因为我们已经在最大的核心
    assert(sc.requestExecutors(1))
    apps = getApplications()
    assert(apps.head.executors.size === 1)
    assert(apps.head.getExecutorLimit === 2)
    // request 1 more; this one also won't go through for the same reason
    //请求1个；这一个也不会去通过同样的原因
    assert(sc.requestExecutors(1))
    apps = getApplications()
    assert(apps.head.executors.size === 1)
    assert(apps.head.getExecutorLimit === 3)
    // kill all existing executors; we should end up with 3 - 1 = 2 executor
    //杀死所有现有的执行者,我们应该以3 - 1 = 2执行者
    // Note: we scheduled these executors together, so their cores should be evenly distributed
    //注：我们将这些执行者在一起,所以他们的核心应该是均匀分布的
    assert(killAllExecutors(sc))
    apps = getApplications()
    assert(apps.head.executors.size === 2)
    assert(apps.head.executors.values.map(_.cores).toArray === Array(4, 4))
    assert(apps.head.getExecutorLimit === 2)
    // kill all executors again; this time we'll have 1 - 1 = 0 executors left
    //杀死所有的执行者了;这个时候我们会有1 - 1 = 0者离开
    assert(killAllExecutors(sc))
    apps = getApplications()
    assert(apps.head.executors.size === 0)
    assert(apps.head.getExecutorLimit === 0)
    // request many more; this increases the limit well beyond the cluster capacity
    //要求更多,这增加了限制,远远超出了集群容量
    assert(sc.requestExecutors(1000))
    apps = getApplications()
    assert(apps.head.executors.size === 2)
    assert(apps.head.executors.values.map(_.cores).toArray === Array(4, 4))
    assert(apps.head.getExecutorLimit === 1000)
  }
  //动态分配最大内核大于等于每个Worker 内存数
  test("dynamic allocation with max cores > cores per worker") {
   //当运行在一个独立部署集群上或者是一个粗粒度共享模式的Mesos集群上的时候,最多可以请求多少个CPU核心。默认是所有的都能用
    sc = new SparkContext(appConf.set("spark.cores.max", "16"))
    val appId = sc.applicationId
    eventually(timeout(10.seconds), interval(10.millis)) {
      val apps = getApplications()
      assert(apps.size === 1)
      assert(apps.head.id === appId)
      assert(apps.head.executors.size === 2)
      assert(apps.head.executors.values.map(_.cores).toArray === Array(8, 8))
      assert(apps.head.getExecutorLimit === Int.MaxValue)
    }
    // kill all executors
    //杀死所有执行者
    assert(killAllExecutors(sc))
    var apps = getApplications()
    assert(apps.head.executors.size === 0)
    assert(apps.head.getExecutorLimit === 0)
    // request 1 请求1
    assert(sc.requestExecutors(1))
    apps = getApplications()
    assert(apps.head.executors.size === 1)
    assert(apps.head.executors.values.head.cores === 10)
    assert(apps.head.getExecutorLimit === 1)
    // request 1 more 请求1个以上
    // Note: the cores are not evenly distributed because we scheduled these executors 1 by 1
    assert(sc.requestExecutors(1))
    apps = getApplications()
    assert(apps.head.executors.size === 2)
    assert(apps.head.executors.values.map(_.cores).toSet === Set(10, 6))
    assert(apps.head.getExecutorLimit === 2)
    // request 1 more; this one won't go through
    //请求1个以上,这一个不会去通过
    assert(sc.requestExecutors(1))
    apps = getApplications()
    assert(apps.head.executors.size === 2)
    assert(apps.head.getExecutorLimit === 3)
    // kill all existing executors; we should end up with 3 - 2 = 1 executor
    //杀了所有现有的执行者,我们应该结束了3 - 2 = 1执行
    assert(killAllExecutors(sc))
    apps = getApplications()
    assert(apps.head.executors.size === 1)
    assert(apps.head.executors.values.head.cores === 10)
    assert(apps.head.getExecutorLimit === 1)
    // kill all executors again; this time we'll have 1 - 1 = 0 executors left
    //杀死所有的执行者了,这个时候我们会有1 - 1 = 0者离开
    assert(killAllExecutors(sc))
    apps = getApplications()
    assert(apps.head.executors.size === 0)
    assert(apps.head.getExecutorLimit === 0)
    // request many more; this increases the limit well beyond the cluster capacity
    //要求更多,这增加了限制,远远超出了集群容量
    assert(sc.requestExecutors(1000))
    apps = getApplications()
    assert(apps.head.executors.size === 2)
    assert(apps.head.executors.values.map(_.cores).toArray === Array(8, 8))
    assert(apps.head.getExecutorLimit === 1000)
  }

  test("dynamic allocation with cores per executor") {//动态分配执行者的每个内核
    sc = new SparkContext(appConf.set("spark.executor.cores", "2"))
    val appId = sc.applicationId
    eventually(timeout(10.seconds), interval(10.millis)) {
      val apps = getApplications()
      assert(apps.size === 1)
      assert(apps.head.id === appId)
      //总20内核
      assert(apps.head.executors.size === 10) // 20 cores total
      assert(apps.head.getExecutorLimit === Int.MaxValue)
    }
    // kill all executors 杀死所有执行者
    assert(killAllExecutors(sc))
    var apps = getApplications()
    assert(apps.head.executors.size === 0)
    assert(apps.head.getExecutorLimit === 0)
    // request 1 一个请求者
    assert(sc.requestExecutors(1))
    apps = getApplications()
    assert(apps.head.executors.size === 1)
    assert(apps.head.getExecutorLimit === 1)
    // request 3 more 要求3以上
    assert(sc.requestExecutors(3))
    apps = getApplications()
    assert(apps.head.executors.size === 4)
    assert(apps.head.getExecutorLimit === 4)
    // request 10 more; only 6 will go through 再要求10个,只有6个会通过
    assert(sc.requestExecutors(10))
    apps = getApplications()
    assert(apps.head.executors.size === 10)
    assert(apps.head.getExecutorLimit === 14)
    // kill 2 executors; we should get 2 back immediately
    //杀死2的执行者,我们应该立即回2
    assert(killNExecutors(sc, 2))
    apps = getApplications()
    assert(apps.head.executors.size === 10)
    assert(apps.head.getExecutorLimit === 12)
    // kill 4 executors; we should end up with 12 - 4 = 8 executors
    //杀死4的执行者,我们应该结束了12 - 4 = 8执行者
    assert(killNExecutors(sc, 4))
    apps = getApplications()
    assert(apps.head.executors.size === 8)
    assert(apps.head.getExecutorLimit === 8)
    // kill all executors; this time we'll have 8 - 8 = 0 executors left
    //杀死所有的执行者,这一次我们会有8 - 8 = 0者离开
    assert(killAllExecutors(sc))
    apps = getApplications()
    assert(apps.head.executors.size === 0)
    assert(apps.head.getExecutorLimit === 0)
    // request many more; this increases the limit well beyond the cluster capacity
    //要求更多,这增加了限制,远远超出了集群容量
    assert(sc.requestExecutors(1000))
    apps = getApplications()
    assert(apps.head.executors.size === 10)
    assert(apps.head.getExecutorLimit === 1000)
  }
  //动态分配每个执行器最大内核
  test("dynamic allocation with cores per executor AND max cores") {
   //当运行在一个独立部署集群上或者是一个粗粒度共享模式的Mesos集群上的时候,最多可以请求多少个CPU核心。默认是所有的都能用
    sc = new SparkContext(appConf
      .set("spark.executor.cores", "2")
      .set("spark.cores.max", "8"))
    val appId = sc.applicationId
    eventually(timeout(10.seconds), interval(10.millis)) {
      val apps = getApplications()
      assert(apps.size === 1)
      assert(apps.head.id === appId)
      assert(apps.head.executors.size === 4) // 8 cores total 共有8个内核
      assert(apps.head.getExecutorLimit === Int.MaxValue)
    }
    // kill all executors
    //杀死所有的执行者
   // assert(killAllExecutors(sc))
    var apps = getApplications()
    assert(apps.head.executors.size === 0)
    assert(apps.head.getExecutorLimit === 0)
    // request 1 请求1
    assert(sc.requestExecutors(1))
    apps = getApplications()
    assert(apps.head.executors.size === 1)
    assert(apps.head.getExecutorLimit === 1)
    // request 3 more
    //请求3以上
    assert(sc.requestExecutors(3))
    apps = getApplications()
    assert(apps.head.executors.size === 4)
    assert(apps.head.getExecutorLimit === 4)
    // request 10 more; none will go through
    //再要求10个,没有人会通过
    assert(sc.requestExecutors(10))
    apps = getApplications()
    assert(apps.head.executors.size === 4)
    assert(apps.head.getExecutorLimit === 14)
    // kill all executors; 4 executors will be launched immediately
    //杀死所有的执行者,4执行者将立即启动
    assert(killAllExecutors(sc))
    apps = getApplications()
    assert(apps.head.executors.size === 4)
    assert(apps.head.getExecutorLimit === 10)
    // ... and again
    assert(killAllExecutors(sc))
    apps = getApplications()
    assert(apps.head.executors.size === 4)
    assert(apps.head.getExecutorLimit === 6)
    // ... and again; now we end up with 6 - 4 = 2 executors left
    //现在我们结束了6 - 4 = 2者离开
    assert(killAllExecutors(sc))
    apps = getApplications()
    assert(apps.head.executors.size === 2)
    assert(apps.head.getExecutorLimit === 2)
    // ... and again; this time we have 2 - 2 = 0 executors left
    //一次又一次；这段时间我们有2 - 2 = 0者离开
    assert(killAllExecutors(sc))
    apps = getApplications()
    assert(apps.head.executors.size === 0)
    assert(apps.head.getExecutorLimit === 0)
    // request many more; this increases the limit well beyond the cluster capacity
    //请求更多,这增加了限制,远远超出了集群容量
    assert(sc.requestExecutors(1000))
    apps = getApplications()
    assert(apps.head.executors.size === 4)
    assert(apps.head.getExecutorLimit === 1000)
  }

  test("kill the same executor twice (SPARK-9795)") {//杀死同一执行者两次
    sc = new SparkContext(appConf)
    val appId = sc.applicationId
    eventually(timeout(10.seconds), interval(10.millis)) {
      val apps = getApplications()
      assert(apps.size === 1)
      assert(apps.head.id === appId)
      assert(apps.head.executors.size === 2)
      assert(apps.head.getExecutorLimit === Int.MaxValue)
    }
    // sync executors between the Master and the driver, needed because
    // the driver refuses to kill executors it does not know about
    //同步执行器之间的Master和driver，因为需要driver拒绝杀死不知道的执行者
    syncExecutors(sc)
    // kill the same executor twice
    //杀死同一执行者两次
    val executors = getExecutorIds(sc)
    assert(executors.size === 2)
    assert(sc.killExecutor(executors.head))
    assert(sc.killExecutor(executors.head))
    val apps = getApplications()
    assert(apps.head.executors.size === 1)
    // The limit should not be lowered twice
    //限制不应降低两倍
    assert(apps.head.getExecutorLimit === 1)
  }
  //待替换执行者不应该失去
  test("the pending replacement executors should not be lost (SPARK-10515)") {
    sc = new SparkContext(appConf)
    val appId = sc.applicationId
    eventually(timeout(10.seconds), interval(10.millis)) {
      val apps = getApplications()
      assert(apps.size === 1)
      assert(apps.head.id === appId)
      assert(apps.head.executors.size === 2)
      assert(apps.head.getExecutorLimit === Int.MaxValue)
    }
    // sync executors between the Master and the driver, needed because
    // the driver refuses to kill executors it does not know about
    //同步执行器之间的Master和driver，因为需要driver拒绝杀死不知道的执行者
    syncExecutors(sc)
    val executors = getExecutorIds(sc)
    assert(executors.size === 2)
    // kill executor 1, and replace it
    //杀死执行者1,并取代它
    assert(sc.killAndReplaceExecutor(executors.head))
    eventually(timeout(10.seconds), interval(10.millis)) {
      val apps = getApplications()
      assert(apps.head.executors.size === 2)
    }

    var apps = getApplications()
    // kill executor 1
    //杀死执行者1
    assert(sc.killExecutor(executors.head))
    apps = getApplications()
    assert(apps.head.executors.size === 2)
    assert(apps.head.getExecutorLimit === 2)
    // kill executor 2
    //杀死执行者2
    assert(sc.killExecutor(executors(1)))
    apps = getApplications()
    assert(apps.head.executors.size === 1)
    assert(apps.head.getExecutorLimit === 1)
  }

  // ===============================
  // | Utility methods for testing |用于测试的实用方法
  // ===============================

  /** 
   *  Return a SparkConf for applications that want to talk to our Master.
   *  返回一个应用程序sparkconf 
   *  */
  
  private def appConf: SparkConf = {
    new SparkConf()
      .setMaster(masterRpcEnv.address.toSparkURL)
      .setAppName("test")
      .set("spark.executor.memory", "256m")//分配给每个executor进程总内存
  }

  /** 
   *  Make a master to which our application will send executor requests. 
   *  标记一个主节点应用程序发送执行请求
   *  */
  private def makeMaster(): Master = {
    val master = new Master(masterRpcEnv, masterRpcEnv.address, 0, securityManager, conf)
    masterRpcEnv.setupEndpoint(Master.ENDPOINT_NAME, master)
    master
  }

  /** 
   *  Make a few workers that talk to our master. 
   *  标记一个新worker和我们的主节点交互
   *  */
  private def makeWorkers(cores: Int, memory: Int): Seq[Worker] = {
    (0 until numWorkers).map { i =>
      val rpcEnv = workerRpcEnvs(i)
      val worker = new Worker(rpcEnv, 0, cores, memory, Array(masterRpcEnv.address),
        Worker.SYSTEM_NAME + i, Worker.ENDPOINT_NAME, null, conf, securityManager)
      rpcEnv.setupEndpoint(Worker.ENDPOINT_NAME, worker)
      worker
    }
  }

  /** 
   *  Get the Master state
   *  获得主节点状态
   *   */
  private def getMasterState: MasterStateResponse = {
    master.self.askWithRetry[MasterStateResponse](RequestMasterState)
  }

  /** 
   *  Get the applictions that are active from Master 
   *  得到的是活动主节点的应用程序
   *  */
  private def getApplications(): Seq[ApplicationInfo] = {
    getMasterState.activeApps
  }

  /** 
   *  Kill all executors belonging to this application. *
   *  杀死应用程序所属的执行器
   */

  private def killAllExecutors(sc: SparkContext): Boolean = {
    killNExecutors(sc, Int.MaxValue)
  }

  /** 
   *  Kill N executors belonging to this application. 
   *  杀死N的执行者属于这个应用程序
   *  */
  private def killNExecutors(sc: SparkContext, n: Int): Boolean = {
    syncExecutors(sc)
    sc.killExecutors(getExecutorIds(sc).take(n))
  }

  /**
   * Return a list of executor IDs belonging to this application.
   * 返回应用程序所属的列表的执行器的ID
   * Note that we must use the executor IDs according to the Master, which has the most
   * 注意:我们必须根据主节点相符的执行器的ID,
   * updated view. We cannot rely on the executor IDs according to the driver because we
   * 其中有最新的视图,我们重试执行器到Driver,
   * don't wait for executors to register. Otherwise the tests will take much longer to run.
   * 我们不要等到注册执行者,否则测试将需要更长的时间来运行,
   */
  private def getExecutorIds(sc: SparkContext): Seq[String] = {
    //获得相等的ApplicationInfo
    val app = getApplications().find(_.id == sc.applicationId)
    assert(app.isDefined)
    // Although executors is transient, master is in the same process so the message won't be
    //虽然执行者是短暂,主节点相同进程信息不会序列化,这里很安全
    // serialized and it's safe here.
    //keys获得HashMap的keys值,把Int转换字符串
    app.get.executors.keys.map(_.toString).toSeq
  }

  /**
   * Sync executor IDs between the driver and the Master.
   * 同步执行器IDs列表主节点和Driver
   * This allows us to avoid waiting for new executors to register with the driver before
   * 这使用我们避免等待一个新执行器注册Driver之前,我们提交一个请求杀了他们
   * we submit a request to kill them. This must be called before each kill request.
   * 这必须被调用之前杀死每个请求
   */
  private def syncExecutors(sc: SparkContext): Unit = {
    //getExecutorStorageStatus 返回存储在从节点中的所有块的信息
    val driverExecutors = sc.getExecutorStorageStatus
      .map(_.blockManagerId.executorId)
      .filter { _ != SparkContext.DRIVER_IDENTIFIER}//驱动程序的执行者
    val masterExecutors = getExecutorIds(sc)
    //丢失执行器ID,以masterExecutors为参考比较driverExecutors集合的差集(diff),intersect 两个集合相交(相同的元素), union 合并集合(两个集合相加,去掉重复元素)   
    val missingExecutors = masterExecutors.toSet.diff(driverExecutors.toSet).toSeq.sorted
    missingExecutors.foreach { id =>
      // Fake an executor registration so the driver knows about us
      //注册假的执行器到到Driver,让知道我们
      val port = System.currentTimeMillis % 65536
      val endpointRef = mock(classOf[RpcEndpointRef])
      val mockAddress = mock(classOf[RpcAddress])
      when(endpointRef.address).thenReturn(mockAddress)
      //注册假的执行器到到Driver
      val message = RegisterExecutor(id, endpointRef, s"localhost:$port", 10, Map.empty)
      val backend = sc.schedulerBackend.asInstanceOf[CoarseGrainedSchedulerBackend]
      backend.driverEndpoint.askWithRetry[CoarseGrainedClusterMessage](message)
    }
  }

}
