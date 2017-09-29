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

package org.apache.spark

import java.util.concurrent.{ ExecutorService, TimeUnit }

import scala.collection.Map
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

import org.scalatest.{ BeforeAndAfterEach, PrivateMethodTester }
import org.mockito.Mockito.{ mock, spy, verify, when }
import org.mockito.Matchers
import org.mockito.Matchers._

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.rpc.{ RpcCallContext, RpcEndpoint, RpcEnv, RpcEndpointRef }
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.ManualClock

/**
 * A test suite for the heartbeating behavior between the driver and the executors.
 * executors和driver之间心跳测试
 */
class HeartbeatReceiverSuite//接收心跳测试
    extends SparkFunSuite
    with BeforeAndAfterEach
    with PrivateMethodTester
    with LocalSparkContext {

  private val executorId1 = "executor-1"
  private val executorId2 = "executor-2"

  // Shared state that must be reset before and after each test
  //每个测试之前和之后必须重新设置的共享状态
  private var scheduler: TaskSchedulerImpl = null //任务调度
  private var heartbeatReceiver: HeartbeatReceiver = null //心跳接收
  private var heartbeatReceiverRef: RpcEndpointRef = null //消息发送
  private var heartbeatReceiverClock: ManualClock = null //通常频率

  // Helper private method accessors for HeartbeatReceiver
  // 辅助私有方法访问心跳接收器
  private val _executorLastSeen = PrivateMethod[collection.Map[String, Long]]('executorLastSeen)//最后一次看到
  private val _executorTimeoutMs = PrivateMethod[Long]('executorTimeoutMs)//执行超时毫秒
  private val _killExecutorThread = PrivateMethod[ExecutorService]('killExecutorThread)//杀死执行线程

  /**
   * Before each test, set up the SparkContext and a custom [[HeartbeatReceiver]]
   * that uses a manual clock.
   * 每次测试之前,设置sparkcontext和自定义HeartbeatReceiver使用手动时钟
   */
  override def beforeEach(): Unit = {

    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("test")
      .set("spark.dynamicAllocation.testing", "true")
    sc = spy(new SparkContext(conf))

    scheduler = mock(classOf[TaskSchedulerImpl])//模拟TaskSchedulerImpl
    when(sc.taskScheduler).thenReturn(scheduler)
    when(scheduler.sc).thenReturn(sc)
    heartbeatReceiverClock = new ManualClock//创建一个手动时钟
    heartbeatReceiver = new HeartbeatReceiver(sc, heartbeatReceiverClock)
    heartbeatReceiverRef = sc.env.rpcEnv.setupEndpoint("heartbeat", heartbeatReceiver)
    when(scheduler.executorHeartbeatReceived(any(), any(), any())).thenReturn(true)
  }

  /**
   * After each test, clean up all state and stop the [[SparkContext]].
   * 每次测试后,清理所有状态并停止SparkContext
   */
  override def afterEach(): Unit = {
    super.afterEach()
    scheduler = null
    heartbeatReceiver = null
    heartbeatReceiverRef = null
    heartbeatReceiverClock = null
  }

  test("task scheduler is set correctly") {//设置任务调度程序正确
    assert(heartbeatReceiver.scheduler === null)
    //发送消息TaskSchedulerIsSet,HeartbeatReceiver类receiveAndReply方法接收消息设置TaskScheduler,并返回boolean类型    
    val bll = heartbeatReceiverRef.askWithRetry[Boolean](TaskSchedulerIsSet) //返回值Boolean
    println("bll:"+bll)
    assert(heartbeatReceiver.scheduler !== null)
  }

  test("normal heartbeat") {///正常的心跳
    heartbeatReceiverRef.askWithRetry[Boolean](TaskSchedulerIsSet)
    addExecutorAndVerify(executorId1)
    addExecutorAndVerify(executorId2)
    triggerHeartbeat(executorId1, executorShouldReregister = false)
    triggerHeartbeat(executorId2, executorShouldReregister = false)
    val trackedExecutors = getTrackedExecutors//获得跟踪执行者
    assert(trackedExecutors.size === 2)
    assert(trackedExecutors.contains(executorId1))
    assert(trackedExecutors.contains(executorId2))
  }

  test("reregister if scheduler is not ready yet") {//如果调度器是还没有准备好,从新注册
    addExecutorAndVerify(executorId1)
    // Task scheduler is not set yet in HeartbeatReceiver, so executors should reregister
    //任务调度是不在HeartbeatReceiver,所以执行者应该重新注册
    triggerHeartbeat(executorId1, executorShouldReregister = true)
  }
  //如果未注册的执行者的心跳,则重新注册
  test("reregister if heartbeat from unregistered executor") {
    heartbeatReceiverRef.askWithRetry[Boolean](TaskSchedulerIsSet) 
    // Received heartbeat from unknown executor, so we ask it to re-register
    //从未知的执行器接收到的心跳,所以我们要求它重新注册
    triggerHeartbeat(executorId1, executorShouldReregister = true)
    assert(getTrackedExecutors.isEmpty)
  }
//如果执行者删除心跳
  test("reregister if heartbeat from removed executor") {
    heartbeatReceiverRef.askWithRetry[Boolean](TaskSchedulerIsSet)
    addExecutorAndVerify(executorId1)
    addExecutorAndVerify(executorId2)
    // Remove the second executor but not the first
    //删除第二个执行者,但不是第一个
    removeExecutorAndVerify(executorId2)
    // Now trigger the heartbeats
    //现在引发的心跳
    // A heartbeat from the second executor should require reregistering
    //第二者心跳需要重新注册
    triggerHeartbeat(executorId1, executorShouldReregister = false)
    triggerHeartbeat(executorId2, executorShouldReregister = true)
    val trackedExecutors = getTrackedExecutors
    assert(trackedExecutors.size === 1)
    assert(trackedExecutors.contains(executorId1))
    assert(!trackedExecutors.contains(executorId2))
  }

  test("expire dead hosts") {//过期主机,不活动 
    val executorTimeout = heartbeatReceiver.invokePrivate(_executorTimeoutMs())
    heartbeatReceiverRef.askWithRetry[Boolean](TaskSchedulerIsSet)
    addExecutorAndVerify(executorId1)
    addExecutorAndVerify(executorId2)
    triggerHeartbeat(executorId1, executorShouldReregister = false)
    triggerHeartbeat(executorId2, executorShouldReregister = false)
    // Advance the clock and only trigger a heartbeat for the first executor
    //提前时钟,触发第一个执行者的心跳
    heartbeatReceiverClock.advance(executorTimeout / 2)
    triggerHeartbeat(executorId1, executorShouldReregister = false)
    heartbeatReceiverClock.advance(executorTimeout)
    heartbeatReceiverRef.askWithRetry[Boolean](ExpireDeadHosts)
    // Only the second executor should be expired as a dead host    
    //只有第二个executor是过期,设置executorId2为不可用
    verify(scheduler).executorLost(Matchers.eq(executorId2), any())
    val trackedExecutors = getTrackedExecutors
    assert(trackedExecutors.size === 1)
    assert(trackedExecutors.contains(executorId1))
    assert(!trackedExecutors.contains(executorId2))
  }
  //到期的主机应该更换杀者死
  test("expire dead hosts should kill executors with replacement (SPARK-8119)") {
    // Set up a fake backend and cluster manager to simulate killing executors
    //建立一个假的后端和集群管理器来模拟杀死executors
    val rpcEnv = sc.env.rpcEnv
    val fakeClusterManager = new FakeClusterManager(rpcEnv)
    val fakeClusterManagerRef = rpcEnv.setupEndpoint("fake-cm", fakeClusterManager)
    val fakeSchedulerBackend = new FakeSchedulerBackend(scheduler, rpcEnv, fakeClusterManagerRef)
    when(sc.schedulerBackend).thenReturn(fakeSchedulerBackend)

    // Register fake executors with our fake scheduler backend
    //注册假执行者与我们的假调度后台
    // This is necessary because the backend refuses to kill executors it does not know about
    fakeSchedulerBackend.start()
    val dummyExecutorEndpoint1 = new FakeExecutorEndpoint(rpcEnv)
    val dummyExecutorEndpoint2 = new FakeExecutorEndpoint(rpcEnv)
    val dummyExecutorEndpointRef1 = rpcEnv.setupEndpoint("fake-executor-1", dummyExecutorEndpoint1)
    val dummyExecutorEndpointRef2 = rpcEnv.setupEndpoint("fake-executor-2", dummyExecutorEndpoint2)
    fakeSchedulerBackend.driverEndpoint.askWithRetry[RegisteredExecutor.type](
      RegisterExecutor(executorId1, dummyExecutorEndpointRef1, "dummy:4040", 0, Map.empty))
    fakeSchedulerBackend.driverEndpoint.askWithRetry[RegisteredExecutor.type](
      RegisterExecutor(executorId2, dummyExecutorEndpointRef2, "dummy:4040", 0, Map.empty))
    heartbeatReceiverRef.askWithRetry[Boolean](TaskSchedulerIsSet)
    addExecutorAndVerify(executorId1)
    addExecutorAndVerify(executorId2)
    triggerHeartbeat(executorId1, executorShouldReregister = false)
    triggerHeartbeat(executorId2, executorShouldReregister = false)

    // Adjust the target number of executors on the cluster manager side
    //对集群管理方执行目标数
    assert(fakeClusterManager.getTargetNumExecutors === 0)
    sc.requestTotalExecutors(2, 0, Map.empty)
    assert(fakeClusterManager.getTargetNumExecutors === 2)
    assert(fakeClusterManager.getExecutorIdsToKill.isEmpty)

    // Expire the executors. This should trigger our fake backend to kill the executors.
    //到期的executors,这触发后台杀死executors
    // Since the kill request is sent to the cluster manager asynchronously, we need to block
    // on the kill thread to ensure that the cluster manager actually received our requests.
    //由于“杀”请求被异步发送到群集管理器,我们需要阻止“杀死线程”,以确保群集管理器实际上收到了我们的请求
    // Here we use a timeout of O(seconds), but in practice this whole test takes O(10ms).
    val executorTimeout = heartbeatReceiver.invokePrivate(_executorTimeoutMs())
    heartbeatReceiverClock.advance(executorTimeout * 2)
    heartbeatReceiverRef.askWithRetry[Boolean](ExpireDeadHosts)
    val killThread = heartbeatReceiver.invokePrivate(_killExecutorThread())
    killThread.shutdown() // needed for awaitTermination 等待终止
    killThread.awaitTermination(10L, TimeUnit.SECONDS)

    // The target number of executors should not change! Otherwise, having an expired
    // executor means we permanently adjust the target number downwards until we
    //执行目标数不应改变,否则,有过期的执行意味着我们永久的调整目标数下直到我们
    // explicitly request new executors. For more detail, see SPARK-8119.
    //明确要求新的执行者
    assert(fakeClusterManager.getTargetNumExecutors === 2)
    assert(fakeClusterManager.getExecutorIdsToKill === Set(executorId1, executorId2))
  }

  /**
   *  Manually send a heartbeat and return the response.
   *  手动发送心跳并返回响应
   */
  private def triggerHeartbeat(
    executorId: String,
    executorShouldReregister: Boolean): Unit = {
    val metrics = new TaskMetrics //任务测量
    val blockManagerId = BlockManagerId(executorId, "localhost", 12345)
    //发送Heartbeat消息到HeartbeatReceiver.receiveAndReply,接收executors发送的Heartbeat信息,
    //通知blockManagerMaster,此Executor上的blockManager依然活着,    
    val response = heartbeatReceiverRef.askWithRetry[HeartbeatResponse](
      Heartbeat(executorId, Array(1L -> metrics), blockManagerId))
    if (executorShouldReregister) {
      assert(response.reregisterBlockManager)
    } else {
      assert(!response.reregisterBlockManager)
      // Additionally verify that the scheduler callback is called with the correct parameters
      //此外,验证调度程序回调调用的正确参数
      verify(scheduler).executorHeartbeatReceived(
        Matchers.eq(executorId), Matchers.eq(Array(1L -> metrics)), Matchers.eq(blockManagerId))
    }
  }

  private def addExecutorAndVerify(executorId: String): Unit = {
    assert(
      heartbeatReceiver.addExecutor(executorId).map { f =>
        Await.result(f, 10.seconds)//等待返回結果
      } === Some(true))//接收注册executor消息成功
  }

  private def removeExecutorAndVerify(executorId: String): Unit = {
    assert(
      heartbeatReceiver.removeExecutor(executorId).map { f =>
        Await.result(f, 10.seconds)
      } === Some(true))//接收移除executor消息成功
  }
  /**
   * 跟踪Executor
   */
  private def getTrackedExecutors: Map[String, Long] = {
    // We may receive undesired SparkListenerExecutorAdded from LocalBackend, so exclude it from
    // the map. See SPARK-10800.
    //使用反射调用HeartbeatReceiver类executorLastSeen得到Executor
    heartbeatReceiver.invokePrivate(_executorLastSeen()).
      filterKeys(_ != SparkContext.DRIVER_IDENTIFIER)//过滤掉Driver
  }
}

// TODO: use these classes to add end-to-end tests for dynamic allocation!

/**
 * Dummy RPC endpoint to simulate executors.
 * 模拟执行RPC终端点
 */
private class FakeExecutorEndpoint(override val rpcEnv: RpcEnv) extends RpcEndpoint

/**
 * Dummy scheduler backend to simulate executor allocation requests to the cluster manager.
 * 虚拟调度后端来模拟执行器分配给群集管理器的请求
 */
private class FakeSchedulerBackend(
  scheduler: TaskSchedulerImpl,
  rpcEnv: RpcEnv,
  clusterManagerEndpoint: RpcEndpointRef)
    extends CoarseGrainedSchedulerBackend(scheduler, rpcEnv) {

  protected override def doRequestTotalExecutors(requestedTotal: Int): Boolean = {
    clusterManagerEndpoint.askWithRetry[Boolean](
      RequestExecutors(requestedTotal, localityAwareTasks, hostToLocalTaskCount))
  }

  protected override def doKillExecutors(executorIds: Seq[String]): Boolean = {
    clusterManagerEndpoint.askWithRetry[Boolean](KillExecutors(executorIds))
  }
}

/**
 * Dummy cluster manager to simulate responses to executor allocation requests.
 * 模拟集群管理器来模拟执行分配请求的响应
 * Fake假装,冒充
 */
private class FakeClusterManager(override val rpcEnv: RpcEnv) extends RpcEndpoint {
  private var targetNumExecutors = 0
  private val executorIdsToKill = new mutable.HashSet[String]

  def getTargetNumExecutors: Int = targetNumExecutors
  def getExecutorIdsToKill: Set[String] = executorIdsToKill.toSet
  //处理RpcEndpointRef.ask方法,如果不匹配消息,将抛出异常
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RequestExecutors(requestedTotal, _, _) =>
      targetNumExecutors = requestedTotal
      context.reply(true)
    case KillExecutors(executorIds) =>
      executorIdsToKill ++= executorIds
      context.reply(true)
  }
}
