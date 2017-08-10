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

import java.util.concurrent.{ScheduledFuture, TimeUnit}

import scala.collection.mutable
import scala.concurrent.Future

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.rpc.{ThreadSafeRpcEndpoint, RpcEnv, RpcCallContext}
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.scheduler._
import org.apache.spark.util.{Clock, SystemClock, ThreadUtils, Utils}

/**
 * A heartbeat from executors to the driver. This is a shared message used by several internal
 * components to convey liveness or execution information for in-progress tasks. It will also
 * expire the hosts that have not heartbeated for more than spark.network.timeout.
  * 从执行者到driver的心跳,这是几个内部组件使用的共享消息,用于传达正在进行的任务的活动或执行信息。
  * 它也将使不超过spark.network.timeout的心跳的主机过期。
 */
private[spark] case class Heartbeat(
    executorId: String,
    taskMetrics: Array[(Long, TaskMetrics)], // taskId -> TaskMetrics
    blockManagerId: BlockManagerId)

/**
 * An event that SparkContext uses to notify HeartbeatReceiver that SparkContext.taskScheduler is
 * created.
  * SparkContext用于通知HeartbeatReceiver SparkContext.taskScheduler的事件。
 */
private[spark] case object TaskSchedulerIsSet
//过期死亡主机
private[spark] case object ExpireDeadHosts

private case class ExecutorRegistered(executorId: String)

private case class ExecutorRemoved(executorId: String)
//心跳反应
private[spark] case class HeartbeatResponse(reregisterBlockManager: Boolean)

/**
 * Lives in the driver to receive heartbeats from executors..
  * driver活着得到执行者的心跳
 */
private[spark] class HeartbeatReceiver(sc: SparkContext, clock: Clock)
  extends ThreadSafeRpcEndpoint with SparkListener with Logging {

  def this(sc: SparkContext) {
    this(sc, new SystemClock)
  }

  sc.addSparkListener(this)

  override val rpcEnv: RpcEnv = sc.env.rpcEnv

  private[spark] var scheduler: TaskScheduler = null

  // executor ID -> timestamp of when the last heartbeat from this executor was received
  //Key值executor ID,value值最后一次心跳被接收时间
  private val executorLastSeen = new mutable.HashMap[String, Long]

  // "spark.network.timeout" uses "seconds", while `spark.storage.blockManagerSlaveTimeoutMs` uses
  // "milliseconds"从节点超时120秒
  //“spark.network.timeout”使用“秒”,而`spark.storage.blockManagerSlaveTimeoutMs`使用“毫秒”
  private val slaveTimeoutMs =
    sc.conf.getTimeAsMs("spark.storage.blockManagerSlaveTimeoutMs", "120s")
    //executor网络超时1200毫秒
  private val executorTimeoutMs =
    sc.conf.getTimeAsSeconds("spark.network.timeout", s"${slaveTimeoutMs}ms") * 1000

  // "spark.network.timeoutInterval" uses "seconds", while
  // "spark.storage.blockManagerTimeoutIntervalMs" uses "milliseconds"
  // “spark.network.timeoutInterval”使用“秒”,
  // 而“spark.storage.blockManagerTimeoutIntervalMs”使用“毫秒”
  //指定检查BlockManager超时时间间隔
  private val timeoutIntervalMs =
    sc.conf.getTimeAsMs("spark.storage.blockManagerTimeoutIntervalMs", "60s")
    //Worker超时60秒
  private val checkTimeoutIntervalMs =
    sc.conf.getTimeAsSeconds("spark.network.timeoutInterval", s"${timeoutIntervalMs}ms") * 1000

  private var timeoutCheckingTask: ScheduledFuture[_] = null

  // "eventLoopThread" is used to run some pretty fast actions. The actions running in it should not
  // block the thread for a long time.
  //“eventLoopThread”用于运行一些非常快的动作,其中运行的操作不应该长时间堵塞线程。
  private val eventLoopThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("heartbeat-receiver-event-loop-thread")

  private val killExecutorThread = ThreadUtils.newDaemonSingleThreadExecutor("kill-executor-thread")

  override def onStart(): Unit = {
    //schedule和scheduleAtFixedRate的区别在于,如果指定开始执行的时间在当前系统运行时间之前,
    // scheduleAtFixedRate会把已经过去的时间也作为周期执行,而schedule不会把过去的时间算上。
    timeoutCheckingTask = eventLoopThread.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        //删除心跳超时的Executor,默认超进
        Option(self).foreach(_.ask[Boolean](ExpireDeadHosts))
      }
    }, 0, checkTimeoutIntervalMs, TimeUnit.MILLISECONDS)
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {

    // Messages sent and received locally
    //消息在本地发送和接收
    case ExecutorRegistered(executorId) =>
      executorLastSeen(executorId) = clock.getTimeMillis()
      context.reply(true)
    case ExecutorRemoved(executorId) =>
      executorLastSeen.remove(executorId)
      context.reply(true)
    case TaskSchedulerIsSet =>
      scheduler = sc.taskScheduler
      //使用方法askWithRetry[Boolean]请求TaskSchedulerIsSet消息并要求返回Boolean值
      //println("receiveAndReply:"+scheduler.applicationId()+"==="+scheduler.defaultParallelism())
      context.reply(true)
    case ExpireDeadHosts =>
      //删除心跳超时的executor
      expireDeadHosts()
      context.reply(true)

    // Messages received from executors
    //发送HeartbeatResponse消息到HeartbeatReceiver.receiveAndReply
     //接收executors发送的Heartbeat信息,
     //通知blockManagerMaster,此Executor上的blockManager依然活着,    
    case heartbeat @ Heartbeat(executorId, taskMetrics, blockManagerId) =>
      if (scheduler != null) {
        if (executorLastSeen.contains(executorId)) {
          executorLastSeen(executorId) = clock.getTimeMillis()
          //submit,则该call方法自动在一个线程上执行,并且会返回执行结果Future对象
          eventLoopThread.submit(new Runnable {
            override def run(): Unit = Utils.tryLogNonFatalError {
              //用于更新Stage的各种测量数据.
              //blockManagerMaster持有blockManagerMasterActor发送BlockManagerHeartBeat消息到 BlockManagerMasterEndpoint   
              val unknownExecutor = !scheduler.executorHeartbeatReceived(
                executorId, taskMetrics, blockManagerId)
              val response = HeartbeatResponse(reregisterBlockManager = unknownExecutor)
              //返回HeartbeatResponse
              context.reply(response)
            }
          })
        } else {
          // This may happen if we get an executor's in-flight heartbeat immediately
          // after we just removed it. It's not really an error condition so we should
          // not log warning here. Otherwise there may be a lot of noise especially if
          // we explicitly remove executors (SPARK-4134).
          //如果我们刚刚删除后立即得到执行者的飞行心跳，可能会发生这种情况,这不是真的错误的条件,所以我们不应该在这里记录警告。
          // 否则可能会有很多噪音,特别是如果我们明确删除执行器（SPARK-4134）。
          logDebug(s"Received heartbeat from unknown executor $executorId")
          context.reply(HeartbeatResponse(reregisterBlockManager = true))
        }
      } else {
        // Because Executor will sleep several seconds before sending the first "Heartbeat", this
        // case rarely happens. However, if it really happens, log it and ask the executor to
        // register itself again.
        //因为执行者将在发送第一个“心跳”之前睡眠几秒钟,这种情况很少发生,但是,如果真的发生了,请登录并请执行人再次注册。
        logWarning(s"Dropping $heartbeat because TaskScheduler is not ready yet")
        context.reply(HeartbeatResponse(reregisterBlockManager = true))
      }
  }

  /**
   * Send ExecutorRegistered to the event loop to add a new executor. Only for test.
    * 发送执行者注册到事件循环添加一个新的执行器,只用于测试
   *
   * @return if HeartbeatReceiver is stopped, return None. Otherwise, return a Some(Future) that
   *         indicate if this operation is successful.
    *         如果HeartbeatReceiver被停止,则返回None,否则,返回一个(Future),指示此操作是否成功。
   */
  def addExecutor(executorId: String): Option[Future[Boolean]] = {
    //向自己发送ExecutorRegistered消息,receiveAndReply方法接收消息
    Option(self).map(_.ask[Boolean](ExecutorRegistered(executorId)))
  }

  /**
   * If the heartbeat receiver is not stopped, notify it of executor registrations.
    * 如果心跳接收器未停止,请通知执行器注册。
   */
  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    addExecutor(executorAdded.executorId)
  }

  /**
   * Send ExecutorRemoved to the event loop to remove a executor. Only for test.
    * 将执行程序发送到事件循环以删除执行程序,只用于测试
   *
   * @return if HeartbeatReceiver is stopped, return None. Otherwise, return a Some(Future) that
   *         indicate if this operation is successful.
    *         如果HeartbeatReceiver被停止,则返回None,否则,返回一个(Future),指示此操作是否成功。
   */
  def removeExecutor(executorId: String): Option[Future[Boolean]] = {
    Option(self).map(_.ask[Boolean](ExecutorRemoved(executorId)))
  }

  /**
   * If the heartbeat receiver is not stopped, notify it of executor removals so it doesn't
   * log superfluous errors.
    * 如果心跳接收器没有停止,请通知它的执行器删除,否则不会记录多余的错误
   *
   * Note that we must do this after the executor is actually removed to guard against the
   * following race condition: if we remove an executor's metadata from our data structure
   * prematurely, we may get an in-flight heartbeat from the executor before the executor is
   * actually removed, in which case we will still mark the executor as a dead host later
   * and expire it with loud error messages.
    * 请注意:我们必须在执行者被实际删除以防止以下竞争条件之后执行此操作：
    * 如果我们过早地从数据结构中删除了执行者的元数据,则在执行者实际删除之前,我们可能会从执行器获取一个正在运行中的心跳,
    * 在这种情况下,我们仍然会将执行人标记为死主,稍后会出现大声错误讯息。
   */
  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    removeExecutor(executorRemoved.executorId)
  }
  /**
   * 过期不活动机器
   */
  private def expireDeadHosts(): Unit = {
    logTrace("Checking for hosts with no recent heartbeats in HeartbeatReceiver.")
    val now = clock.getTimeMillis()
    for ((executorId, lastSeenMs) <- executorLastSeen) {//executorLastSeen最后一次接收到信息
      if (now - lastSeenMs > executorTimeoutMs) {
        //当前时间减去executor发送最新心跳时间大于超时时间,判断executor是不可用状态
        logWarning(s"Removing executor $executorId with no recent heartbeats: " +
          s"${now - lastSeenMs} ms exceeds timeout $executorTimeoutMs ms")
        scheduler.executorLost(executorId, SlaveLost("Executor heartbeat " +
          s"timed out after ${now - lastSeenMs} ms"))
          // Asynchronously kill the executor to avoid blocking the current thread
        //异步地杀死执行器以避免阻止当前线程
        killExecutorThread.submit(new Runnable {
          override def run(): Unit = Utils.tryLogNonFatalError {
            // Note: we want to get an executor back after expiring this one,
            // so do not simply call `sc.killExecutor` here (SPARK-8119)
            //注意:我们希望在过期之后让执行者回来,所以不要简单地在这里调用`sc.killExecutor`(SPARK-8119)
            sc.killAndReplaceExecutor(executorId)
          }
        })
        executorLastSeen.remove(executorId)
      }
    }
  }

  override def onStop(): Unit = {
    if (timeoutCheckingTask != null) {
      timeoutCheckingTask.cancel(true)
    }
    eventLoopThread.shutdownNow()
    killExecutorThread.shutdownNow()
  }
}

object HeartbeatReceiver {
  val ENDPOINT_NAME = "HeartbeatReceiver"
}
