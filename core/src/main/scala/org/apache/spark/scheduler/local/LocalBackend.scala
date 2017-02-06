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

package org.apache.spark.scheduler.local

import java.io.File
import java.net.URL
import java.nio.ByteBuffer

import org.apache.spark.{Logging, SparkConf, SparkContext, SparkEnv, TaskState}
import org.apache.spark.TaskState.TaskState
import org.apache.spark.executor.{Executor, ExecutorBackend}
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.ExecutorInfo
/**
 * ReviveOffers本身是一个空的case object对象,只是起到触发底层资源调度的作用,
 * 在有Task提交或者计算资源变动的时候会发送ReviveOffers这个消息作为触发器
 */
private case class ReviveOffers()

private case class StatusUpdate(taskId: Long, state: TaskState, serializedData: ByteBuffer)

private case class KillTask(taskId: Long, interruptThread: Boolean)

private case class StopExecutor()

/**
 * Calls to LocalBackend are all serialized through LocalEndpoint. Using an RpcEndpoint makes the
 * calls on LocalBackend asynchronous, which is necessary to prevent deadlock between LocalBackend
 * and the TaskSchedulerImpl.
 * 
 */
private[spark] class LocalEndpoint(
    override val rpcEnv: RpcEnv,
    userClassPath: Seq[URL],
    scheduler: TaskSchedulerImpl,
    executorBackend: LocalBackend,
    private val totalCores: Int)
  extends ThreadSafeRpcEndpoint with Logging {
 //可用CPUS内核数
  private var freeCores = totalCores
  val localExecutorId = SparkContext.DRIVER_IDENTIFIER
  val localExecutorHostname = "localhost"
  //创建本地Executor
  private val executor = new Executor(
    localExecutorId, localExecutorHostname, SparkEnv.get, userClassPath, isLocal = true)
/**
 * 处理RpcEndpointRef.send或RpcCallContext.reply方法,如果收到不匹配的消息
 * PartialFunction[Any, Unit]Any接收任务类型,Unit返回值
 */
  override def receive: PartialFunction[Any, Unit] = {
    case ReviveOffers =>
      reviveOffers()

    case StatusUpdate(taskId, state, serializedData) =>
      scheduler.statusUpdate(taskId, state, serializedData)
      if (TaskState.isFinished(state)) {
        freeCores += scheduler.CPUS_PER_TASK
        reviveOffers()
      }

    case KillTask(taskId, interruptThread) =>
      executor.killTask(taskId, interruptThread)
  }
//处理RpcEndpointRef.ask方法,默认如果不匹配消息
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case StopExecutor =>
      executor.stop()
      context.reply(true)
  }
  /**
   * 申请分配资源
   */
  def reviveOffers() {
    //使用localExecutorId,localExecutorHostname,freeCores空闲CPU核数创建WorkerOffer
    val offers = Seq(new WorkerOffer(localExecutorId, localExecutorHostname, freeCores))
    //调用TaskSchedulerImpl的resourceOffers方法用于任务的资源分配
    for (task <- scheduler.resourceOffers(offers).flatten) {//flatten把嵌套的结构(Seq)展开,成一个(Seq)      
      freeCores -= scheduler.CPUS_PER_TASK
      //调用launchTask方法运行任务
      executor.launchTask(executorBackend, taskId = task.taskId, attemptNumber = task.attemptNumber,
        task.name, task.serializedTask)
    }
  }
}

/**
 * LocalBackend is used when running a local version of Spark where the executor, backend, and
 * master all run in the same JVM. It sits behind a TaskSchedulerImpl and handles launching tasks
 * on a single Executor (created by the LocalBackend) running locally.
 * 任务调度
 */
private[spark] class LocalBackend(
    conf: SparkConf,
    scheduler: TaskSchedulerImpl,
    val totalCores: Int)
  extends SchedulerBackend with ExecutorBackend with Logging {

  private val appId = "local-" + System.currentTimeMillis
  
  private var localEndpoint: RpcEndpointRef = null
  
  private val userClassPath = getUserClasspath(conf)
  
  private val listenerBus = scheduler.sc.listenerBus

  /**
   * Returns a list of URLs representing the user classpath.
   * 返回一个列表,代表用户类路径的URL
   * @param conf Spark configuration.
   */
  def getUserClasspath(conf: SparkConf): Seq[URL] = {
    //追加到executor类路径中的附加类路径,主要为了兼容旧版本的Spark
    val userClassPathStr = conf.getOption("spark.executor.extraClassPath")
    userClassPathStr.map(_.split(File.pathSeparator)).toSeq.flatten.map(new File(_).toURI.toURL)
  }

  override def start() {
    //LocalActor与ActorSystem进行消息通信
    //RpcEndpointRef与ActorSystem进行消息通信
    val rpcEnv = SparkEnv.get.rpcEnv
    //LocalEndpoint 创建本地Exceutor
    val executorEndpoint = new LocalEndpoint(rpcEnv, userClassPath, scheduler, this, totalCores)
    
    localEndpoint = rpcEnv.setupEndpoint("LocalBackendEndpoint", executorEndpoint)
    //向SparkListenerBus发送事件SparkListenerExecutorAdded
    listenerBus.post(SparkListenerExecutorAdded(
      System.currentTimeMillis,
      executorEndpoint.localExecutorId,
      new ExecutorInfo(executorEndpoint.localExecutorHostname, totalCores, Map.empty)))
  }

  override def stop() {
    localEndpoint.ask(StopExecutor)
  }

  override def reviveOffers() {
    localEndpoint.send(ReviveOffers)
  }
  //控制Spark中的分布式shuffle过程默认使用的task数量,默认为8个
  override def defaultParallelism(): Int =
    scheduler.conf.getInt("spark.default.parallelism", totalCores)

  override def killTask(taskId: Long, executorId: String, interruptThread: Boolean) {
    
    localEndpoint.send(KillTask(taskId, interruptThread))
  }

  override def statusUpdate(taskId: Long, state: TaskState, serializedData: ByteBuffer) {
    //实际向LocalActor发送StatusUpdate消息
    //LocalActor接收到StatusUpdate事件时,匹配执行TaskSchedulerImpl的statusUpdate方法
    //并根据Task的最新状态做一系列处理
    localEndpoint.send(StatusUpdate(taskId, state, serializedData))
  }

  override def applicationId(): String = appId

}
