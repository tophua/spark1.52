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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.rpc.RpcEnv
import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.deploy.worker.Worker
import org.apache.spark.deploy.master.Master
import org.apache.spark.util.Utils

/**
 * Testing class that creates a Spark standalone process in-cluster (that is, running the
 * spark.deploy.master.Master and spark.deploy.worker.Workers in the same JVMs). Executors launched
 * by the Workers still run in separate JVMs. This can be used to test distributed operation and
 * fault recovery without spinning up a lot of processes.
  *
 * 在集群中创建Spark独立进程的测试类(即,在同一个JVM中运行spark.deploy.master.Master和spark.deploy.worker.Workers),
  * Executors发起的执行者仍然运行在单独的JVM中。这可以用于测试分布式操作和故障恢复,而不会产生大量流程。
 */
private[spark]
class LocalSparkCluster(
    numWorkers: Int,
    coresPerWorker: Int,
    memoryPerWorker: Int,
    conf: SparkConf)
  extends Logging {
  
  private val localHostname = Utils.localHostName()
  //用于缓存所有的master的RpcEnv
  private val masterRpcEnvs = ArrayBuffer[RpcEnv]()
  //维护所有的workerRpcEnvs
  private val workerRpcEnvs = ArrayBuffer[RpcEnv]()
  // exposed for testing
  var masterWebUIPort = -1

  def start(): Array[String] = {
    /**
     * 步骤处理
     * 1)首先启动Master,Master启动检测Worker是否死亡的定时调度,启动webUI,启动测量系统,
     * 2)Mster Start 选择故障恢复的持久引擎,最后向Master的RpcEvn注册领导选举代理(默认MonarchyLeaderAgent)
     * 3)注册MonarchyLeaderAgent时,Master的Start方法RpcEvn会调MonarchyLeaderAgent.electedLeader方法,
     *   因此它将被选举为激活状态的Master
     * 4)LocalSparkCluste启动多个Worker,每个Worker都会完成创建工作目录,启动ShuffleService,启动WebUI,启动测量系统
     *   最后向Master注册Worker等工作
     * 5)Master收到RegisterWorker消息后完成创建与注册WorkerInfo,向Worker发送RegisteredWorker消息以表示注册完成,
     *   最后调用Schedule方法进行资源调度
     * 6)启动taskSchedule时调用SparkDeploySchedulerBackend的start方法
     * 7)SparkDeploySchedulerBackend调用父类CoarseGrainedSchedulerBackend的Start方法,向RpcEvn注册DriverAction
     * 8)进行一些参数,java选项,类路径的设置,这些配置封装为Command,然后由ApplicationDescription持有,最后启动AppClient.
     *   这个Command用于在Worker上启动CoarseGrainedSchedulerBackend进程,此进程将创建Executor执行任务.
     * 9)启动AppClient时,向RpcEvn注册ClientAction
     * 10)注册向RpcEvn注册时,RpcEvn回调Start方法,并且调用RegisterWithMaster方法向Master发送RegisterApplication消息
     * 11)Master接收到RegisterApplication消息后,完成创建ApplicationInfo,注册ApplicationInfo,
     *    向ClientActor发送RegisteredApplication消息,最后执行调度.
     */
    logInfo("Starting a local Spark cluster with " + numWorkers + " workers.")

    // Disable REST server on Master in this mode unless otherwise specified
    val _conf = conf.clone()
      .setIfMissing("spark.master.rest.enabled", "false")
      .set("spark.shuffle.service.enabled", "false")

    /* Start the Master */
    //创建,启动master的ActionSystem与多个Worker的ActionSystem
    //startRpcEnvAndEndpoint 用于创建,启动Master的ActorSystem,然后将Master注册到ActorSystem
    val (rpcEnv, webUiPort, _) = Master.startRpcEnvAndEndpoint(localHostname, 0, 0, _conf)
    masterWebUIPort = webUiPort
    masterRpcEnvs += rpcEnv
    val masterUrl = "spark://" + Utils.localHostNameForURI() + ":" + rpcEnv.address.port
    //将Master的Akka地址注册到缓存masters
    val masters = Array(masterUrl)

    /* Start the Workers
    * 启动Worker*/
    for (workerNum <- 1 to numWorkers) {
      //coresPerWorker 每个Worker占用的CPU核数
      //memoryPerWorker 每个Worker占用的内存大小
      val workerEnv = Worker.startRpcEnvAndEndpoint(localHostname, 0, 0, coresPerWorker,
        memoryPerWorker, masters, null, Some(workerNum), _conf)
      //将workerEnv的Akka地址注册到缓存workerRpcEnvs
      workerRpcEnvs += workerEnv
    }

    masters
  }

  def stop() {
    /**
     * 关闭,清理Master的masterRpcEnvs与多个Worker的masterRpcEnvs
     */
    logInfo("Shutting down local Spark cluster.")
    // Stop the workers before the master so they don't get upset that it disconnected
    //在workers面前停止工作,不要因为断开连接而感到烦恼
    workerRpcEnvs.foreach(_.shutdown())
    masterRpcEnvs.foreach(_.shutdown())
    masterRpcEnvs.clear()
    workerRpcEnvs.clear()
  }
}
