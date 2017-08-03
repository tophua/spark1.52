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

package org.apache.spark.scheduler

import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.storage.BlockManagerId

/**
 * Low-level task scheduler interface, currently implemented exclusively by
 * [[org.apache.spark.scheduler.TaskSchedulerImpl]].
 * This interface allows plugging in different task schedulers. Each TaskScheduler schedules tasks
 * for a single SparkContext. These schedulers get sets of tasks submitted to them from the
 * DAGScheduler for each stage, and are responsible for sending the tasks to the cluster, running
 * them, retrying if there are failures, and mitigating stragglers. They return events to the
 * DAGScheduler.
  *
  * 低级任务调度器接口,目前由[[org.apache.spark.scheduler.TaskSchedulerImpl]]专门实现,该接口允许插入不同的任务调度器。
  * 每个TaskScheduler为单个SparkContext调度任务。 这些调度程序从DAGScheduler获取每个阶段提交的任务,并负责将任务发送到集群,
  * 运行它们,如果发生故障,重新尝试并减轻分类器,他们将事件返回给DAGScheduler。
 */
private[spark] trait TaskScheduler {

  private val appId = "spark-application-" + System.currentTimeMillis

  def rootPool: Pool

  def schedulingMode: SchedulingMode

  def start(): Unit

  // Invoked after system has successfully initialized (typically in spark context).
  // Yarn uses this to bootstrap allocation of resources based on preferred locations,
  // wait for slave registrations, etc.
  //在系统成功初始化之后调用(通常在sparkcontext中),Yarn使用它来引导基于首选位置的资源分配,等待从属注册等。
  def postStartHook() { }

  // Disconnect from the cluster.
  // 断开群集
  def stop(): Unit

  // Submit a sequence of tasks to run.
  // 提交一系列任务到运行状态
  def submitTasks(taskSet: TaskSet): Unit

  // Cancel a stage.
  // 取消一个Stage
  def cancelTasks(stageId: Int, interruptThread: Boolean)

  // Set the DAG scheduler for upcalls. This is guaranteed to be set before submitTasks is called.
  //设置DAG调度程序进行调用,这是保证在调用submitTasks之前设置的。
  def setDAGScheduler(dagScheduler: DAGScheduler): Unit

  // Get the default level of parallelism to use in the cluster, as a hint for sizing jobs.
  //获取在集群中使用的默认级别并行度,作为调整作业大小的提示
  def defaultParallelism(): Int

  /**
   * Update metrics for in-progress tasks and let the master know that the BlockManager is still
   * alive. Return true if the driver knows about the given block manager. Otherwise, return false,
   * indicating that the block manager should re-register.
    *
    * 更新正在进行的任务的指标,让master知道BlockManager仍然存在,如果driver知道给定的块管理器,则返回true。
    * 否则返回false，表示块管理器应该重新注册。
    *
   * 周期性的接收executor的心跳,更新运行中tasks的元信息,并让master知晓BlockManager仍然存活
   */
  def executorHeartbeatReceived(execId: String, taskMetrics: Array[(Long, TaskMetrics)],
    blockManagerId: BlockManagerId): Boolean

  /**
   * Get an application ID associated with the job.
   * 获取与Job关联的应用程序标识
   * @return An application ID
   */
  def applicationId(): String = appId

  /**
   * Process a lost executor
   * 处理丢失的executor
   */
  def executorLost(executorId: String, reason: ExecutorLossReason): Unit

  /**
   * Get an application's attempt ID associated with the job.
   * 获取与作业相关的应用程序尝试标识
   * @return An application's Attempt ID
   */
  def applicationAttemptId(): Option[String]

}
