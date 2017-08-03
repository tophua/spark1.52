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

import scala.collection.mutable.HashSet

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.CallSite

/**
 * A stage is a set of independent tasks all computing the same function that need to run as part
 * of a Spark job, where all the tasks have the same shuffle dependencies. Each DAG of tasks run
 * by the scheduler is split up into stages at the boundaries where shuffle occurs, and then the
 * DAGScheduler runs these stages in topological order.
 * 一个(stage)阶段是一组独立的任务,所有计算的相同的功能,需要运行作为一个Spark作业的一部分,
 * 在所有的任务都有相同的Shuufle依赖关系,每个DAG任务运行的界限Shuffle,然后dagscheduler运行这些阶段的拓扑顺序
 * 
 * Each Stage can either be a shuffle map stage, in which case its tasks' results are input for
 * another stage, or a result stage, in which case its tasks directly compute the action that
 * initiated a job (e.g. count(), save(), etc). For shuffle map stages, we also track the nodes
 * that each output partition is on.
 * 每个Stage都可以是一个Shuffle stage,在这种情况下,它的任务的结果是另一个Stage的输入,或结果阶段,
 * 在这种情况下,它的任务直接计算发起了一个作业的动作,
 * Each Stage also has a firstJobId, identifying the job that first submitted the stage.  When FIFO
 * scheduling is used, this allows Stages from earlier jobs to be computed first or recovered
 * faster on failure.
 *
 * The callSite provides a location in user code which relates to the stage. For a shuffle map
 * stage, the callSite gives the user code that created the RDD being shuffled. For a result
 * stage, the callSite gives the user code that executes the associated action (e.g. count()).
 *
 * A single stage can consist of multiple attempts. In that case, the latestInfo field will
 * be updated for each attempt.
 * Job分成的阶段,一个Job可能被划分为一到多个Stage
 *
 */
private[spark] abstract class Stage(
    val id: Int,//stage序号,数值越大,越优先执行,如3,2,1,
    val rdd: RDD[_],//归属于本stage的最后一个Rdd
    val numTasks: Int,//创建的Task数目,等于父rdd的输出partition数目
    val parents: List[Stage],//父stage列表
    val firstJobId: Int,//作业Id
    val callSite: CallSite)
  extends Logging {

  val numPartitions = rdd.partitions.size

  /** Set of jobs that this stage belongs to. */
  //阶段(Stage)的Job(作业)集合
  val jobIds = new HashSet[Int]
 //存储等待处理的Task
  var pendingTasks = new HashSet[Task[_]]

  /** 
   *  The ID to use for the next new attempt for this stage.
   *  该stage下一次新尝试的id  
   *  */
  private var nextAttemptId: Int = 0

  val name = callSite.shortForm
  val details = callSite.longForm

  private var _internalAccumulators: Seq[Accumulator[Long]] = Seq.empty

  /** 
   *  Internal accumulators shared across all tasks in this stage. 
   *  Stage内部累加器共享在所有的任务
   *  */
  def internalAccumulators: Seq[Accumulator[Long]] = _internalAccumulators

  /**
   * Re-initialize the internal accumulators associated with this stage.
   * 重新初始化内部累加器
   * This is called every time the stage is submitted, *except* when a subset of tasks
   * belonging to this stage has already finished. Otherwise, reinitializing the internal
   * accumulators here again will override partial values from the finished tasks.
    * 每当提交阶段时，这被称为* *当属于此阶段的任务的子集已经完成时除外*。
    * 否则，再次重新初始化内部累加器将覆盖完成的任务中的部分值。
   */
  def resetInternalAccumulators(): Unit = {
    _internalAccumulators = InternalAccumulator.create(rdd.sparkContext)
  }

  /**
   * Pointer to the [StageInfo] object for the most recent attempt. 
   * 指向最新StageInfo对象,这需要一个初始化,This needs to be initialized
   * here, before any attempts have actually been created, because the DAGScheduler uses this
   * StageInfo to tell SparkListeners when a job starts (which happens before any stage attempts
   * have been created).
    * 在这里，在实际创建任何尝试之前，因为DAGScheduler
    * 使用此StageInfo来在作业启动时告知SparkListeners(在任何阶段尝试创建之前发生)
   * 因为dagscheduler使用本stageinfo告诉sparklisteners一个Job开始运行
   */
  
  private var _latestInfo: StageInfo = StageInfo.fromStage(this, nextAttemptId)

  /** 
   *  Creates a new attempt for this stage by creating a new StageInfo with a new attempt ID.
   *  通过创建具有新尝试ID的新StageInfo，创建此阶段的新尝试ID自增1
   *  */
  def makeNewStageAttempt(
      numPartitionsToCompute: Int,
      taskLocalityPreferences: Seq[Seq[TaskLocation]] = Seq.empty): Unit = {
    _latestInfo = StageInfo.fromStage(
      this, nextAttemptId, Some(numPartitionsToCompute), taskLocalityPreferences)
    nextAttemptId += 1
  }

  /** 
   *  Returns the StageInfo for the most recent attempt for this stage.
   *  返回StageInfo 尝试最新stage
   *   */
  def latestInfo: StageInfo = _latestInfo

  override final def hashCode(): Int = id
  override final def equals(other: Any): Boolean = other match {
    case stage: Stage => stage != null && stage.id == id
    case _ => false
  }
}
