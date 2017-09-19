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

package org.apache.spark.ui.scope

import scala.collection.mutable

import org.apache.spark.SparkConf
import org.apache.spark.scheduler._
import org.apache.spark.ui.SparkUI

/**
 * A SparkListener that constructs a DAG of RDD operations.
  * 构建RDD操作的DAG的SparkListener
 */
private[ui] class RDDOperationGraphListener(conf: SparkConf) extends SparkListener {

  // Note: the fate of jobs and stages are tied. This means when we clean up a job,
  // we always clean up all of its stages. Similarly, when we clean up a stage, we
  // always clean up its job (and, transitively, other stages in the same job).
  //注意：工作和阶段的命运是束缚的。 这意味着当我们清理一份工作时，
 // 我们总是清理所有的阶段,同样,当我们清理一个阶段时,我们总是清理自己的工作（而且在过程中也是清理同一工作中的其他阶段）。
  private[ui] val jobIdToStageIds = new mutable.HashMap[Int, Seq[Int]]
  private[ui] val jobIdToSkippedStageIds = new mutable.HashMap[Int, Seq[Int]]
  private[ui] val stageIdToJobId = new mutable.HashMap[Int, Int]
  private[ui] val stageIdToGraph = new mutable.HashMap[Int, RDDOperationGraph]
  private[ui] val completedStageIds = new mutable.HashSet[Int]

  // Keep track of the order in which these are inserted so we can remove old ones
  //跟踪这些插入的顺序，以便我们可以删除旧的
  private[ui] val jobIds = new mutable.ArrayBuffer[Int]
  private[ui] val stageIds = new mutable.ArrayBuffer[Int]

  // How many jobs or stages to retain graph metadata for
  //有多少工作或阶段来保留图表元数据
  private val retainedJobs =
    conf.getInt("spark.ui.retainedJobs", SparkUI.DEFAULT_RETAINED_JOBS)
  private val retainedStages =
    conf.getInt("spark.ui.retainedStages", SparkUI.DEFAULT_RETAINED_STAGES)

  /**
   * Return the graph metadata for all stages in the given job.
   * An empty list is returned if one or more of its stages has been cleaned up.
    * 返回给定作业中所有阶段的图元数据,如果一个或多个阶段已被清理,则返回空列表。
   */
  def getOperationGraphForJob(jobId: Int): Seq[RDDOperationGraph] = synchronized {
    val skippedStageIds = jobIdToSkippedStageIds.get(jobId).getOrElse(Seq.empty)
    val graphs = jobIdToStageIds.get(jobId)
      .getOrElse(Seq.empty)
      .flatMap { sid => stageIdToGraph.get(sid) }
    // Mark any skipped stages as such 标记任何跳过的阶段
    graphs.foreach { g =>
      val stageId = g.rootCluster.id.replaceAll(RDDOperationGraph.STAGE_CLUSTER_PREFIX, "").toInt
      if (skippedStageIds.contains(stageId) && !g.rootCluster.name.contains("skipped")) {
        g.rootCluster.setName(g.rootCluster.name + " (skipped)")
      }
    }
    graphs
  }

  /** Return the graph metadata for the given stage, or None if no such information exists.
    * 返回给定阶段的图表元数据;如果没有此类信息，则返回None*/
  def getOperationGraphForStage(stageId: Int): Option[RDDOperationGraph] = synchronized {
    stageIdToGraph.get(stageId)
  }

  /** On job start, construct a RDDOperationGraph for each stage in the job for display later.
    * 在作业开始时，为作业中的每个阶段构建RDDOperationGraph，以便稍后显示*/
  override def onJobStart(jobStart: SparkListenerJobStart): Unit = synchronized {
    val jobId = jobStart.jobId
    val stageInfos = jobStart.stageInfos

    jobIds += jobId
    jobIdToStageIds(jobId) = jobStart.stageInfos.map(_.stageId).sorted

    stageInfos.foreach { stageInfo =>
      val stageId = stageInfo.stageId
      stageIds += stageId
      stageIdToJobId(stageId) = jobId
      stageIdToGraph(stageId) = RDDOperationGraph.makeOperationGraph(stageInfo)
      trimStagesIfNecessary()
    }

    trimJobsIfNecessary()
  }

  /** Keep track of stages that have completed.
    * 跟踪已完成的阶段*/
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = synchronized {
    val stageId = stageCompleted.stageInfo.stageId
    if (stageIdToJobId.contains(stageId)) {
      // Note: Only do this if the stage has not already been cleaned up
      // Otherwise, we may never clean this stage from `completedStageIds`
      //注意：只有在舞台尚未清理的情况下才可以做到,否则我们可能永远不会从“completedStageIds”
      completedStageIds += stageCompleted.stageInfo.stageId
    }
  }

  /** On job end, find all stages in this job that are skipped and mark them as such.
    * 在工作结束时,找到被跳过的这个工作的所有阶段,并将其标记为这样。
    * */
  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = synchronized {
    val jobId = jobEnd.jobId
    jobIdToStageIds.get(jobId).foreach { stageIds =>
      val skippedStageIds = stageIds.filter { sid => !completedStageIds.contains(sid) }
      // Note: Only do this if the job has not already been cleaned up
      // Otherwise, we may never clean this job from `jobIdToSkippedStageIds`
      //注意：只有在作业尚未清理的情况下才可以执行此操作。否则，我们可能永远不会从“jobIdToSkippedStageIds”中清除此作业。
      jobIdToSkippedStageIds(jobId) = skippedStageIds
    }
  }

  /** Clean metadata for old stages if we have exceeded the number to retain.
    * 如果我们已经超过要保留的数字，则清除旧阶段的元数据。 */
  private def trimStagesIfNecessary(): Unit = {
    if (stageIds.size >= retainedStages) {
      val toRemove = math.max(retainedStages / 10, 1)
      stageIds.take(toRemove).foreach { id => cleanStage(id) }
      stageIds.trimStart(toRemove)
    }
  }

  /** Clean metadata for old jobs if we have exceeded the number to retain.
    * 如果我们已经超过要保留的号码，则清除旧作业的元数据。*/
  private def trimJobsIfNecessary(): Unit = {
    if (jobIds.size >= retainedJobs) {
      val toRemove = math.max(retainedJobs / 10, 1)
      jobIds.take(toRemove).foreach { id => cleanJob(id) }
      jobIds.trimStart(toRemove)
    }
  }

  /** Clean metadata for the given stage, its job, and all other stages that belong to the job.
    * 清除给定阶段，其工作以及属于该工作的所有其他阶段的元数据。 */
  private[ui] def cleanStage(stageId: Int): Unit = {
    completedStageIds.remove(stageId)
    stageIdToGraph.remove(stageId)
    stageIdToJobId.remove(stageId).foreach { jobId => cleanJob(jobId) }
  }

  /** Clean metadata for the given job and all stages that belong to it.
    * 清除给定作业的元数据以及属于该作业的所有阶段*/
  private[ui] def cleanJob(jobId: Int): Unit = {
    jobIdToSkippedStageIds.remove(jobId)
    jobIdToStageIds.remove(jobId).foreach { stageIds =>
      stageIds.foreach { stageId => cleanStage(stageId) }
    }
  }

}
