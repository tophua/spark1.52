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

package org.apache.spark.ui.jobs

import java.util.concurrent.TimeoutException

import scala.collection.mutable.{HashMap, HashSet, ListBuffer}

import com.google.common.annotations.VisibleForTesting

import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.ui.SparkUI
import org.apache.spark.ui.jobs.UIData._

/**
 * :: DeveloperApi ::
 * JobProgressListener 通过监听listenerBus中的事件更新任务进度,SparkStatusTracker和SparkUI实际上也是通过JobProgressListener
 * 来实现任务状态跟踪
 * Tracks task-level information to be displayed in the UI.
 * 构造JobProgressListener,作用是通过HashMap,ListBuffer等数据结构存储JobId及对应JobUIData信息,
        并按照激活,完成,失败等job状态统计,对于StageId,StageInfo等信息按照激活,完成,忽略,失败等Stage状态统计,
        并且存储StageID与JobId的一对多关系.
 * All access to the data structures in this class must be synchronized on the
 * class, since the UI thread and the EventBus loop may otherwise be reading and
 * updating the internal data structures concurrently.
  * 对类中的数据结构的所有访问必须在类上同步,因为UI线程和EventBus循环可能会同时读取和更新内部数据结构。
 */
@DeveloperApi
class JobProgressListener(conf: SparkConf) extends SparkListener with Logging {

  // Define a handful of type aliases so that data structures' types can serve as documentation.
  //定义一些类型的别名,以便数据结构的类型可以作为文档。
  // These type aliases are public because they're used in the types of public fields:
  //这些类型的别名是公共的，因为它们用于公共字段的类型：

  type JobId = Int
  type JobGroupId = String
  type StageId = Int
  type StageAttemptId = Int
  type PoolName = String
  type ExecutorId = String

  // Applicatin:
  @volatile var startTime = -1L

  // Jobs:
  val activeJobs = new HashMap[JobId, JobUIData]//活动Jobs
  val completedJobs = ListBuffer[JobUIData]()//完成Jobs
  val failedJobs = ListBuffer[JobUIData]()//失败Jobs
  val jobIdToData = new HashMap[JobId, JobUIData]//JobId到Job数据
  val jobGroupToJobIds = new HashMap[JobGroupId, HashSet[JobId]]

  // Stages:
  val pendingStages = new HashMap[StageId, StageInfo]//未发现Stages
  val activeStages = new HashMap[StageId, StageInfo]//活动Stage
  val completedStages = ListBuffer[StageInfo]()//完成Stage
  val skippedStages = ListBuffer[StageInfo]()//忽略Stage
  val failedStages = ListBuffer[StageInfo]()//失败Stage
  val stageIdToData = new HashMap[(StageId, StageAttemptId), StageUIData]
  val stageIdToInfo = new HashMap[StageId, StageInfo]
  val stageIdToActiveJobIds = new HashMap[StageId, HashSet[JobId]]
  val poolToActiveStages = HashMap[PoolName, HashMap[StageId, StageInfo]]()
  // Total of completed and failed stages that have ever been run.  These may be greater than
  // `completedStages.size` and `failedStages.size` if we have run more stages or jobs than
  // JobProgressListener's retention limits.
  //已经运行的完成和失败阶段的总计,如果我们比JobProgressListener的保留限制运行更多的阶段或作业,
  // 这些可能大于`completedStages.size`和`failedStages.size'
  var numCompletedStages = 0 //总共完成的stage数量
  var numFailedStages = 0 //总共完成失败的stage数量
  var numCompletedJobs = 0 //总共完成jobs数量
  var numFailedJobs = 0 //总共完成失败jobs数量

  // Misc:
  val executorIdToBlockManagerId = HashMap[ExecutorId, BlockManagerId]()

  def blockManagerIds: Seq[BlockManagerId] = executorIdToBlockManagerId.values.toSeq

  var schedulingMode: Option[SchedulingMode] = None

  // To limit the total memory usage of JobProgressListener, we only track information for a fixed
  // number of non-active jobs and stages (there is no limit for active jobs and stages):
  //为了限制JobProgressListener的总内存使用量,我们只跟踪固定数量的非活动作业和阶段的信息(活动作业和阶段没有限制)：
  //在GC之前保留的stage数量
  val retainedStages = conf.getInt("spark.ui.retainedStages", SparkUI.DEFAULT_RETAINED_STAGES)
  val retainedJobs = conf.getInt("spark.ui.retainedJobs", SparkUI.DEFAULT_RETAINED_JOBS)

  // We can test for memory leaks by ensuring that collections that track non-active jobs and
  // stages do not grow without bound and that collections for active jobs/stages eventually become
  // empty once Spark is idle.  Let's partition our collections into ones that should be empty
  // once Spark is idle and ones that should have a hard- or soft-limited sizes.
  // These methods are used by unit tests, but they're defined here so that people don't forget to
  // update the tests when adding new collections.  Some collections have multiple levels of
  // nesting, etc, so this lets us customize our notion of "size" for each structure:
  //我们可以通过确保跟踪非活动作业和阶段的集合不会无限增长来测试内存泄漏,并且一旦Spark处于空闲状态,则活动作业/阶段的集合最终将变为空。
  // 我们将我们的集合分成一个，一旦Spark是空闲的，应该是空的，应该有一个硬或软限制的大小。
  // 这些方法是通过单元测试使用的，但是它们在这里定义，以便人们在添加新集合时不要忘记更新测试。
  // 一些集合有多个层次的嵌套等，所以这让我们可以自定义我们对每个结构的“大小”的概念：
  // These collections should all be empty once Spark is idle (no active stages / jobs):
  //一旦Spark闲置（没有活动阶段/作业）,这些集合都应该是空的：
  private[spark] def getSizesOfActiveStateTrackingCollections: Map[String, Int] = {
    Map(
      "activeStages" -> activeStages.size,
      "activeJobs" -> activeJobs.size,
      "poolToActiveStages" -> poolToActiveStages.values.map(_.size).sum,
      "stageIdToActiveJobIds" -> stageIdToActiveJobIds.values.map(_.size).sum
    )
  }

  // These collections should stop growing once we have run at least `spark.ui.retainedStages`
  // stages and `spark.ui.retainedJobs` jobs:
  //一旦我们至少运行了'spark.ui.retainedStages`阶段和`spark.ui.retainedJobs`作业，这些集合应该停止增长
  private[spark] def getSizesOfHardSizeLimitedCollections: Map[String, Int] = {
    Map(
      "completedJobs" -> completedJobs.size,
      "failedJobs" -> failedJobs.size,
      "completedStages" -> completedStages.size,
      "skippedStages" -> skippedStages.size,
      "failedStages" -> failedStages.size
    )
  }

  // These collections may grow arbitrarily, but once Spark becomes idle they should shrink back to
  // some bound based on the `spark.ui.retainedStages` and `spark.ui.retainedJobs` settings:
  //这些集合可能会任意增长，但一旦Spark变得空闲，它们将根据“spark.ui.retainedStages”和“spark.ui.retainedJobs”设置缩小到一些约束条件：
  private[spark] def getSizesOfSoftSizeLimitedCollections: Map[String, Int] = {
    Map(
      "jobIdToData" -> jobIdToData.size,
      "stageIdToData" -> stageIdToData.size,
      "stageIdToStageInfo" -> stageIdToInfo.size,
      "jobGroupToJobIds" -> jobGroupToJobIds.values.map(_.size).sum,
      // Since jobGroupToJobIds is map of sets, check that we don't leak keys with empty values:
      //由于jobGroupToJobIds是集合的映射，请检查我们不会将空值泄漏密钥：
      "jobGroupToJobIds keySet" -> jobGroupToJobIds.keys.size
    )
  }

  /** If stages is too large, remove and garbage collect old stages
    * 如果阶段太大，删除和垃圾收集旧阶段*/
  private def trimStagesIfNecessary(stages: ListBuffer[StageInfo]) = synchronized {
    if (stages.size > retainedStages) {
      val toRemove = math.max(retainedStages / 10, 1)
      stages.take(toRemove).foreach { s =>
        stageIdToData.remove((s.stageId, s.attemptId))
        stageIdToInfo.remove(s.stageId)
      }
      stages.trimStart(toRemove)
    }
  }

  /** If jobs is too large, remove and garbage collect old jobs
    * 如果作业太大，请移除垃圾收集旧作业*/
  private def trimJobsIfNecessary(jobs: ListBuffer[JobUIData]) = synchronized {
    if (jobs.size > retainedJobs) {
      val toRemove = math.max(retainedJobs / 10, 1)
      jobs.take(toRemove).foreach { job =>
        // Remove the job's UI data, if it exists
        //删除作业的UI数据（如果存在）
        jobIdToData.remove(job.jobId).foreach { removedJob =>
          // A null jobGroupId is used for jobs that are run without a job group
          //null jobGroupId用于没有作业组运行的作业
          val jobGroupId = removedJob.jobGroup.orNull
          // Remove the job group -> job mapping entry, if it exists
          //删除作业组 - >作业映射条目（如果存在）
          jobGroupToJobIds.get(jobGroupId).foreach { jobsInGroup =>
            jobsInGroup.remove(job.jobId)
            // If this was the last job in this job group, remove the map entry for the job group
            //如果这是此作业组中的最后一个作业,请删除作业组的映射条目
            if (jobsInGroup.isEmpty) {
              jobGroupToJobIds.remove(jobGroupId)
            }
          }
        }
      }
      jobs.trimStart(toRemove)
    }
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = synchronized {
    val jobGroup = for (
      props <- Option(jobStart.properties);
      group <- Option(props.getProperty(SparkContext.SPARK_JOB_GROUP_ID))
    ) yield group
    val jobData: JobUIData =
      new JobUIData(
        jobId = jobStart.jobId,
        submissionTime = Option(jobStart.time).filter(_ >= 0),
        stageIds = jobStart.stageIds,
        jobGroup = jobGroup,
        status = JobExecutionStatus.RUNNING)
    // A null jobGroupId is used for jobs that are run without a job group
    //null jobGroupId用于没有作业组运行的作业
    jobGroupToJobIds.getOrElseUpdate(jobGroup.orNull, new HashSet[JobId]).add(jobStart.jobId)
    jobStart.stageInfos.foreach(x => pendingStages(x.stageId) = x)
    // Compute (a potential underestimate of) the number of tasks that will be run by this job.
    // This may be an underestimate because the job start event references all of the result
    // stages' transitive stage dependencies, but some of these stages might be skipped if their
    // output is available from earlier runs.
    //计算（潜在低估）此作业将运行的任务数量。这可能是低估的，因为作业开始事件引用了所有的结果
    //阶段的传递阶段依赖关系，但是如果从早期的运行可以获得其输出，则可能会跳过这些阶段中的一些阶段。
    // See https://github.com/apache/spark/pull/3009 for a more extensive discussion.
    jobData.numTasks = {
      val allStages = jobStart.stageInfos
      val missingStages = allStages.filter(_.completionTime.isEmpty)
      missingStages.map(_.numTasks).sum
    }
    jobIdToData(jobStart.jobId) = jobData
    activeJobs(jobStart.jobId) = jobData
    for (stageId <- jobStart.stageIds) {
      stageIdToActiveJobIds.getOrElseUpdate(stageId, new HashSet[StageId]).add(jobStart.jobId)
    }
    // If there's no information for a stage, store the StageInfo received from the scheduler
    // so that we can display stage descriptions for pending stages:
    //如果没有一个阶段的信息,存储从调度程序接收到的StageInfo,以便我们可以显示待处理阶段的阶段描述：
    for (stageInfo <- jobStart.stageInfos) {
      stageIdToInfo.getOrElseUpdate(stageInfo.stageId, stageInfo)
      stageIdToData.getOrElseUpdate((stageInfo.stageId, stageInfo.attemptId), new StageUIData)
    }
  }
/**
* 作业Job完成
**/
  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = synchronized {
    val jobData = activeJobs.remove(jobEnd.jobId).getOrElse {
      logWarning(s"Job completed for unknown job ${jobEnd.jobId}")
      new JobUIData(jobId = jobEnd.jobId)
    }
    jobData.completionTime = Option(jobEnd.time).filter(_ >= 0)

    jobData.stageIds.foreach(pendingStages.remove)
    jobEnd.jobResult match {
      case JobSucceeded =>
        completedJobs += jobData
        trimJobsIfNecessary(completedJobs)
        jobData.status = JobExecutionStatus.SUCCEEDED
        numCompletedJobs += 1
      case JobFailed(exception) =>
        failedJobs += jobData
        trimJobsIfNecessary(failedJobs)
        jobData.status = JobExecutionStatus.FAILED
        numFailedJobs += 1
    }
    for (stageId <- jobData.stageIds) {
      stageIdToActiveJobIds.get(stageId).foreach { jobsUsingStage =>
        jobsUsingStage.remove(jobEnd.jobId)
        if (jobsUsingStage.isEmpty) {
          stageIdToActiveJobIds.remove(stageId)
        }
        stageIdToInfo.get(stageId).foreach { stageInfo =>
          if (stageInfo.submissionTime.isEmpty) {
            // if this stage is pending, it won't complete, so mark it as "skipped":
            //如果这个阶段正在等待，它将不会完成，所以将其标记为“跳过”：
            skippedStages += stageInfo
            trimStagesIfNecessary(skippedStages)
            jobData.numSkippedStages += 1
            jobData.numSkippedTasks += stageInfo.numTasks
          }
        }
      }
    }
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = synchronized {
    val stage = stageCompleted.stageInfo
    stageIdToInfo(stage.stageId) = stage
    val stageData = stageIdToData.getOrElseUpdate((stage.stageId, stage.attemptId), {
      logWarning("Stage completed for unknown stage " + stage.stageId)
      new StageUIData
    })

    for ((id, info) <- stageCompleted.stageInfo.accumulables) {
      stageData.accumulables(id) = info
    }

    poolToActiveStages.get(stageData.schedulingPool).foreach { hashMap =>
      hashMap.remove(stage.stageId)
    }
    activeStages.remove(stage.stageId)
    if (stage.failureReason.isEmpty) {
      completedStages += stage
      numCompletedStages += 1
      trimStagesIfNecessary(completedStages)
    } else {
      failedStages += stage
      numFailedStages += 1
      trimStagesIfNecessary(failedStages)
    }

    for (
      activeJobsDependentOnStage <- stageIdToActiveJobIds.get(stage.stageId);
      jobId <- activeJobsDependentOnStage;
      jobData <- jobIdToData.get(jobId)
    ) {
      jobData.numActiveStages -= 1
      if (stage.failureReason.isEmpty) {
        if (!stage.submissionTime.isEmpty) {
          jobData.completedStageIndices.add(stage.stageId)
        }
      } else {
        jobData.numFailedStages += 1
      }
    }
  }

  /** For FIFO, all stages are contained by "default" pool but "default" pool here is meaningless
    * 对于FIFO，所有阶段由“默认”池包含，但这里的“默认”池是无意义的*/
  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = synchronized {
    val stage = stageSubmitted.stageInfo
    activeStages(stage.stageId) = stage
    pendingStages.remove(stage.stageId)
    val poolName = Option(stageSubmitted.properties).map {
      p => p.getProperty("spark.scheduler.pool", SparkUI.DEFAULT_POOL_NAME)
    }.getOrElse(SparkUI.DEFAULT_POOL_NAME)

    stageIdToInfo(stage.stageId) = stage
    val stageData = stageIdToData.getOrElseUpdate((stage.stageId, stage.attemptId), new StageUIData)
    stageData.schedulingPool = poolName

    stageData.description = Option(stageSubmitted.properties).flatMap {
      p => Option(p.getProperty(SparkContext.SPARK_JOB_DESCRIPTION))
    }

    val stages = poolToActiveStages.getOrElseUpdate(poolName, new HashMap[Int, StageInfo])
    stages(stage.stageId) = stage

    for (
      activeJobsDependentOnStage <- stageIdToActiveJobIds.get(stage.stageId);
      jobId <- activeJobsDependentOnStage;
      jobData <- jobIdToData.get(jobId)
    ) {
      jobData.numActiveStages += 1

      // If a stage retries again, it should be removed from completedStageIndices set
      //如果一个阶段再次重试，应该从completedStageIndices集中删除
      jobData.completedStageIndices.remove(stage.stageId)
    }
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = synchronized {
    val taskInfo = taskStart.taskInfo
    if (taskInfo != null) {
      val stageData = stageIdToData.getOrElseUpdate((taskStart.stageId, taskStart.stageAttemptId), {
        logWarning("Task start for unknown stage " + taskStart.stageId)
        new StageUIData
      })
      stageData.numActiveTasks += 1
      stageData.taskData.put(taskInfo.taskId, new TaskUIData(taskInfo))
    }
    for (
      activeJobsDependentOnStage <- stageIdToActiveJobIds.get(taskStart.stageId);
      jobId <- activeJobsDependentOnStage;
      jobData <- jobIdToData.get(jobId)
    ) {
      jobData.numActiveTasks += 1
    }
  }

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult) {
    // Do nothing: because we don't do a deep copy of the TaskInfo, the TaskInfo in
    // stageToTaskInfos already has the updated status.
    //什么都不做：因为我们不做TaskInfo的深层副本，所以StageToTaskInfos中的TaskInfo已经有了更新的状态。
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {
    val info = taskEnd.taskInfo
    // If stage attempt id is -1, it means the DAGScheduler had no idea which attempt this task
    // completion event is for. Let's just drop it here. This means we might have some speculation
    // tasks on the web ui that's never marked as complete.
    //如果stage尝试id为-1，则表示DAGScheduler不知道该尝试该任务完成事件是为。
    // 让我们放在这里。 这意味着我们可能在网站上有一些投机任务，从未被标记为完整。
    if (info != null && taskEnd.stageAttemptId != -1) {
      val stageData = stageIdToData.getOrElseUpdate((taskEnd.stageId, taskEnd.stageAttemptId), {
        logWarning("Task end for unknown stage " + taskEnd.stageId)
        new StageUIData
      })

      for (accumulableInfo <- info.accumulables) {
        stageData.accumulables(accumulableInfo.id) = accumulableInfo
      }

      val execSummaryMap = stageData.executorSummary
      val execSummary = execSummaryMap.getOrElseUpdate(info.executorId, new ExecutorSummary)

      taskEnd.reason match {
        case Success =>
          execSummary.succeededTasks += 1
        case _ =>
          execSummary.failedTasks += 1
      }
      execSummary.taskTime += info.duration
      stageData.numActiveTasks -= 1

      val (errorMessage, metrics): (Option[String], Option[TaskMetrics]) =
        taskEnd.reason match {
          case org.apache.spark.Success =>
            stageData.completedIndices.add(info.index)
            stageData.numCompleteTasks += 1
            (None, Option(taskEnd.taskMetrics))
            //处理异常失败，因为我们可能有指标
          case e: ExceptionFailure =>  // Handle ExceptionFailure because we might have metrics
            stageData.numFailedTasks += 1
            (Some(e.toErrorString), e.metrics)
            //所有其他故障案例
          case e: TaskFailedReason =>  // All other failure cases
            stageData.numFailedTasks += 1
            (Some(e.toErrorString), None)
        }

      if (!metrics.isEmpty) {
        val oldMetrics = stageData.taskData.get(info.taskId).flatMap(_.taskMetrics)
        updateAggregateMetrics(stageData, info.executorId, metrics.get, oldMetrics)
      }

      val taskData = stageData.taskData.getOrElseUpdate(info.taskId, new TaskUIData(info))
      taskData.taskInfo = info
      taskData.taskMetrics = metrics
      taskData.errorMessage = errorMessage

      for (
        activeJobsDependentOnStage <- stageIdToActiveJobIds.get(taskEnd.stageId);
        jobId <- activeJobsDependentOnStage;
        jobData <- jobIdToData.get(jobId)
      ) {
        jobData.numActiveTasks -= 1
        taskEnd.reason match {
          case Success =>
            jobData.numCompletedTasks += 1
          case _ =>
            jobData.numFailedTasks += 1
        }
      }
    }
  }

  /**
   * Upon receiving new metrics for a task, updates the per-stage and per-executor-per-stage
   * aggregate metrics by calculating deltas between the currently recorded metrics and the new
   * metrics.
    * 在收到任务的新指标后,通过计算当前记录的指标和新指标之间的增量来更新每阶段和每个执行者的每个阶段的汇总指标。
   */
  def updateAggregateMetrics(
      stageData: StageUIData,
      execId: String,
      taskMetrics: TaskMetrics,
      oldMetrics: Option[TaskMetrics]) {
    val execSummary = stageData.executorSummary.getOrElseUpdate(execId, new ExecutorSummary)

    val shuffleWriteDelta =
      (taskMetrics.shuffleWriteMetrics.map(_.shuffleBytesWritten).getOrElse(0L)
      - oldMetrics.flatMap(_.shuffleWriteMetrics).map(_.shuffleBytesWritten).getOrElse(0L))
    stageData.shuffleWriteBytes += shuffleWriteDelta
    execSummary.shuffleWrite += shuffleWriteDelta

    val shuffleWriteRecordsDelta =
      (taskMetrics.shuffleWriteMetrics.map(_.shuffleRecordsWritten).getOrElse(0L)
      - oldMetrics.flatMap(_.shuffleWriteMetrics).map(_.shuffleRecordsWritten).getOrElse(0L))
    stageData.shuffleWriteRecords += shuffleWriteRecordsDelta
    execSummary.shuffleWriteRecords += shuffleWriteRecordsDelta

    val shuffleReadDelta =
      (taskMetrics.shuffleReadMetrics.map(_.totalBytesRead).getOrElse(0L)
        - oldMetrics.flatMap(_.shuffleReadMetrics).map(_.totalBytesRead).getOrElse(0L))
    stageData.shuffleReadTotalBytes += shuffleReadDelta
    execSummary.shuffleRead += shuffleReadDelta

    val shuffleReadRecordsDelta =
      (taskMetrics.shuffleReadMetrics.map(_.recordsRead).getOrElse(0L)
      - oldMetrics.flatMap(_.shuffleReadMetrics).map(_.recordsRead).getOrElse(0L))
    stageData.shuffleReadRecords += shuffleReadRecordsDelta
    execSummary.shuffleReadRecords += shuffleReadRecordsDelta

    val inputBytesDelta =
      (taskMetrics.inputMetrics.map(_.bytesRead).getOrElse(0L)
      - oldMetrics.flatMap(_.inputMetrics).map(_.bytesRead).getOrElse(0L))
    stageData.inputBytes += inputBytesDelta
    execSummary.inputBytes += inputBytesDelta

    val inputRecordsDelta =
      (taskMetrics.inputMetrics.map(_.recordsRead).getOrElse(0L)
      - oldMetrics.flatMap(_.inputMetrics).map(_.recordsRead).getOrElse(0L))
    stageData.inputRecords += inputRecordsDelta
    execSummary.inputRecords += inputRecordsDelta

    val outputBytesDelta =
      (taskMetrics.outputMetrics.map(_.bytesWritten).getOrElse(0L)
        - oldMetrics.flatMap(_.outputMetrics).map(_.bytesWritten).getOrElse(0L))
    stageData.outputBytes += outputBytesDelta
    execSummary.outputBytes += outputBytesDelta

    val outputRecordsDelta =
      (taskMetrics.outputMetrics.map(_.recordsWritten).getOrElse(0L)
        - oldMetrics.flatMap(_.outputMetrics).map(_.recordsWritten).getOrElse(0L))
    stageData.outputRecords += outputRecordsDelta
    execSummary.outputRecords += outputRecordsDelta

    val diskSpillDelta =
      taskMetrics.diskBytesSpilled - oldMetrics.map(_.diskBytesSpilled).getOrElse(0L)
    stageData.diskBytesSpilled += diskSpillDelta
    execSummary.diskBytesSpilled += diskSpillDelta

    val memorySpillDelta =
      taskMetrics.memoryBytesSpilled - oldMetrics.map(_.memoryBytesSpilled).getOrElse(0L)
    stageData.memoryBytesSpilled += memorySpillDelta
    execSummary.memoryBytesSpilled += memorySpillDelta

    val timeDelta =
      taskMetrics.executorRunTime - oldMetrics.map(_.executorRunTime).getOrElse(0L)
    stageData.executorRunTime += timeDelta
  }

  override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate) {
    for ((taskId, sid, sAttempt, taskMetrics) <- executorMetricsUpdate.taskMetrics) {
      val stageData = stageIdToData.getOrElseUpdate((sid, sAttempt), {
        logWarning("Metrics update for task in unknown stage " + sid)
        new StageUIData
      })
      val taskData = stageData.taskData.get(taskId)
      taskData.map { t =>
        if (!t.taskInfo.finished) {
          updateAggregateMetrics(stageData, executorMetricsUpdate.execId, taskMetrics,
            t.taskMetrics)

          // Overwrite task metrics
          //覆盖任务指标
          t.taskMetrics = Some(taskMetrics)
        }
      }
    }
  }

  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate) {
    synchronized {
      schedulingMode = environmentUpdate
        .environmentDetails("Spark Properties").toMap
        .get("spark.scheduler.mode")//SparkContext对job进行调度所采用的模式
        .map(SchedulingMode.withName)
    }
  }

  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded) {
    synchronized {
      val blockManagerId = blockManagerAdded.blockManagerId
      val executorId = blockManagerId.executorId
      executorIdToBlockManagerId(executorId) = blockManagerId
    }
  }

  override def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved) {
    synchronized {
      val executorId = blockManagerRemoved.blockManagerId.executorId
      executorIdToBlockManagerId.remove(executorId)
    }
  }

  override def onApplicationStart(appStarted: SparkListenerApplicationStart) {
    startTime = appStarted.time
  }

  /**
   * For testing only. Wait until at least `numExecutors` executors are up, or throw
   * `TimeoutException` if the waiting time elapsed before `numExecutors` executors up.
   * Exposed for testing.
    * 仅用于测试。 等到至少`numExecutors`执行程序启动，
    * 或者在`numExecutors`执行程序之前等待时间过去的时候抛出'TimeoutException'。
   *
   * @param numExecutors the number of executors to wait at least 至少等待执行的数量
   * @param timeout time to wait in milliseconds 等待时间以毫秒为单位
   */
  private[spark] def waitUntilExecutorsUp(numExecutors: Int, timeout: Long): Unit = {
    val finishTime = System.currentTimeMillis() + timeout
    while (System.currentTimeMillis() < finishTime) {
      val numBlockManagers = synchronized {
        blockManagerIds.size
      }
      if (numBlockManagers >= numExecutors + 1) {
        // Need to count the block manager in driver
        //需要在驱动程序中计算块管理器
        return
      }
      // Sleep rather than using wait/notify, because this is used only for testing and wait/notify
      // add overhead in the general case.
      //睡眠而不是使用wait / notify,因为这仅用于测试和等待/通知在一般情况下添加开销,
      Thread.sleep(10)
    }
    throw new TimeoutException(
      s"Can't find $numExecutors executors before $timeout milliseconds elapsed")
  }
}
