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

import org.apache.spark.JobExecutionStatus
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.{AccumulableInfo, TaskInfo}
import org.apache.spark.util.collection.OpenHashSet

import scala.collection.mutable
import scala.collection.mutable.HashMap

private[spark] object UIData {

  class ExecutorSummary {
    var taskTime : Long = 0//任务时间
    var failedTasks : Int = 0//失败任务数
    var succeededTasks : Int = 0//完成任务数
    var inputBytes : Long = 0
    var inputRecords : Long = 0
    var outputBytes : Long = 0
    var outputRecords : Long = 0
    var shuffleRead : Long = 0
    var shuffleReadRecords : Long = 0
    var shuffleWrite : Long = 0
    var shuffleWriteRecords : Long = 0
    var memoryBytesSpilled : Long = 0
    var diskBytesSpilled : Long = 0
  }

  class JobUIData(
    var jobId: Int = -1,
    var submissionTime: Option[Long] = None,//提交时间
    var completionTime: Option[Long] = None,//完成时间
    var stageIds: Seq[Int] = Seq.empty,
    var jobGroup: Option[String] = None,
    var status: JobExecutionStatus = JobExecutionStatus.UNKNOWN,
    /* Tasks */
    // `numTasks` is a potential underestimate of the true number of tasks that this job will run.
    // This may be an underestimate because the job start event references all of the result
    // stages' transitive stage dependencies, but some of these stages might be skipped if their
    // output is available from earlier runs.
    //`numTasks`是一个潜在的低估这个工作将运行的真正的任务数量。这可能是一个低估，
    // 因为作业开始事件引用所有结果阶段的传递阶段依赖关系，但是如果其输出可以从较早的运行中获取，则可能会跳过其中一些阶段。
    // See https://github.com/apache/spark/pull/3009 for a more extensive discussion.
    var numTasks: Int = 0,
    var numActiveTasks: Int = 0,
    var numCompletedTasks: Int = 0,
    var numSkippedTasks: Int = 0,
    var numFailedTasks: Int = 0,
    /* Stages */
    var numActiveStages: Int = 0,
    // This needs to be a set instead of a simple count to prevent double-counting of rerun stages:
    //这需要一个集合而不是一个简单的计数,以防止重新运行阶段的重复计数：
    var completedStageIndices: mutable.HashSet[Int] = new mutable.HashSet[Int](),
    var numSkippedStages: Int = 0,
    var numFailedStages: Int = 0
  )

  class StageUIData {
    var numActiveTasks: Int = _//活动任务数
    var numCompleteTasks: Int = _//完成任务数
    var completedIndices = new OpenHashSet[Int]() //
    var numFailedTasks: Int = _ //失败任务数

    var executorRunTime: Long = _ //运行执行时间

    var inputBytes: Long = _
    var inputRecords: Long = _
    var outputBytes: Long = _
    var outputRecords: Long = _
    var shuffleReadTotalBytes: Long = _
    var shuffleReadRecords : Long = _
    var shuffleWriteBytes: Long = _
    var shuffleWriteRecords: Long = _
    var memoryBytesSpilled: Long = _
    var diskBytesSpilled: Long = _

    var schedulingPool: String = ""
    var description: Option[String] = None

    var accumulables = new HashMap[Long, AccumulableInfo]
    var taskData = new HashMap[Long, TaskUIData]
    var executorSummary = new HashMap[String, ExecutorSummary]

    def hasInput: Boolean = inputBytes > 0
    def hasOutput: Boolean = outputBytes > 0
    def hasShuffleRead: Boolean = shuffleReadTotalBytes > 0
    def hasShuffleWrite: Boolean = shuffleWriteBytes > 0
    def hasBytesSpilled: Boolean = memoryBytesSpilled > 0 && diskBytesSpilled > 0
  }

  /**
   * These are kept mutable and reused throughout a task's lifetime to avoid excessive reallocation.
    * 这些在整个任务的生命周期中保持可变和重复使用,以避免过度重新分配。
   */
  case class TaskUIData(
      var taskInfo: TaskInfo,
      var taskMetrics: Option[TaskMetrics] = None,
      var errorMessage: Option[String] = None)

  case class ExecutorUIData(
      val startTime: Long,
      var finishTime: Option[Long] = None,
      var finishReason: Option[String] = None)
}
