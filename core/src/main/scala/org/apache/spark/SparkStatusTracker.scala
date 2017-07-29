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

/**
 * Low-level status reporting APIs for monitoring job and stage progress.
  * 用于监控作业和阶段进度的低级状态报告API
 *
 * These APIs intentionally provide very weak consistency semantics; consumers of these APIs should
 * be prepared to handle empty / missing information.  For example, a job's stage ids may be known
 * but the status API may not have any information about the details of those stages, so
 * `getStageInfo` could potentially return `None` for a valid stage id.
 *这些API有意提供非常弱的一致性语义,这些API的消费者应该准备好处理空/丢失的信息,
  * 例如，作业的阶段id可能是已知的，但是状态API可能没有关于这些阶段的细节的任何信息，因此`getStageInfo'可能会为有效的阶段id返回`None`。
 * To limit memory usage, these APIs only provide information on recent jobs / stages.  These APIs
 * will provide information for the last `spark.ui.retainedStages` stages and
 * `spark.ui.retainedJobs` jobs.
 *为了限制内存使用量,这些API仅提供有关最近作业/阶段的信息,这些API将为最后一个“spark.ui.retainedStages”阶段和“spark.ui.retainedJobs”作业提供信息。
 * NOTE: this class's constructor should be considered private and may be subject to change.
  * 注意：该类的构造函数应该被视为私有的,并且可能会发生变化。
 */
class SparkStatusTracker private[spark] (sc: SparkContext) {

  private val jobProgressListener = sc.jobProgressListener

  /**
   * Return a list of all known jobs in a particular job group.  If `jobGroup` is `null`, then
   * returns all known jobs that are not associated with a job group.
   * 返回一个特定工作组中所有已知的工作的列表,如果`jobGroup`为'null'，则返回与作业组不相关联的所有已知作业。
   * The returned list may contain running, failed, and completed jobs, and may vary across
   * invocations of this method.  This method does not guarantee the order of the elements in
   * its result.
    * 返回的列表可能包含运行,失败和已完成的作业,并且可能会因此方法的调用,该方法不保证其结果中元素的顺序。
   */
  def getJobIdsForGroup(jobGroup: String): Array[Int] = {
    jobProgressListener.synchronized {
      jobProgressListener.jobGroupToJobIds.getOrElse(jobGroup, Seq.empty).toArray
    }
  }

  /**
   * Returns an array containing the ids of all active stages.
   * 返回包含所有活动阶段的stageId的数组。
   * This method does not guarantee the order of the elements in its result.
   * 这种方法不能保证其结果的元素的顺序
   */
  def getActiveStageIds(): Array[Int] = {
    jobProgressListener.synchronized {
      jobProgressListener.activeStages.values.map(_.stageId).toArray
    }
  }

  /**
   * Returns an array containing the ids of all active jobs.
   * 返回包含所有活动Job的JobId的数组,该方法不保证其结果中元素的顺序
   * This method does not guarantee the order of the elements in its result.
   */
  def getActiveJobIds(): Array[Int] = {
    jobProgressListener.synchronized {
      jobProgressListener.activeJobs.values.map(_.jobId).toArray
    }
  }

  /**
   * Returns job information, or `None` if the job info could not be found or was garbage collected.
   * 返回Job信息,如果返回None,找不到job信息或者垃圾回收了
   */
  def getJobInfo(jobId: Int): Option[SparkJobInfo] = {
    jobProgressListener.synchronized {
      jobProgressListener.jobIdToData.get(jobId).map { data =>
        new SparkJobInfoImpl(jobId, data.stageIds.toArray, data.status)
      }
    }
  }

  /**
   * 返回stage信息,如果返回None,找不到Stage信息或者垃圾回收了
   * Returns stage information, or `None` if the stage info could not be found or was
   * garbage collected.
   */
  def getStageInfo(stageId: Int): Option[SparkStageInfo] = {
    jobProgressListener.synchronized {
      for (
        info <- jobProgressListener.stageIdToInfo.get(stageId);
        data <- jobProgressListener.stageIdToData.get((stageId, info.attemptId))
      ) yield {
        new SparkStageInfoImpl(
          stageId,
          info.attemptId,
          info.submissionTime.getOrElse(0),
          info.name,
          info.numTasks,
          data.numActiveTasks,
          data.numCompleteTasks,
          data.numFailedTasks)
      }
    }
  }
}
