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

package org.apache.spark.api.java

import org.apache.spark.{SparkStageInfo, SparkJobInfo, SparkContext}

/**
 * Low-level status reporting APIs for monitoring job and stage progress.
 * 用于监视作业和阶段进度的低级状态报告API
 * These APIs intentionally provide very weak consistency semantics; consumers of these APIs should
 * be prepared to handle empty / missing information.  For example, a job's stage ids may be known
 * but the status API may not have any information about the details of those stages, so
 * `getStageInfo` could potentially return `null` for a valid stage id.
 *
 * To limit memory usage, these APIs only provide information on recent jobs / stages.  These APIs
 * will provide information for the last `spark.ui.retainedStages` stages and
 * `spark.ui.retainedJobs` jobs.
 *
 * NOTE: this class's constructor should be considered private and may be subject to change.
 */
class JavaSparkStatusTracker private[spark] (sc: SparkContext) {

  /**
   * Return a list of all known jobs in a particular job group.  If `jobGroup` is `null`, then
   * returns all known jobs that are not associated with a job group.
    * 返回特定作业组中所有已知作业的列表,如果`jobGroup`为`null`,则返回与作业组无关的所有已知作业。
   *
   * The returned list may contain running, failed, and completed jobs, and may vary across
   * invocations of this method.  This method does not guarantee the order of the elements in
   * its result.
    * 返回的列表可能包含正在运行,失败和已完成的作业,并且可能会因此方法的调用而异,
    * 此方法不保证元素在其结果中的顺序。
   */
  def getJobIdsForGroup(jobGroup: String): Array[Int] = sc.statusTracker.getJobIdsForGroup(jobGroup)

  /**
   * Returns an array containing the ids of all active stages.
    * 返回包含所有活动阶段的ID的数组
   *
   * This method does not guarantee the order of the elements in its result.
    * 此方法不保证元素在其结果中的顺序
   */
  def getActiveStageIds(): Array[Int] = sc.statusTracker.getActiveStageIds()

  /**
   * Returns an array containing the ids of all active jobs.
    * 返回包含所有活动作业的ID的数组
   *
   * This method does not guarantee the order of the elements in its result.
    * 此方法不保证元素在其结果中的顺序
   */
  def getActiveJobIds(): Array[Int] = sc.statusTracker.getActiveJobIds()

  /**
   * Returns job information, or `null` if the job info could not be found or was garbage collected.
    * 返回作业信息，如果找不到作业信息或者是垃圾回收，则返回“null”
   */
  def getJobInfo(jobId: Int): SparkJobInfo = sc.statusTracker.getJobInfo(jobId).orNull

  /**
   * Returns stage information, or `null` if the stage info could not be found or was
   * garbage collected.
    * 返回阶段信息,如果找不到阶段信息或者是垃圾收集,则返回“null”
   */
  def getStageInfo(stageId: Int): SparkStageInfo = sc.statusTracker.getStageInfo(stageId).orNull
}
