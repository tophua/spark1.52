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

import scala.collection.mutable.HashMap

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.storage.RDDInfo

/**
 * :: DeveloperApi ::
 * Stores information about a stage to pass from the scheduler to SparkListeners.
 * 存储一个阶段的信息,从调度程序到SparkListeners
 */
@DeveloperApi
class StageInfo(
    val stageId: Int,//ID
    val attemptId: Int,//尝试数
    val name: String,//名称
    val numTasks: Int,//任务数
    val rddInfos: Seq[RDDInfo],//RDD
    val parentIds: Seq[Int],//父Stage列表
    val details: String,
    private[spark] val taskLocalityPreferences: Seq[Seq[TaskLocation]] = Seq.empty) {
  /** 
   *  When this stage was submitted from the DAGScheduler to a TaskScheduler. 
   *  当阶段的所有任务提交的时间
   *  */
  var submissionTime: Option[Long] = None
  /** 
   *  Time when all tasks in the stage completed or when the stage was cancelled.
   *  当阶段的所有任务完成或取消阶段时的时间
   *   */
  var completionTime: Option[Long] = None
  /** 
   *  If the stage failed, the reason why. 
   *  如果阶段失败了,原因为什么
   *  */
  var failureReason: Option[String] = None
  /** 
   *  Terminal values of accumulables updated during this stage. 
   *  在这一阶段的累积数据更新值。
   *  */
  val accumulables = HashMap[Long, AccumulableInfo]()

  def stageFailed(reason: String) {
    failureReason = Some(reason)
    completionTime = Some(System.currentTimeMillis)
  }

  private[spark] def getStatusString: String = {
    if (completionTime.isDefined) {
      if (failureReason.isDefined) {
        "failed"
      } else {
        "succeeded"
      }
    } else {
      "running"
    }
  }
}

private[spark] object StageInfo {
  /**
   * Construct a StageInfo from a Stage.
   * 构造一个StageInfo信息,每个Stage关联一个或多个RDD,这是标记Shuffle依赖的边界.
   * Each Stage is associated with one or many RDDs, with the boundary of a Stage marked by
   * shuffle dependencies. 
   * 因此,所有祖先RDDs通过这个阶段(Stage)的RDD相关的窄依赖序列这应该该关联Stage
   * Therefore, all ancestor RDDs related to this Stage's RDD through a
   * sequence of narrow dependencies should also be associated with this Stage.
   */
  def fromStage(
      stage: Stage,
      attemptId: Int,
      numTasks: Option[Int] = None,
      taskLocalityPreferences: Seq[Seq[TaskLocation]] = Seq.empty
    ): StageInfo = {
    //方法获取RDD的所有直接或间接的NarrowDependency的RDD
    //RDDInfo.fromRdd创建RDDInfo信息,包括RDD父依赖关系
    val ancestorRddInfos = stage.rdd.getNarrowAncestors.map(RDDInfo.fromRdd)
    //对当前stage的RDD也生成RDDInfo,然后所有生成的RDDInfo合并到rddInfos
    val rddInfos = Seq(RDDInfo.fromRdd(stage.rdd)) ++ ancestorRddInfos
    new StageInfo(
      stage.id,
      attemptId,
      stage.name,
      numTasks.getOrElse(stage.numTasks),
      rddInfos,
      stage.parents.map(_.id),
      stage.details,
      taskLocalityPreferences)
  }
}
