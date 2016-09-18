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

import scala.collection.mutable.ListBuffer

import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 * Information about a running task attempt inside(内部) a TaskSet.
 * 任务集内一个正在运行任务的信息
 */
@DeveloperApi
class TaskInfo(
    val taskId: Long,
    val index: Int,
    val attemptNumber: Int,//尝试数
    val launchTime: Long,//开始时间
    val executorId: String,
    val host: String,//主要
    val taskLocality: TaskLocality.TaskLocality,//任务位置策略 
    val speculative: Boolean) {//speculative是否使用推理

  /**
   * The time when the task started remotely getting the result.(任务远程正在获得结果) Will not be set if the
   * task result was sent immediately when the task finished (as opposed to sending an
   * IndirectTaskResult and later fetching the result from the block manager).
   */
  var gettingResultTime: Long = 0

  /**
   * Intermediate updates to accumulables during this task. Note that it is valid for the same
   * accumulable to be updated multiple times in a single task or for two accumulables with the
   * same name but different IDs to exist in a task.
   */
  val accumulables = ListBuffer[AccumulableInfo]()

  /**
   * The time when the task has completed successfully (including the time to remotely fetch
   * results, if necessary).
   * 任务完成时间,包括完成时间失败时间
   */
  var finishTime: Long = 0
  //任务失败
  var failed = false

  private[spark] def markGettingResult(time: Long = System.currentTimeMillis) {
    gettingResultTime = time
  }

  private[spark] def markSuccessful(time: Long = System.currentTimeMillis) {
    finishTime = time
  }

  private[spark] def markFailed(time: Long = System.currentTimeMillis) {
    finishTime = time
    failed = true
  }

  def gettingResult: Boolean = gettingResultTime != 0

  def finished: Boolean = finishTime != 0

  def successful: Boolean = finished && !failed

  def running: Boolean = !finished

  def status: String = {
    if (running) {
      if (gettingResult) {
        "GET RESULT"
      } else {
        "RUNNING"
      }
    } else if (failed) {
      "FAILED"
    } else if (successful) {
      "SUCCESS"
    } else {
      "UNKNOWN"
    }
  }

  @deprecated("Use attemptNumber", "1.6.0")
  def attempt: Int = attemptNumber

  def id: String = s"$index.$attemptNumber"
  //任务执行的持续的时间
  def duration: Long = {
    if (!finished) {
      throw new UnsupportedOperationException("duration() called on unfinished task")
    } else {
      finishTime - launchTime
    }
  }

  private[spark] def timeRunning(currentTime: Long): Long = currentTime - launchTime
}
