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

/**
 * An object that waits for a DAGScheduler job to complete. As tasks finish, it passes their
 * results to the given handler function.
 * 一个对象用来监听dagscheduler Job执行状态,Job是由多个task组织的tasks,
 * 因此只有Job的所有Task都完成,Job才标记完成,任意一个Task失败都标记该Job失败 
 * 
 */
private[spark] class JobWaiter[T](
    dagScheduler: DAGScheduler,
    val jobId: Int,
    totalTasks: Int,
    resultHandler: (Int, T) => Unit)
  extends JobListener {

  private var finishedTasks = 0

  // Is the job as a whole finished (succeeded or failed)?
  // 一个任务全完成或失败
  @volatile
  private var _jobFinished = totalTasks == 0

  def jobFinished: Boolean = _jobFinished

  // If the job is finished, this will be its result. In the case of 0 task jobs (e.g. zero
  // partition RDDs), we set the jobResult directly to JobSucceeded.
  private var jobResult: JobResult = if (jobFinished) JobSucceeded else null

  /**
   * Sends a signal to the DAGScheduler to cancel the job. The cancellation itself is handled
   * asynchronously. After the low level scheduler cancels all the tasks belonging to this job, it
   * will fail this job with a SparkException.
   */
  def cancel() {
    dagScheduler.cancelJob(jobId)
  }
/**
 * 1)resultHandler代表匿名函数,通过回调此匿名函数,将当前任务的结果加入最终结果集
 * 2)finishedTasks自增,当完成任务数finishedTasks等于全部任务数totalTasks时,标记Job完成,并且唤醒等待的线程
 *   即执行awaitResult方法线程,
 * 3)此方法被阻塞
 */
  override def taskSucceeded(index: Int, result: Any): Unit = synchronized {
    if (_jobFinished) {
      throw new UnsupportedOperationException("taskSucceeded() called on a finished JobWaiter")
    }
    //调用用户逻辑处理结果
    resultHandler(index, result.asInstanceOf[T])
    //只有Job的所有Task都完成,Job才标记完成,任意一个Task失败都标记该Job失败 
    finishedTasks += 1
    if (finishedTasks == totalTasks) {//该Job结束
      _jobFinished = true
      jobResult = JobSucceeded
      //会通知JobWaiter.awaitResult任务结束
      this.notifyAll()
    }
  }

  override def jobFailed(exception: Exception): Unit = synchronized {
    _jobFinished = true
    jobResult = JobFailed(exception)
    this.notifyAll()
  }

  def awaitResult(): JobResult = synchronized {
    while (!_jobFinished) {
      this.wait()
    }
    return jobResult
  }
}
