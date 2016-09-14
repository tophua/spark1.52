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

import java.io.Serializable

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.metrics.source.Source
import org.apache.spark.unsafe.memory.TaskMemoryManager
import org.apache.spark.util.TaskCompletionListener


object TaskContext {
  /**
   * Return the currently active TaskContext. This can be called inside of
   * user functions to access contextual information about running tasks.
   * 返回当前运行任务的上下文信息
   */
  def get(): TaskContext = taskContext.get

  /**
   * Returns the partition id of currently active TaskContext. It will return 0
   * if there is no active TaskContext for cases like local execution.
   * 返回当前活动的上下文信息的分区ID
   */
  def getPartitionId(): Int = {
    val tc = taskContext.get()
    if (tc eq null) {
      0
    } else {
      tc.partitionId()
    }
  }

  private[this] val taskContext: ThreadLocal[TaskContext] = new ThreadLocal[TaskContext]

  // Note: protected[spark] instead of private[spark] to prevent the following two from
  // showing up in JavaDoc.
  /**
   * Set the thread local TaskContext. Internal to Spark.
   * 设置线程局部任务的上下文信息
   */
  protected[spark] def setTaskContext(tc: TaskContext): Unit = taskContext.set(tc)

  /**
   * Unset the thread local TaskContext. Internal to Spark.
   * 删除线程局部的任务上下文信息
   */
  protected[spark] def unset(): Unit = taskContext.remove()

  /**
   * An empty task context that does not represent an actual task.
   * 一个不代表实际任务的空任务上下文
   */
  private[spark] def empty(): TaskContextImpl = {
    new TaskContextImpl(0, 0, 0, 0, null, null, Seq.empty)
  }

}


/**
 * Contextual information about a task which can be read or mutated during
 * execution. To access the TaskContext for a running task, use:
 * 在执行过程中可以读取的任务的上下文信息,taskcontext访问正在运行的任务
 * {{{
 *   org.apache.spark.TaskContext.get()
 * }}}
 */
abstract class TaskContext extends Serializable {
  // Note: TaskContext must NOT define a get method. Otherwise it will prevent the Scala compiler
  // from generating a static get method (based on the companion object's get method).

  // Note: Update JavaTaskContextCompileCheck when new methods are added to this class.

  // Note: getters in this class are defined with parentheses to maintain backward compatibility.

  /**
   * Returns true if the task has completed.
   * 如果任务已完成，则返回真
   */
  def isCompleted(): Boolean

  /**
   * Returns true if the task has been killed.
   * 如果任务已被杀死，返回真
   */
  def isInterrupted(): Boolean

  @deprecated("use isRunningLocally", "1.2.0")
  def runningLocally(): Boolean

  /**
   * Returns true if the task is running locally in the driver program.
   * 如果任务在驱动程序中运行本地运行，则返回真
   * @return
   */
  def isRunningLocally(): Boolean

  /**
   * Adds a (Java friendly) listener to be executed on task completion.
   * This will be called in all situation - success, failure, or cancellation.
   * An example use is for HadoopRDD to register a callback to close the input stream.
   * 添加一个完成任务的执行的侦听器,这记录任务的成功,失败,或者取消.
   */
  def addTaskCompletionListener(listener: TaskCompletionListener): TaskContext

  /**
   * 添加一个完成任务的执行的侦听器,这记录任务的成功,失败,或者取消.
   * Adds a listener in the form of a Scala closure to be executed on task completion.
   * This will be called in all situations - success, failure, or cancellation.
   * An example use is for HadoopRDD to register a callback to close the input stream.
   */
  def addTaskCompletionListener(f: (TaskContext) => Unit): TaskContext

  /**
   * Adds a callback function to be executed on task completion. An example use
   * is for HadoopRDD to register a callback to close the input stream.
   * Will be called in any situation - success, failure, or cancellation.
   *
   * @param f Callback function.
   */
  @deprecated("use addTaskCompletionListener", "1.2.0")
  def addOnCompleteCallback(f: () => Unit)

  /**
   * The ID of the stage that this task belong to.
   * 这个任务的Stage的标识
   */
  def stageId(): Int

  /**
   * The ID of the RDD partition that is computed by this task.
   * 返回计算任务RDD分区的ID
   */
  def partitionId(): Int

  /**
   * How many times this task has been attempted.  The first task attempt will be assigned
   * attemptNumber = 0, and subsequent attempts will have increasing attempt numbers.
   * 任务已被尝试次数,第一次被分配为0,随后的尝试将有越来越多的尝试
   */
  def attemptNumber(): Int

  @deprecated("use attemptNumber", "1.3.0")
  def attemptId(): Long

  /**
   * An ID that is unique to this task attempt (within the same SparkContext, no two task attempts
   * will share the same attempt ID).  This is roughly equivalent to Hadoop's TaskAttemptID.
   * 此任务尝试唯一的标识,没有两个任务共享相同的标识,
   */
  def taskAttemptId(): Long

  /** ::DeveloperApi:: */
  @DeveloperApi
  def taskMetrics(): TaskMetrics

  /**
   * ::DeveloperApi::
   * Returns all metrics sources with the given name which are associated with the instance
   * which runs the task. For more information see [[org.apache.spark.metrics.MetricsSystem!]].
   */
  @DeveloperApi
  def getMetricsSources(sourceName: String): Seq[Source]

  /**
   * Returns the manager for this task's managed memory.
   * 返回任务内存的管理器
   */
  private[spark] def taskMemoryManager(): TaskMemoryManager

  /**
   * Register an accumulator that belongs to this task. Accumulators must call this method when
   * deserializing in executors.
   * 注册一个任务累加器,累加器执行方法执行反序列化
   */
  private[spark] def registerAccumulator(a: Accumulable[_, _]): Unit

  /**
   * Return the local values of internal accumulators that belong to this task. The key of the Map
   * is the accumulator id and the value of the Map is the latest accumulator local value.   
   */
  private[spark] def collectInternalAccumulators(): Map[Long, Any]

  /**
   * Return the local values of accumulators that belong to this task. The key of the Map is the
   * accumulator id and the value of the Map is the latest accumulator local value.
   */
  private[spark] def collectAccumulators(): Map[Long, Any]

  /**
   * Accumulators for tracking internal metrics indexed by the name.
   */
  private[spark] val internalMetricsToAccumulators: Map[String, Accumulator[Long]]
}
