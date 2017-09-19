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
    * 返回当前运行的TaskContext(任务的上下文信息),这可以在用户函数内部调用,以访问有关运行任务的上下文信息。
    *
    * 返回此线程局部变量的当前线程副本中的值，如果这是线程第一次调用该方法，则创建并初始化此副本
   */
  def get(): TaskContext = taskContext.get

  /**
   * Returns the partition id of currently active TaskContext. It will return 0
   * if there is no active TaskContext for cases like local execution.
   * 返回当前活动的上下文信息的分区ID,如果本地执行的情况下没有活动的TaskContext,则返回0。
   */
  def getPartitionId(): Int = {
    val tc = taskContext.get()
    if (tc eq null) {
      0
    } else {
      tc.partitionId()
    }
  }
  /**
    * ThreadLocal
    * 1)每个线程都有一个独立于其他线程的上下文来保存这个变量,一个线程的本地变量对其他线程是不可见的
    * 2)ThreadLocal可以给一个初始值,而每个线程都会获得这个初始化值的一个副本,这样才能保证不同的线程都有一份拷贝。
    * 3)ThreadLocal不是用于解决共享变量的问题的,不是为了协调线程同步而存在,而是为了方便每个线程处理自己的状态而引入的一个机制
    *
    *ThreadLocal为每个线程的中并发访问的数据提供一个副本
    *
    *Synchronized用于线程间的数据共享,而ThreadLocal则用于线程间的数据隔离。
    *synchronized是利用锁的机制,使变量或代码块在某一时该只能被一个线程访问。
    * 而ThreadLocal为每一个线程都提供了变量的副本,使得每个线程在某一时间访问到的并不是同一个对象,这样就隔离了多个线程对数据的数据共享。
    scala[]泛型
    */

  private[this] val taskContext: ThreadLocal[TaskContext] = new ThreadLocal[TaskContext]

  // Note: protected[spark] instead of private[spark] to prevent the following two from
  // showing up in JavaDoc.
  /**
   * Set the thread local TaskContext. Internal to Spark.
   * 设置线程局部变量的当前线程副本中的值TaskContext,(任务的上下文信息)
   */
  protected[spark] def setTaskContext(tc: TaskContext): Unit = taskContext.set(tc)

  /**
   * Unset the thread local TaskContext. Internal to Spark.
   * 删除本地线程的任务上下文信息
   */
  protected[spark] def unset(): Unit = taskContext.remove()

  /**
   * An empty task context that does not represent an actual task.
    * 一个空的任务上下文，不代表实际的任务。
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
  //注意：TaskContext不能定义get方法,否则会阻止Scala编译器,生成一个静态get方法(基于对象的get方法)

  // Note: Update JavaTaskContextCompileCheck when new methods are added to this class.

  // Note: getters in this class are defined with parentheses to maintain backward compatibility.
  //注意：该类中的getter用括号定义,以保持向后兼容性。

  /**
   * Returns true if the task has completed.
   * 如果任务已完成,则返回真
   */
  def isCompleted(): Boolean

  /**
   * Returns true if the task has been killed.
   * 如果任务已被杀死,返回真
   */
  def isInterrupted(): Boolean

  @deprecated("use isRunningLocally", "1.2.0")
  def runningLocally(): Boolean

  /**
   * Returns true if the task is running locally in the driver program.
   * 如果任务在驱动程序中运行本地运行,则返回真
   * @return
   */
  def isRunningLocally(): Boolean

  /**
   * Adds a (Java friendly) listener to be executed on task completion.
   * This will be called in all situation - success, failure, or cancellation.
   * An example use is for HadoopRDD to register a callback to close the input stream.
   * 添加一个完成执行任务的侦听器,在任务完成时执行,这记录任务的成功,失败,或者取消.
    * 例如HadoopRDD注册一个回调来关闭输入流
   */
  def addTaskCompletionListener(listener: TaskCompletionListener): TaskContext

  /**
   *
   * Adds a listener in the form of a Scala closure to be executed on task completion.
   * This will be called in all situations - success, failure, or cancellation.
   * An example use is for HadoopRDD to register a callback to close the input stream.
    * 以Scala闭包的形式添加一个侦听器,以在任务完成时执行。
    * 添加一个完成任务执行的侦听器,这记录任务的成功,失败,或者取消.
   */
  def addTaskCompletionListener(f: (TaskContext) => Unit): TaskContext

  /**
   * Adds a callback function to be executed on task completion. An example use
   * is for HadoopRDD to register a callback to close the input stream.
   * Will be called in any situation - success, failure, or cancellation.
    * 添加在任务完成时执行的回调函数,一个例子使用,于HadoopRDD注册回调以关闭输入流。
    * 将在任何情况下被调用 - 成功,失败或取消。
   *
   * @param f Callback function.
   */
  @deprecated("use addTaskCompletionListener", "1.2.0")
  def addOnCompleteCallback(f: () => Unit)

  /**
   * The ID of the stage that this task belong to.
   * 此任务所属阶段的ID
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
   * 任务已被尝试次数,第一次被分配为0,随后的尝试将增加尝试次数
   */
  def attemptNumber(): Int

  @deprecated("use attemptNumber", "1.3.0")
  def attemptId(): Long

  /**
   * An ID that is unique to this task attempt (within the same SparkContext, no two task attempts
   * will share the same attempt ID).  This is roughly equivalent to Hadoop's TaskAttemptID.
    * 此任务尝试唯一的ID(在相同的SparkContext中,没有两个任务尝试将共享相同的尝试ID),这大概相当于Hadoop的TaskAttemptID
   */
  def taskAttemptId(): Long

  /** ::DeveloperApi:: */
  @DeveloperApi
  def taskMetrics(): TaskMetrics

  /**
   * ::DeveloperApi::
   * Returns all metrics sources with the given name which are associated with the instance
   * which runs the task. For more information see [[org.apache.spark.metrics.MetricsSystem!]].
    * 返回与实例相关联的给定名称的所有度量来源运行任务,有关更多信息,请参阅[[org.apache.spark.metrics.MetricsSystem!]]。
   */
  @DeveloperApi
  def getMetricsSources(sourceName: String): Seq[Source]

  /**
   * Returns the manager for this task's managed memory.
   * 返回管理任务内存的管理器
   */
  private[spark] def taskMemoryManager(): TaskMemoryManager

  /**
   * Register an accumulator that belongs to this task. Accumulators must call this method when
   * deserializing in executors.
   * 注册一个任务累加器,累加器必须执行调用执行器的反序列化方法
   */
  private[spark] def registerAccumulator(a: Accumulable[_, _]): Unit

  /**
   * Return the local values of internal accumulators that belong to this task. The key of the Map
   * is the accumulator id and the value of the Map is the latest accumulator local value.
    * 返回属于此任务的内部累加器的本地值, Map的关键是累加器ID,Map的值是最新的累加器本地值
   */
  private[spark] def collectInternalAccumulators(): Map[Long, Any]

  /**
   * Return the local values of accumulators that belong to this task. The key of the Map is the
   * accumulator id and the value of the Map is the latest accumulator local value.
    * 返回属于此任务的累加器的本地值,Map的关键是累加器ID,Map的值是最新的累加器本地值。
   */
  private[spark] def collectAccumulators(): Map[Long, Any]

  /**
   * Accumulators for tracking internal metrics indexed by the name.
    * 用于跟踪由名称索引的内部度量的累加器
   */
  private[spark] val internalMetricsToAccumulators: Map[String, Accumulator[Long]]
}
