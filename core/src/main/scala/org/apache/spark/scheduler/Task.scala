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

import java.io.{ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.nio.ByteBuffer

import scala.collection.mutable.HashMap

import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.{Accumulator, SparkEnv, TaskContextImpl, TaskContext}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.unsafe.memory.TaskMemoryManager
import org.apache.spark.util.ByteBufferInputStream
import org.apache.spark.util.Utils


/**
 * A unit of execution. We have two kinds of Task's in Spark:
 * Spark执行单位,
 * - [[org.apache.spark.scheduler.ShuffleMapTask]]
 * - [[org.apache.spark.scheduler.ResultTask]]
 *
 * A Spark job consists of one or more stages. The very last stage in a job consists of multiple
 * ResultTasks, while earlier stages consist of ShuffleMapTasks. A ResultTask executes the task
 * and sends the task output back to the driver application. A ShuffleMapTask executes the task
 * and divides the task output to multiple buckets (based on the task's partitioner).
 * 一个Spark Job是由一个或多个Stage组成,一个resulttask执行任务,将任务发送输出到Driver端,
 * 一个shufflemaptask执行任务,将任务的输出到多个任务的分配
 * @param stageId id of the stage this task belongs to
 * @param partitionId index of the number in the RDD
 */
private[spark] abstract class Task[T](
    val stageId: Int,
    val stageAttemptId: Int,
    val partitionId: Int,
    internalAccumulators: Seq[Accumulator[Long]]) extends Serializable {

  /**
   * The key of the Map is the accumulator id and the value of the Map is the latest accumulator
   * local value.
   * 
   */
  type AccumulatorUpdates = Map[Long, Any]

  /**
   * Called by [[Executor]] to run this task.
   * 被Executor调用以执行Task,TaskRunner.run调用此方法
   * @param taskAttemptId an identifier for this task attempt that is unique within a SparkContext.
   * @param attemptNumber how many times this task has been attempted (0 for the first attempt)
   * @return the result of the task along with updates of Accumulators.
   */
  
  final def run(
    taskAttemptId: Long,
    attemptNumber: Int,
    metricsSystem: MetricsSystem)
  : (T, AccumulatorUpdates) = {
    //创建一个Task上下文实例：TaskContextImpl类型的context 
    context = new TaskContextImpl(
      stageId,//Task所属Stage的stageId
      partitionId,//Task对应数据分区的partitionId
      taskAttemptId,//Task执行的taskAttemptId
      attemptNumber,//Task执行的序号attemptNumber
      taskMemoryManager,//Task内存管理器
      metricsSystem,//指标度量系统
      internalAccumulators,//内部累加器
      runningLocally = false)//是否本地运行的标志位
    //将context放入TaskContext的taskContext变量中  
    //taskContext变量为ThreadLocal[TaskContext] 
    TaskContext.setTaskContext(context)
    //设置主机名localHostName、内部累加器internalAccumulators等Metrics信息  
    context.taskMetrics.setHostname(Utils.localHostName())
    context.taskMetrics.setAccumulatorsUpdater(context.collectInternalAccumulators)
    //task线程为当前线程 ,在被打断的时候可以通过来停止该线程
    taskThread = Thread.currentThread()
    if (_killed) {//如果需要杀死task,调用kill()方法，且调用的方式为不中断线程  
      kill(interruptThread = false)
    }
    try {
      //调用runTask()方法,传入Task上下文信息context,执行Task,并调用Task上下文的collectAccumulators()方法,收集累加器
      (runTask(context), context.collectAccumulators())
    } finally {
      //任务结束,执行任务结束时的回调函数
      context.markTaskCompleted()
      try {
        //内存清理工作
        Utils.tryLogNonFatalError {
          // Release memory used by this thread for shuffles 
          //释放当前线程使用的内存通过ShuffleMemoryManager获得的内存
          SparkEnv.get.shuffleMemoryManager.releaseMemoryForThisTask()
        }
        Utils.tryLogNonFatalError {
          //Release memory used by this thread for unrolling blocks
          //释放当前线程在MemoryStore的releaseUnrollMemoryForThisTask中展开占用的内存
          SparkEnv.get.blockManager.memoryStore.releaseUnrollMemoryForThisTask()
        }
      } finally {
         //释放当前线程用于聚合计算占用的内存 
        TaskContext.unset()
      }
    }
  }

  private var taskMemoryManager: TaskMemoryManager = _

  def setTaskMemoryManager(taskMemoryManager: TaskMemoryManager): Unit = {
    this.taskMemoryManager = taskMemoryManager
  }

  def runTask(context: TaskContext): T
  //task最佳位置
  def preferredLocations: Seq[TaskLocation] = Nil

  // Map output tracker epoch. Will be set by TaskScheduler.
  // map 输出跟踪失败值,将由任务调度器
  var epoch: Long = -1

  var metrics: Option[TaskMetrics] = None

  // Task context, to be initialized in run().
  // 任务的上下文,在run()被初始化
  @transient protected var context: TaskContextImpl = _

  // The actual Thread on which the task is running, if any. Initialized in run().
  //任务正在运行的实际线程，如果有。在run()初始化。
  @volatile @transient private var taskThread: Thread = _

  // A flag to indicate whether the task is killed. This is used in case context is not yet
  // initialized when kill() is invoked.
  // 指示任务是否被杀死的标志
  @volatile @transient private var _killed = false

  protected var _executorDeserializeTime: Long = 0

  /**
   * Whether the task has been killed.
   * 是否任务已经被杀死了
   */
  def killed: Boolean = _killed

  /**
   * Returns the amount of time spent deserializing the RDD and function to be run.
   * 返回返序列化运行时间
   */
  def executorDeserializeTime: Long = _executorDeserializeTime

  /**
   * Kills a task by setting the interrupted flag to true. This relies on the upper level Spark
   * code and user code to properly handle the flag. This function should be idempotent so it can
   * be called multiple times.
   * 通过设置中断的标志来杀死一个任务
   * If interruptThread is true, we will also call Thread.interrupt() on the Task's executor thread.
   */
  def kill(interruptThread: Boolean) {
    _killed = true
    if (context != null) {
      //设置中断
      context.markInterrupted()
    }
    if (interruptThread && taskThread != null) {
      //方法不会中断一个正在运行的线程。在线程受到阻塞时抛出一个中断信号，这样线程就得以退出阻塞的状态
      taskThread.interrupt()
    }
  }
}

/**
 * Handles transmission of tasks and their dependencies, because this can be slightly tricky. We
 * need to send the list of JARs and files added to the SparkContext with each task to ensure that
 * worker nodes find out about it, but we can't make it part of the Task because the user's code in
 * the task might depend on one of the JARs. Thus we serialize each task as multiple objects, by
 * first writing out its dependencies.
 * 处理任务和它们的依赖关系的传输,
 */
private[spark] object Task {
  /**
   * Serialize a task and the current app dependencies (files and JARs added to the SparkContext)
   * Task序列化依赖文件或jar
   */
  def serializeWithDependencies(
      task: Task[_],
      currentFiles: HashMap[String, Long],
      currentJars: HashMap[String, Long],
      serializer: SerializerInstance)
    : ByteBuffer = {

    val out = new ByteArrayOutputStream(4096)
    val dataOut = new DataOutputStream(out)

    // Write currentFiles
    //写入文件大小
    dataOut.writeInt(currentFiles.size)
    for ((name, timestamp) <- currentFiles) {
      dataOut.writeUTF(name)//输出名称
      dataOut.writeLong(timestamp)//输出时间戳
    }

    // Write currentJars
    dataOut.writeInt(currentJars.size)
    for ((name, timestamp) <- currentJars) {
      dataOut.writeUTF(name)
      dataOut.writeLong(timestamp)
    }

    // Write the task itself and finish
    // 自身任务输出完成
    dataOut.flush()
    val taskBytes = serializer.serialize(task).array()
    out.write(taskBytes)
    ByteBuffer.wrap(out.toByteArray)
  }

  /**
   * Deserialize the list of dependencies in a task serialized with serializeWithDependencies,
   * 
   * and return the task itself as a serialized ByteBuffer. The caller can then update its
   * ClassLoaders and deserialize the task.  
   * @return (taskFiles, taskJars, taskBytes)
   */
  def deserializeWithDependencies(serializedTask: ByteBuffer)
    : (HashMap[String, Long], HashMap[String, Long], ByteBuffer) = {

    val in = new ByteBufferInputStream(serializedTask)
    val dataIn = new DataInputStream(in)

    // Read task's files
    val taskFiles = new HashMap[String, Long]()
    val numFiles = dataIn.readInt()
    for (i <- 0 until numFiles) {
      taskFiles(dataIn.readUTF()) = dataIn.readLong()
    }

    // Read task's JARs
    val taskJars = new HashMap[String, Long]()
    val numJars = dataIn.readInt()
    for (i <- 0 until numJars) {
      taskJars(dataIn.readUTF()) = dataIn.readLong()
    }

    // Create a sub-buffer for the rest of the data, which is the serialized Task object
    val subBuffer = serializedTask.slice()  // ByteBufferInputStream will have read just up to task
    (taskFiles, taskJars, subBuffer)
  }
}
