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

package org.apache.spark.shuffle

import scala.collection.mutable

import com.google.common.annotations.VisibleForTesting

import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.{Logging, SparkException, SparkConf, TaskContext}

/**
 * Allocates a pool of memory to tasks for use in shuffle operations. Each disk-spilling
 * collection (ExternalAppendOnlyMap or ExternalSorter) used by these tasks can acquire memory
 * from this pool and release it as it spills data out. When a task ends, all its memory will be
 * released by the Executor.
  * 将一个内存池分配给随机操作中使用的任务,这些任务使用的每个磁盘溢出集合（ExternalAppendOnlyMap或ExternalSorter）
  * 可以从此池获取内存,并在数据溢出时释放它,当任务结束时,其所有内存将由执行者释放。
 *
 * This class tries to ensure that each task gets a reasonable share of memory, instead of some
 * task ramping up to a large amount first and then causing others to spill to disk repeatedly.
 * If there are N tasks, it ensures that each tasks can acquire at least 1 / 2N of the memory
 * before it has to spill, and at most 1 / N. Because N varies dynamically, we keep track of the
 * set of active tasks and redo the calculations of 1 / 2N and 1 / N in waiting tasks whenever
 * this set changes. This is all done by synchronizing access on "this" to mutate state and using
 * wait() and notifyAll() to signal changes.
 *
 * Use `ShuffleMemoryManager.create()` factory method to create a new instance.
 *  负责管理Shuffle线程占有内存的分配与释放,
 * @param maxMemory total amount of memory available for execution, in bytes.
 * @param pageSizeBytes number of bytes for each page, by default.
 * 
 * 负责全局计数和内存调度(policy enforcement)。它是核心仲裁者,根据task当前内存用量决定如何进行分配。
 * 一个JVM里仅有一个实例
 * ShuffleMemoryManager 用于为执行Shuffle操作的线程分配内存池,每种磁盘溢出集合都能从内存池获得内存
 * 当溢出集合的数据已经输出到存储系统,获得的内存会释放,当线程执行的任务结束,整个内存池都会被Executor释放
 * ShuffleMemoryManager 会保证每个线程都能合理地共享内存,而不会使得一些线程获得了很大的内存,导致其他线程不得
 * 将溢出的数据写入磁盘.
 * 
 */
private[spark]
class ShuffleMemoryManager protected (
    val maxMemory: Long,
    val pageSizeBytes: Long)
  extends Logging {
 //缓存每个线程的内存字节数,
  private val taskMemory = new mutable.HashMap[Long, Long]()  // taskAttemptId -> memory bytes

  private def currentTaskAttemptId(): Long = {
    // In case this is called on the driver, return an invalid task attempt id.
    //如果这是在驱动程序上调用，返回一个无效的任务尝试ID
    Option(TaskContext.get()).map(_.taskAttemptId()).getOrElse(-1L)
  }

  /**
   * Try to acquire up to numBytes memory for the current task, and return the number of bytes
   * obtained, or 0 if none can be allocated. This call may block until there is enough free memory
   * in some situations, to make sure each task has a chance to ramp up to at least 1 / 2N of the
   * total memory pool (where N is the # of active tasks) before it is forced to spill. This can
   * happen if the number of tasks increases but an older task had a lot of memory already.\
   * 获得内存方法
   * 
   * 处理逻辑:假设当前有N个线程,必须保证每个线程在溢出之前至少获得1/2N的内存,并且每个线程获得1/N的内存,由于是动
   * 态变化的变量,所以要持续对这些线程跟踪,以便无论何时在这些线程发生变化时重新按照1/2N和1/N计算
   */
  def tryToAcquire(numBytes: Long): Long = synchronized {//同步
    val taskAttemptId = currentTaskAttemptId()//获得当前线程任务的Id
    assert(numBytes > 0, "invalid number of bytes requested: " + numBytes)

    // Add this task to the taskMemory map just so we can keep an accurate count of the number
    // of active tasks, to let other tasks ramp down their memory in calls to tryToAcquire
    //将此任务添加到任务记忆库Map，以便我们可以准确计算活动任务的数量，让其他任务在调用TryToAcquire时降低内存
    if (!taskMemory.contains(taskAttemptId)) {
      taskMemory(taskAttemptId) = 0L
      //稍后会导致等待任务唤醒并再次检查numThreads
      notifyAll()  // Will later cause waiting tasks to wake up and check numThreads again
    }

    // Keep looping until we're either sure that we don't want to grant this request (because this
    // task would have more than 1 / numActiveTasks of the memory) or we have enough free
    // memory to give it (we always let each task get at least 1 / (2 * numActiveTasks)).
    //保持循环，直到我们确定我们不想授予此请求（因为此任务将具有超过1个/ numActiveTasks的内存）,
    // 或者我们有足够的可用内存来给它（我们总是让每个任务得到 至少1 /（2 * numActiveTasks））。
    while (true) {
      val numActiveTasks = taskMemory.keys.size
      val curMem = taskMemory(taskAttemptId)
      val freeMemory = maxMemory - taskMemory.values.sum//当前可以内存

      // How much we can grant this task; don't let it grow to more than 1 / numActiveTasks;
      //我们可以授予这个任务多少钱 不要让它增长到超过1 / numActiveTasks;
      // don't let it be negative 不要让它是负
      val maxToGrant = math.min(numBytes, math.max(0, (maxMemory / numActiveTasks) - curMem))

      if (curMem < maxMemory / (2 * numActiveTasks)) {
        // We want to let each task get at least 1 / (2 * numActiveTasks) before blocking;
        // if we can't give it this much now, wait for other tasks to free up memory
        // (this happens if older tasks allocated lots of memory before N grew)
        if (freeMemory >= math.min(maxToGrant, maxMemory / (2 * numActiveTasks) - curMem)) {
          val toGrant = math.min(maxToGrant, freeMemory)
          taskMemory(taskAttemptId) += toGrant
          return toGrant
        } else {
          logInfo(
            s"TID $taskAttemptId waiting for at least 1/2N of shuffle memory pool to be free")
          wait()
        }
      } else {
        // Only give it as much memory as is free, which might be none if it reached 1 / numThreads
        //只给它一个空闲的内存，如果它达到1 / numThreads可能没有
        val toGrant = math.min(maxToGrant, freeMemory)
        taskMemory(taskAttemptId) += toGrant
        return toGrant
      }
    }
    0L  // Never reached 从来没有达到
  }

  /** 
   *  Release numBytes bytes for the current task. 
   *  释放当前任务numbytes字节
   *  */
  def release(numBytes: Long): Unit = synchronized {//同步,释放当前任务
    val taskAttemptId = currentTaskAttemptId()//获得当前任务ID
    val curMem = taskMemory.getOrElse(taskAttemptId, 0L)//返回当前任务的内存
    if (curMem < numBytes) {
      throw new SparkException(
        s"Internal error: release called on ${numBytes} bytes but task only has ${curMem}")
    }
    taskMemory(taskAttemptId) -= numBytes
    //通知在waitToAcquire中释放内存的服务器
    notifyAll()  // Notify waiters who locked "this" in tryToAcquire that memory has been freed
  }

  /** 
   *  Release all memory for the current task and mark it as inactive (e.g. when a task ends).
    *  释放当前任务的所有内存,并将其标记为非活动状态(例如任务结束时)
   *  释放当前线程使用的内存通过ShuffleMemoryManager获得的内存
   *  */
  def releaseMemoryForThisTask(): Unit = synchronized {
    val taskAttemptId = currentTaskAttemptId()
    taskMemory.remove(taskAttemptId)
    //通知tryToAcquire中的“this”这个内存已被释放的服务员
    notifyAll()  // Notify waiters who locked "this" in tryToAcquire that memory has been freed
  }

  /**
   *  Returns the memory consumption, in bytes, for the current task 
   *  返回当前任务的占用的内存,
   *  */
  def getMemoryConsumptionForThisTask(): Long = synchronized {//返回当前任务的内存大小
    val taskAttemptId = currentTaskAttemptId()
    taskMemory.getOrElse(taskAttemptId, 0L)
  }
}

/**
 * 负责管理Shuffle线程占有内存的分配与释放
 */
private[spark] object ShuffleMemoryManager {

  def create(conf: SparkConf, numCores: Int): ShuffleMemoryManager = {
    val maxMemory = ShuffleMemoryManager.getMaxMemory(conf)//获取shuffle所有线程占用的最大内存
    val pageSize = ShuffleMemoryManager.getPageSize(conf, maxMemory, numCores)//
    new ShuffleMemoryManager(maxMemory, pageSize)
  }

  def create(maxMemory: Long, pageSizeBytes: Long): ShuffleMemoryManager = {
    new ShuffleMemoryManager(maxMemory, pageSizeBytes)
  }

  @VisibleForTesting
  def createForTesting(maxMemory: Long): ShuffleMemoryManager = {
    new ShuffleMemoryManager(maxMemory, 4 * 1024 * 1024)
  }

  /**
   * Figure out the shuffle memory limit from a SparkConf. We currently have both a fraction
   * of the memory pool and a safety factor since collections can sometimes grow bigger than
   * the size we target before we estimate their sizes again.
   *
    * 从SparkConf中找出shuffle内存限制,我们目前拥有一部分内存池和一个安全因素,
    * 因为收集有时会比我们目标的大小更大,然后我们再次估计它们的大小。
    * 获取Shuffle所有线程占用的最大内存
   */
  private def getMaxMemory(conf: SparkConf): Long = {
    //Shuffle最大内存占比
    //memoryFraction Shuffle过程中使用的内存达到总内存多少比例的时候开始Spill(临时写入外部存储或一直使用内存)
    //取Execution区域(即运行区域,为shuffle使用)在总内存中所占比重,由参数spark.shuffle.memoryFraction确定，默认为0.2
    val memoryFraction = conf.getDouble("spark.shuffle.memoryFraction", 0.2)
    //shuffle的安全内存占比
    // 取Execution区域(即运行区域,为shuffle使用)在系统为其可分配最大内存的安全系数,主要为了防止OOM,取参数spark.shuffle.safetyFraction，默认为0.8
    val safetyFraction = conf.getDouble("spark.shuffle.safetyFraction", 0.8)
    //java运行最大内存*Spark的Shuffle最大内存占比*Spark的安全内存占比
    //返回为Execution区域(即运行区域，为shuffle使用)分配的可用内存总大小,计算公式：系统可用最大内存 * 在系统可用最大内存中所占比重 * 安全系数
    (Runtime.getRuntime.maxMemory * memoryFraction * safetyFraction).toLong
  }

  /**
   * Sets the page size, in bytes.
    * 设置页面大小(以字节为单位)
   *
   * If user didn't explicitly set "spark.buffer.pageSize", we figure out the default value
   * by looking at the number of cores available to the process, and the total amount of memory,
   * and then divide it by a factor of safety.
    * 如果用户未明确设置“spark.buffer.pageSize”,我们通过查看可用于进程的内核数量和总内存量,然后将其除以安全系数来计算出默认值
   */
  private def getPageSize(conf: SparkConf, maxMemory: Long, numCores: Int): Long = {
    val minPageSize = 1L * 1024 * 1024   // 1MB
    val maxPageSize = 64L * minPageSize  // 64MB
    //获得当前可以cpu 核数
    val cores = if (numCores > 0) numCores else Runtime.getRuntime.availableProcessors()
    // Because of rounding to next power of 2, we may have safetyFactor as 8 in worst case
    //由于四舍五入到2的下一个电源,我们可能在最坏的情况下安全反应器为8
    val safetyFactor = 16
    // TODO(davies): don't round to next power of 2
    val size = ByteArrayMethods.nextPowerOf2(maxMemory / cores / safetyFactor)
    val default = math.min(maxPageSize, math.max(minPageSize, size))
    conf.getSizeAsBytes("spark.buffer.pageSize", default)
  }
}
