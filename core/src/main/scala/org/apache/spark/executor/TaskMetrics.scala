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

package org.apache.spark.executor

import java.io.{IOException, ObjectInputStream}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.executor.DataReadMethod.DataReadMethod
import org.apache.spark.storage.{BlockId, BlockStatus}
import org.apache.spark.util.Utils

/**
 * :: DeveloperApi ::
 * Metrics tracked during the execution of a task.
 * 在任务执行过程中跟踪的测量信息
 * This class is used to house metrics both for in-progress and completed tasks. In executors,
 * both the task thread and the heartbeat thread write to the TaskMetrics. The heartbeat thread
 * reads it to send in-progress metrics, and the task thread reads it to send metrics along with
 * the completed task.
 *
 * So, when adding new fields, take into consideration that the whole object can be serialized for
 * shipping off at any time to consumers of the SparkListener interface.
 */
@DeveloperApi
class TaskMetrics extends Serializable {
  /**
   * 任务运行的主机名称
   * Host's name the task runs on
   */
  private var _hostname: String = _
  def hostname: String = _hostname
  private[spark] def setHostname(value: String) = _hostname = value

  /**
   * Time taken on the executor to deserialize this task
   * 执行任务反序列化的时间
   */
  private var _executorDeserializeTime: Long = _
  def executorDeserializeTime: Long = _executorDeserializeTime
  private[spark] def setExecutorDeserializeTime(value: Long) = _executorDeserializeTime = value


  /**
   * Time the executor spends actually running the task (including fetching shuffle data)
   * 执行实际任务运行的时间(包括获取shuffle数据)
   */
  private var _executorRunTime: Long = _
  def executorRunTime: Long = _executorRunTime
  private[spark] def setExecutorRunTime(value: Long) = _executorRunTime = value

  /**
   * The number of bytes this task transmitted back to the driver as the TaskResult
   * 任务结果回传给Driver字节数
   */
  private var _resultSize: Long = _
  def resultSize: Long = _resultSize
  private[spark] def setResultSize(value: Long) = _resultSize = value


  /**
   * Amount of time the JVM spent in garbage collection while executing this task
   * 执行任务JVM的垃圾收集花费的时间
   */
  private var _jvmGCTime: Long = _
  def jvmGCTime: Long = _jvmGCTime
  private[spark] def setJvmGCTime(value: Long) = _jvmGCTime = value

  /**
   * Amount of time spent serializing the task result
   * 序列化任务结果的时间
   */
  private var _resultSerializationTime: Long = _
  def resultSerializationTime: Long = _resultSerializationTime
  private[spark] def setResultSerializationTime(value: Long) = _resultSerializationTime = value

  /**
   * The number of in-memory bytes spilled by this task
   * 任务溢出所需的内存字节数
   */
  private var _memoryBytesSpilled: Long = _
  def memoryBytesSpilled: Long = _memoryBytesSpilled
  private[spark] def incMemoryBytesSpilled(value: Long): Unit = _memoryBytesSpilled += value
  private[spark] def decMemoryBytesSpilled(value: Long): Unit = _memoryBytesSpilled -= value

  /**
   * The number of on-disk bytes spilled by this task
   * 任务溢出所需的磁盘字节数
   */
  private var _diskBytesSpilled: Long = _
  def diskBytesSpilled: Long = _diskBytesSpilled
  private[spark] def incDiskBytesSpilled(value: Long): Unit = _diskBytesSpilled += value
  private[spark] def decDiskBytesSpilled(value: Long): Unit = _diskBytesSpilled -= value

  /**
   * If this task reads from a HadoopRDD or from persisted data, metrics on how much data was read
   * are stored here.
   * 如果这个任务读取一个hadooprdd或持久化数据,测量读取多少存储数据
   */
  private var _inputMetrics: Option[InputMetrics] = None

  def inputMetrics: Option[InputMetrics] = _inputMetrics

  /**
   * This should only be used when recreating TaskMetrics, not when updating input metrics in
   * executors
   * 当更新输入测量的executors,重新创建taskmetrics(任务测量)
   */
  private[spark] def setInputMetrics(inputMetrics: Option[InputMetrics]) {
    _inputMetrics = inputMetrics
  }

  /**
   * If this task writes data externally (e.g. to a distributed filesystem), metrics on how much
   * data was written are stored here.
   * 如果任务外部写入数据(对于一个分布式的文件系统),测量读取多少存储数据
   */
  var outputMetrics: Option[OutputMetrics] = None

  /**
   * If this task reads from shuffle output, metrics on getting shuffle data will be collected here.
   * This includes read metrics aggregated over all the task's shuffle dependencies.
   * 如果任务读取从洗牌输出,测量获取洗牌数据的在这里收集,包括读取所有任务的洗牌依赖关系的读取数据
   */
  private var _shuffleReadMetrics: Option[ShuffleReadMetrics] = None

  def shuffleReadMetrics: Option[ShuffleReadMetrics] = _shuffleReadMetrics

  /**
   * This should only be used when recreating TaskMetrics, not when updating read metrics in
   * executors.
   * 重新创造taskmetrics(任务测量),当更新读取executors测量信息
   */
  private[spark] def setShuffleReadMetrics(shuffleReadMetrics: Option[ShuffleReadMetrics]) {
    _shuffleReadMetrics = shuffleReadMetrics
  }

  /**
   * ShuffleReadMetrics per dependency for collecting independently while task is in progress.
   * shufflereadmetrics依赖
   */
  @transient private lazy val depsShuffleReadMetrics: ArrayBuffer[ShuffleReadMetrics] =
    new ArrayBuffer[ShuffleReadMetrics]()

  /**
   * If this task writes to shuffle output, metrics on the written shuffle data will be collected
   * here
   * 如果任务写入到随机输出,收集度量shuffl写数据
   */
  var shuffleWriteMetrics: Option[ShuffleWriteMetrics] = None

  /**
   * Storage statuses of any blocks that have been updated as a result of this task.
   * 任务更新任何块存储的状态
   */
  var updatedBlocks: Option[Seq[(BlockId, BlockStatus)]] = None

  /**
   * A task may have multiple shuffle readers for multiple dependencies. To avoid synchronization
   * issues from readers in different threads, in-progress tasks use a ShuffleReadMetrics for each
   * dependency, and merge these metrics before reporting them to the driver. This method returns
   * a ShuffleReadMetrics for a dependency and registers it for merging later.
    * 任务可能有多个随机阅读器用于多个依赖关系,为避免来自不同线程的读者的同步问题,正在进行的任务对每个依赖关系使用ShuffleReadMetrics,
    * 并将这些指标合并，然后将其报告给驱动程序,此方法返回一个依赖关系的ShuffleReadMetrics,并将其注册后进行合并。
   */
  private [spark] def createShuffleReadMetricsForDependency(): ShuffleReadMetrics = synchronized {
    val readMetrics = new ShuffleReadMetrics()
    depsShuffleReadMetrics += readMetrics
    readMetrics
  }

  /**
   * Returns the input metrics object that the task should use. Currently, if
   * there exists an input metric with the same readMethod, we return that one
   * so the caller can accumulate bytes read. If the readMethod is different
   * than previously seen by this task, we return a new InputMetric but don't
   * record it.
    * 返回任务应该使用的输入度量对象。 目前，如果存在具有相同readMethod的输入度量，
    * 那么我们返回一个调用者可以累加读取的字节。如果readMethod与此任务以前看到的不同,则返回一个新的InputMetric，但不记录它。
   *
   * Once https://issues.apache.org/jira/browse/SPARK-5225 is addressed,
   * we can store all the different inputMetrics (one per readMethod).
   */
  private[spark] def getInputMetricsForReadMethod(readMethod: DataReadMethod): InputMetrics = {
    synchronized {
      _inputMetrics match {
        case None =>
          val metrics = new InputMetrics(readMethod)
          _inputMetrics = Some(metrics)
          metrics
        case Some(metrics @ InputMetrics(method)) if method == readMethod =>
          metrics
        case Some(InputMetrics(method)) =>
          new InputMetrics(readMethod)
      }
    }
  }

  /**
   * Aggregates shuffle read metrics for all registered dependencies into shuffleReadMetrics.
   * 注册的依赖shufflereadmetrics,聚合读取所有的指标
   */
  private[spark] def updateShuffleReadMetrics(): Unit = synchronized {
    if (!depsShuffleReadMetrics.isEmpty) {
      val merged = new ShuffleReadMetrics()
      for (depMetrics <- depsShuffleReadMetrics) {
        merged.incFetchWaitTime(depMetrics.fetchWaitTime)
        merged.incLocalBlocksFetched(depMetrics.localBlocksFetched)
        merged.incRemoteBlocksFetched(depMetrics.remoteBlocksFetched)
        merged.incRemoteBytesRead(depMetrics.remoteBytesRead)
        merged.incLocalBytesRead(depMetrics.localBytesRead)
        merged.incRecordsRead(depMetrics.recordsRead)
      }
      _shuffleReadMetrics = Some(merged)
    }
  }

  private[spark] def updateInputMetrics(): Unit = synchronized {
    inputMetrics.foreach(_.updateBytesRead())
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    in.defaultReadObject()
    // Get the hostname from cached data, since hostname is the order of number of nodes in
    // cluster, so using cached hostname will decrease the object number and alleviate the GC
    // overhead.
    //从缓存数据获取主机名，因为主机名是节点数量的顺序集群，因此使用缓存的主机名将减少对象编号并减轻GCoverhead。
    _hostname = TaskMetrics.getCachedHostName(_hostname)
  }

  private var _accumulatorUpdates: Map[Long, Any] = Map.empty
  @transient private var _accumulatorsUpdater: () => Map[Long, Any] = null

  private[spark] def updateAccumulators(): Unit = synchronized {
    _accumulatorUpdates = _accumulatorsUpdater()
  }

  /**
   * Return the latest updates of accumulators in this task.
   * 返回任务累加器的最新更新
   */
  def accumulatorUpdates(): Map[Long, Any] = _accumulatorUpdates

  private[spark] def setAccumulatorsUpdater(accumulatorsUpdater: () => Map[Long, Any]): Unit = {
    _accumulatorsUpdater = accumulatorsUpdater
  }
}

private[spark] object TaskMetrics {
  private val hostNameCache = new ConcurrentHashMap[String, String]()

  def empty: TaskMetrics = new TaskMetrics

  def getCachedHostName(host: String): String = {
    val canonicalHost = hostNameCache.putIfAbsent(host, host)
    if (canonicalHost != null) canonicalHost else host
  }
}

/**
 * :: DeveloperApi ::
 * Method by which input data was read.  Network means that the data was read over the network
 * 读取输入数据的方法,网络意味着从远程块管理器上读取网络的数据(这可能已经存储在磁盘上或内存中的数据)
 * from a remote block manager (which may have stored the data on-disk or in-memory). 
 */
@DeveloperApi
object DataReadMethod extends Enumeration with Serializable {
  type DataReadMethod = Value
  val Memory, Disk, Hadoop, Network = Value
}

/**
 * :: DeveloperApi ::
 * Method by which output data was written.
 * 写入输出数据的方法
 */
@DeveloperApi
object DataWriteMethod extends Enumeration with Serializable {
  type DataWriteMethod = Value
  val Hadoop = Value
}

/**
 * :: DeveloperApi ::
 * Metrics about reading input data.
 * 读取测量输入数据
 */
@DeveloperApi
case class InputMetrics(readMethod: DataReadMethod.Value) {

  /**
   * This is volatile so that it is visible to the updater thread.
   * 这是不稳定的,它是更新线程可见
    * 读字节回调
   */
  @volatile @transient var bytesReadCallback: Option[() => Long] = None

  /**
   * Total bytes read.
   * 读取总字节数
   */
  private var _bytesRead: Long = _
  def bytesRead: Long = _bytesRead
  def incBytesRead(bytes: Long): Unit = _bytesRead += bytes

  /**
   * Total records read.
   * 读取总字节数
   */
  private var _recordsRead: Long = _
  def recordsRead: Long = _recordsRead
  def incRecordsRead(records: Long): Unit = _recordsRead += records

  /**
   * Invoke the bytesReadCallback and mutate bytesRead.
    * 调用bytesReadCallback和mutate bytesRead
   */
  def updateBytesRead() {
    bytesReadCallback.foreach { c =>
      _bytesRead = c()
    }
  }

 /**
  * Register a function that can be called to get up-to-date information on how many bytes the task
  * has read from an input source.
   * 注册一个可以调用的函数来获取有关任务从输入源读取的字节数的最新信息
  */
  def setBytesReadCallback(f: Option[() => Long]) {
    bytesReadCallback = f
  }
}

/**
 * :: DeveloperApi ::
 * Metrics about writing output data.
 * 关于测量数据输出
 */
@DeveloperApi
case class OutputMetrics(writeMethod: DataWriteMethod.Value) {
  /**
   * Total bytes written
   * 总字节写入
   */
  private var _bytesWritten: Long = _
  def bytesWritten: Long = _bytesWritten
  private[spark] def setBytesWritten(value : Long): Unit = _bytesWritten = value

  /**
   * Total records written
   * 总字节写入
   */
  private var _recordsWritten: Long = 0L
  def recordsWritten: Long = _recordsWritten
  private[spark] def setRecordsWritten(value: Long): Unit = _recordsWritten = value
}

/**
 * :: DeveloperApi ::
 * Metrics pertaining to shuffle data read in a given task.
 * 给定的任务中读取数据的度量
 */
@DeveloperApi
class ShuffleReadMetrics extends Serializable {
  /**
   * Number of remote blocks fetched in this shuffle by this task
   * 在这个任务中获取的远程块的数量
   */
  private var _remoteBlocksFetched: Int = _
  def remoteBlocksFetched: Int = _remoteBlocksFetched
  private[spark] def incRemoteBlocksFetched(value: Int) = _remoteBlocksFetched += value
  private[spark] def decRemoteBlocksFetched(value: Int) = _remoteBlocksFetched -= value

  /**
   * Number of local blocks fetched in this shuffle by this task
   * 在这个任务中获取的本地块的数量
   */
  private var _localBlocksFetched: Int = _
  def localBlocksFetched: Int = _localBlocksFetched
  private[spark] def incLocalBlocksFetched(value: Int) = _localBlocksFetched += value
  private[spark] def decLocalBlocksFetched(value: Int) = _localBlocksFetched -= value

  /**
   * Time the task spent waiting for remote shuffle blocks. This only includes the time
   * blocking on shuffle input data. For instance if block B is being fetched while the task is
   * still not finished processing block A, it is not considered to be blocking on block B.
   * 等待远程shuffle块的任务花费的时间,包括在shuffle输入数据上的阻塞时间,
   * 例如,如果B块被拿来,而任务还没有完成处理块A,它不被认为是阻塞B块
   */
  private var _fetchWaitTime: Long = _
  def fetchWaitTime: Long = _fetchWaitTime
  private[spark] def incFetchWaitTime(value: Long) = _fetchWaitTime += value
  private[spark] def decFetchWaitTime(value: Long) = _fetchWaitTime -= value

  /**
   * Total number of remote bytes read from the shuffle by this task
   * 读取的远程任务的shuffle总字节数
   */
  private var _remoteBytesRead: Long = _
  def remoteBytesRead: Long = _remoteBytesRead
  private[spark] def incRemoteBytesRead(value: Long) = _remoteBytesRead += value
  private[spark] def decRemoteBytesRead(value: Long) = _remoteBytesRead -= value

  /**
   * Shuffle data that was read from the local disk (as opposed to from a remote executor).
   * 从本地磁盘读取的数据,而不是从一个远程执行
   */
  private var _localBytesRead: Long = _
  def localBytesRead: Long = _localBytesRead
  private[spark] def incLocalBytesRead(value: Long) = _localBytesRead += value

  /**
   * Total bytes fetched in the shuffle by this task (both remote and local).
   * 这个任务在洗牌中获取的总字节数(本地和远程)
   */
  def totalBytesRead: Long = _remoteBytesRead + _localBytesRead

  /**
   * Number of blocks fetched in this shuffle by this task (remote or local)
   * 此任务在这个洗牌中获取的块数(本地或远程)
   */
  def totalBlocksFetched: Int = _remoteBlocksFetched + _localBlocksFetched

  /**
   * Total number of records read from the shuffle by this task
   * 由该任务的随机读取的记录的总数
   */
  private var _recordsRead: Long = _
  def recordsRead: Long = _recordsRead
  private[spark] def incRecordsRead(value: Long) = _recordsRead += value
  private[spark] def decRecordsRead(value: Long) = _recordsRead -= value
}

/**
 * :: DeveloperApi ::
 * Metrics pertaining to shuffle data written in a given task.
 * 度量给定一个的任务中写入的洗牌数据
 */
@DeveloperApi
class ShuffleWriteMetrics extends Serializable {
  /**
   * Number of bytes written for the shuffle by this task
   * 由此任务为随机洗牌写入的字节数
   */
  @volatile private var _shuffleBytesWritten: Long = _
  def shuffleBytesWritten: Long = _shuffleBytesWritten
  private[spark] def incShuffleBytesWritten(value: Long) = _shuffleBytesWritten += value
  private[spark] def decShuffleBytesWritten(value: Long) = _shuffleBytesWritten -= value

  /**
   * Time the task spent blocking on writes to disk or buffer cache, in nanoseconds
   * 写入到磁盘或缓冲区任务的阻塞时间,纳秒
   */
  @volatile private var _shuffleWriteTime: Long = _
  def shuffleWriteTime: Long = _shuffleWriteTime
  private[spark] def incShuffleWriteTime(value: Long) = _shuffleWriteTime += value
  private[spark] def decShuffleWriteTime(value: Long) = _shuffleWriteTime -= value

  /**
   * Total number of records written to the shuffle by this task
   * 由该任务写入的记录的总数
   */
  @volatile private var _shuffleRecordsWritten: Long = _
  def shuffleRecordsWritten: Long = _shuffleRecordsWritten
  private[spark] def incShuffleRecordsWritten(value: Long) = _shuffleRecordsWritten += value
  private[spark] def decShuffleRecordsWritten(value: Long) = _shuffleRecordsWritten -= value
  private[spark] def setShuffleRecordsWritten(value: Long) = _shuffleRecordsWritten = value
}
