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

import java.io.{File, FileNotFoundException, IOException, PrintWriter}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import scala.collection.mutable.HashMap

import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.executor.TaskMetrics

/**
 * :: DeveloperApi ::
 * A logger class to record runtime information for jobs in Spark. This class outputs one log file
 * for each Spark job, containing tasks start/stop and shuffle information. JobLogger is a subclass
 * of SparkListener, use addSparkListener to add JobLogger to a SparkContext after the SparkContext
 * is created. Note that each JobLogger only works for one SparkContext
  *
  * 用于记录Spark中作业的运行时信息的记录器类。 此类为每个Spark作业输出一个日志文件，其中包含任务开始/停止和随机播放信息。
  * JobLogger是SparkListener的子类，在创建SparkContext之后，使用addSparkListener将JobLogger添加到SparkContext。
  * 请注意，每个JobLogger仅适用于一个SparkContext
 *
 * NOTE: The functionality of this class is heavily stripped down to accommodate for a general
 * refactor of the SparkListener interface. In its place, the EventLoggingListener is introduced
 * to log application information as SparkListenerEvents. To enable this functionality, set
 * spark.eventLog.enabled to true.
  * 注意：此类的功能被大量删除以适应SparkListener接口的一般重构,引用了EventLoggingListener来将应用程序信息记录为SparkListenerEvents,
  * 要启用此功能，请将spark.eventLog.enabled设置为true。
 */
@DeveloperApi
@deprecated("Log application information by setting spark.eventLog.enabled.", "1.0.0")
class JobLogger(val user: String, val logDirName: String) extends SparkListener with Logging {

  def this() = this(System.getProperty("user.name", "<unknown>"),
    String.valueOf(System.currentTimeMillis()))

  private val logDir =
    if (System.getenv("SPARK_LOG_DIR") != null) {
      System.getenv("SPARK_LOG_DIR")
    } else {
      "/tmp/spark-%s".format(user)
    }

  private val jobIdToPrintWriter = new HashMap[Int, PrintWriter]
  private val stageIdToJobId = new HashMap[Int, Int]
  private val jobIdToStageIds = new HashMap[Int, Seq[Int]]
  private val dateFormat = new ThreadLocal[SimpleDateFormat]() {
    override def initialValue(): SimpleDateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
  }

  createLogDir()

  /** 
   *  Create a folder for log files, the folder's name is the creation time of jobLogger 
   *  创建日志文件的文件夹,文件夹的名称是jobLogger的创建时间,
   *  */
  protected def createLogDir() {
    val dir = new File(logDir + "/" + logDirName + "/")
    if (dir.exists()) {
      return
    }
    if (!dir.mkdirs()) {
      // JobLogger should throw a exception rather than continue to construct this object.
      //JobLogger应该抛出一个异常，而不是继续构造这个对象
      throw new IOException("create log directory error:" + logDir + "/" + logDirName + "/")
    }
  }

  /**
   * Create a log file for one job
   * 创建一个Job日志文件
   * @param jobId ID of the job
   * @throws FileNotFoundException Fail to create log file 无法创建日志文件
   */
  protected def createLogWriter(jobId: Int) {
    try {
      val fileWriter = new PrintWriter(logDir + "/" + logDirName + "/" + jobId)
      jobIdToPrintWriter += (jobId -> fileWriter)
    } catch {
      case e: FileNotFoundException => e.printStackTrace()
    }
  }

  /**
   * Close log file, and clean the stage relationship in stageIdToJobId
   * 关闭日志文件,清理stage关系,
   * @param jobId ID of the job
   */
  protected def closeLogWriter(jobId: Int) {
    jobIdToPrintWriter.get(jobId).foreach { fileWriter =>
      fileWriter.close()
      jobIdToStageIds.get(jobId).foreach(_.foreach { stageId =>
        stageIdToJobId -= stageId
      })
      jobIdToPrintWriter -= jobId
      jobIdToStageIds -= jobId
    }
  }

  /**
   * Build up the maps that represent stage-job relationships
   * 构建stage-job依赖关系
   * @param jobId ID of the job
   * @param stageIds IDs of the associated stages 相关阶段的ID
   */
  protected def buildJobStageDependencies(jobId: Int, stageIds: Seq[Int]) = {
    jobIdToStageIds(jobId) = stageIds
    stageIds.foreach { stageId => stageIdToJobId(stageId) = jobId }
  }

  /**
   * Write info into log file
   * 将信息写入日志文件
   * @param jobId ID of the job
   * @param info Info to be recorded
   * @param withTime Controls whether to record time stamp before the info, default is true
    *                 控制是否在信息之前记录时间戳,默认为true
   */
  protected def jobLogInfo(jobId: Int, info: String, withTime: Boolean = true) {
    var writeInfo = info
    if (withTime) {
      val date = new Date(System.currentTimeMillis())
      writeInfo = dateFormat.get.format(date) + ": " + info
    }
    // scalastyle:off println
    jobIdToPrintWriter.get(jobId).foreach(_.println(writeInfo))
    // scalastyle:on println
  }

  /**
   * Write info into log file
   * 将信息写入日志文件
   * @param stageId ID of the stage
   * @param info Info to be recorded
   * @param withTime Controls whether to record time stamp before the info, default is true
    *                 控制是否在信息之前记录时间戳,默认为true
   */
  protected def stageLogInfo(stageId: Int, info: String, withTime: Boolean = true) {
    stageIdToJobId.get(stageId).foreach(jobId => jobLogInfo(jobId, info, withTime))
  }

  /**
   * Record task metrics into job log files, including execution info and shuffle metrics
   * 将任务度量记录到工作日志文件中,包括执行(execution)信息和shuffle度量
   * @param stageId Stage ID of the task 任务的阶段ID
   * @param status Status info of the task 任务的状态信息
   * @param taskInfo Task description info 任务说明信息
   * @param taskMetrics Task running metrics 任务运行指标
   */
  protected def recordTaskMetrics(stageId: Int, status: String,
                                taskInfo: TaskInfo, taskMetrics: TaskMetrics) {
    val info = " TID=" + taskInfo.taskId + " STAGE_ID=" + stageId +
               " START_TIME=" + taskInfo.launchTime + " FINISH_TIME=" + taskInfo.finishTime +
               " EXECUTOR_ID=" + taskInfo.executorId +  " HOST=" + taskMetrics.hostname
    val executorRunTime = " EXECUTOR_RUN_TIME=" + taskMetrics.executorRunTime
    val gcTime = " GC_TIME=" + taskMetrics.jvmGCTime
    val inputMetrics = taskMetrics.inputMetrics match {
      case Some(metrics) =>
        " READ_METHOD=" + metrics.readMethod.toString +
        " INPUT_BYTES=" + metrics.bytesRead
      case None => ""
    }
    val outputMetrics = taskMetrics.outputMetrics match {
      case Some(metrics) =>
        " OUTPUT_BYTES=" + metrics.bytesWritten
      case None => ""
    }
    val shuffleReadMetrics = taskMetrics.shuffleReadMetrics match {
      case Some(metrics) =>
        " BLOCK_FETCHED_TOTAL=" + metrics.totalBlocksFetched +
        " BLOCK_FETCHED_LOCAL=" + metrics.localBlocksFetched +
        " BLOCK_FETCHED_REMOTE=" + metrics.remoteBlocksFetched +
        " REMOTE_FETCH_WAIT_TIME=" + metrics.fetchWaitTime +
        " REMOTE_BYTES_READ=" + metrics.remoteBytesRead +
        " LOCAL_BYTES_READ=" + metrics.localBytesRead
      case None => ""
    }
    val writeMetrics = taskMetrics.shuffleWriteMetrics match {
      case Some(metrics) =>
        " SHUFFLE_BYTES_WRITTEN=" + metrics.shuffleBytesWritten +
        " SHUFFLE_WRITE_TIME=" + metrics.shuffleWriteTime
      case None => ""
    }
    stageLogInfo(stageId, status + info + executorRunTime + gcTime + inputMetrics + outputMetrics +
      shuffleReadMetrics + writeMetrics)
  }

  /**
   * When stage is submitted, record stage submit info
   * 提交阶段(Stage)时,记录提交阶段(Stage)信息
   * @param stageSubmitted Stage submitted event
   */
  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) {
    val stageInfo = stageSubmitted.stageInfo
    stageLogInfo(stageInfo.stageId, "STAGE_ID=%d STATUS=SUBMITTED TASK_SIZE=%d".format(
      stageInfo.stageId, stageInfo.numTasks))
  }

  /**
   * When stage is completed, record stage completion status
   * 当阶段(Stage)完成时,记录阶段(Stage)完成状态
   * @param stageCompleted Stage completed event
   */
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) {
    val stageId = stageCompleted.stageInfo.stageId
    if (stageCompleted.stageInfo.failureReason.isEmpty) {
      stageLogInfo(stageId, s"STAGE_ID=$stageId STATUS=COMPLETED")
    } else {
      stageLogInfo(stageId, s"STAGE_ID=$stageId STATUS=FAILED")
    }
  }

  /**
   * When task ends, record task completion status and metrics
   * 当任务结束时,记录任务完成状态和度量
   * @param taskEnd Task end event
   */
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    val taskInfo = taskEnd.taskInfo
    var taskStatus = "TASK_TYPE=%s".format(taskEnd.taskType)
    val taskMetrics = if (taskEnd.taskMetrics != null) taskEnd.taskMetrics else TaskMetrics.empty
    taskEnd.reason match {
      case Success => taskStatus += " STATUS=SUCCESS"
        recordTaskMetrics(taskEnd.stageId, taskStatus, taskInfo, taskMetrics)
      case Resubmitted =>
        taskStatus += " STATUS=RESUBMITTED TID=" + taskInfo.taskId +
                      " STAGE_ID=" + taskEnd.stageId
        stageLogInfo(taskEnd.stageId, taskStatus)
      //对应RDD的partionsID
      case FetchFailed(bmAddress, shuffleId, mapId, reduceId, message) =>
        taskStatus += " STATUS=FETCHFAILED TID=" + taskInfo.taskId + " STAGE_ID=" +
                      taskEnd.stageId + " SHUFFLE_ID=" + shuffleId + " MAP_ID=" +
          //对应RDD的partionsID
                      mapId + " REDUCE_ID=" + reduceId
        stageLogInfo(taskEnd.stageId, taskStatus)
      case _ =>
    }
  }

  /**
   * When job ends, recording job completion status and close log file
   * 当作业结束时,记录作业完成状态和关闭日志文件
   * @param jobEnd Job end event
   */
  override def onJobEnd(jobEnd: SparkListenerJobEnd) {
    val jobId = jobEnd.jobId
    var info = "JOB_ID=" + jobId
    jobEnd.jobResult match {
      case JobSucceeded => info += " STATUS=SUCCESS"
      case JobFailed(exception) =>
        info += " STATUS=FAILED REASON="
        exception.getMessage.split("\\s+").foreach(info += _ + "_")
      case _ =>
    }
    jobLogInfo(jobId, info.substring(0, info.length - 1).toUpperCase)
    closeLogWriter(jobId)
  }

  /**
   * Record job properties into job log file
   * 将作业属性保存到作业日志文件中
   * @param jobId ID of the job
   * @param properties Properties of the job 工作的属性
   */
  protected def recordJobProperties(jobId: Int, properties: Properties) {
    if (properties != null) {
      val description = properties.getProperty(SparkContext.SPARK_JOB_DESCRIPTION, "")
      jobLogInfo(jobId, description, withTime = false)
    }
  }

  /**
   * When job starts, record job property and stage graph
   * 当作业开始,记录Job的属性和Stage
   * @param jobStart Job start event
   */
  override def onJobStart(jobStart: SparkListenerJobStart) {
    val jobId = jobStart.jobId
    val properties = jobStart.properties
    createLogWriter(jobId)
    recordJobProperties(jobId, properties)
    buildJobStageDependencies(jobId, jobStart.stageIds)
    jobLogInfo(jobId, "JOB_ID=" + jobId + " STATUS=STARTED")
  }
}
