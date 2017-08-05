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

import java.io.{ObjectInputStream, ObjectOutputStream}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.Utils

// ==============================================================================================
// NOTE: new task end reasons MUST be accompanied with serialization logic in util.JsonProtocol!
//       新任务结束的原因必须在util.JsonProtocol中附带序列化逻辑！
// ==============================================================================================

/**
 * :: DeveloperApi ::
 * Various possible reasons why a task ended. The low-level TaskScheduler is supposed to retry
 * tasks several times for "ephemeral" failures, and only report back failures that require some
 * old stages to be resubmitted, such as shuffle map fetch failures.
  * 任务结束各种可能的原因,低级TaskScheduler应该为“短暂ephemeral”失败多次重试任务,
  * 只报告需要重新提交一些旧阶段的故障,例如shuffle map提取失败。
  *
 * 任务结束的原因
 */
@DeveloperApi
//sealed trait 仅能被同一文件的的类继承
sealed trait TaskEndReason

/**
 * :: DeveloperApi ::
 * Task succeeded.
  * 任务成功
 */
@DeveloperApi
case object Success extends TaskEndReason

/**
 * :: DeveloperApi ::
 * Various possible reasons why a task failed.
 * 任务失败各种可能的原因
 */
@DeveloperApi
sealed trait TaskFailedReason extends TaskEndReason {
  /** Error message displayed in the web UI.
    * 错误消息显示在Web UI中
    *  */
  def toErrorString: String
}

/**
 * :: DeveloperApi ::
 * A [[org.apache.spark.scheduler.ShuffleMapTask]] that completed successfully earlier, but we
 * lost the executor before the stage completed. This means Spark needs to reschedule the task
 * to be re-executed on a different executor.
  * 一个[[org.apache.spark.scheduler.ShuffleMapTask]]之前成功完成,
  * 但在stage完成之前我们失去了执行者,这意味着Spark需要重新调度在不同的执行器上重新执行任务
 */
@DeveloperApi
case object Resubmitted extends TaskFailedReason {
  //重新提交(因丢失执行而重新提交)
  override def toErrorString: String = "Resubmitted (resubmitted due to lost executor)"
}

/**
 * :: DeveloperApi ::
 * Task failed to fetch shuffle data from a remote node. Probably means we have lost the remote
 * executors the task is trying to fetch from, and thus need to rerun the previous stage.
  * 任务无法从远程节点获取shuffle的数据,可能意味着我们丢失的任务要从获取的远程执行器(executors),因此需要重新运行前一个阶段
 */
@DeveloperApi
case class FetchFailed(
    bmAddress: BlockManagerId,  // Note that bmAddress can be null
    shuffleId: Int,
    mapId: Int,
    reduceId: Int,
    message: String)
  extends TaskFailedReason {
  override def toErrorString: String = {
    val bmAddressString = if (bmAddress == null) "null" else bmAddress.toString
    s"FetchFailed($bmAddressString, shuffleId=$shuffleId, mapId=$mapId, reduceId=$reduceId, " +
      s"message=\n$message\n)"
  }
}

/**
 * :: DeveloperApi ::
 * Task failed due to a runtime exception. This is the most common failure case and also captures
 * user program exceptions.
  * 任务由于运行时异常而失败,这是最常见的故障案例,并且捕获用户程序异常。
 *
 * `stackTrace` contains the stack trace of the exception itself. It still exists for backward
 * compatibility. It's better to use `this(e: Throwable, metrics: Option[TaskMetrics])` to
 * create `ExceptionFailure` as it will handle the backward compatibility properly.
  *
  * `stackTrace`包含异常本身的堆栈跟踪,它仍然存在向后兼容性,
  * 最好使用'this（e：Throwable，metrics：Option [TaskMetrics]）'创建“ExceptionFailure”，因为它会正确处理向后兼容性。
 *
 * `fullStackTrace` is a better representation of the stack trace because it contains the whole
 * stack trace including the exception and its causes
  *
  * `fullStackTrace`是对堆栈跟踪的更好的表示,因为它包含整个堆栈跟踪,包括异常及其原因
 *
 * `exception` is the actual exception that caused the task to fail. It may be `None` in
 * the case that the exception is not in fact serializable. If a task fails more than
 * once (due to retries), `exception` is that one that caused the last failure.
  *
  * `exception`是导致任务失败的实际异常,它可能是“无”该异常实际上不是可序列化的情况,
  * 如果任务失败多次（由于重试），则“异常”是导致上次失败的异常。
 */
@DeveloperApi
case class ExceptionFailure(
    className: String,
    description: String,
    stackTrace: Array[StackTraceElement],
    fullStackTrace: String,
    metrics: Option[TaskMetrics],
    private val exceptionWrapper: Option[ThrowableSerializationWrapper])
  extends TaskFailedReason {

  /**
   * `preserveCause` is used to keep the exception itself so it is available to the
   * driver. This may be set to `false` in the event that the exception is not in fact
   * serializable.
    * `preserveCause`用于保持异常本身,因此可用于驱动程序,如果异常实际上不是可序列化的,则可以将其设置为“false”。
   */
  private[spark] def this(e: Throwable, metrics: Option[TaskMetrics], preserveCause: Boolean) {
    this(e.getClass.getName, e.getMessage, e.getStackTrace, Utils.exceptionString(e), metrics,
      if (preserveCause) Some(new ThrowableSerializationWrapper(e)) else None)
  }

  private[spark] def this(e: Throwable, metrics: Option[TaskMetrics]) {
    this(e, metrics, preserveCause = true)
  }

  def exception: Option[Throwable] = exceptionWrapper.flatMap {
    (w: ThrowableSerializationWrapper) => Option(w.exception)
  }

  override def toErrorString: String =
    if (fullStackTrace == null) {
      // fullStackTrace is added in 1.2.0
      // If fullStackTrace is null, use the old error string for backward compatibility
      //如果fullStackTrace为null,请使用旧的错误字符串进行向后兼容
      exceptionString(className, description, stackTrace)
    } else {
      fullStackTrace
    }

  /**
   * Return a nice string representation of the exception, including the stack trace.
   * Note: It does not include the exception's causes, and is only used for backward compatibility.
    * 返回一个很好的字符串表示的异常,包括堆栈跟踪,注意：它不包括异常的原因，仅用于向后兼容。
   */
  private def exceptionString(
      className: String,
      description: String,
      stackTrace: Array[StackTraceElement]): String = {
    val desc = if (description == null) "" else description
    val st = if (stackTrace == null) "" else stackTrace.map("        " + _).mkString("\n")
    s"$className: $desc\n$st"
  }
}

/**
 * A class for recovering from exceptions when deserializing a Throwable that was
 * thrown in user task code. If the Throwable cannot be deserialized it will be null,
 * but the stacktrace and message will be preserved correctly in SparkException.
  * 用于在反序列化用户任务代码中抛出的Throwable时从异常中恢复的类。
  * 如果Throwable不能被反序列化,它将为null,但是在SparkException中，堆栈跟踪和消息将被正确保存。
 */
private[spark] class ThrowableSerializationWrapper(var exception: Throwable) extends
    Serializable with Logging {
  private def writeObject(out: ObjectOutputStream): Unit = {
    out.writeObject(exception)
  }
  private def readObject(in: ObjectInputStream): Unit = {
    try {
      exception = in.readObject().asInstanceOf[Throwable]
    } catch {
      case e : Exception => log.warn("Task exception could not be deserialized", e)
    }
  }
}

/**
 * :: DeveloperApi ::
 * The task finished successfully, but the result was lost from the executor's block manager before
 * it was fetched.
  * 任务成功完成,但在执行者获取的块管理器之前,其结果已经丢失
 */
@DeveloperApi
case object TaskResultLost extends TaskFailedReason {
  //TaskResultLost(结果从块管理器丢失)
  override def toErrorString: String = "TaskResultLost (result lost from block manager)"
}

/**
 * :: DeveloperApi ::
 * Task was killed intentionally and needs to be rescheduled.
  * 任务被故意杀死,需要重新调度
 */
@DeveloperApi
case object TaskKilled extends TaskFailedReason {
  override def toErrorString: String = "TaskKilled (killed intentionally)"
}

/**
 * :: DeveloperApi ::
 * Task requested the driver to commit, but was denied.
  * 任务请求driver提交,但被拒绝
 */
@DeveloperApi
case class TaskCommitDenied(
    jobID: Int,
    partitionID: Int,
    attemptNumber: Int) extends TaskFailedReason {
  //TaskCommitDenied(驱动程序拒绝任务提交)
  override def toErrorString: String = s"TaskCommitDenied (Driver denied task commit)" +
    s" for job: $jobID, partition: $partitionID, attemptNumber: $attemptNumber"
}

/**
 * :: DeveloperApi ::
 * The task failed because the executor that it was running on was lost. This may happen because
 * the task crashed the JVM.
  * 该任务失败,因为它正在运行的执行程序丢失,这可能是因为任务崩溃了JVM
 */
@DeveloperApi
case class ExecutorLostFailure(execId: String) extends TaskFailedReason {
  override def toErrorString: String = s"ExecutorLostFailure (executor ${execId} lost)"
}

/**
 * :: DeveloperApi ::
 * We don't know why the task ended -- for example, because of a ClassNotFound exception when
 * deserializing the task result.
  * 我们不知道为什么任务结束 - 例如因为ClassNotFound异常的时候反序列出任务结果,
 */
@DeveloperApi
case object UnknownReason extends TaskFailedReason {
  override def toErrorString: String = "UnknownReason"
}
