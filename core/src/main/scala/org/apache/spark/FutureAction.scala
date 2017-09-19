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

import java.util.Collections
import java.util.concurrent.TimeUnit

import org.apache.spark.api.java.JavaFutureAction
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{JobFailed, JobSucceeded, JobWaiter}

import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.util.{Failure, Try}

/**
 * A future for the result of an action to support cancellation. This is an extension of the
 * Scala Future interface to support cancellation.
  * future的行动结果支持取消,这是Scala Future界面的扩展,以支持取消。
 */
trait FutureAction[T] extends Future[T] {
  // Note that we redefine methods of the Future trait here explicitly so we can specify a different
  // documentation (with reference to the word "action").
  //请注意,我们明确地重新定义了“Future”特征的方法,因此我们可以指定一个不同的文档（参考“action”一词）
  /**
   * Cancels the execution of this action. 取消执行此操作
   */
  def cancel()

  /**
   * Blocks until this action completes. 直到此操作完成为止
   * @param atMost maximum wait time, which may be negative (no waiting is done), Duration.Inf
   *               for unbounded waiting, or a finite positive duration
    *               最大等待时间,可能为负数(无等待完成),持续时间。无限等待时间或持续时间有限,
   * @return this FutureAction
   */
  override def ready(atMost: Duration)(implicit permit: CanAwait): FutureAction.this.type

  /**
   * Awaits and returns the result (of type T) of this action.
    * 等待并返回此操作的结果（类型T）
   * @param atMost maximum wait time, which may be negative (no waiting is done), Duration.Inf
   *               for unbounded waiting, or a finite positive duration
    *               最长等待时间，可能为负(不等待完成),Duration.Inf无限等待,或有限的正期
   * @throws Exception exception during action execution 行动执行期间异常
   * @return the result value if the action is completed within the specific maximum wait time
    *         如果操作在特定最大等待时间内完成,则结果值
   */
  @throws(classOf[Exception])
  override def result(atMost: Duration)(implicit permit: CanAwait): T

  /**
   * When this action is completed, either through an exception, or a value, applies the provided
   * function.当此操作完成时,通过异常或值来应用提供的功能。
    * Try的子类Success或者Failure,如果计算成功,返回Success的实例,如果抛出异常,返回Failure并携带相关信息
   */
  def onComplete[U](func: (Try[T]) => U)(implicit executor: ExecutionContext)

  /**
   * Returns whether the action has already been completed with a value or an exception.
    * 返回动作是否已经用值或异常完成
   */
  override def isCompleted: Boolean

  /**
   * Returns whether the action has been cancelled.
    * 返回操作是否已被取消
   */
  def isCancelled: Boolean

  /**
   * The value of this Future.
   * 这个未来的价值。如果未来未完成，返回的值将为“无”。 如果未来完成如果包含有效结果，则值为Some（Success（t））
    * 或者Some（Failure（error））if它包含一个异常。
   * If the future is not completed the returned value will be None. If the future is completed
   * the value will be Some(Success(t)) if it contains a valid result, or Some(Failure(error)) if
   * it contains an exception.
   */
  override def value: Option[Try[T]]

  /**
   * Blocks and returns the result of this job.
    * 阻塞并返回此作业的结果。
   *Await.result会导致当前线程被阻塞,并等待actor通过它的应答来完成Future
   */
  @throws(classOf[Exception])
  def get(): T = Await.result(this, Duration.Inf)

  /**
   * Returns the job IDs run by the underlying async operation.
    * 返回基础异步操作运行的作业ID
   *
   * This returns the current snapshot of the job list. Certain operations may run multiple
   * jobs, so multiple calls to this method may return different lists.
    * 这将返回作业列表的当前快照, 某些操作可以运行多个作业,所以多次调用此方法可能会返回不同的列表。
   */
  def jobIds: Seq[Int]

}


/**
 * A [[FutureAction]] holding the result of an action that triggers a single job. Examples include
 * count, collect, reduce.
  * FutureAction 持有触发单个作业的操作的结果,示例包括计数,收集,减少
 */
class SimpleFutureAction[T] private[spark](jobWaiter: JobWaiter[_], resultFunc: => T)
  extends FutureAction[T] {

  @volatile private var _cancelled: Boolean = false

  override def cancel() {
    _cancelled = true
    jobWaiter.cancel()
  }

  override def ready(atMost: Duration)(implicit permit: CanAwait): SimpleFutureAction.this.type = {
    //isFinite此方法返回此持续时间是否有限
    if (!atMost.isFinite()) {
      awaitResult()
    } else jobWaiter.synchronized {
      val finishTime = System.currentTimeMillis() + atMost.toMillis
      while (!isCompleted) {
        val time = System.currentTimeMillis()
        if (time >= finishTime) {
          throw new TimeoutException
        } else {
          jobWaiter.wait(finishTime - time)
        }
      }
    }
    this
  }

  @throws(classOf[Exception])
  override def result(atMost: Duration)(implicit permit: CanAwait): T = {
    ready(atMost)(permit)
    awaitResult() match {
      case scala.util.Success(res) => res
      case scala.util.Failure(e) => throw e
    }
  }

  override def onComplete[U](func: (Try[T]) => U)(implicit executor: ExecutionContext) {
    executor.execute(new Runnable {
      override def run() {
        func(awaitResult())
      }
    })
  }

  override def isCompleted: Boolean = jobWaiter.jobFinished

  override def isCancelled: Boolean = _cancelled
  /**
    * Try的子类Success或者Failure,如果计算成功,返回Success的实例,如果抛出异常,返回Failure并携带相关信息
    * @return
    */
  override def value: Option[Try[T]] = {
    if (jobWaiter.jobFinished) {
      Some(awaitResult())
    } else {
      None
    }
  }

  private def awaitResult(): Try[T] = {
    //awaitResult 直到Job执行完成之后返回所得的结果
    jobWaiter.awaitResult() match {
      case JobSucceeded => scala.util.Success(resultFunc)
      case JobFailed(e: Exception) => scala.util.Failure(e)
    }
  }

  def jobIds: Seq[Int] = Seq(jobWaiter.jobId)
}


/**
 * A [[FutureAction]] for actions that could trigger multiple Spark jobs. Examples include take,
 * takeSample. Cancellation works by setting the cancelled flag to true and interrupting the
 * action thread if it is being blocked by a job.
  * A [[FutureAction]]可以触发多个Spark作业的操作,示例包括take,takeSample,
  * 通过将取消的标志设置为true并中断操作线程(如果被作业阻止),取消工作。
 */
class ComplexFutureAction[T] extends FutureAction[T] {

  // Pointer to the thread that is executing the action. It is set when the action is run.
  //指向正在执行操作的线程的指针,运行动作时设置。
  @volatile private var thread: Thread = _

  // A flag indicating whether the future has been cancelled. This is used in case the future
  // is cancelled before the action was even run (and thus we have no thread to interrupt).
  //指示未来是否被取消的标志,这是为了在将来被取消之前被使用,在动作被平均运行之前(因此我们没有线程中断)。
  @volatile private var _cancelled: Boolean = false

  @volatile private var jobs: Seq[Int] = Nil

  // A promise used to signal the future.
  //用来表示未来的承诺
  private val p = promise[T]()

  override def cancel(): Unit = this.synchronized {
    _cancelled = true
    if (thread != null) {
      thread.interrupt()
    }
  }

  /**
   * Executes some action enclosed in the closure. To properly enable cancellation, the closure
   * should use runJob implementation in this promise. See takeAsync for example.
    * 执行封闭中包含的一些操作,为了正确启用取消,关闭应该在本承诺中使用runJob实现,请参见takeAsync例如
   */
  def run(func: => T)(implicit executor: ExecutionContext): this.type = {
    scala.concurrent.future {
      thread = Thread.currentThread
      try {
        p.success(func)
      } catch {
        case e: Exception => p.failure(e)
      } finally {
        // This lock guarantees when calling `thread.interrupt()` in `cancel`,
        // thread won't be set to null.
        ///在`cancel`中调用`thread.interrupt（）`时,线程不会被设置为null。
        ComplexFutureAction.this.synchronized {
          thread = null
        }
      }
    }
    this
  }

  /**
   * Runs a Spark job. This is a wrapper around the same functionality provided by SparkContext
   * to enable cancellation.
    * 运行Spark工作,这是SparkContext提供的相同功能的封装启用取消。
   */
  def runJob[T, U, R](
      rdd: RDD[T],
      processPartition: Iterator[T] => U,
      partitions: Seq[Int],
      resultHandler: (Int, U) => Unit,
      resultFunc: => R) {
    // If the action hasn't been cancelled yet, submit the job. The check and the submitJob
    // command need to be in an atomic block.
    //如果该动作尚未被取消,请提交作业,检查和submitJob命令需要在原子块中
    val job = this.synchronized {
      if (!isCancelled) {
        rdd.context.submitJob(rdd, processPartition, partitions, resultHandler, resultFunc)
      } else {
        throw new SparkException("Action has been cancelled")
      }
    }

    this.jobs = jobs ++ job.jobIds

    // Wait for the job to complete. If the action is cancelled (with an interrupt),
    //等待工作完成,如果动作被取消(中断),取消作业并停止执行,这不是因为同步的块Await.ready最终等待在FutureJob.jobWaiter的显示器上
    // cancel the job and stop the execution. This is not in a synchronized block because
    // Await.ready eventually waits on the monitor in FutureJob.jobWaiter.
    try {
      Await.ready(job, Duration.Inf)
    } catch {
      case e: InterruptedException =>
        job.cancel()
        throw new SparkException("Action has been cancelled")
    }
  }

  override def isCancelled: Boolean = _cancelled

  @throws(classOf[InterruptedException])
  @throws(classOf[scala.concurrent.TimeoutException])
  override def ready(atMost: Duration)(implicit permit: CanAwait): this.type = {
    p.future.ready(atMost)(permit)
    this
  }

  @throws(classOf[Exception])
  override def result(atMost: Duration)(implicit permit: CanAwait): T = {
    p.future.result(atMost)(permit)
  }

  override def onComplete[U](func: (Try[T]) => U)(implicit executor: ExecutionContext): Unit = {
    p.future.onComplete(func)(executor)
  }

  override def isCompleted: Boolean = p.isCompleted

  override def value: Option[Try[T]] = p.future.value

  def jobIds: Seq[Int] = jobs

}

private[spark]
class JavaFutureActionWrapper[S, T](futureAction: FutureAction[S], converter: S => T)
  extends JavaFutureAction[T] {

  import scala.collection.JavaConverters._

  override def isCancelled: Boolean = futureAction.isCancelled

  override def isDone: Boolean = {
    // According to java.util.Future's Javadoc, this returns True if the task was completed,
    // whether that completion was due to successful execution, an exception, or a cancellation.
    //根据java.util.Future的Javadoc,如果任务完成,则返回True,是否完成是由于执行成功，异常或取消。
    futureAction.isCancelled || futureAction.isCompleted
  }

  override def jobIds(): java.util.List[java.lang.Integer] = {
    //unmodifiableList 将参数中的List返回一个不可修改的List.
    Collections.unmodifiableList(futureAction.jobIds.map(Integer.valueOf).asJava)
  }

  private def getImpl(timeout: Duration): T = {
    // This will throw TimeoutException on timeout:
    //这将在超时时抛出TimeoutException
    //等待“completed”的awaitable状态
    Await.ready(futureAction, timeout)
    futureAction.value.get match {
      case scala.util.Success(value) => converter(value)
      case Failure(exception) =>
        if (isCancelled) {
          throw new CancellationException("Job cancelled").initCause(exception)
        } else {
          // java.util.Future.get() wraps exceptions in ExecutionException
          //java.util.Future.get（）在ExecutionException中包装异常
          throw new ExecutionException("Exception thrown by job", exception)
        }
    }
  }

  override def get(): T = getImpl(Duration.Inf)

  override def get(timeout: Long, unit: TimeUnit): T =
    getImpl(Duration.fromNanos(unit.toNanos(timeout)))

  override def cancel(mayInterruptIfRunning: Boolean): Boolean = synchronized {
    if (isDone) {
      // According to java.util.Future's Javadoc, this should return false if the task is completed.
      //根据java.util.Future的Javadoc,如果任务完成,则返回false,
      false
    } else {
      // We're limited in terms of the semantics we can provide here; our cancellation is
      // asynchronous and doesn't provide a mechanism to not cancel if the job is running.
      //我们在这里提供的语义有限,我们的取消是异步的,如果作业正在运行,则不提供不取消的机制
      futureAction.cancel()
      true
    }
  }

}
