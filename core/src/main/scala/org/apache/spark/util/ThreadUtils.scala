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


package org.apache.spark.util

import java.util.concurrent._

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.util.control.NonFatal

import com.google.common.util.concurrent.{MoreExecutors, ThreadFactoryBuilder}

private[spark] object ThreadUtils {

  private val sameThreadExecutionContext =
    ExecutionContext.fromExecutorService(MoreExecutors.sameThreadExecutor())

  /**
   * An `ExecutionContextExecutor` that runs each task in the thread that invokes `execute/submit`.
   * The caller should make sure the tasks running in this `ExecutionContextExecutor` are short and
   * never block.
   */
  def sameThread: ExecutionContextExecutor = sameThreadExecutionContext

  /**
   * Create a thread factory that names threads with a prefix and also sets the threads to daemon.
   * 创建线程序名称,并设置守护线程
   */
  def namedThreadFactory(prefix: String): ThreadFactory = {
    new ThreadFactoryBuilder().setDaemon(true).setNameFormat(prefix + "-%d").build()
  }

  /**
   * Wrapper over newCachedThreadPool. Thread names are formatted as prefix-ID, where ID is a
   * unique, sequentially assigned integer.
   */ 
  def newDaemonCachedThreadPool(prefix: String): ThreadPoolExecutor = {
    val threadFactory = namedThreadFactory(prefix)
    Executors.newCachedThreadPool(threadFactory).asInstanceOf[ThreadPoolExecutor]
  }

  /**
   * Create a cached thread pool whose max number of threads is `maxThreadNumber`. Thread names
   * are formatted as prefix-ID, where ID is a unique, sequentially assigned integer.
   */
  def newDaemonCachedThreadPool(
      prefix: String, maxThreadNumber: Int, keepAliveSeconds: Int = 60): ThreadPoolExecutor = {
    val threadFactory = namedThreadFactory(prefix)    
    val threadPool = new ThreadPoolExecutor(
      maxThreadNumber, // corePoolSize: 核心线程数，会一直存活，即使没有任务，线程池也会维护线程的最少数量
      maxThreadNumber, // maximumPoolSize:线程池维护线程的最大数量
      keepAliveSeconds,//线程池维护线程所允许的空闲时间，当线程空闲时间达到keepAliveTime，该线程会退出，
      //直到线程数量等于corePoolSize。如果allowCoreThreadTimeout设置为true，则所有线程均会退出直到线程数量为0
      TimeUnit.SECONDS,//线程池维护线程所允许的空闲时间的单位
      new LinkedBlockingQueue[Runnable],//线程池所使用的缓冲队列
      threadFactory)//执行程序创建新线程时使用的工厂
    threadPool.allowCoreThreadTimeOut(true)//允许超时自动关闭
    threadPool
  }

  /**
   * Wrapper over newFixedThreadPool. Thread names are formatted as prefix-ID, where ID is a
   * unique, sequentially assigned integer.
   */
  def newDaemonFixedThreadPool(nThreads: Int, prefix: String): ThreadPoolExecutor = {
    val threadFactory = namedThreadFactory(prefix)
    Executors.newFixedThreadPool(nThreads, threadFactory).asInstanceOf[ThreadPoolExecutor]
  }

  /**
   * Wrapper over newSingleThreadExecutor.
   */
  def newDaemonSingleThreadExecutor(threadName: String): ExecutorService = {
    val threadFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat(threadName).build()
    Executors.newSingleThreadExecutor(threadFactory)
  }

  /**
   * Wrapper over ScheduledThreadPoolExecutor.
   * ScheduledExecutorService定时周期执行指定的任务,基于时间的延迟，不会由于系统时间的改变发生执行变化
   * Timer执行周期任务时依赖系统时间,如果当前系统时间发生变化会出现一些执行上的变化
   */
  def newDaemonSingleThreadScheduledExecutor(threadName: String): ScheduledExecutorService = {
    //setDaemon设置为守护线程（也称为后台线程）
    val threadFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat(threadName).build()
    Executors.newSingleThreadScheduledExecutor(threadFactory)//创建大小为1的固定线程池
  }

  /**
   * Run a piece of code in a new thread and return the result. Exception in the new thread is
   * thrown in the caller thread with an adjusted stack trace that removes references to this
   * method for clarity. The exception stack traces will be like the following
   *
   * SomeException: exception-message
   *   at CallerClass.body-method (sourcefile.scala)
   *   at ... run in separate thread using org.apache.spark.util.ThreadUtils ... ()
   *   at CallerClass.caller-method (sourcefile.scala)
   *   ...
   */
  def runInNewThread[T](
      threadName: String,
      isDaemon: Boolean = true)(body: => T): T = {
    @volatile var exception: Option[Throwable] = None
    @volatile var result: T = null.asInstanceOf[T]

    val thread = new Thread(threadName) {
      override def run(): Unit = {
        try {
          result = body
        } catch {
          case NonFatal(e) =>
            exception = Some(e)
        }
      }
    }
    thread.setDaemon(isDaemon)
    thread.start()
    thread.join()

    exception match {
      case Some(realException) =>
        // Remove the part of the stack that shows method calls into this helper method
        // This means drop everything from the top until the stack element
        // ThreadUtils.runInNewThread(), and then drop that as well (hence the `drop(1)`).
        val baseStackTrace = Thread.currentThread().getStackTrace().dropWhile(
          ! _.getClassName.contains(this.getClass.getSimpleName)).drop(1)

        // Remove the part of the new thread stack that shows methods call from this helper method
        val extraStackTrace = realException.getStackTrace.takeWhile(
          ! _.getClassName.contains(this.getClass.getSimpleName))

        // Combine the two stack traces, with a place holder just specifying that there
        // was a helper method used, without any further details of the helper
        val placeHolderStackElem = new StackTraceElement(
          s"... run in separate thread using ${ThreadUtils.getClass.getName.stripSuffix("$")} ..",
          " ", "", -1)
        val finalStackTrace = extraStackTrace ++ Seq(placeHolderStackElem) ++ baseStackTrace

        // Update the stack trace and rethrow the exception in the caller thread
        realException.setStackTrace(finalStackTrace)
        throw realException
      case None =>
        result
    }
  }
}
