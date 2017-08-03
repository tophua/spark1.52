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
   * 一个`ExecutionContextExecutor`，它运行线程中调用`execute / submit`的每个任务。
   * The caller should make sure the tasks running in this `ExecutionContextExecutor` are short and
   * never block.
    * 调用者应该确保任务在这个“ExecutionContextExecutor”中运行的任务很短，从不堵塞。
   */
  def sameThread: ExecutionContextExecutor = sameThreadExecutionContext

  /**
   * Create a thread factory that names threads with a prefix and also sets the threads to daemon.
   * 创建一个线程工厂,它使用前缀命名线程,并将线程设置为守护进程。
   */
  def namedThreadFactory(prefix: String): ThreadFactory = {
    new ThreadFactoryBuilder().setDaemon(true).setNameFormat(prefix + "-%d").build()
  }

  /**
   * Wrapper over newCachedThreadPool. Thread names are formatted as prefix-ID, where ID is a
   * unique, sequentially assigned integer.
    * 在newcachedthreadpool包装,线程名被格式化为前缀ID,其中id是唯一的、按顺序分配的整数。
   */ 
  def newDaemonCachedThreadPool(prefix: String): ThreadPoolExecutor = {
    val threadFactory = namedThreadFactory(prefix)
    //SynchronousQueue：一个不存储元素的阻塞队列。每个插入操作必须等到另一个线程调用移除操作，否则插入操作一直处于阻塞状态，
    //吞吐量通常要高于LinkedBlockingQueue，静态工厂方法Executors.newCachedThreadPool使用了这个队列。
    Executors.newCachedThreadPool(threadFactory).asInstanceOf[ThreadPoolExecutor]
  }

  /**
   * Create a cached thread pool whose max number of threads is `maxThreadNumber`. Thread names
   * are formatted as prefix-ID, where ID is a unique, sequentially assigned integer.
    * 创建一个缓存的线程池的最大线程数是` maxthreadnumber `,线程名被格式化为前缀ID，其中id是唯一的、按顺序分配的整数。
   */
  def newDaemonCachedThreadPool(
      prefix: String, maxThreadNumber: Int, keepAliveSeconds: Int = 60): ThreadPoolExecutor = {
    val threadFactory = namedThreadFactory(prefix)    
    val threadPool = new ThreadPoolExecutor(
      maxThreadNumber, // corePoolSize: 核心线程数,会一直存活,即使没有任务,线程池也会维护线程的最少数量
      maxThreadNumber, // maximumPoolSize:线程池维护线程的最大数量
      keepAliveSeconds,//线程池维护线程所允许的空闲时间,当线程空闲时间达到keepAliveTime,该线程会退出,
      //直到线程数量等于corePoolSize。如果allowCoreThreadTimeout设置为true,则所有线程均会退出直到线程数量为0
      TimeUnit.SECONDS,//线程池维护线程所允许的空闲时间的单位
      new LinkedBlockingQueue[Runnable],//线程池所使用的缓冲队列
      threadFactory)//执行程序创建新线程时使用的工厂
    threadPool.allowCoreThreadTimeOut(true)//允许超时自动关闭
    threadPool
  }

  /**
   * Wrapper over newFixedThreadPool. Thread names are formatted as prefix-ID, where ID is a
   * unique, sequentially assigned integer.
    * 包装在创建固定数目线程的线程池,线程名被格式化为前缀ID，其中id是唯一的、按顺序分配的整数,
   */
  def newDaemonFixedThreadPool(nThreads: Int, prefix: String): ThreadPoolExecutor = {
    val threadFactory = namedThreadFactory(prefix)
    //用于保存等待执行的任务的阻塞队列,
    //LinkedBlockingQueue：一个基于链表结构的阻塞队列，此队列按FIFO(先进先出)排序元素，吞吐量通常要高于ArrayBlockingQueue
    //Executors.newFixedThreadPool()使用了这个队列
    Executors.newFixedThreadPool(nThreads, threadFactory).asInstanceOf[ThreadPoolExecutor]
  }

  /**
   * Wrapper over newSingleThreadExecutor.
    * 包装在newSingleThreadExecutor上
    *ExecutorService提供了管理终止的方法，以及可为跟踪一个或多个异步任务执行状况而生成 Future 的方法。 可以关闭 ExecutorService，这将导致其停止接受新任务。关闭后，执行程序将最后终止，这时没有任务在执行，也没有任务在等待执行，并且无法提交新任务。
   */
  def newDaemonSingleThreadExecutor(threadName: String): ExecutorService = {
    val threadFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat(threadName).build()
    Executors.newSingleThreadExecutor(threadFactory)
  }

  /**
   * Wrapper over ScheduledThreadPoolExecutor.
   * ScheduledExecutorService定时周期执行指定的任务,基于时间的延迟,不会由于系统时间的改变发生执行变化
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
    * 在新线程中运行一段代码并返回结果。 新线程中的异常被调用调用者线程抛出,调整后的堆栈跟踪将清除对该方法的引用,异常堆栈跟踪将如下所示
   * SomeException: exception-message
    * SomeException：异常消息
   *   at CallerClass.body-method (sourcefile.scala)
   *   at ... run in separate thread using org.apache.spark.util.ThreadUtils ... ()
    *   在...运行在单独的线程使用org.apache.spark.util.ThreadUtils ...（）
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
        //将方法调用的部分删除到此帮助方法中这意味着将所有内容从顶部拖放到堆栈元素ThreadUtils.runInNewThread（）,然后删除它（因此“drop（1）”）
        val baseStackTrace = Thread.currentThread().getStackTrace().dropWhile(
          ! _.getClassName.contains(this.getClass.getSimpleName)).drop(1)

        // Remove the part of the new thread stack that shows methods call from this helper method
        //删除新的线程堆栈的部分，显示从该帮助方法调用的方法
        val extraStackTrace = realException.getStackTrace.takeWhile(
          ! _.getClassName.contains(this.getClass.getSimpleName))

        // Combine the two stack traces, with a place holder just specifying that there
        // was a helper method used, without any further details of the helper
        //结合两个堆栈跟踪,一个占位符只是指定使用了一个帮助方法,没有任何进一步的帮助细节
        val placeHolderStackElem = new StackTraceElement(
          s"... run in separate thread using ${ThreadUtils.getClass.getName.stripSuffix("$")} ..",
          " ", "", -1)
        val finalStackTrace = extraStackTrace ++ Seq(placeHolderStackElem) ++ baseStackTrace

        // Update the stack trace and rethrow the exception in the caller thread
        //更新堆栈跟踪并在调用者线程中重新抛出异常
        realException.setStackTrace(finalStackTrace)
        throw realException
      case None =>
        result
    }
  }
}
