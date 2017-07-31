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

import java.io.File
import java.util.PriorityQueue

import scala.util.{Failure, Success, Try}
import tachyon.client.TachyonFile

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.Logging

/**
 * Various utility methods used by Spark.
  * Spark使用的各种实用方法
 */
private[spark] object ShutdownHookManager extends Logging {
  val DEFAULT_SHUTDOWN_PRIORITY = 100

  /**
   * The shutdown priority of the SparkContext instance. This is lower than the default
   * priority, so that by default hooks are run before the context is shut down.
    * SparkContext实例的关闭优先级,这低于默认优先级,因此默认情况下钩子在上下文关闭之前运行。
   */
  val SPARK_CONTEXT_SHUTDOWN_PRIORITY = 50

  /**
   * The shutdown priority of temp directory must be lower than the SparkContext shutdown
   * priority. Otherwise cleaning the temp directories while Spark jobs are running can
   * throw undesirable errors at the time of shutdown.
    * 临时目录的关机优先级必须低于SparkContext关闭优先级,否则在Spark作业运行时清理临时目录可能会在关机时产生不良的错误。
   */
  val TEMP_DIR_SHUTDOWN_PRIORITY = 25

  private lazy val shutdownHooks = {
    val manager = new SparkShutdownHookManager()
    manager.install()
    manager
  }

  private val shutdownDeletePaths = new scala.collection.mutable.HashSet[String]()
  private val shutdownDeleteTachyonPaths = new scala.collection.mutable.HashSet[String]()

  // Add a shutdown hook to delete the temp dirs when the JVM exits
  //添加一个JVM退出关机钩子删除临时目录时,
  addShutdownHook(TEMP_DIR_SHUTDOWN_PRIORITY) { () =>
    logInfo("Shutdown hook called")
    // we need to materialize the paths to delete because deleteRecursively removes items from
    // shutdownDeletePaths as we are traversing through it.
    //我们需要实现要删除的路径,因为deleteRecursively从shutdownDeletePath中删除项,因为我们正在遍历它。
    shutdownDeletePaths.toArray.foreach { dirPath =>
      try {
        logInfo("Deleting directory " + dirPath)
        Utils.deleteRecursively(new File(dirPath))
      } catch {
        case e: Exception => logError(s"Exception while deleting Spark temp dir: $dirPath", e)
      }
    }
  }

  // Register the path to be deleted via shutdown hook
  //将目录注册到shutdownDeletePaths中,以便在进程退出时删除.
  def registerShutdownDeleteDir(file: File) {
    val absolutePath = file.getAbsolutePath()
    shutdownDeletePaths.synchronized {
      shutdownDeletePaths += absolutePath
    }
  }

  // Register the tachyon path to be deleted via shutdown hook
  //注册通过shutdown hook删除的tachyon路径
  def registerShutdownDeleteDir(tachyonfile: TachyonFile) {
    val absolutePath = tachyonfile.getPath()
    shutdownDeleteTachyonPaths.synchronized {
      shutdownDeleteTachyonPaths += absolutePath
    }
  }

  // Remove the path to be deleted via shutdown hook
  // 通过关闭钩子删除要删除的路径
  def removeShutdownDeleteDir(file: File) {
    val absolutePath = file.getAbsolutePath()
    shutdownDeletePaths.synchronized {
      shutdownDeletePaths.remove(absolutePath)
    }
  }

  // Remove the tachyon path to be deleted via shutdown hook
  //通过关闭挂钩删除要删除的速度路径
  def removeShutdownDeleteDir(tachyonfile: TachyonFile) {
    val absolutePath = tachyonfile.getPath()
    shutdownDeleteTachyonPaths.synchronized {
      shutdownDeleteTachyonPaths.remove(absolutePath)
    }
  }

  // Is the path already registered to be deleted via a shutdown hook ?
  //已注册的路径是否通过关闭挂钩进行删除？
  def hasShutdownDeleteDir(file: File): Boolean = {
    val absolutePath = file.getAbsolutePath()
    shutdownDeletePaths.synchronized {
      shutdownDeletePaths.contains(absolutePath)
    }
  }

  // Is the path already registered to be deleted via a shutdown hook ?
  //已注册的路径是否通过关闭挂钩进行删除？
  def hasShutdownDeleteTachyonDir(file: TachyonFile): Boolean = {
    val absolutePath = file.getPath()
    shutdownDeleteTachyonPaths.synchronized {
      shutdownDeleteTachyonPaths.contains(absolutePath)
    }
  }

  // Note: if file is child of some registered path, while not equal to it, then return true;
  // else false. This is to ensure that two shutdown hooks do not try to delete each others
  // paths - resulting in IOException and incomplete cleanup.
  //注意：如果文件是某个注册路径的子对象,而不等于它,则返回true;否则为false。
  // 这是为了确保两个关机挂钩不会尝试删除彼此的路径 - 导致IOException和不完整的清理。
  //判断文件是否匹配关闭时要删除的文件及目录,
  //shutdownDeletePaths存储在进程关闭的文件及目录shutdownDeletePaths中移除此文件或目录
  def hasRootAsShutdownDeleteDir(file: File): Boolean = {
    val absolutePath = file.getAbsolutePath()
    val retval = shutdownDeletePaths.synchronized {
      shutdownDeletePaths.exists ( path =>
        !absolutePath.equals(path) && absolutePath.startsWith(path)
      )
    }
    if (retval) {
      logInfo("path = " + file + ", already present as root for deletion.")
    }
    retval
  }

  // Note: if file is child of some registered path, while not equal to it, then return true;
  // else false. This is to ensure that two shutdown hooks do not try to delete each others
  // paths - resulting in Exception and incomplete cleanup.
  //注意：如果文件是某个注册路径的子对象，而不等于它，则返回true;否则假。
  // 这是为了确保两个关机挂钩不要尝试删除彼此的路径 - 导致异常和不完整的清除。
  def hasRootAsShutdownDeleteDir(file: TachyonFile): Boolean = {
    val absolutePath = file.getPath()
    val retval = shutdownDeleteTachyonPaths.synchronized {
      shutdownDeleteTachyonPaths.exists { path =>
        !absolutePath.equals(path) && absolutePath.startsWith(path)
      }
    }
    if (retval) {
      logInfo("path = " + file + ", already present as root for deletion.")
    }
    retval
  }

  /**
   * Detect whether this thread might be executing a shutdown hook. Will always return true if
   * the current thread is a running a shutdown hook but may spuriously return true otherwise (e.g.
   * if System.exit was just called by a concurrent thread).
   *检测此线程是否可能正在执行关闭挂接。 如果会永远返回true 当前的线程是运行一个关闭钩子,但是可能会虚假地返回true(例如,如果System.exit刚刚被并发线程调用)。
   * Currently, this detects whether the JVM is shutting down by Runtime#addShutdownHook throwing
   * an IllegalStateException.
    * 目前,这检测JVM是否由Runtime＃addShutdownHook抛出IllegalStateException关闭。
   */
  def inShutdown(): Boolean = {
    try {
      val hook = new Thread {
        override def run() {}
      }
      /**      
       * addShutdownHook就是在jvm中增加一个关闭的钩子,当jvm关闭的时候,会执行系统中已经设置的所有通过方法addShutdownHook添加的钩子,
       * 当系统执行完这些钩子后,jvm才会关闭。所以这些钩子可以在jvm关闭的时候进行内存清理、对象销毁等操作。
       */
      Runtime.getRuntime.addShutdownHook(hook)
      Runtime.getRuntime.removeShutdownHook(hook)
    } catch {
      case ise: IllegalStateException => return true
    }
    false
  }

  /**
   * Adds a shutdown hook with default priority.
   * 添加一个默认优先级的shutdown hook
   * @param hook The code to run during shutdown.
   * @return A handle that can be used to unregister the shutdown hook.
   */
  def addShutdownHook(hook: () => Unit): AnyRef = {
    addShutdownHook(DEFAULT_SHUTDOWN_PRIORITY)(hook)
  }

  /**
   * Adds a shutdown hook with the given priority. Hooks with lower priority values run
   * first.
   * 添加一个具有给定优先级的shutdown hook,具有较低优先级值的钩首先运行。
   * @param hook The code to run during shutdown.
   * @return A handle that can be used to unregister the shutdown hook.
   */
  def addShutdownHook(priority: Int)(hook: () => Unit): AnyRef = {
    shutdownHooks.add(priority, hook)
  }

  /**
   * Remove a previously installed shutdown hook.
   * 删除以前安装的关机挂钩
   * @param ref A handle returned by `addShutdownHook`.
   * @return Whether the hook was removed.
   */
  def removeShutdownHook(ref: AnyRef): Boolean = {
    shutdownHooks.remove(ref)
  }

}

private [util] class SparkShutdownHookManager {

  private val hooks = new PriorityQueue[SparkShutdownHook]()
  @volatile private var shuttingDown = false

  /**
   * Install a hook to run at shutdown and run all registered hooks in order. Hadoop 1.x does not
   * have `ShutdownHookManager`, so in that case we just use the JVM's `Runtime` object and hope for
   * the best.
    *安装挂钩,在关机时运行,并按顺序运行所有注册的挂钩。
    * Hadoop 1.x没有`ShutdownHookManager`,所以在这种情况下,我们只是使用JVM的`Runtime`对象，希望最好。
   */
  def install(): Unit = {
    val hookTask = new Runnable() {
      override def run(): Unit = runAll()
    }
    Try(Utils.classForName("org.apache.hadoop.util.ShutdownHookManager")) match {
      case Success(shmClass) =>
        val fsPriority = classOf[FileSystem].getField("SHUTDOWN_HOOK_PRIORITY").get()
          .asInstanceOf[Int]
        val shm = shmClass.getMethod("get").invoke(null)
        shm.getClass().getMethod("addShutdownHook", classOf[Runnable], classOf[Int])
          .invoke(shm, hookTask, Integer.valueOf(fsPriority + 30))

      case Failure(_) =>
        Runtime.getRuntime.addShutdownHook(new Thread(hookTask, "Spark Shutdown Hook"));
    }
  }

  def runAll(): Unit = {
    shuttingDown = true
    var nextHook: SparkShutdownHook = null
    while ({ nextHook = hooks.synchronized { hooks.poll() }; nextHook != null }) {
      Try(Utils.logUncaughtExceptions(nextHook.run()))
    }
  }

  def add(priority: Int, hook: () => Unit): AnyRef = {
    hooks.synchronized {
      if (shuttingDown) {
        throw new IllegalStateException("Shutdown hooks cannot be modified during shutdown.")
      }
      val hookRef = new SparkShutdownHook(priority, hook)
      hooks.add(hookRef)
      hookRef
    }
  }

  def remove(ref: AnyRef): Boolean = {
    hooks.synchronized { hooks.remove(ref) }
  }

}

private class SparkShutdownHook(private val priority: Int, hook: () => Unit)
  extends Comparable[SparkShutdownHook] {

  override def compareTo(other: SparkShutdownHook): Int = {
    other.priority - priority
  }

  def run(): Unit = hook()

}
