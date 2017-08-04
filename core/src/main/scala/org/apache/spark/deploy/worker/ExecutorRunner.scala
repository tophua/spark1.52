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

package org.apache.spark.deploy.worker

import java.io._

import scala.collection.JavaConversions._

import com.google.common.base.Charsets.UTF_8
import com.google.common.io.Files

import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.{SecurityManager, SparkConf, Logging}
import org.apache.spark.deploy.{ApplicationDescription, ExecutorState}
import org.apache.spark.deploy.DeployMessages.ExecutorStateChanged
import org.apache.spark.util.{ShutdownHookManager, Utils}
import org.apache.spark.util.logging.FileAppender

/**
 * Worker进程中ExcetorRuner具体负责executor的启动和停止,每一个executor进程对应于一个ExecutorRunner
 * ExecutorRunner担任监工角色
 * 
 * Manages the execution of one executor process.
 * This is currently only used in standalone mode.
 * 管理一个执行的executor进程,只使用standalone模式
 */
private[deploy] class ExecutorRunner(
    val appId: String,
    val execId: Int,
    val appDesc: ApplicationDescription,
    val cores: Int,
    val memory: Int,
    val worker: RpcEndpointRef,//Worker Actor的引用
    val workerId: String,
    val host: String,
    val webUiPort: Int,
    val publicAddress: String,
    val sparkHome: File,
    val executorDir: File,
    val workerUrl: String,
    conf: SparkConf,
    val appLocalDirs: Seq[String],
    @volatile var state: ExecutorState.Value)
  extends Logging {

  private val fullId = appId + "/" + execId
  private var workerThread: Thread = null
  private var process: Process = null
  private var stdoutAppender: FileAppender = null
  private var stderrAppender: FileAppender = null

  // NOTE: This is now redundant with the automated shut-down enforced by the Executor. It might 
  // make sense to remove this in the future.
  //注意：执行者执行的自动关闭现在已经是多余的,将来删除这个可能是有意义的
  private var shutdownHook: AnyRef = null
  /**
   * 启动ExecutorRunner的时候实际创建了线程workerThread和shutdownHook
   */
  private[worker] def start() {
    workerThread = new Thread("ExecutorRunner for " + fullId) {
      override def run() { fetchAndRunExecutor() }
    }
    //启动ExecutorRunner的时候实际创建了线程workerThread和shutdownHook
    workerThread.start()
    // Shutdown hook that kills actors on shutdown. 
    //shutdownHook用于在Worker关闭时杀掉所有的Executor进程
    shutdownHook = ShutdownHookManager.addShutdownHook { () =>
     killProcess(Some("Worker shutting down")) }
  }

  /**
   * Kill executor process, wait for exit and notify worker to update resource status.
   * 停止executor进行,将停止的结果反馈给worker本身
   * @param message the exception message which caused the executor's death
   */
  private def killProcess(message: Option[String]) {
    var exitCode: Option[Int] = None
    if (process != null) {
      logInfo("Killing process!")
      if (stdoutAppender != null) {
        stdoutAppender.stop()
      }
      if (stderrAppender != null) {
        stderrAppender.stop()
      }
      process.destroy()
      exitCode = Some(process.waitFor())
    }
    //停止executor执行,将停止的结果反馈给worker本身
    worker.send(ExecutorStateChanged(appId, execId, state, message, exitCode))
  }

  /** 
   *  Stop this executor runner, including killing the process it launched
   *  停止运行executor,包括杀死启动进程
   *  */
  private[worker] def kill() {
    if (workerThread != null) {
      // the workerThread will kill the child process when interrupted
      // 将workerThread杀死子的过程中断时
      workerThread.interrupt()
      workerThread = null
      state = ExecutorState.KILLED
      try {
        ShutdownHookManager.removeShutdownHook(shutdownHook)
      } catch {
        case e: IllegalStateException => None
      }
    }
  }

  /** Replace variables such as {{EXECUTOR_ID}} and {{CORES}} in a command argument passed to us */
  //通过Commmand启动时,需要将这些参数替换成真实分配的值
  private[worker] def substituteVariables(argument: String): String = argument match {
    case "{{WORKER_URL}}" => workerUrl
    case "{{EXECUTOR_ID}}" => execId.toString
    case "{{HOSTNAME}}" => host
    case "{{CORES}}" => cores.toString
    case "{{APP_ID}}" => appId
    case other => other
  }

  /**
   * Download and run the executor described in our ApplicationDescription
   * 下载并运行应用程序描述
   */
  private def fetchAndRunExecutor() {
    try {
      // Launch the process
      //构造ProcessBuilder
      val builder = CommandUtils.buildProcessBuilder(appDesc.command, new SecurityManager(conf),
        memory, sparkHome.getAbsolutePath, substituteVariables)
      val command = builder.command()
      logInfo("Launch command: " + command.mkString("\"", "\" \"", "\""))
      //ProcessBuilder设置执行目录,环境变量
      builder.directory(executorDir)
      builder.environment.put("SPARK_EXECUTOR_DIRS", appLocalDirs.mkString(File.pathSeparator))
      // In case we are running this from within the Spark Shell, avoid creating a "scala"
      //如果我们正在运行从Spark Shell
      // parent process for the executor command
      //执行命令的父进程
      builder.environment.put("SPARK_LAUNCH_WITH_SCALA", "0")

      // Add webUI log urls
      val baseUrl =
        s"http://$publicAddress:$webUiPort/logPage/?appId=$appId&executorId=$execId&logType="
      builder.environment.put("SPARK_LOG_URL_STDERR", s"${baseUrl}stderr")
      builder.environment.put("SPARK_LOG_URL_STDOUT", s"${baseUrl}stdout")
      //启动ProessBuilder生成进程
      process = builder.start()
      val header = "Spark Executor Command: %s\n%s\n\n".format(
        command.mkString("\"", "\" \"", "\""), "=" * 40)

      // Redirect its stdout and stderr to files
      //重定向进程的文件输出流与错误流为executorDir目录下的文件stdout与stderr
      val stdout = new File(executorDir, "stdout")
      stdoutAppender = FileAppender(process.getInputStream, stdout, conf)

      val stderr = new File(executorDir, "stderr")
      Files.write(header, stderr, UTF_8)
      stderrAppender = FileAppender(process.getErrorStream, stderr, conf)

      // Wait for it to exit; executor may exit with code 0 (when driver instructs it to shutdown)
      // or with nonzero exit code
      //等待获取进程的退出状态,一旦收到退出状态,则向Worker发送ExecutorStatChange消息
      val exitCode = process.waitFor()
      state = ExecutorState.EXITED
      val message = "Command exited with code " + exitCode
      //向Worker发送ExecutorStateChanged消息,Worker会将这个消息转发到Master,由于Executor是异常退出,
      //Master将会为该Apllcation分配新的Executor
      worker.send(ExecutorStateChanged(appId, execId, state, Some(message), Some(exitCode)))
    } catch {
      case interrupted: InterruptedException => {
        logInfo("Runner thread for executor " + fullId + " interrupted")
        state = ExecutorState.KILLED
        killProcess(None)
      }
      case e: Exception => {
        logError("Error running executor", e)
        state = ExecutorState.FAILED
        killProcess(Some(e.toString))
      }
    }
  }
}
