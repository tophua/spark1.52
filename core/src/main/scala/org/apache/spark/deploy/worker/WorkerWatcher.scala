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

import org.apache.spark.Logging
import org.apache.spark.rpc._

/**
 * Actor which connects to a worker process and terminates the JVM if the connection is severed.
 * Provides fate sharing between a worker and its associated child processes.
 * 断开连接到一个工作进程并终止JVM,提供共享worker和相关的子进程
 */
private[spark] class WorkerWatcher(override val rpcEnv: RpcEnv, workerUrl: String)
  extends RpcEndpoint with Logging {

  override def onStart() {
    logInfo(s"Connecting to worker $workerUrl")
    if (!isTesting) {
      rpcEnv.asyncSetupEndpointRefByURI(workerUrl)
    }
  }

  // Used to avoid shutting down JVM during tests 为了避免关闭JVM在测试
  // In the normal case, exitNonZero will call `System.exit(-1)` to shutdown the JVM. In the unit
  // test, the user should call `setTesting(true)` so that `exitNonZero` will set `isShutDown` to
  // true rather than calling `System.exit`. The user can check `isShutDown` to know if
  // `exitNonZero` is called.
  //在正常情况下，exitNonZero将调用`System.exit（-1）`来关闭JVM。 在单位测试,用户应该调用`setTesting（true）`,
  // 这样`exitNonZero`将会将`isShutDown`设置为true而不是调用`System.exit`,
  // 用户可以检查`isShutDown`来知道是否`exitNonZero`被调用。
  private[deploy] var isShutDown = false
  private[deploy] def setTesting(testing: Boolean) = isTesting = testing
  private var isTesting = false

  // Lets filter events only from the worker's rpc system
  //让我们只从worker的rpc系统中过滤事件
  private val expectedAddress = RpcAddress.fromURIString(workerUrl)
  private def isWorker(address: RpcAddress) = expectedAddress == address

  private def exitNonZero() = if (isTesting) isShutDown = true else System.exit(-1)

  override def receive: PartialFunction[Any, Unit] = {
    case e => logWarning(s"Received unexpected message: $e")
  }

  override def onConnected(remoteAddress: RpcAddress): Unit = {
    if (isWorker(remoteAddress)) {
      logInfo(s"Successfully connected to $workerUrl")
    }
  }

  override def onDisconnected(remoteAddress: RpcAddress): Unit = {
    if (isWorker(remoteAddress)) {
      // This log message will never be seen
      //此日志消息将永远不会被看到
      logError(s"Lost connection to worker rpc endpoint $workerUrl. Exiting.")
      exitNonZero()
    }
  }

  override def onNetworkError(cause: Throwable, remoteAddress: RpcAddress): Unit = {
    if (isWorker(remoteAddress)) {
      // These logs may not be seen if the worker (and associated pipe) has died
      //如果worker(和相关管道)已经死亡,则可能无法看到这些日志
      logError(s"Could not initialize connection to worker $workerUrl. Exiting.")
      logError(s"Error was: $cause")
      exitNonZero()
    }
  }
}
