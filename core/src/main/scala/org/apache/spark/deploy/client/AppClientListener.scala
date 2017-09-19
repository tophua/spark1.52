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

package org.apache.spark.deploy.client

/**
 * Callbacks invoked by deploy client when various events happen. There are currently four events:
 * 部署客户端各种事件的发生发时调用的回调函数,目前有四个事件:
 * connecting to the cluster, disconnecting, being given an executor, and having an executor
 * 连接到群集,断开连接,添加一个Executor,删除一个Executor(由于失败或由于撤销)
 * removed (either due to failure or due to revocation).
 * 主要为了SchedulerBackend和AppClient之间的函数回调
 * Users of this API should *not* block inside the callback methods.
 */
private[spark] trait AppClientListener {
  /**向Master成功注册Application,即成功链接到集群；**/
  def connected(appId: String): Unit
  /**断开连接,如果当前SparkDeploySchedulerBackend::stop == false,那么可能原来的Master实效了,待新的Master ready后,会重新恢复原来的连接**/
  /** Disconnection may be a temporary state, as we fail over to a new Master. */
  def disconnected(): Unit
  /**Application由于不可恢复的错误停止了,这个时候需要重新提交出错的TaskSet**/
  /** An application death is an unrecoverable failure condition. */
  def dead(reason: String): Unit
  /**添加一个Executor,在这里的实现仅仅是打印了log,并没有额外的逻辑**/
  def executorAdded(fullId: String, workerId: String, hostPort: String, cores: Int, memory: Int)
  /**删除一个Executor,可能有两个原因,一个是Executor退出了,这里可以得到Executor的退出码,或者由于Worker的退出导致了运行其上的Executor退出**/
  def executorRemoved(fullId: String, message: String, exitStatus: Option[Int]): Unit
}
