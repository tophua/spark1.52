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

package org.apache.spark.deploy

import java.net.URI

private[spark] class ApplicationDescription(
    val name: String,//Apllication 名称,可以通过Spark.app.name设置
    //这个application最多需要的core个数,可以通过Spark.core.Max设置
    val maxCores: Option[Int],
    //每一个Executor进程的memory大小,可以通过Spark.Executo.memory,SPARK_EXECUTOR_MEMORY设置
    //默认1024MB
    val memoryPerExecutorMB: Int,
    //worker Node拉起的ExecutorBanckEnd进程的Command,在Worker接收Master LaunchExecutor
    //会通过ExecutorRunner启动这个Command.Command包含了启动一个Java进程所需要的信息包括启动
    //ClassName所需参数,环境信息等
    val command: Command,
    //Application的Web UI的Hostname,Port
    var appUiUrl: String,
    //如果Spark.eventLog.enabled(默认false),指定为true的话,eventLogFile就设置Spark.eventLog.dir定义目录
    val eventLogDir: Option[URI] = None,
    // short name of compression codec used when writing event logs, if any (e.g. lzf)
    val eventLogCodec: Option[String] = None,
    //每一个Executor进程的core个数
    val coresPerExecutor: Option[Int] = None)
  extends Serializable {

  val user = System.getProperty("user.name", "<unknown>")

  def copy(
      name: String = name,
      maxCores: Option[Int] = maxCores,
      memoryPerExecutorMB: Int = memoryPerExecutorMB,
      command: Command = command,
      appUiUrl: String = appUiUrl,
      eventLogDir: Option[URI] = eventLogDir,
      eventLogCodec: Option[String] = eventLogCodec): ApplicationDescription =
    new ApplicationDescription(
      name, maxCores, memoryPerExecutorMB, command, appUiUrl, eventLogDir, eventLogCodec)

  override def toString: String = "ApplicationDescription(" + name + ")"
}
