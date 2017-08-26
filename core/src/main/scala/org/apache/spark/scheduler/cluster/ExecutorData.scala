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

package org.apache.spark.scheduler.cluster

import org.apache.spark.rpc.{RpcEndpointRef, RpcAddress}

/**
 * Grouping of data for an executor used by CoarseGrainedSchedulerBackend.
 * 为CoarseGrainedSchedulerBackend使用的执行程序分组数据
 * @param executorEndpoint The RpcEndpointRef representing this executor RpcEndpointRef表示这个执行器
 * @param executorAddress The network address of this executor 这个执行者的网络地址
 * @param executorHost The hostname that this executor is running on 此执行程序正在运行的主机名
 * @param freeCores  The current number of cores available for work on the executor 当前可用于执行程序的核心数量
 * @param totalCores The total number of cores available to the executor 可执行程序可用的核心总数
 */
private[cluster] class ExecutorData(
   val executorEndpoint: RpcEndpointRef,//RPC终端的引用,用于数据通信
   val executorAddress: RpcAddress, //RPC地址,用于数据通信,此执行任务的网络地址
   override val executorHost: String,//executor的主机
   var freeCores: Int,//可用处理器cores
   override val totalCores: Int,//处理器cores总数
   override val logUrlMap: Map[String, String]//日志url映射集合
) extends ExecutorInfo(executorHost, totalCores, logUrlMap)
