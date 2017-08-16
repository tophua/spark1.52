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

package org.apache.spark.network.netty

import org.apache.spark.SparkConf
import org.apache.spark.network.util.{TransportConf, ConfigProvider}

/**
 * Provides a utility for transforming from a SparkConf inside a Spark JVM (e.g., Executor,
 * Driver, or a standalone shuffle service) into a TransportConf with details on our environment
 * like the number of cores that are allocated to this JVM.
  *
  * 提供了一个实用程序,用于从Spark JVM内部的SparkConf进行转换（例如，Executor，驱动程序或独立的随机播放服务）
  * 转移到TransportConf中,其中包含有关我们环境的详细信息,例如分配给此JVM的内核数
 */
object SparkTransportConf {
  /**
   * Specifies an upper bound on the number of Netty threads that Spark requires by default.
   * In practice, only 2-4 cores should be required to transfer roughly 10 Gb/s, and each core
   * that we use will have an initial overhead of roughly 32 MB of off-heap memory, which comes
   * at a premium.
    *
   *指定Spark默认情况下需要的Netty线程数上限,实际上,只需要2-4个核心来传输大约10 Gb/s和每个核心
   * 我们使用的将具有大约32 MB的堆内存的初始开销,这是非常重要的
    *
   * Thus, this value should still retain maximum throughput and reduce wasted off-heap memory
   * allocation. It can be overridden by setting the number of serverThreads and clientThreads
   * manually in Spark's configuration.
    * 因此，此值仍应保留最大吞吐量并减少浪费的堆内存分配,
    * 可以通过在Spark的配置中手动设置serverThreads和clientThreads的数量来覆盖它。
    *
   */
  private val MAX_DEFAULT_NETTY_THREADS = 8

  /**
   * Utility for creating a [[TransportConf]] from a [[SparkConf]].
    * 从[[SparkConf]]创建[[TransportConf]]的实用程序
   * @param numUsableCores if nonzero, this will restrict the server and client threads to only
   *                       use the given number of cores, rather than all of the machine's cores.
   *                       This restriction will only occur if these properties are not already set.
    *                      如果非零,这将限制服务器和客户端线程仅使用给定数量的内核,而不是所有机器的内核,
    *                      只有这些属性尚未设置,才会发生此限制。
   */
  def fromSparkConf(_conf: SparkConf, numUsableCores: Int = 0): TransportConf = {
    val conf = _conf.clone

    // Specify thread configuration based on our JVM's allocation of cores (rather than necessarily
    // assuming we have all the machine's cores).
    // NB: Only set if serverThreads/clientThreads not already set.
    //根据我们的JVM分配核心指定线程配置（而不是一定假定我们拥有所有机器的核心）.
    // NB：只有在serverThreads / clientThreads尚未设置的情况下才设置。
    //服务端线程数量
    val numThreads = defaultNumThreads(numUsableCores)
    conf.set("spark.shuffle.io.serverThreads",
      conf.get("spark.shuffle.io.serverThreads", numThreads.toString))
    conf.set("spark.shuffle.io.clientThreads",
      conf.get("spark.shuffle.io.clientThreads", numThreads.toString))

    new TransportConf(new ConfigProvider {
      override def get(name: String): String = conf.get(name)
    })
  }

  /**
   * Returns the default number of threads for both the Netty client and server thread pools.
   * If numUsableCores is 0, we will use Runtime get an approximate number of available cores.
    * 返回Netty客户端和服务器线程池的默认线程数,如果numUsableCores为0，我们将使用运行时获取可用内核的大致数量。
   */
  private def defaultNumThreads(numUsableCores: Int): Int = {
    val availableCores =
      //Runtime.getRuntime().availableProcessors()方法获得当前设备的CPU个数
      if (numUsableCores > 0) numUsableCores else Runtime.getRuntime.availableProcessors()
    math.min(availableCores, MAX_DEFAULT_NETTY_THREADS)
  }
}
