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

package org.apache.spark.metrics

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.spark.util.Utils

import scala.collection.mutable

import com.codahale.metrics.{ Metric, MetricFilter, MetricRegistry }
import org.eclipse.jetty.servlet.ServletContextHandler

import org.apache.spark.{ Logging, SecurityManager, SparkConf }
import org.apache.spark.metrics.sink.{ MetricsServlet, Sink }
import org.apache.spark.metrics.source.Source

/**
 * Spark Metrics System, created by specific "instance", combined by source,
 * sink, periodically poll source metrics data to sink destinations.
  *
  * Spark指标系统由特定的“实例”instance",由源代码组合,sink,定期轮询来源度量数据到目的地。
 *
 * "instance" specify "who" (the role) use metrics system. In spark there are several roles
 * like master, worker, executor, client driver, these roles will create metrics system
 * for monitoring. So instance represents these roles. Currently in Spark, several instances
 * have already implemented: master, worker, executor, driver, applications.
  *
  * 实例“指定”谁“(角色)使用度量系统,在Spark中有几个角色,如master，工作节点，执行者，客户端驱动,这些角色将创建监控系统,所以实例代表这些角色，
  * 目前在Spark中,几个实例已经实现了：master,worker,executor,driver,applications。
 *
 * "source" specify "where" (source) to collect metrics data. In metrics system, there exists
 * two kinds of source:
  * “source”指定“where”（源）来收集度量数据。在度量系统中存在两种来源：
  *
 *   1. Spark internal source, like MasterSource, WorkerSource, etc, which will collect
 *   Spark component's internal state, these sources are related to instance and will be
 *   added after specific metrics system is created.
  *   Spark内部源代码,如MasterSource,WorkerSource等,将会收集Spark组件的内部状态,这些来源与实例相关,
  *   将是在创建特定指标系统后添加
 *   2. Common source, like JvmSource, which will collect low level state, is configured by
 *   configuration and loaded through reflection.
  *   通常来源,如收集低级别状态的Jvm Source,由配置配置并通过反射加载
 *
 * "sink" specify "where" (destination) to output metrics data to. Several sinks can be
 * coexisted and flush metrics to all these sinks.
  *
  * “sink”指定“where”(目的地)输出度量数据,几个水槽可以共存并冲洗所有这些水槽的指标
 *
 * Metrics configuration format is like below:
  * 度量配置格式如下：
 * [instance].[sink|source].[name].[options] = xxxx
 *
 * [instance] can be "master", "worker", "executor", "driver", "applications" which means only
 * the specified instance has this property.
  *
  * [实例]可以是“master”,“worker”,“executor”,“driver”,“applications”,这意味着只有指定的实例具有此属性
  *
 * wild card "*" can be used to replace instance name, which means all the instances will have
 * this property.
 *
  * 通配符“*”可用于替换实例名称,这意味着所有实例都将具有此属性
  *
 * [sink|source] means this property belongs to source or sink. This field can only be
 * source or sink.
 * [sink | source]表示此属性属于源或汇,该字段只能是源或汇
  *
 * [name] specify the name of sink or source, it is custom defined.
  * [name]指定sink或source的名称,它是自定义的
 *
 * [options] is the specific property of this source or sink.
  * [options]是此源或汇的特定属性
 */
private[spark] class MetricsSystem private (
  val instance: String, //指定了谁在使用测量系统
  conf: SparkConf, //指定了从那里收集测量数据
  securityMgr: SecurityManager) //指定了往哪里测量数据
    extends Logging {

  private[this] val metricsConfig = new MetricsConfig(conf)

  private val sinks = new mutable.ArrayBuffer[Sink]
  private val sources = new mutable.ArrayBuffer[Source]
  private val registry = new MetricRegistry()

  private var running: Boolean = false

  // Treat MetricsServlet as a special sink as it should be exposed to add handlers to web ui
  //将MetricsServlet视为一个特殊的接收器,因为它应该暴露给web ui添加处理程序
  private var metricsServlet: Option[MetricsServlet] = None

  /**
   * Get any UI handlers used by this metrics system; can only be called after start().
   * 为了能够在SparkUI(页面)访问到测量数据,所以需要给Sinks增加Jetty的ServletHandlers
   */
  def getServletHandlers: Array[ServletContextHandler] = {
    require(running, "Can only call getServletHandlers on a running MetricsSystem")
    metricsServlet.map(_.getHandlers).getOrElse(Array())
  }

  metricsConfig.initialize()
  //启动
  def start() {
    require(!running, "Attempting to start a MetricsSystem that is already running")
    running = true
    registerSources()
    registerSinks()
    sinks.foreach(_.start)
  }

  def stop() {
    if (running) {
      sinks.foreach(_.stop)
    } else {
      logWarning("Stopping a MetricsSystem that is not running")
    }
    running = false
  }

  def report() {
    sinks.foreach(_.report())
  }

  /**
   * Build a name that uniquely identifies each metric source.
    * 构建唯一标识每个度量来源的名称
   * The name is structured as follows: <app ID>.<executor ID (or "driver")>.<source name>.
   * If either ID is not available, this defaults to just using <source name>.
    * 名称的结构如下：<app ID>。<executor ID（或“driver”）>,<source name>,如果两个ID都不可用,则默认为使用<source name>
   *
   * @param source Metric source to be named by this method.
   * @return An unique metric name for each combination of
   *         application, executor/driver and metric source.
   */
  private[spark] def buildRegistryName(source: Source): String = {
    val appId = conf.getOption("spark.app.id")
    val executorId = conf.getOption("spark.executor.id")
    val defaultName = MetricRegistry.name(source.sourceName)

    if (instance == "driver" || instance == "executor") {
      if (appId.isDefined && executorId.isDefined) {
        MetricRegistry.name(appId.get, executorId.get, source.sourceName)
      } else {
        // Only Driver and Executor set spark.app.id and spark.executor.id.
        //只有驱动程序和执行器设置spark.app.id和spark.executor.id
        // Other instance types, e.g. Master and Worker, are not related to a specific application.
        //其他实例类型,例如 Master和Worker,与具体应用无关
        val warningMsg = s"Using default name $defaultName for source because %s is not set."
        if (appId.isEmpty) { logWarning(warningMsg.format("spark.app.id")) }
        if (executorId.isEmpty) { logWarning(warningMsg.format("spark.executor.id")) }
        defaultName
      }
    } else { defaultName }
  }

  def getSourcesByName(sourceName: String): Seq[Source] =
    sources.filter(_.sourceName == sourceName)

  def registerSource(source: Source) {
    sources += source
    try {
      val regName = buildRegistryName(source)
      registry.register(regName, source.metricRegistry)
    } catch {
      case e: IllegalArgumentException => logInfo("Metrics already registered", e)
    }
  }

  def removeSource(source: Source) {
    sources -= source
    val regName = buildRegistryName(source)
    registry.removeMatching(new MetricFilter {
      def matches(name: String, metric: Metric): Boolean = name.startsWith(regName)
    })
  }
  //注册Souurces,告诉测量系统从哪里收集测量数据
  private def registerSources() {
    //从instance获取properties,
    val instConfig = metricsConfig.getInstance(instance)
    //使用正则匹配properties中以source.开头的属性
    val sourceConfigs = metricsConfig.subProperties(instConfig, MetricsSystem.SOURCE_REGEX)

    // Register all the sources related to instance
    //注册与实例相关的所有源

    sourceConfigs.foreach { kv =>
      val classPath = kv._2.getProperty("class")
      try {
        val source = Utils.classForName(classPath).newInstance()
        //将每个source的实例注册到ArrayBuffer
        registerSource(source.asInstanceOf[Source])
      } catch {
        case e: Exception => logError("Source class " + classPath + " cannot be instantiated", e)
      }
    }
  }
  //注册Sinks,即告诉测量系统MetricsSystem往哪里输出测量数据
  private def registerSinks() {
    //从instance获取properties,
    val instConfig = metricsConfig.getInstance(instance)
    //使用正则匹配properties中以sink.开头的属性
    val sinkConfigs = metricsConfig.subProperties(instConfig, MetricsSystem.SINK_REGEX)

    sinkConfigs.foreach { kv =>
      val classPath = kv._2.getProperty("class")
      if (null != classPath) {
        try {
          //将子属性class对应的类metricsServleter反射得到MetricsServlet实例
          //如果属性的Key是servlet,将其设置为MetricsServlet,如果是Sink,则加入ArrayBuffer
          val sink = Utils.classForName(classPath)
            .getConstructor(classOf[Properties], classOf[MetricRegistry], classOf[SecurityManager])
            .newInstance(kv._2, registry, securityMgr)
          if (kv._1 == "servlet") {
            metricsServlet = Some(sink.asInstanceOf[MetricsServlet])
          } else {
            sinks += sink.asInstanceOf[Sink]
          }
        } catch {
          case e: Exception => {
            logError("Sink class " + classPath + " cannot be instantiated")
            throw e
          }
        }
      }
    }
  }
}

private[spark] object MetricsSystem {
  //匹配以sink.开头
  val SINK_REGEX = "^sink\\.(.+)\\.(.+)".r
  //匹配以source.开头
  val SOURCE_REGEX = "^source\\.(.+)\\.(.+)".r

  private[this] val MINIMAL_POLL_UNIT = TimeUnit.SECONDS
  private[this] val MINIMAL_POLL_PERIOD = 1

  def checkMinimalPollingPeriod(pollUnit: TimeUnit, pollPeriod: Int) {
    val period = MINIMAL_POLL_UNIT.convert(pollPeriod, pollUnit)
    if (period < MINIMAL_POLL_PERIOD) {
      throw new IllegalArgumentException("Polling period " + pollPeriod + " " + pollUnit +
        " below than minimal polling period ")
    }
  }

  def createMetricsSystem(
    instance: String, conf: SparkConf, securityMgr: SecurityManager): MetricsSystem = {
    new MetricsSystem(instance, conf, securityMgr)
  }
}
