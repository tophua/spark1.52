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

package org.apache.spark.deploy.yarn

import java.util.{List => JList}

import scala.collection.JavaConversions._
import scala.collection.{Map, Set}
import scala.util.Try

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.ConverterUtils
import org.apache.hadoop.yarn.webapp.util.WebAppUtils

import org.apache.spark.{Logging, SecurityManager, SparkConf}
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler.SplitInfo
import org.apache.spark.util.Utils

/**
 * Handles registering and unregistering the application with the YARN ResourceManager.
  * 处理使用YARN ResourceManager注册和取消注册应用程序
 */
private[spark] class YarnRMClient(args: ApplicationMasterArguments) extends Logging {

  private var amClient: AMRMClient[ContainerRequest] = _
  private var uiHistoryAddress: String = _
  private var registered: Boolean = false

  /**
   * Registers the application master with the RM.
   * 使用RM注册应用程序主服务器
   * @param conf The Yarn configuration.
   * @param sparkConf The Spark configuration.
   * @param preferredNodeLocations Map with hints about where to allocate containers.
    *                               映射有关分配容器的位置的提示
   * @param uiAddress Address of the SparkUI.SparkUI的地址
   * @param uiHistoryAddress Address of the application on the History Server.历史记录服务器上的应用程序的地址
   */
  def register(
      driverUrl: String,
      driverRef: RpcEndpointRef,
      conf: YarnConfiguration,
      sparkConf: SparkConf,
      preferredNodeLocations: Map[String, Set[SplitInfo]],
      uiAddress: String,
      uiHistoryAddress: String,
      securityMgr: SecurityManager
    ): YarnAllocator = {
    amClient = AMRMClient.createAMRMClient()
    amClient.init(conf)
    amClient.start()
    this.uiHistoryAddress = uiHistoryAddress

    logInfo("Registering the ApplicationMaster")
    synchronized {
      amClient.registerApplicationMaster(Utils.localHostName(), 0, uiAddress)
      registered = true
    }
    new YarnAllocator(driverUrl, driverRef, conf, sparkConf, amClient, getAttemptId(), args,
      securityMgr)
  }

  /**
   * Unregister the AM. Guaranteed to only be called once.
   * 取消注册AM,保证只被调用一次。
   * @param status The final status of the AM.AM的最终状态
   * @param diagnostics Diagnostics message to include in the final status.
    *                    诊断消息包含在最终状态中
   */
  def unregister(status: FinalApplicationStatus, diagnostics: String = ""): Unit = synchronized {
    if (registered) {
      amClient.unregisterApplicationMaster(status, diagnostics, uiHistoryAddress)
    }
  }

  /** Returns the attempt ID. 返回尝试ID*/
  def getAttemptId(): ApplicationAttemptId = {
    YarnSparkHadoopUtil.get.getContainerId.getApplicationAttemptId()
  }

  /** Returns the configuration for the AmIpFilter to add to the Spark UI.
    * 返回要添加到Spark UI的AmIpFilter的配置*/
  def getAmIpFilterParams(conf: YarnConfiguration, proxyBase: String): Map[String, String] = {
    // Figure out which scheme Yarn is using. Note the method seems to have been added after 2.2,
    // so not all stable releases have it.
    //找出Yarn使用的方案,请注意,该方法似乎是在2.2之后添加的,因此并非所有稳定版本都具有该方法。
    val prefix = Try(classOf[WebAppUtils].getMethod("getHttpSchemePrefix", classOf[Configuration])
      .invoke(null, conf).asInstanceOf[String]).getOrElse("http://")

    // If running a new enough Yarn, use the HA-aware API for retrieving the RM addresses.
    //如果运行足够新的Yarn,请使用支持HA的API来检索RM地址
    try {
      val method = classOf[WebAppUtils].getMethod("getProxyHostsAndPortsForAmFilter",
        classOf[Configuration])
      val proxies = method.invoke(null, conf).asInstanceOf[JList[String]]
      val hosts = proxies.map { proxy => proxy.split(":")(0) }
      val uriBases = proxies.map { proxy => prefix + proxy + proxyBase }
      Map("PROXY_HOSTS" -> hosts.mkString(","), "PROXY_URI_BASES" -> uriBases.mkString(","))
    } catch {
      case e: NoSuchMethodException =>
        val proxy = WebAppUtils.getProxyHostAndPort(conf)
        val parts = proxy.split(":")
        val uriBase = prefix + proxy + proxyBase
        Map("PROXY_HOST" -> parts(0), "PROXY_URI_BASE" -> uriBase)
    }
  }

  /** Returns the maximum number of attempts to register the AM.
    * 返回注册AM的最大尝试次数*/
  def getMaxRegAttempts(sparkConf: SparkConf, yarnConf: YarnConfiguration): Int = {
    val sparkMaxAttempts = sparkConf.getOption("spark.yarn.maxAppAttempts").map(_.toInt)
    val yarnMaxAttempts = yarnConf.getInt(
      YarnConfiguration.RM_AM_MAX_ATTEMPTS, YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS)
    val retval: Int = sparkMaxAttempts match {
      case Some(x) => if (x <= yarnMaxAttempts) x else yarnMaxAttempts
      case None => yarnMaxAttempts
    }

    retval
  }

}
