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

package org.apache.spark.deploy.history

import java.util.NoSuchElementException
import java.util.zip.ZipOutputStream
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import com.google.common.cache._
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}

import org.apache.spark.{Logging, SecurityManager, SparkConf}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.status.api.v1.{ApiRootResource, ApplicationInfo, ApplicationsListResource,
  UIRoot}
import org.apache.spark.ui.{SparkUI, UIUtils, WebUI}
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.util.{ShutdownHookManager, SignalLogger, Utils}

/**
 * A web server that renders SparkUIs of completed applications.
 *一个Web服务器,提供完整的申请sparkuis
 * 
 * For the standalone mode, MasterWebUI already achieves this functionality. Thus, the
 * 在独立模式下,masterwebui已经实现此功能,该historyserver主要使用的情况在其他部署模式
 * main use case of the HistoryServer is in other deploy modes (e.g. Yarn or Mesos).
 *
 * The logging directory structure is as follows: Within the given base directory, each
 * 日志记录目录结构如下:在给定的基本目录中,
 * application's event logs are maintained in the application's own sub-directory. This
 * 每个应用程序的事件日志都保存在应用程序自己的子目录中,
 * is the same structure as maintained in the event log write code path in
 * 这是相同的结构,保持在事件日志中写入的代码路径eventlogginglistener
 * EventLoggingListener.
 */
class HistoryServer(
    conf: SparkConf,
    provider: ApplicationHistoryProvider,
    securityManager: SecurityManager,
    port: Int)
  extends WebUI(securityManager, port, conf) with Logging with UIRoot {

  // How many applications to retain 保留有多少个应用程序
  private val retainedApplications = conf.getInt("spark.history.retainedApplications", 50)

  private val appLoader = new CacheLoader[String, SparkUI] {
    override def load(key: String): SparkUI = {
      val parts = key.split("/")
      require(parts.length == 1 || parts.length == 2, s"Invalid app key $key")
      val ui = provider
        .getAppUI(parts(0), if (parts.length > 1) Some(parts(1)) else None)
        .getOrElse(throw new NoSuchElementException(s"no app with key $key"))
      attachSparkUI(ui)
      ui
    }
  }

  private val appCache = CacheBuilder.newBuilder()
    .maximumSize(retainedApplications)
    .removalListener(new RemovalListener[String, SparkUI] {
      override def onRemoval(rm: RemovalNotification[String, SparkUI]): Unit = {
        detachSparkUI(rm.getValue())
      }
    })
    .build(appLoader)

  private val loaderServlet = new HttpServlet {
    protected override def doGet(req: HttpServletRequest, res: HttpServletResponse): Unit = {
      // Parse the URI created by getAttemptURI(). It contains an app ID and an optional
      // attempt ID (separated by a slash).
      val parts = Option(req.getPathInfo()).getOrElse("").split("/")
      if (parts.length < 2) {
        res.sendError(HttpServletResponse.SC_BAD_REQUEST,
          s"Unexpected path info in request (URI = ${req.getRequestURI()}")
        return
      }

      val appId = parts(1)
      val attemptId = if (parts.length >= 3) Some(parts(2)) else None

      // Since we may have applications with multiple attempts mixed with applications with a
      // single attempt, we need to try both. Try the single-attempt route first, and if an
      // error is raised, then try the multiple attempt route.
      if (!loadAppUi(appId, None) && (!attemptId.isDefined || !loadAppUi(appId, attemptId))) {
        val msg = <div class="row-fluid">Application {appId} not found.</div>
        res.setStatus(HttpServletResponse.SC_NOT_FOUND)
        UIUtils.basicSparkPage(msg, "Not Found").foreach { n =>
          res.getWriter().write(n.toString)
        }
        return
      }

      // Note we don't use the UI retrieved from the cache; the cache loader above will register
      // the app's UI, and all we need to do is redirect the user to the same URI that was
      // requested, and the proper data should be served at that point.
      res.sendRedirect(res.encodeRedirectURL(req.getRequestURI()))
    }

    // SPARK-5983 ensure TRACE is not supported
    protected override def doTrace(req: HttpServletRequest, res: HttpServletResponse): Unit = {
      res.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED)
    }
  }

  def getSparkUI(appKey: String): Option[SparkUI] = {
    Option(appCache.get(appKey))
  }

  initialize()

  /**
   * Initialize the history server.
   * 初始化历史服务器
   * This starts a background thread that periodically synchronizes information displayed on
   * 这开始的日志中提供的根目录的后台线程定期同步显示在UI事件信息
   * this UI with the event logs in the provided base directory.
   */
  def initialize() {
    attachPage(new HistoryPage(this))

    attachHandler(ApiRootResource.getServletHandler(this))

    attachHandler(createStaticHandler(SparkUI.STATIC_RESOURCE_DIR, "/static"))

    val contextHandler = new ServletContextHandler
    contextHandler.setContextPath(HistoryServer.UI_PATH_PREFIX)
    contextHandler.addServlet(new ServletHolder(loaderServlet), "/*")
    attachHandler(contextHandler)
  }

  /** 
   *  Bind to the HTTP server behind this web interface.
   *  在这个网页界面绑定到HTTP服务器
   *   */
  override def bind() {
    super.bind()
  }

  /** 
   *  Stop the server and close the file system. 
   *  停止服务器并关闭文件系统
   *  */
  override def stop() {
    super.stop()
    provider.stop()
  }

  /** 
   *  Attach a reconstructed UI to this server. Only valid after bind().
   *  将重构的用户界面附加到该服务器上,只有绑定后有效
   *   */
  private def attachSparkUI(ui: SparkUI) {
    assert(serverInfo.isDefined, "HistoryServer must be bound before attaching SparkUIs")
    ui.getHandlers.foreach(attachHandler)
    addFilters(ui.getHandlers, conf)
  }

  /** 
   *  Detach a reconstructed UI from this server. Only valid after bind(). 
   *  从该服务器上分离重构的用户界面,只有绑定后有效
   *  */
  private def detachSparkUI(ui: SparkUI) {
    assert(serverInfo.isDefined, "HistoryServer must be bound before detaching SparkUIs")
    ui.getHandlers.foreach(detachHandler)
  }

  /**
   * Returns a list of available applications, in descending order according to their end time.
   * 返回可用的应用程序的列表,按照他们的结束时间倒序排序
   * @return List of all known applications.
   */
  def getApplicationList(): Iterable[ApplicationHistoryInfo] = {
    provider.getListing()
  }

  def getApplicationInfoList: Iterator[ApplicationInfo] = {
    getApplicationList().iterator.map(ApplicationsListResource.appHistoryInfoToPublicAppInfo)
  }

  override def writeEventLogs(
      appId: String,
      attemptId: Option[String],
      zipStream: ZipOutputStream): Unit = {
    provider.writeEventLogs(appId, attemptId, zipStream)
  }

  /**
   * Returns the provider configuration to show in the listing page.
   * 返回在列表页中显示的提供程序配置
   *
   * @return A map with the provider's configuration.
   */
  def getProviderConfig(): Map[String, String] = provider.getConfig()

  private def loadAppUi(appId: String, attemptId: Option[String]): Boolean = {
    try {
      appCache.get(appId + attemptId.map { id => s"/$id" }.getOrElse(""))
      true
    } catch {
      case e: Exception => e.getCause() match {
        case nsee: NoSuchElementException =>
          false

        case cause: Exception => throw cause
      }
    }
  }

}

/**
 * The recommended way of starting and stopping a HistoryServer is through the scripts
 * 启动和停止historyserver推荐的方式是通过脚本start-history-server.sh and stop-history-server.sh
 * start-history-server.sh and stop-history-server.sh. The path to a base log directory,
 * 一个基本日志目录的路径,
 * as well as any other relevant history server configuration, should be specified via
 * 以及任何其他相关的历史服务器配置,应通过 $SPARK_HISTORY_OPTS 指定环境变量
 * the $SPARK_HISTORY_OPTS environment variable. For example: 例如
 *
 *   export SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=/tmp/spark-events"
 *   ./sbin/start-history-server.sh
 *
 * This launches the HistoryServer as a Spark daemon.
 * 这将启动historyserver作为Spark守护线程
 */
object HistoryServer extends Logging {
  private val conf = new SparkConf

  val UI_PATH_PREFIX = "/history"

  def main(argStrings: Array[String]) {
    SignalLogger.register(log)
    new HistoryServerArguments(conf, argStrings)
    initSecurity()
    val securityManager = new SecurityManager(conf)

    val providerName = conf.getOption("spark.history.provider")
      .getOrElse(classOf[FsHistoryProvider].getName())
    val provider = Utils.classForName(providerName)
      .getConstructor(classOf[SparkConf])
      .newInstance(conf)
      .asInstanceOf[ApplicationHistoryProvider]

    val port = conf.getInt("spark.history.ui.port", 18080)

    val server = new HistoryServer(conf, provider, securityManager, port)
    server.bind()

    ShutdownHookManager.addShutdownHook { () => server.stop() }

    // Wait until the end of the world... or if the HistoryServer process is manually stopped
    //到世界的尽头…如果historyserver过程是手动停止
    while(true) { Thread.sleep(Int.MaxValue) }
  }

  def initSecurity() {
    // If we are accessing HDFS and it has security enabled (Kerberos), we have to login
    //如果我们访问HDFS和已启用安全的(Kerberos),我们必须登录从keytab文件以便我们能够获得超越Kerberos票证过期HDFS
    // from a keytab file so that we can access HDFS beyond the kerberos ticket expiration.
    // As long as it is using Hadoop rpc (hdfs://), a relogin will automatically
    // occur from the keytab.
    if (conf.getBoolean("spark.history.kerberos.enabled", false)) {
      // if you have enabled kerberos the following 2 params must be set
      val principalName = conf.get("spark.history.kerberos.principal")
      val keytabFilename = conf.get("spark.history.kerberos.keytab")
      SparkHadoopUtil.get.loginUserFromKeytab(principalName, keytabFilename)
    }
  }

  private[history] def getAttemptURI(appId: String, attemptId: Option[String]): String = {
    val attemptSuffix = attemptId.map { id => s"/$id" }.getOrElse("")
    s"${HistoryServer.UI_PATH_PREFIX}/${appId}${attemptSuffix}"
  }

}
