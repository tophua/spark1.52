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

package org.apache.spark.ui

import javax.servlet.http.HttpServletRequest

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.xml.Node

import org.eclipse.jetty.servlet.ServletContextHandler
import org.json4s.JsonAST.{JNothing, JValue}

import org.apache.spark.ui.JettyUtils._
import org.apache.spark.util.Utils
import org.apache.spark.{Logging, SecurityManager, SparkConf}

/**
 * The top level component of the UI hierarchy that contains the server.
 * 包含服务器的UI层次结构的顶层组件
 * Each WebUI represents a collection of tabs, each of which in turn represents a collection of
 * pages. The use of tabs is optional, however; a WebUI may choose to include pages directly.
  * 每个WebUI表示一组选项卡,每个选项卡依次表示一组页面,然而,使用选项卡是可选的;WebUI可以选择直接包括页面。
 */
private[spark] abstract class WebUI(
    val securityManager: SecurityManager,
    port: Int,
    conf: SparkConf,
    basePath: String = "",
    name: String = "")
  extends Logging {

  protected val tabs = ArrayBuffer[WebUITab]()
  protected val handlers = ArrayBuffer[ServletContextHandler]()
  protected val pageToHandlers = new HashMap[WebUIPage, ArrayBuffer[ServletContextHandler]]
  protected var serverInfo: Option[ServerInfo] = None
  protected val localHostName = Utils.localHostNameForURI()
   //Spark master和workers使用的公共DNS(默认空)
  protected val publicHostName = Option(conf.getenv("SPARK_PUBLIC_DNS")).getOrElse(localHostName)
  private val className = Utils.getFormattedClassName(this)

  def getBasePath: String = basePath
  def getTabs: Seq[WebUITab] = tabs.toSeq
  def getHandlers: Seq[ServletContextHandler] = handlers.toSeq
  def getSecurityManager: SecurityManager = securityManager

  /** Attach a tab to this UI, along with all of its attached pages.
    * 将该标签附加到此UI，以及其所附的所有页面
    * */
  def attachTab(tab: WebUITab) {
    tab.pages.foreach(attachPage)
    tabs += tab
  }

  def detachTab(tab: WebUITab) {
    tab.pages.foreach(detachPage)
    tabs -= tab
  }

  def detachPage(page: WebUIPage) {
    pageToHandlers.remove(page).foreach(_.foreach(detachHandler))
  }

  /** Attach a page to this UI.
    * 将页面附加到此UI */
  def attachPage(page: WebUIPage) {
    val pagePath = "/" + page.prefix
    val renderHandler = createServletHandler(pagePath,
      (request: HttpServletRequest) => page.render(request), securityManager, basePath)
    val renderJsonHandler = createServletHandler(pagePath.stripSuffix("/") + "/json",
      (request: HttpServletRequest) => page.renderJson(request), securityManager, basePath)
    attachHandler(renderHandler)
    attachHandler(renderJsonHandler)
    pageToHandlers.getOrElseUpdate(page, ArrayBuffer[ServletContextHandler]())
      .append(renderHandler)
  }

  /** Attach a handler to this UI.
    * 将处理程序附加到此UI*/
  def attachHandler(handler: ServletContextHandler) {
    handlers += handler
    serverInfo.foreach { info =>
      info.rootHandler.addHandler(handler)
      if (!handler.isStarted) {
        handler.start()
      }
    }
  }

  /** Detach a handler from this UI.
    * 从此UI中分离处理程序 */
  def detachHandler(handler: ServletContextHandler) {
    handlers -= handler
    serverInfo.foreach { info =>
      info.rootHandler.removeHandler(handler)
      if (handler.isStarted) {
        handler.stop()
      }
    }
  }

  /**
   * Add a handler for static content.
   * 添加静态内容的处理程序
   * @param resourceBase Root of where to find resources to serve.
   * @param path Path in UI where to mount the resources.
   */
  def addStaticHandler(resourceBase: String, path: String): Unit = {
    attachHandler(JettyUtils.createStaticHandler(resourceBase, path))
  }

  /**
   * Remove a static content handler.
   * 删除静态内容处理程序
   * @param path Path in UI to unmount.
   */
  def removeStaticHandler(path: String): Unit = {
    handlers.find(_.getContextPath() == path).foreach(detachHandler)
  }

  /** Initialize all components of the server.
    * 初始化服务器的所有组件 */
  def initialize()

  /** Bind to the HTTP server behind this web interface.
    * 绑定到此Web界面后面的HTTP服务器*/
  def bind() {
    assert(!serverInfo.isDefined, "Attempted to bind %s more than once!".format(className))
    try {
      serverInfo = Some(startJettyServer("0.0.0.0", port, handlers, conf, name))
      logInfo("Started %s at http://%s:%d".format(className, publicHostName, boundPort))
    } catch {
      case e: Exception =>
        logError("Failed to bind %s".format(className), e)
        System.exit(1)
    }
  }

  /** Return the actual port to which this server is bound. Only valid after bind().
    * 返回此服务器绑定到的实际端口。 只有在bind（）后才有效*/
  def boundPort: Int = serverInfo.map(_.boundPort).getOrElse(-1)

  /** Stop the server behind this web interface. Only valid after bind().
    * 停止此Web界面后面的服务器。 只有在bind（）后才有效 */
  def stop() {
    assert(serverInfo.isDefined,
      "Attempted to stop %s before binding to a server!".format(className))
    serverInfo.get.server.stop()
  }
}


/**
 * A tab that represents a collection of pages.
 * The prefix is appended to the parent address to form a full path, and must not contain slashes.
  * 表示页面集合的选项卡,前缀附加到父地址以形成完整路径,并且不能包含斜杠。
 */
private[spark] abstract class WebUITab(parent: WebUI, val prefix: String) {
  val pages = ArrayBuffer[WebUIPage]()
  val name = prefix.capitalize

  /** Attach a page to this tab. This prepends the page's prefix with the tab's own prefix.
    * 将页面附加到此选项卡,这是使用标签自己的前缀添加页面的前缀 */
  def attachPage(page: WebUIPage) {
    page.prefix = (prefix + "/" + page.prefix).stripSuffix("/")
    pages += page
  }

  /** Get a list of header tabs from the parent UI.
    * 从父UI获取标题选项卡的列表 */
  def headerTabs: Seq[WebUITab] = parent.getTabs

  def basePath: String = parent.getBasePath
}


/**
 * A page that represents the leaf node in the UI hierarchy.
 * 表示UI层次结构中叶节点的页面。
  *
 * The direct parent of a WebUIPage is not specified as it can be either a WebUI or a WebUITab.
 * If the parent is a WebUI, the prefix is appended to the parent's address to form a full path.
 * Else, if the parent is a WebUITab, the prefix is appended to the super prefix of the parent
 * to form a relative path. The prefix must not contain slashes.
  * 没有指定WebUIPage的直接父级，因为它可以是WebUI或WebUITab,如果父级是WebUI,
  * 则前缀将附加到父级地址以形成完整路径,否则,如果父级是WebUITab,则该前缀将附加到父级的超级前缀以形成相对路径,前缀不能包含斜杠
 */
private[spark] abstract class WebUIPage(var prefix: String) {
  def render(request: HttpServletRequest): Seq[Node]
  def renderJson(request: HttpServletRequest): JValue = JNothing
}
