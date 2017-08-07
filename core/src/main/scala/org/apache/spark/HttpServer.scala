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

package org.apache.spark

import java.io.File

import org.eclipse.jetty.server.ssl.SslSocketConnector
import org.eclipse.jetty.util.security.{Constraint, Password}
import org.eclipse.jetty.security.authentication.DigestAuthenticator
import org.eclipse.jetty.security.{ConstraintMapping, ConstraintSecurityHandler, HashLoginService}

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.bio.SocketConnector
import org.eclipse.jetty.server.handler.{DefaultHandler, HandlerList, ResourceHandler}
import org.eclipse.jetty.util.thread.QueuedThreadPool

import org.apache.spark.util.Utils


/**
 * Exception type thrown by HttpServer when it is in the wrong state for an operation.
  * HttpServer在操作错误状态时抛出的异常类型
 */
private[spark] class ServerStateException(message: String) extends Exception(message)

/**
 * An HTTP server for static content used to allow worker nodes to access JARs added to SparkContext
 * as well as classes created by the interpreter when the user types in code. This is just a wrapper
 * around a Jetty server.
  * 用于静态内容的HTTP服务器,用于允许工作节点访问添加到SparkContext的JAR以及用户键入代码时由解释器创建的类,这只是Jetty服务器上的一个包装。
 */
private[spark] class HttpServer(
    conf: SparkConf,
    resourceBase: File,
    securityManager: SecurityManager,
    requestedPort: Int = 0,
    serverName: String = "HTTP server")
  extends Logging {

  private var server: Server = null
  private var port: Int = requestedPort

  def start() {
    if (server != null) {
      throw new ServerStateException("Server is already started")
    } else {
      logInfo("Starting HTTP Server")
      val (actualServer, actualPort) =
        Utils.startServiceOnPort[Server](requestedPort, doStart, conf, serverName)
      server = actualServer
      port = actualPort
    }
  }

  /**
   * Actually start the HTTP server on the given port.
    * 实际上启动给定端口上的HTTP服务器
   *
   * Note that this is only best effort in the sense that we may end up binding to a nearby port
   * in the event of port collision. Return the bound server and the actual port used.
    * 请注意,这只是最大的努力，因为在端口冲突的情况下,我们可能会最终绑定到附近的端口,返回绑定的服务器和实际使用的端口
   */
  private def doStart(startPort: Int): (Server, Int) = {
    val server = new Server()

    val connector = securityManager.fileServerSSLOptions.createJettySslContextFactory()
      .map(new SslSocketConnector(_)).getOrElse(new SocketConnector)
    //最大空闲时间是一分钟啊
    connector.setMaxIdleTime(60 * 1000)
    connector.setSoLingerTime(-1)
    connector.setPort(startPort)
    server.addConnector(connector)

    val threadPool = new QueuedThreadPool
    threadPool.setDaemon(true)
    server.setThreadPool(threadPool)
    val resHandler = new ResourceHandler
    resHandler.setResourceBase(resourceBase.getAbsolutePath)

    val handlerList = new HandlerList
    handlerList.setHandlers(Array(resHandler, new DefaultHandler))

    if (securityManager.isAuthenticationEnabled()) {
      logDebug("HttpServer is using security")
      val sh = setupSecurityHandler(securityManager)
      // make sure we go through security handler to get resources
      //确保我们通过安全处理程序获取资源
      sh.setHandler(handlerList)
      server.setHandler(sh)
    } else {
      logDebug("HttpServer is not using security")
      server.setHandler(handlerList)
    }

    server.start()
    val actualPort = server.getConnectors()(0).getLocalPort

    (server, actualPort)
  }

  /**
   * Setup Jetty to the HashLoginService using a single user with our
   * shared secret. Configure it to use DIGEST-MD5 authentication so that the password
   * isn't passed in plaintext.
    * 使用具有共享密钥的单个用户将Jetty设置为HashLoginService。 将其配置为使用DIGEST-MD5认证，以便密码不以明文形式传递。
   */
  private def setupSecurityHandler(securityMgr: SecurityManager): ConstraintSecurityHandler = {
    val constraint = new Constraint()
    // use DIGEST-MD5 as the authentication mechanism
    //使用DIGEST-MD5作为认证机制
    constraint.setName(Constraint.__DIGEST_AUTH)
    constraint.setRoles(Array("user"))
    constraint.setAuthenticate(true)
    constraint.setDataConstraint(Constraint.DC_NONE)

    val cm = new ConstraintMapping()
    cm.setConstraint(constraint)
    cm.setPathSpec("/*")
    val sh = new ConstraintSecurityHandler()

    // the hashLoginService lets us do a single user and
    // secret right now. This could be changed to use the
    // JAASLoginService for other options.
    //hashLoginService使我们现在可以使用一个用户和密码,这可以更改为使用JAASLoginService进行其他选项。
    val hashLogin = new HashLoginService()

    val userCred = new Password(securityMgr.getSecretKey())
    if (userCred == null) {
      throw new Exception("Error: secret key is null with authentication on")
    }
    hashLogin.putUser(securityMgr.getHttpUser(), userCred, Array("user"))
    sh.setLoginService(hashLogin)
    sh.setAuthenticator(new DigestAuthenticator());
    sh.setConstraintMappings(Array(cm))
    sh
  }

  def stop() {
    if (server == null) {
      throw new ServerStateException("Server is already stopped")
    } else {
      server.stop()
      port = -1
      server = null
    }
  }

  /**
   * Get the URI of this HTTP server (http://host:port or https://host:port)
    * 获取此HTTP服务器的URI(http：// host：port或https：// host：port)
   */
  def uri: String = {
    if (server == null) {
      throw new ServerStateException("Server is not started")
    } else {
      val scheme = if (securityManager.fileServerSSLOptions.enabled) "https" else "http"
      s"$scheme://${Utils.localHostNameForURI()}:$port"
    }
  }
}
