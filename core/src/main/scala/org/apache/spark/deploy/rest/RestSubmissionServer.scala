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

package org.apache.spark.deploy.rest

import java.net.InetSocketAddress
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import scala.io.Source
import com.fasterxml.jackson.core.JsonProcessingException
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{ServletHolder, ServletContextHandler}
import org.eclipse.jetty.util.thread.QueuedThreadPool
import org.json4s._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.{Logging, SparkConf, SPARK_VERSION => sparkVersion}
import org.apache.spark.util.Utils

/**
 * A server that responds to requests submitted by the [[RestSubmissionClient]].
 * 响应由所提交的RestSubmissionClient请求的服务器
 * This server responds with different HTTP codes depending on the situation:
 * 该服务器具有不同的HTTP代码根据情况作出反应
 *   200 OK - Request was processed successfully
 *   200 OK - 请求成功处理
 *   400 BAD REQUEST - Request was malformed, not successfully validated, or of unexpected type
 *   400 BAD REQUEST - 请求格式错误,没有成功验证,或异常类型
 *   468 UNKNOWN PROTOCOL VERSION - Request specified a protocol this server does not understand
 *   468 未知协议版本 -请求指定此服务器不理解的协议
 *   500 INTERNAL SERVER ERROR - Server throws an exception internally while processing the request
 *   500 内部服务器错误--服务器在处理请求时发生异常
 *
 * The server always includes a JSON representation of the relevant [[SubmitRestProtocolResponse]]
 * 服务器总是包含一个JSON表示有关的submitrestprotocolresponse,并在HTTP文档体中传递.
 * in the HTTP body. If an error occurs, however, the server will include an [[ErrorResponse]]
 * 如果发生错误,但是服务器将包括一个ErrorResponse
 * instead of the one expected by the client. If the construction of this error response itself
 * 而不是客户期望的,如果此错误响应本身的构造失败
 * fails, the response will consist of an empty body with a response code that indicates internal
 * 响应将包括一个空,一个响应代码,表示内部服务器错误
 * server error.
 */
private[spark] abstract class RestSubmissionServer(
    val host: String,
    val requestedPort: Int,
    val masterConf: SparkConf) extends Logging {
  protected val submitRequestServlet: SubmitRequestServlet
  protected val killRequestServlet: KillRequestServlet
  protected val statusRequestServlet: StatusRequestServlet

  private var _server: Option[Server] = None

  // A mapping from URL prefixes to servlets that serve them. Exposed for testing.
  //一个URL前缀servlet,为他们服务的映射,暴露测试
  protected val baseContext = s"/${RestSubmissionServer.PROTOCOL_VERSION}/submissions"
  protected lazy val contextToServlet = Map[String, RestServlet](
    s"$baseContext/create/*" -> submitRequestServlet,
    s"$baseContext/kill/*" -> killRequestServlet,
    s"$baseContext/status/*" -> statusRequestServlet,
    "/*" -> new ErrorServlet // default handler
  )

  /** 
   *  Start the server and return the bound port.
   *  启动服务器并返回绑定端口 
   *  */
  def start(): Int = {
    val (server, boundPort) = Utils.startServiceOnPort[Server](requestedPort, doStart, masterConf)
    _server = Some(server)
    logInfo(s"Started REST server for submitting applications on port $boundPort")
    boundPort
  }

  /**
   * Map the servlets to their corresponding contexts and attach them to a server.
   * 映射servlet对应的上下文和附加到服务器
   * Return a 2-tuple of the started server and the bound port.
   * 返回 一个元组已启动的服务器和绑定端口
   */
  private def doStart(startPort: Int): (Server, Int) = {
    val server = new Server(new InetSocketAddress(host, startPort))
    //
    val threadPool = new QueuedThreadPool
    threadPool.setDaemon(true)
    server.setThreadPool(threadPool)
    val mainHandler = new ServletContextHandler
    mainHandler.setContextPath("/")
    contextToServlet.foreach { case (prefix, servlet) =>
      mainHandler.addServlet(new ServletHolder(servlet), prefix)
    }
    server.setHandler(mainHandler)
    server.start()
    val boundPort = server.getConnectors()(0).getLocalPort
    (server, boundPort)
  }

  def stop(): Unit = {
    _server.foreach(_.stop())
  }
}

private[rest] object RestSubmissionServer {
  val PROTOCOL_VERSION = RestSubmissionClient.PROTOCOL_VERSION
  val SC_UNKNOWN_PROTOCOL_VERSION = 468
}

/**
 * An abstract servlet for handling requests passed to the [[RestSubmissionServer]].
 * 抽象Servlet处理请求传递到RestSubmissionServer
 */
private[rest] abstract class RestServlet extends HttpServlet with Logging {

  /**
   * Serialize the given response message to JSON and send it through the response servlet.
   * 序列化的JSON响应消息发送给它通过响应Servlet
   * This validates the response before sending it to ensure it is properly constructed.
   * 这验证了在发送它之前,以确保它是正确构建的响应
   */
  protected def sendResponse(
      responseMessage: SubmitRestProtocolResponse,
      responseServlet: HttpServletResponse): Unit = {
    val message = validateResponse(responseMessage, responseServlet)
    responseServlet.setContentType("application/json")
    responseServlet.setCharacterEncoding("utf-8")
    responseServlet.getWriter.write(message.toJson)
  }

  /**
   * Return any fields in the client request message that the server does not know about.
   * 返回服务器未知的客户端请求消息中的任何字段
   *
   * The mechanism for this is to reconstruct the JSON on the server side and compare the
   * 这个机制是重建在服务器端的JSON和比较JSON和一个在客户端生成之间的差异
   * diff between this JSON and the one generated on the client side. Any fields that are
   * 任何字段,只有在客户端的JSON作为意外。
   * only in the client JSON are treated as unexpected.
   */
  protected def findUnknownFields(
      requestJson: String,
      requestMessage: SubmitRestProtocolMessage): Array[String] = {
    val clientSideJson = parse(requestJson)
    val serverSideJson = parse(requestMessage.toJson)
    val Diff(_, _, unknown) = clientSideJson.diff(serverSideJson)
    unknown match {
      case j: JObject => j.obj.map { case (k, _) => k }.toArray
      case _ => Array.empty[String] // No difference
    }
  }

  /** 
   *  Return a human readable String representation of the exception.
   *  返回异常的人类可读的字符串表示形式 
   *  */
  protected def formatException(e: Throwable): String = {
    val stackTraceString = e.getStackTrace.map { "\t" + _ }.mkString("\n")
    s"$e\n$stackTraceString"
  }

  /** 
   *  Construct an error message to signal the fact that an exception has been thrown. 
   *  构造一个错误消息来表示一个异常被抛出的事实
   *  */
  protected def handleError(message: String): ErrorResponse = {
    val e = new ErrorResponse
    e.serverSparkVersion = sparkVersion
    e.message = message
    e
  }

  /**
   * Parse a submission ID from the relative path, assuming it is the first part of the path.
   * 从相对路径解析提交标识,假设它是路径的第一部分
   * For instance, we expect the path to take the form /[submission ID]/maybe/something/else.
   * 例如,我们期望的路径采取的形式 /[submission ID]/maybe/something/else
   * The returned submission ID cannot be empty. If the path is unexpected, return None.
   * 返回的提交ID不能为空,如果路径是异常,返回None
   */
  protected def parseSubmissionId(path: String): Option[String] = {
    if (path == null || path.isEmpty) {
      None
    } else {
      //Seq(1,2,3).headOption
      //res0: Option[Int] = Some(1)
      path.stripPrefix("/").split("/").headOption.filter(_.nonEmpty)
    }
  }

  /**
   * Validate the response to ensure that it is correctly constructed.
   * 确认响应,以确保它正确地构建
   *
   * If it is, simply return the message as is. Otherwise, return an error response instead
   * 如果是,简单地返回的消息是,否则,返回一个错误响应
   * to propagate the exception back to the client and set the appropriate error code.
   * 将异常传递到客户端并设置相应的错误代码
   */
  private def validateResponse(
      responseMessage: SubmitRestProtocolResponse,
      responseServlet: HttpServletResponse): SubmitRestProtocolResponse = {
    try {
      responseMessage.validate()
      responseMessage
    } catch {
      case e: Exception =>
        responseServlet.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR)
        handleError("Internal server error: " + formatException(e))
    }
  }
}

/**
 * A servlet for handling kill requests passed to the [[RestSubmissionServer]].
 * 一个servlet处理杀死请求传递到[ restsubmissionserver ] 
 */
private[rest] abstract class KillRequestServlet extends RestServlet {

  /**
   * If a submission ID is specified in the URL, have the Master kill the corresponding
   * 如果在网址中指定提交的ID,让主节点杀死了相应驱动程序,并返回一个适当的响应客户端
   * driver and return an appropriate response to the client. Otherwise, return error.
   * 否则,返回错误
   */
  protected override def doPost(
      request: HttpServletRequest,
      response: HttpServletResponse): Unit = {
    val submissionId = parseSubmissionId(request.getPathInfo)
    val responseMessage = submissionId.map(handleKill).getOrElse {
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST)
      handleError("Submission ID is missing in kill request.")
    }
    sendResponse(responseMessage, response)
  }

  protected def handleKill(submissionId: String): KillSubmissionResponse
}

/**
 * A servlet for handling status requests passed to the [[RestSubmissionServer]].
 * 一种处理状态请求传递给restsubmissionserver的servlet
 */
private[rest] abstract class StatusRequestServlet extends RestServlet {

  /**
   * If a submission ID is specified in the URL, request the status of the corresponding
   * driver from the Master and include it in the response. Otherwise, return error.
    * 如果在URL中指定了提交ID,请从主服务器请求相应驱动程序的状态并将其包含在响应中,否则返回错误
   */
  protected override def doGet(
      request: HttpServletRequest,
      response: HttpServletResponse): Unit = {
    val submissionId = parseSubmissionId(request.getPathInfo)
    val responseMessage = submissionId.map(handleStatus).getOrElse {
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST)
      handleError("Submission ID is missing in status request.")
    }
    sendResponse(responseMessage, response)
  }

  protected def handleStatus(submissionId: String): SubmissionStatusResponse
}

/**
 * A servlet for handling submit requests passed to the [[RestSubmissionServer]].
  * 用于处理传递给[[RestSubmissionServer]]的提交请求的servlet
 */
private[rest] abstract class SubmitRequestServlet extends RestServlet {

  /**
   * Submit an application to the Master with parameters specified in the request.
    * 使用请求中指定的参数向Master提交应用程序
   *
   * The request is assumed to be a [[SubmitRestProtocolRequest]] in the form of JSON.
   * If the request is successfully processed, return an appropriate response to the
   * client indicating so. Otherwise, return error instead.
    * 该请求被假定为[[SubmitRestProtocolRequest]]形式的JSON。
    * 如果请求成功处理，则返回适当的响应给客户机，这样做。 否则返回错误。
   */
  protected override def doPost(
      requestServlet: HttpServletRequest,
      responseServlet: HttpServletResponse): Unit = {
    val responseMessage =
      try {
        val requestMessageJson = Source.fromInputStream(requestServlet.getInputStream).mkString
        val requestMessage = SubmitRestProtocolMessage.fromJson(requestMessageJson)
        // The response should have already been validated on the client.
        //响应应该已经在客户端上验证了
        // In case this is not true, validate it ourselves to avoid potential NPEs.
        //如果这不是真的,请自行验证以避免潜在的NPE。
        requestMessage.validate()
        handleSubmit(requestMessageJson, requestMessage, responseServlet)
      } catch {
        // The client failed to provide a valid JSON, so this is not our fault
        //客户端无法提供有效的JSON,因此这不是我们的错误
        case e @ (_: JsonProcessingException | _: SubmitRestProtocolException) =>
          responseServlet.setStatus(HttpServletResponse.SC_BAD_REQUEST)
          handleError("Malformed request: " + formatException(e))
      }
    sendResponse(responseMessage, responseServlet)
  }

  protected def handleSubmit(
      requestMessageJson: String,
      requestMessage: SubmitRestProtocolMessage,
      responseServlet: HttpServletResponse): SubmitRestProtocolResponse
}

/**
 * A default servlet that handles error cases that are not captured by other servlets.
 * 默认servlet处理错误的情况下,不被其他servlet捕获。
 */
private class ErrorServlet extends RestServlet {
  private val serverVersion = RestSubmissionServer.PROTOCOL_VERSION

  /** 
   *  Service a faulty request by returning an appropriate error message to the client.
   *  返回一个适当的错误消息到客户端,服务一个错误的请求
   *   */
  protected override def service(
      request: HttpServletRequest,
      response: HttpServletResponse): Unit = {
    val path = request.getPathInfo
    val parts = path.stripPrefix("/").split("/").filter(_.nonEmpty).toList
    var versionMismatch = false
    var msg =
      parts match {
        case Nil =>
          // http://host:port/
          "Missing protocol version."
        case `serverVersion` :: Nil =>
          // http://host:port/correct-version
          "Missing the /submissions prefix."
        case `serverVersion` :: "submissions" :: tail =>
          // http://host:port/correct-version/submissions/*
          "Missing an action: please specify one of /create, /kill, or /status."
        case unknownVersion :: tail =>
          // http://host:port/unknown-version/*
          versionMismatch = true
          s"Unknown protocol version '$unknownVersion'."
        case _ =>
          // never reached
          s"Malformed path $path."
      }
    msg += s" Please submit requests through http://[host]:[port]/$serverVersion/submissions/..."
    val error = handleError(msg)
    // If there is a version mismatch, include the highest protocol version that
    //如果有版本不匹配,包括最高的协议版本,此服务器支持的情况下,客户端要重试与我们的版本一致
    // this server supports in case the client wants to retry with our version
    //该服务器支持客户端想要重试我们的版本
    if (versionMismatch) {
      error.highestProtocolVersion = serverVersion
      response.setStatus(RestSubmissionServer.SC_UNKNOWN_PROTOCOL_VERSION)
    } else {
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST)
    }
    sendResponse(error, response)
  }
}
