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

import java.io.File
import javax.servlet.http.HttpServletResponse

import org.apache.spark.deploy.ClientArguments._
import org.apache.spark.deploy.{Command, DeployMessages, DriverDescription}
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.util.Utils
import org.apache.spark.{SPARK_VERSION => sparkVersion, SparkConf}

/**
 * A server that responds to requests submitted by the [[RestSubmissionClient]].
 * 响应由所提交的RestSubmissionClient请求的服务器
 * This is intended to be embedded in the standalone Master and used in cluster mode only.
 * 这是打算被嵌入在独立的主节点,并在集群模式中使用
 *
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
 * in the HTTP body. If an error occurs, however, the server will include an [[ErrorResponse]]
 * instead of the one expected by the client. If the construction of this error response itself
 * fails, the response will consist of an empty body with a response code that indicates internal
 * server error.
 *
 * @param host the address this server should bind to
 * @param requestedPort the port this server will attempt to bind to
 * @param masterConf the conf used by the Master
 * @param masterEndpoint reference to the Master endpoint to which requests can be sent
 * @param masterUrl the URL of the Master new drivers will attempt to connect to
 */
private[deploy] class StandaloneRestServer(
    host: String,
    requestedPort: Int,
    masterConf: SparkConf,
    masterEndpoint: RpcEndpointRef,
    masterUrl: String)
  extends RestSubmissionServer(host, requestedPort, masterConf) {

  protected override val submitRequestServlet =
    new StandaloneSubmitRequestServlet(masterEndpoint, masterUrl, masterConf)
  protected override val killRequestServlet =
    new StandaloneKillRequestServlet(masterEndpoint, masterConf)
  protected override val statusRequestServlet =
    new StandaloneStatusRequestServlet(masterEndpoint, masterConf)
}

/**
 * A servlet for handling kill requests passed to the [[StandaloneRestServer]].
 * 一个servlet处理杀死请求传递给 [ standalonerestserver ]
 */
private[rest] class StandaloneKillRequestServlet(masterEndpoint: RpcEndpointRef, conf: SparkConf)
  extends KillRequestServlet {

  protected def handleKill(submissionId: String): KillSubmissionResponse = {
    val response = masterEndpoint.askWithRetry[DeployMessages.KillDriverResponse](
      DeployMessages.RequestKillDriver(submissionId))
    val k = new KillSubmissionResponse
    k.serverSparkVersion = sparkVersion
    k.message = response.message
    k.submissionId = submissionId
    k.success = response.success
    k
  }
}

/**
 * A servlet for handling status requests passed to the [[StandaloneRestServer]].
 * 一个servlet处理状态请求传递到standalonerestserver
 */
private[rest] class StandaloneStatusRequestServlet(masterEndpoint: RpcEndpointRef, conf: SparkConf)
  extends StatusRequestServlet {

  protected def handleStatus(submissionId: String): SubmissionStatusResponse = {
    val response = masterEndpoint.askWithRetry[DeployMessages.DriverStatusResponse](
      DeployMessages.RequestDriverStatus(submissionId))
    val message = response.exception.map { s"Exception from the cluster:\n" + formatException(_) }
    val d = new SubmissionStatusResponse
    d.serverSparkVersion = sparkVersion
    d.submissionId = submissionId
    d.success = response.found
    d.driverState = response.state.map(_.toString).orNull
    d.workerId = response.workerId.orNull
    d.workerHostPort = response.workerHostPort.orNull
    d.message = message.orNull
    d
  }
}

/**
 * A servlet for handling submit requests passed to the [[StandaloneRestServer]].
 * 一个servlet处理提交请求传递到standalonerestserver
 */
private[rest] class StandaloneSubmitRequestServlet(
    masterEndpoint: RpcEndpointRef,
    masterUrl: String,
    conf: SparkConf)
  extends SubmitRequestServlet {

  /**
   * Build a driver description from the fields specified in the submit request.
   * 从提交请求中指定的字段中生成一个驱动程序描述
   *
   * This involves constructing a command that takes into account memory, java options,
   * 这涉及到构建一个需要考虑内存的命令,java选项,
   * classpath and other settings to launch the driver. This does not currently consider
   * 路径和其他设置启动驱动器,这目前不考虑
   * fields used by python applications since python is not supported in standalone
   * cluster mode yet.
   */
  private def buildDriverDescription(request: CreateSubmissionRequest): DriverDescription = {
    // Required fields, including the main class because python is not yet supported
    //必填字段,包括主要的阶级因为尚未支持Python
    val appResource = Option(request.appResource).getOrElse {
      throw new SubmitRestMissingFieldException("Application jar is missing.")
    }
    val mainClass = Option(request.mainClass).getOrElse {
      throw new SubmitRestMissingFieldException("Main class is missing.")
    }

    // Optional fields 可选字段
    val sparkProperties = request.sparkProperties
    val driverMemory = sparkProperties.get("spark.driver.memory")
    val driverCores = sparkProperties.get("spark.driver.cores")
    val driverExtraJavaOptions = sparkProperties.get("spark.driver.extraJavaOptions")
    val driverExtraClassPath = sparkProperties.get("spark.driver.extraClassPath")
    val driverExtraLibraryPath = sparkProperties.get("spark.driver.extraLibraryPath")
    val superviseDriver = sparkProperties.get("spark.driver.supervise")
    val appArgs = request.appArgs
    val environmentVariables = request.environmentVariables

    // Construct driver description 构建驱动描述
    val conf = new SparkConf(false)
      .setAll(sparkProperties)
      .set("spark.master", masterUrl)
    val extraClassPath = driverExtraClassPath.toSeq.flatMap(_.split(File.pathSeparator))
    val extraLibraryPath = driverExtraLibraryPath.toSeq.flatMap(_.split(File.pathSeparator))
    val extraJavaOpts = driverExtraJavaOptions.map(Utils.splitCommandString).getOrElse(Seq.empty)
    val sparkJavaOpts = Utils.sparkJavaOpts(conf)
    val javaOpts = sparkJavaOpts ++ extraJavaOpts
    val command = new Command(
      "org.apache.spark.deploy.worker.DriverWrapper",
      Seq("{{WORKER_URL}}", "{{USER_JAR}}", mainClass) ++ appArgs, // args to the DriverWrapper
      environmentVariables, extraClassPath, extraLibraryPath, javaOpts)
    val actualDriverMemory = driverMemory.map(Utils.memoryStringToMb).getOrElse(DEFAULT_MEMORY)
    val actualDriverCores = driverCores.map(_.toInt).getOrElse(DEFAULT_CORES)
    val actualSuperviseDriver = superviseDriver.map(_.toBoolean).getOrElse(DEFAULT_SUPERVISE)
    new DriverDescription(
      appResource, actualDriverMemory, actualDriverCores, actualSuperviseDriver, command)
  }

  /**
   * Handle the submit request and construct an appropriate response to return to the client.
   * 处理提交请求并构造一个适当的响应返回到客户端
   *
   * This assumes that the request message is already successfully validated.
   * 这假定请求消息已经成功地验证
   * If the request message is not of the expected type, return error to the client.
   * 如果请求消息不是预期类型,返回客户端的错误
   */
  protected override def handleSubmit(
      requestMessageJson: String,
      requestMessage: SubmitRestProtocolMessage,
      responseServlet: HttpServletResponse): SubmitRestProtocolResponse = {
    requestMessage match {
      case submitRequest: CreateSubmissionRequest =>
        val driverDescription = buildDriverDescription(submitRequest)
        val response = masterEndpoint.askWithRetry[DeployMessages.SubmitDriverResponse](
          DeployMessages.RequestSubmitDriver(driverDescription))
        val submitResponse = new CreateSubmissionResponse
        submitResponse.serverSparkVersion = sparkVersion
        submitResponse.message = response.message
        submitResponse.success = response.success
        submitResponse.submissionId = response.driverId.orNull
        val unknownFields = findUnknownFields(requestMessageJson, requestMessage)
        if (unknownFields.nonEmpty) {
          // If there are fields that the server does not know about, warn the client
          //如果服务器不知道的字段,警告客户端
          submitResponse.unknownFields = unknownFields
        }
        submitResponse
      case unexpected =>
        responseServlet.setStatus(HttpServletResponse.SC_BAD_REQUEST)
        handleError(s"Received message of unexpected type ${unexpected.messageType}.")
    }
  }
}
