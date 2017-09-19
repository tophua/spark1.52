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

import java.io.DataOutputStream
import java.net.{HttpURLConnection, URL}
import javax.servlet.http.HttpServletResponse

import scala.collection.mutable

import com.google.common.base.Charsets
import org.scalatest.BeforeAndAfterEach
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods._

import org.apache.spark._
import org.apache.spark.rpc._
import org.apache.spark.util.Utils
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.deploy.{SparkSubmit, SparkSubmitArguments}
import org.apache.spark.deploy.master.DriverState._

/**
 * Tests for the REST application submission protocol used in standalone cluster mode.
 * 测试独立集群模式中使用的剩余应用提交协议
 */
class StandaloneRestSubmitSuite extends SparkFunSuite with BeforeAndAfterEach {
  private var rpcEnv: Option[RpcEnv] = None
  private var server: Option[RestSubmissionServer] = None

  override def afterEach() {
    rpcEnv.foreach(_.shutdown())//关闭
    server.foreach(_.stop())//暂停
  }

  test("construct submit request") {//构造提交请求
    val appArgs = Array("one", "two", "three")
    //map 初始化
    val sparkProperties = Map("spark.app.name" -> "pi")
    //环境变量，map 初始化以逗号分隔
    val environmentVariables = Map("SPARK_ONE" -> "UN", "SPARK_TWO" -> "DEUX")
    //Rest提交客户端
    val request = new RestSubmissionClient("spark://host:port").constructSubmitRequest(
      "my-app-resource", "my-main-class", appArgs, sparkProperties, environmentVariables)
    assert(request.action === Utils.getFormattedClassName(request))
    assert(request.clientSparkVersion === SPARK_VERSION)
    assert(request.appResource === "my-app-resource")
    assert(request.mainClass === "my-main-class")
    assert(request.appArgs === appArgs)
    assert(request.sparkProperties === sparkProperties)
    assert(request.environmentVariables === environmentVariables)
  }

  test("create submission") {//创建提交
    val submittedDriverId = "my-driver-id"
    val submitMessage = "your driver is submitted"
    val masterUrl = startDummyServer(submitId = submittedDriverId, submitMessage = submitMessage)
    val appArgs = Array("one", "two", "four")
    val request = constructSubmitRequest(masterUrl, appArgs)
    assert(request.appArgs === appArgs)
    assert(request.sparkProperties("spark.master") === masterUrl)
    
    val response = new RestSubmissionClient(masterUrl).createSubmission(request)
    val submitResponse = getSubmitResponse(response)
    assert(submitResponse.action === Utils.getFormattedClassName(submitResponse))
    assert(submitResponse.serverSparkVersion === SPARK_VERSION)
    assert(submitResponse.message === submitMessage)
    assert(submitResponse.submissionId === submittedDriverId)
    assert(submitResponse.success)
  }

  test("create submission from main method") {//从main方法创建提交
    val submittedDriverId = "your-driver-id"
    val submitMessage = "my driver is submitted"
    val masterUrl = startDummyServer(submitId = submittedDriverId, submitMessage = submitMessage)
    val conf = new SparkConf(loadDefaults = false)
    conf.set("spark.master", masterUrl)
    conf.set("spark.app.name", "dreamer")
    val appArgs = Array("one", "two", "six")
    // main method calls this
    //main方法调用此
    val response = RestSubmissionClient.run("app-resource", "main-class", appArgs, conf)
    val submitResponse = getSubmitResponse(response)
    assert(submitResponse.action === Utils.getFormattedClassName(submitResponse))
    assert(submitResponse.serverSparkVersion === SPARK_VERSION)
    assert(submitResponse.message === submitMessage)
    assert(submitResponse.submissionId === submittedDriverId)
    assert(submitResponse.success)
  }

  test("kill submission") {//杀死提交
    val submissionId = "my-lyft-driver"
    val killMessage = "your driver is killed"
    val masterUrl = startDummyServer(killMessage = killMessage)
    val response = new RestSubmissionClient(masterUrl).killSubmission(submissionId)
    val killResponse = getKillResponse(response)
    assert(killResponse.action === Utils.getFormattedClassName(killResponse))
    assert(killResponse.serverSparkVersion === SPARK_VERSION)
    assert(killResponse.message === killMessage)
    assert(killResponse.submissionId === submissionId)
    assert(killResponse.success)
  }

  test("request submission status") {//请求提交状态
    val submissionId = "my-uber-driver"
    val submissionState = KILLED
    val submissionException = new Exception("there was an irresponsible mix of alcohol and cars")
    val masterUrl = startDummyServer(state = submissionState, exception = Some(submissionException))
    val response = new RestSubmissionClient(masterUrl).requestSubmissionStatus(submissionId)
    val statusResponse = getStatusResponse(response)
    assert(statusResponse.action === Utils.getFormattedClassName(statusResponse))
    assert(statusResponse.serverSparkVersion === SPARK_VERSION)
    assert(statusResponse.message.contains(submissionException.getMessage))
    assert(statusResponse.submissionId === submissionId)
    assert(statusResponse.driverState === submissionState.toString)
    assert(statusResponse.success)
  }

  test("create then kill") {//创建然后杀死
    val masterUrl = startSmartServer()
    val request = constructSubmitRequest(masterUrl)
    val client = new RestSubmissionClient(masterUrl)
    val response1 = client.createSubmission(request)
    val submitResponse = getSubmitResponse(response1)
    assert(submitResponse.success)
    assert(submitResponse.submissionId != null)
    // kill submission that was just created
    //刚刚创建的杀死提交
    val submissionId = submitResponse.submissionId
    val response2 = client.killSubmission(submissionId)
    val killResponse = getKillResponse(response2)
    assert(killResponse.success)
    assert(killResponse.submissionId === submissionId)
  }

  test("create then request status") {//创建然后请求状态
    val masterUrl = startSmartServer()
    val request = constructSubmitRequest(masterUrl)
    val client = new RestSubmissionClient(masterUrl)
    val response1 = client.createSubmission(request)
    val submitResponse = getSubmitResponse(response1)
    assert(submitResponse.success)
    assert(submitResponse.submissionId != null)
    // request status of submission that was just created
    //请求刚刚创建的提交的请求状态
    val submissionId = submitResponse.submissionId
    val response2 = client.requestSubmissionStatus(submissionId)
    val statusResponse = getStatusResponse(response2)
    assert(statusResponse.success)
    assert(statusResponse.submissionId === submissionId)
    assert(statusResponse.driverState === RUNNING.toString)
  }

  test("create then kill then request status") {//创建然后杀死然后请求状态
    val masterUrl = startSmartServer()
    val request = constructSubmitRequest(masterUrl)
    val client = new RestSubmissionClient(masterUrl)
    val response1 = client.createSubmission(request)
    val response2 = client.createSubmission(request)
    val submitResponse1 = getSubmitResponse(response1)
    val submitResponse2 = getSubmitResponse(response2)
    assert(submitResponse1.success)
    assert(submitResponse2.success)
    assert(submitResponse1.submissionId != null)
    assert(submitResponse2.submissionId != null)
    val submissionId1 = submitResponse1.submissionId
    val submissionId2 = submitResponse2.submissionId
    // kill only submission 1, but not submission 2
    //只杀死提交1,但不提交2
    val response3 = client.killSubmission(submissionId1)
    val killResponse = getKillResponse(response3)
    assert(killResponse.success)
    assert(killResponse.submissionId === submissionId1)
    // request status for both submissions: 1 should be KILLED but 2 should be RUNNING still
    //提交意见书的请求状态：1应该被杀死,但2应该运行
    val response4 = client.requestSubmissionStatus(submissionId1)
    val response5 = client.requestSubmissionStatus(submissionId2)
    val statusResponse1 = getStatusResponse(response4)
    val statusResponse2 = getStatusResponse(response5)
    assert(statusResponse1.submissionId === submissionId1)
    assert(statusResponse2.submissionId === submissionId2)
    assert(statusResponse1.driverState === KILLED.toString)
    assert(statusResponse2.driverState === RUNNING.toString)
  }

  test("kill or request status before create") {//创建前杀死或请求状态
    val masterUrl = startSmartServer()
    val doesNotExist = "does-not-exist"
    val client = new RestSubmissionClient(masterUrl)
    // kill a non-existent submission
    //杀死一个不存在的提交
    val response1 = client.killSubmission(doesNotExist)
    val killResponse = getKillResponse(response1)
    assert(!killResponse.success)
    assert(killResponse.submissionId === doesNotExist)
    // request status for a non-existent submission
    //请求不存在提交的状态
    val response2 = client.requestSubmissionStatus(doesNotExist)
    val statusResponse = getStatusResponse(response2)
    assert(!statusResponse.success)
    assert(statusResponse.submissionId === doesNotExist)
  }

  /* ---------------------------------------- *
   |     Aberrant client / server behavior    | 异常的客户机/服务器行为
   * ---------------------------------------- */

  test("good request paths") {//好的请求路径
    val masterUrl = startSmartServer()
    val httpUrl = masterUrl.replace("spark://", "http://")
    val v = RestSubmissionServer.PROTOCOL_VERSION
    val json = constructSubmitRequest(masterUrl).toJson
    val submitRequestPath = s"$httpUrl/$v/submissions/create"
    val killRequestPath = s"$httpUrl/$v/submissions/kill"
    val statusRequestPath = s"$httpUrl/$v/submissions/status"
    val (response1, code1) = sendHttpRequestWithResponse(submitRequestPath, "POST", json)
    val (response2, code2) = sendHttpRequestWithResponse(s"$killRequestPath/anything", "POST")
    val (response3, code3) = sendHttpRequestWithResponse(s"$killRequestPath/any/thing", "POST")
    val (response4, code4) = sendHttpRequestWithResponse(s"$statusRequestPath/anything", "GET")
    val (response5, code5) = sendHttpRequestWithResponse(s"$statusRequestPath/any/thing", "GET")
    // these should all succeed and the responses should be of the correct types
    //这些都应该是成功的,应该是正确的类型
    getSubmitResponse(response1)
    val killResponse1 = getKillResponse(response2)
    val killResponse2 = getKillResponse(response3)
    val statusResponse1 = getStatusResponse(response4)
    val statusResponse2 = getStatusResponse(response5)
    assert(killResponse1.submissionId === "anything")
    assert(killResponse2.submissionId === "any")
    assert(statusResponse1.submissionId === "anything")
    assert(statusResponse2.submissionId === "any")
    assert(code1 === HttpServletResponse.SC_OK)
    assert(code2 === HttpServletResponse.SC_OK)
    assert(code3 === HttpServletResponse.SC_OK)
    assert(code4 === HttpServletResponse.SC_OK)
    assert(code5 === HttpServletResponse.SC_OK)
  }

  test("good request paths, bad requests") {//好的请求路径,坏的请求
    val masterUrl = startSmartServer()
    val httpUrl = masterUrl.replace("spark://", "http://")
    val v = RestSubmissionServer.PROTOCOL_VERSION
    val submitRequestPath = s"$httpUrl/$v/submissions/create"
    val killRequestPath = s"$httpUrl/$v/submissions/kill"
    val statusRequestPath = s"$httpUrl/$v/submissions/status"
    val goodJson = constructSubmitRequest(masterUrl).toJson
    val badJson1 = goodJson.replaceAll("action", "fraction") // invalid JSON
    val badJson2 = goodJson.substring(goodJson.size / 2) // malformed JSON
    val notJson = "\"hello, world\""
    val (response1, code1) = sendHttpRequestWithResponse(submitRequestPath, "POST") // missing JSON
    val (response2, code2) = sendHttpRequestWithResponse(submitRequestPath, "POST", badJson1)
    val (response3, code3) = sendHttpRequestWithResponse(submitRequestPath, "POST", badJson2)
    val (response4, code4) = sendHttpRequestWithResponse(killRequestPath, "POST") // missing ID
    val (response5, code5) = sendHttpRequestWithResponse(s"$killRequestPath/", "POST")
    val (response6, code6) = sendHttpRequestWithResponse(statusRequestPath, "GET") // missing ID
    val (response7, code7) = sendHttpRequestWithResponse(s"$statusRequestPath/", "GET")
    val (response8, code8) = sendHttpRequestWithResponse(submitRequestPath, "POST", notJson)
    // these should all fail as error responses
    //这些都应该作为错误响应失败
    getErrorResponse(response1)
    getErrorResponse(response2)
    getErrorResponse(response3)
    getErrorResponse(response4)
    getErrorResponse(response5)
    getErrorResponse(response6)
    getErrorResponse(response7)
    getErrorResponse(response8)
    assert(code1 === HttpServletResponse.SC_BAD_REQUEST)
    assert(code2 === HttpServletResponse.SC_BAD_REQUEST)
    assert(code3 === HttpServletResponse.SC_BAD_REQUEST)
    assert(code4 === HttpServletResponse.SC_BAD_REQUEST)
    assert(code5 === HttpServletResponse.SC_BAD_REQUEST)
    assert(code6 === HttpServletResponse.SC_BAD_REQUEST)
    assert(code7 === HttpServletResponse.SC_BAD_REQUEST)
    assert(code8 === HttpServletResponse.SC_BAD_REQUEST)
  }

  test("bad request paths") {//错误的请求路径
    val masterUrl = startSmartServer()
    val httpUrl = masterUrl.replace("spark://", "http://")
    val v = RestSubmissionServer.PROTOCOL_VERSION
    val (response1, code1) = sendHttpRequestWithResponse(httpUrl, "GET")
    val (response2, code2) = sendHttpRequestWithResponse(s"$httpUrl/", "GET")
    val (response3, code3) = sendHttpRequestWithResponse(s"$httpUrl/$v", "GET")
    val (response4, code4) = sendHttpRequestWithResponse(s"$httpUrl/$v/", "GET")
    val (response5, code5) = sendHttpRequestWithResponse(s"$httpUrl/$v/submissions", "GET")
    val (response6, code6) = sendHttpRequestWithResponse(s"$httpUrl/$v/submissions/", "GET")
    val (response7, code7) = sendHttpRequestWithResponse(s"$httpUrl/$v/submissions/bad", "GET")
    val (response8, code8) = sendHttpRequestWithResponse(s"$httpUrl/bad-version", "GET")
    assert(code1 === HttpServletResponse.SC_BAD_REQUEST)
    assert(code2 === HttpServletResponse.SC_BAD_REQUEST)
    assert(code3 === HttpServletResponse.SC_BAD_REQUEST)
    assert(code4 === HttpServletResponse.SC_BAD_REQUEST)
    assert(code5 === HttpServletResponse.SC_BAD_REQUEST)
    assert(code6 === HttpServletResponse.SC_BAD_REQUEST)
    assert(code7 === HttpServletResponse.SC_BAD_REQUEST)
    assert(code8 === RestSubmissionServer.SC_UNKNOWN_PROTOCOL_VERSION)
    // all responses should be error responses
    //所有的响应都应该是错误的
    val errorResponse1 = getErrorResponse(response1)
    val errorResponse2 = getErrorResponse(response2)
    val errorResponse3 = getErrorResponse(response3)
    val errorResponse4 = getErrorResponse(response4)
    val errorResponse5 = getErrorResponse(response5)
    val errorResponse6 = getErrorResponse(response6)
    val errorResponse7 = getErrorResponse(response7)
    val errorResponse8 = getErrorResponse(response8)
    // only the incompatible version response should have server protocol version set
    //只有不兼容的版本响应应该有服务器协议版本集
    assert(errorResponse1.highestProtocolVersion === null)
    assert(errorResponse2.highestProtocolVersion === null)
    assert(errorResponse3.highestProtocolVersion === null)
    assert(errorResponse4.highestProtocolVersion === null)
    assert(errorResponse5.highestProtocolVersion === null)
    assert(errorResponse6.highestProtocolVersion === null)
    assert(errorResponse7.highestProtocolVersion === null)
    assert(errorResponse8.highestProtocolVersion === RestSubmissionServer.PROTOCOL_VERSION)
  }

  test("server returns unknown fields") {//服务器返回未知字段
    val masterUrl = startSmartServer()
    val httpUrl = masterUrl.replace("spark://", "http://")
    val v = RestSubmissionServer.PROTOCOL_VERSION
    val submitRequestPath = s"$httpUrl/$v/submissions/create"
    val oldJson = constructSubmitRequest(masterUrl).toJson
    val oldFields = parse(oldJson).asInstanceOf[JObject].obj
    val newFields = oldFields ++ Seq(
      JField("tomato", JString("not-a-fruit")),
      JField("potato", JString("not-po-tah-to"))
    )
    val newJson = pretty(render(JObject(newFields)))
    // send two requests, one with the unknown fields and the other without
    //发送两个请求,一个与未知字段和另一个没有
    val (response1, code1) = sendHttpRequestWithResponse(submitRequestPath, "POST", oldJson)
    val (response2, code2) = sendHttpRequestWithResponse(submitRequestPath, "POST", newJson)
    val submitResponse1 = getSubmitResponse(response1)
    val submitResponse2 = getSubmitResponse(response2)
    assert(code1 === HttpServletResponse.SC_OK)
    assert(code2 === HttpServletResponse.SC_OK)
    // only the response to the modified request should have unknown fields set
    //只有对修改后的请求的响应应该具有未知字段集
    assert(submitResponse1.unknownFields === null)
    assert(submitResponse2.unknownFields === Array("tomato", "potato"))
  }

  test("client handles faulty server") {//客户端处理故障服务器
    val masterUrl = startFaultyServer()
    val client = new RestSubmissionClient(masterUrl)
    val httpUrl = masterUrl.replace("spark://", "http://")
    val v = RestSubmissionServer.PROTOCOL_VERSION
    val submitRequestPath = s"$httpUrl/$v/submissions/create"
    val killRequestPath = s"$httpUrl/$v/submissions/kill/anything"
    val statusRequestPath = s"$httpUrl/$v/submissions/status/anything"
    val json = constructSubmitRequest(masterUrl).toJson
    // server returns malformed response unwittingly 服务器返回的响应中的畸形
    // client should throw an appropriate exception to indicate server failure
    //客户端应该抛出一个适当的异常来指示服务器故障
    val conn1 = sendHttpRequest(submitRequestPath, "POST", json)
    intercept[SubmitRestProtocolException] { client.readResponse(conn1) }
    // server attempts to send invalid response, but fails internally on validation
    //服务器试图发送无效的响应,但在内部验证失败
    // client should receive an error response as server is able to recover
    //客户端应该接收到一个错误响应,因为服务器能够恢复
    val conn2 = sendHttpRequest(killRequestPath, "POST")
    val response2 = client.readResponse(conn2)
    getErrorResponse(response2)
    assert(conn2.getResponseCode === HttpServletResponse.SC_INTERNAL_SERVER_ERROR)
    // server explodes internally beyond recovery 服务器内部超越恢复
    // client should throw an appropriate exception to indicate server failure
    //客户端应该抛出一个适当的异常来指示服务器故障
    val conn3 = sendHttpRequest(statusRequestPath, "GET")
    intercept[SubmitRestProtocolException] { client.readResponse(conn3) } // empty response
    assert(conn3.getResponseCode === HttpServletResponse.SC_INTERNAL_SERVER_ERROR)
  }

  /* --------------------- *
   |     Helper methods    |
   * --------------------- */

  /** 
   *  Start a dummy server that responds to requests using the specified parameters. 
   *  使用指定的参数启动一个响应请求的虚拟服务器
   *  */
  private def startDummyServer(
      submitId: String = "fake-driver-id",
      submitMessage: String = "driver is submitted",
      killMessage: String = "driver is killed",
      state: DriverState = FINISHED,
      exception: Option[Exception] = None): String = {
    startServer(new DummyMaster(_, submitId, submitMessage, killMessage, state, exception))
  }

  /** 
   *  Start a smarter dummy server that keeps track of submitted driver states. 
   *  启动一个更聪明的虚拟服务器,跟踪提交的驱动程序状态
   *  */
  private def startSmartServer(): String = {
    startServer(new SmarterMaster(_))
  }

  /** 
   *  Start a dummy server that is faulty in many ways... 
   *  在许多方面启动一个错误的虚拟服务器…
   *  */
  private def startFaultyServer(): String = {
    //_占位符
    startServer(new DummyMaster(_), faulty = true)
  }

  /**
   * Start a [[StandaloneRestServer]] that communicates with the given endpoint.
   * 启动一个与给定的端点通信的
   * If `faulty` is true, start an [[FaultyStandaloneRestServer]] instead.
   * Return the master URL that corresponds to the address of this server.
   * 返回对应于该服务器的地址的主地址。
   */
  private def startServer(
      makeFakeMaster: RpcEnv => RpcEndpoint, faulty: Boolean = false): String = {
    val name = "test-standalone-rest-protocol"
    val conf = new SparkConf
    val localhost = Utils.localHostName()
    val securityManager = new SecurityManager(conf)
    val _rpcEnv = RpcEnv.create(name, localhost, 0, conf, securityManager)
    val fakeMasterRef = _rpcEnv.setupEndpoint("fake-master", makeFakeMaster(_rpcEnv))
    val _server =
      if (faulty) {
        new FaultyStandaloneRestServer(localhost, 0, conf, fakeMasterRef, "spark://fake:7077")
      } else {
        new StandaloneRestServer(localhost, 0, conf, fakeMasterRef, "spark://fake:7077")
      }
    val port = _server.start()
    // set these to clean them up after every test
    //设置这些清理后,每一个测试
    rpcEnv = Some(_rpcEnv)
    server = Some(_server)
    s"spark://$localhost:$port"
  }

  /** 
   *  Create a submit request with real parameters using Spark submit.
   *  创建一个提交请求与实际参数使用Spark提交
   *   */
  private def constructSubmitRequest(
      masterUrl: String,
      appArgs: Array[String] = Array.empty): CreateSubmissionRequest = {
    val mainClass = "main-class-not-used"
    val mainJar = "dummy-jar-not-used.jar"
    //数组初始化
    val commandLineArgs = Array(
      "--deploy-mode", "cluster",
      "--master", masterUrl,
      "--name", mainClass,
      "--class", mainClass,
      mainJar) ++ appArgs
    val args = new SparkSubmitArguments(commandLineArgs)
    //准备提交环境
    val (_, _, sparkProperties, _) = SparkSubmit.prepareSubmitEnvironment(args)
    //创建一个提交请求与实际参数使用Spark提交
    new RestSubmissionClient("spark://host:port").constructSubmitRequest(
      mainJar, mainClass, appArgs, sparkProperties.toMap, Map.empty)
  }

  /** 
   *  Return the response as a submit response, or fail with error otherwise. 
   *  作为提交响应返回响应,否则失败,否则为错误
   *  */
  private def getSubmitResponse(response: SubmitRestProtocolResponse): CreateSubmissionResponse = {
    response match {
      case s: CreateSubmissionResponse => s
      case e: ErrorResponse => fail(s"Server returned error: ${e.message}")
      case r => fail(s"Expected submit response. Actual: ${r.toJson}")
    }
  }

  /** 
   *  Return the response as a kill response, or fail with error otherwise. 
   *  返回响应作为一个杀死响应,否则失败与错误,否则
   *  */
  private def getKillResponse(response: SubmitRestProtocolResponse): KillSubmissionResponse = {
    response match {
      case k: KillSubmissionResponse => k
      case e: ErrorResponse => fail(s"Server returned error: ${e.message}")
      case r => fail(s"Expected kill response. Actual: ${r.toJson}")
    }
  }

  /** 
   *  Return the response as a status response, or fail with error otherwise. 
   *  将响应作为状态响应返回,否则会错误地失败。
   *  */
  private def getStatusResponse(response: SubmitRestProtocolResponse): SubmissionStatusResponse = {
    response match {
      case s: SubmissionStatusResponse => s
      case e: ErrorResponse => fail(s"Server returned error: ${e.message}")
      case r => fail(s"Expected status response. Actual: ${r.toJson}")
    }
  }

  /** 
   *  Return the response as an error response, or fail if the response was not an error. 
   *  返回响应作为错误响应,或失败,如果响应不是一个错误
   *  */
  private def getErrorResponse(response: SubmitRestProtocolResponse): ErrorResponse = {
    response match {
      case e: ErrorResponse => e
      case r => fail(s"Expected error response. Actual: ${r.toJson}")
    }
  }

  /**
   * Send an HTTP request to the given URL using the method and the body specified.
   * 使用方法和指定的主体向给定的URL发送HTTP请求。
   * Return the connection object.
   * 返回连接对象
   */
  private def sendHttpRequest(
      url: String,
      method: String,
      body: String = ""): HttpURLConnection = {
    val conn = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
    conn.setRequestMethod(method)
    //非空
    if (body.nonEmpty) {
      //URL 连接可用于输入和/或输出。如果打算使用 URL 连接进行输出，则将 DoOutput 标志设置为 true；如果不打算使用，则设置为 false。默认值为 false。
      conn.setDoOutput(true)
      //数据输出流
      val out = new DataOutputStream(conn.getOutputStream)
      out.write(body.getBytes(Charsets.UTF_8))
      out.close()
    }
    conn
  }

  /**
   * Send an HTTP request to the given URL using the method and the body specified.
   * 发送HTTP请求到指定的URL的使用方法和机构规定
   * Return a 2-tuple of the response message from the server and the response code.
   * 返回一个元组从服务器响应代码响应消息。
   */
  private def sendHttpRequestWithResponse(
      url: String,
      method: String,
      body: String = ""): (SubmitRestProtocolResponse, Int) = {
    val conn = sendHttpRequest(url, method, body)
    (new RestSubmissionClient("spark://host:port").readResponse(conn), conn.getResponseCode)
  }
}

/**
 * A mock standalone Master that responds with dummy messages.
 * 用虚拟消息响应的模拟独立主,
 * In all responses, the success parameter is always true.
 * 在所有的反应中,成功的参数始终是真实的
 */
private class DummyMaster(
    override val rpcEnv: RpcEnv,
    submitId: String = "fake-driver-id",
    submitMessage: String = "submitted",
    killMessage: String = "killed",
    state: DriverState = FINISHED,
    exception: Option[Exception] = None)
  extends RpcEndpoint {

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RequestSubmitDriver(driverDesc) =>
      context.reply(SubmitDriverResponse(self, success = true, Some(submitId), submitMessage))
    case RequestKillDriver(driverId) =>
      context.reply(KillDriverResponse(self, driverId, success = true, killMessage))
    case RequestDriverStatus(driverId) =>
      context.reply(DriverStatusResponse(found = true, Some(state), None, None, exception))
  }
}

/**
 * A mock standalone Master that keeps track of drivers that have been submitted.
 * 模拟已提交的驱动程序的模拟独立主机
 * If a driver is submitted, its state is immediately set to RUNNING.
  * 如果提交了一个驱动程序,它的状态立即设置为RUNNING
 * If an existing driver is killed, its state is immediately set to KILLED.
  * 如果现有的驱动程序被杀死,则其状态立即设置为KILLED
 * If an existing driver's status is requested, its state is returned in the response.
  * 如果请求现有driver的状态,则在响应中返回其状态
 * Submits are always successful while kills and status requests are successful only
 * if the driver was submitted in the past.
  * 提交总是成功的,只有当过去提交驱动程序时,杀死和状态请求才能成功
 */
private class SmarterMaster(override val rpcEnv: RpcEnv) extends ThreadSafeRpcEndpoint {
  private var counter: Int = 0
  private val submittedDrivers = new mutable.HashMap[String, DriverState]

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RequestSubmitDriver(driverDesc) =>
      val driverId = s"driver-$counter"
      submittedDrivers(driverId) = RUNNING
      counter += 1
      context.reply(SubmitDriverResponse(self, success = true, Some(driverId), "submitted"))

    case RequestKillDriver(driverId) =>
      val success = submittedDrivers.contains(driverId)
      if (success) {
        submittedDrivers(driverId) = KILLED
      }
      context.reply(KillDriverResponse(self, driverId, success, "killed"))

    case RequestDriverStatus(driverId) =>
      val found = submittedDrivers.contains(driverId)
      val state = submittedDrivers.get(driverId)
      context.reply(DriverStatusResponse(found, state, None, None, None))
  }
}

/**
 * A [[StandaloneRestServer]] that is faulty in many ways.
 *
 * When handling a submit request, the server returns a malformed JSON.
  * 处理提交请求时,服务器返回格式错误的JSON
 * When handling a kill request, the server returns an invalid JSON.
  * 处理kill请求时,服务器返回无效的JSON
 * When handling a status request, the server throws an internal exception.
  * 处理状态请求时,服务器会引发内部异常
 * The purpose of this class is to test that client handles these cases gracefully.
  * 这个类的目的是测试客户端优雅地处理这些情况
 */
private class FaultyStandaloneRestServer(
    host: String,
    requestedPort: Int,
    masterConf: SparkConf,
    masterEndpoint: RpcEndpointRef,
    masterUrl: String)
  extends RestSubmissionServer(host, requestedPort, masterConf) {

  protected override val submitRequestServlet = new MalformedSubmitServlet
  protected override val killRequestServlet = new InvalidKillServlet
  protected override val statusRequestServlet = new ExplodingStatusServlet

  /**
    * A faulty servlet that produces malformed responses.
    * 产生错误响应的错误servlet
    * */
  class MalformedSubmitServlet
    extends StandaloneSubmitRequestServlet(masterEndpoint, masterUrl, masterConf) {
    protected override def sendResponse(
        responseMessage: SubmitRestProtocolResponse,
        responseServlet: HttpServletResponse): Unit = {
      val badJson = responseMessage.toJson.drop(10).dropRight(20)
      responseServlet.getWriter.write(badJson)
    }
  }

  /**
    *  A faulty servlet that produces invalid responses.
    *  产生无效响应的错误servlet
    * */
  class InvalidKillServlet extends StandaloneKillRequestServlet(masterEndpoint, masterConf) {
    protected override def handleKill(submissionId: String): KillSubmissionResponse = {
      val k = super.handleKill(submissionId)
      k.submissionId = null
      k
    }
  }

  /**
    * A faulty status servlet that explodes.
    * 爆炸状态错误的servlet
    *  */
  class ExplodingStatusServlet extends StandaloneStatusRequestServlet(masterEndpoint, masterConf) {
    private def explode: Int = 1 / 0
    protected override def handleStatus(submissionId: String): SubmissionStatusResponse = {
      val s = super.handleStatus(submissionId)
      s.workerId = explode.toString
      s
    }
  }
}
