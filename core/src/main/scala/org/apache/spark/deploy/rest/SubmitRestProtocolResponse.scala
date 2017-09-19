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

import java.lang.Boolean

/**
 * An abstract response sent from the server in the REST application submission protocol.
 * 在REST应用程序提交协议中从服务器发送的一个抽象响应
 */
private[rest] abstract class SubmitRestProtocolResponse extends SubmitRestProtocolMessage {
  var serverSparkVersion: String = null
  var success: Boolean = null
  var unknownFields: Array[String] = null
  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(serverSparkVersion, "serverSparkVersion")
  }
}

/**
 * A response to a [[CreateSubmissionRequest]] in the REST application submission protocol.
 */
private[spark] class CreateSubmissionResponse extends SubmitRestProtocolResponse {
  var submissionId: String = null
  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(success, "success")
  }
}

/**
 * A response to a kill request in the REST application submission protocol.
 * 在REST应用程序提交协议中对一个杀死请求的响应
 */
private[spark] class KillSubmissionResponse extends SubmitRestProtocolResponse {
  var submissionId: String = null
  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(submissionId, "submissionId")
    assertFieldIsSet(success, "success")
  }
}

/**
 * A response to a status request in the REST application submission protocol.
 * 针对REST应用程序提交协议中的状态请求的响应
 */
private[spark] class SubmissionStatusResponse extends SubmitRestProtocolResponse {
  var submissionId: String = null
  var driverState: String = null
  var workerId: String = null
  var workerHostPort: String = null

  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(submissionId, "submissionId")
    assertFieldIsSet(success, "success")
  }
}

/**
 * An error response message used in the REST application submission protocol.
 * 一个用于REST应用程序提交协议的错误响应消息
 */
private[rest] class ErrorResponse extends SubmitRestProtocolResponse {
  // The highest protocol version that the server knows about
  // This is set when the client specifies an unknown version
  var highestProtocolVersion: String = null
  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(message, "message")
  }
}
