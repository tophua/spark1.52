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

package org.apache.spark.network.nio

import java.nio.ByteBuffer

import scala.collection.mutable.{ArrayBuffer, StringBuilder}

import org.apache.spark._

/**
 * SecurityMessage is class that contains the connectionId and sasl token
 * used in SASL negotiation. SecurityMessage has routines for converting
 * it to and from a BufferMessage so that it can be sent by the ConnectionManager
 * and easily consumed by users when received.
 * The api was modeled after BlockMessage.
  *
  * SecurityMessage是包含connectionId和sasl令牌的类用于SASL协商。 SecurityMessage有用于转换的例程
  *它来自一个BufferMessage，以便它可以由ConnectionManager发送用户在收到时容易消费
  * api是在BlockMessage之后建模的
 *
 * The connectionId is the connectionId of the client side. Since
 * message passing is asynchronous and its possible for the server side (receiving)
 * to get multiple different types of messages on the same connection the connectionId
 * is used to know which connnection the security message is intended for.
 *
  *
  * connectionId是客户端的connectionId,以来消息传递是异步的,它可能为服务器端（接收）
  * 在同一个连接上获取多个不同类型的消息connectionId用于知道安全消息的连接。
  *
 * For instance, lets say we are node_0. We need to send data to node_1. The node_0 side
 * is acting as a client and connecting to node_1. SASL negotiation has to occur
 * between node_0 and node_1 before node_1 trusts node_0 so node_0 sends a security message.
 * node_1 receives the message from node_0 but before it can process it and send a response,
 * some thread on node_1 decides it needs to send data to node_0 so it connects to node_0
 * and sends a security message of its own to authenticate as a client. Now node_0 gets
 * the message and it needs to decide if this message is in response to it being a client
 * (from the first send) or if its just node_1 trying to connect to it to send data.  This
 * is where the connectionId field is used. node_0 can lookup the connectionId to see if
 * it is in response to it being a client or if its in response to someone sending other data.
  *
  * 例如，让我们说node_0。 我们需要向node_1发送数据。 node_0方面作为客户端并连接到node_1。 SASL谈判必须发生
  node_1和node_1之间，node_1信任node_0，因此node_0发送安全消息。node_1从node_0接收消息，但在处理它并发送响应之前，
  *node_1上的一些线程决定需要将数据发送到node_0，以便连接到node_0并发送自己的安全消息作为客户端认证。 现在node_0得到
  *消息，它需要决定这个消息是否响应它作为一个客户端（从第一次发送）或者如果它的只是node_1试图连接到它来发送数据。 这个
  *是使用connectionId字段的位置。 node_0可以查找connectionId以查看是否它是响应它作为客户端或响应某人发送其他数据。
 *
 * The format of a SecurityMessage as its sent is:
 *   - Length of the ConnectionId
 *   - ConnectionId
 *   - Length of the token
 *   - Token
 */
private[nio] class SecurityMessage extends Logging {

  private var connectionId: String = null
  private var token: Array[Byte] = null

  def set(byteArr: Array[Byte], newconnectionId: String) {
    if (byteArr == null) {
      token = new Array[Byte](0)
    } else {
      token = byteArr
    }
    connectionId = newconnectionId
  }

  /**
   * Read the given buffer and set the members of this class.
    * 读取给定的缓冲区并设置此类的成员
   */
  def set(buffer: ByteBuffer) {
    val idLength = buffer.getInt()
    val idBuilder = new StringBuilder(idLength)
    for (i <- 1 to idLength) {
        idBuilder += buffer.getChar()
    }
    connectionId = idBuilder.toString()

    val tokenLength = buffer.getInt()
    token = new Array[Byte](tokenLength)
    if (tokenLength > 0) {
      buffer.get(token, 0, tokenLength)
    }
  }

  def set(bufferMsg: BufferMessage) {
    val buffer = bufferMsg.buffers.apply(0)
    buffer.clear()
    set(buffer)
  }

  def getConnectionId: String = {
    return connectionId
  }

  def getToken: Array[Byte] = {
    return token
  }

  /**
   * Create a BufferMessage that can be sent by the ConnectionManager containing
   * the security information from this class.
    * 创建一个可以由ConnectionManager发送的BufferMessage.该消息包含此类的安全信息。
   * @return BufferMessage
   */
  def toBufferMessage: BufferMessage = {
    val buffers = new ArrayBuffer[ByteBuffer]()

    // 4 bytes for the length of the connectionId 连接ID的长度为4个字节
    // connectionId is of type char so multiple the length by 2 to get number of bytes
    // 4 bytes for the length of token
    //connectionId的类型为char，所以多个长度乘以2，获取字节数为4个字节的令牌长度
    // token is a byte buffer so just take the length 令牌是一个字节缓冲区，所以只需要长度
    //ByteBuffer.allocate在能够读和写之前,必须有一个缓冲区,用静态方法 allocate() 来分配缓冲区
    var buffer = ByteBuffer.allocate(4 + connectionId.length() * 2 + 4 + token.length)
    buffer.putInt(connectionId.length())
    connectionId.foreach((x: Char) => buffer.putChar(x))
    buffer.putInt(token.length)

    if (token.length > 0) {
      buffer.put(token)
    }
    buffer.flip()
    buffers += buffer

    var message = Message.createBufferMessage(buffers)
    logDebug("message total size is : " + message.size)
    message.isSecurityNeg = true
    return message
  }

  override def toString: String = {
    "SecurityMessage [connId= " + connectionId + ", Token = " + token + "]"
  }
}

private[nio] object SecurityMessage {

  /**
   * Convert the given BufferMessage to a SecurityMessage by parsing the contents
   * of the BufferMessage and populating the SecurityMessage fields.
    * 通过解析BufferMessage的内容并填充SecurityMessage字段,将给定的BufferMessage转换为SecurityMessage
   * @param bufferMessage is a BufferMessage that was received
   * @return new SecurityMessage
   */
  def fromBufferMessage(bufferMessage: BufferMessage): SecurityMessage = {
    val newSecurityMessage = new SecurityMessage()
    newSecurityMessage.set(bufferMessage)
    newSecurityMessage
  }

  /**
   * Create a SecurityMessage to send from a given saslResponse.
    * 创建一个从给定saslResponse发送的SecurityMessage
   * @param response is the response to a challenge from the SaslClient or Saslserver
   * @param connectionId the client connectionId we are negotiation authentication for
   * @return a new SecurityMessage
   */
  def fromResponse(response : Array[Byte], connectionId : String) : SecurityMessage = {
    val newSecurityMessage = new SecurityMessage()
    newSecurityMessage.set(response, connectionId)
    newSecurityMessage
  }
}
