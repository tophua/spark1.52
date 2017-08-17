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

import java.io.IOException
import java.nio._

import scala.concurrent.duration._
import scala.concurrent.{Await, TimeoutException}
import scala.language.postfixOps

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.util.Utils

/**
  * Test the ConnectionManager with various security settings.
  * 测试各种安全设置的连接管理器
  */
class ConnectionManagerSuite extends SparkFunSuite {

  test("security default off") {//安全默认关闭
    val conf = new SparkConf
    val securityManager = new SecurityManager(conf)
    val manager = new ConnectionManager(0, conf, securityManager)
    var receivedMessage = false
    manager.onReceiveMessage((msg: Message, id: ConnectionManagerId) => {
      receivedMessage = true
      None
    })
    //10485760字节是10MB
    val size = 10 * 1024 * 1024
    //ByteBuffer.allocate在能够读和写之前,必须有一个缓冲区,用静态方法 allocate() 来分配缓冲区
    val buffer = ByteBuffer.allocate(size).put(Array.tabulate[Byte](size)(x => x.toByte))
    buffer.flip

    val bufferMessage = Message.createBufferMessage(buffer.duplicate)
    //Await.result或者Await.ready会导致当前线程被阻塞,并等待actor通过它的应答来完成Future
    Await.result(manager.sendMessageReliably(manager.id, bufferMessage), 10 seconds)

    assert(receivedMessage == true)

    manager.stop()
  }

  test("security on same password") {//安全相同密码
    val conf = new SparkConf
     //是否启用内部身份验证
    conf.set("spark.authenticate", "true")
    //设置组件之间进行身份验证的密钥
    conf.set("spark.authenticate.secret", "good")
    conf.set("spark.app.id", "app-id")
    val securityManager = new SecurityManager(conf)
    val manager = new ConnectionManager(0, conf, securityManager)
    var numReceivedMessages = 0

    manager.onReceiveMessage((msg: Message, id: ConnectionManagerId) => {
      numReceivedMessages += 1
      None
    })
    val managerServer = new ConnectionManager(0, conf, securityManager)
    var numReceivedServerMessages = 0
    managerServer.onReceiveMessage((msg: Message, id: ConnectionManagerId) => {
      numReceivedServerMessages += 1
      None
    })
    //10485760字节是10MB
    val size = 10 * 1024 * 1024
    val count = 10
    //ByteBuffer.allocate在能够读和写之前,必须有一个缓冲区,用静态方法 allocate() 来分配缓冲区
    val buffer = ByteBuffer.allocate(size).put(Array.tabulate[Byte](size)(x => x.toByte))
    buffer.flip

    (0 until count).map(i => {
      val bufferMessage = Message.createBufferMessage(buffer.duplicate)
      Await.result(manager.sendMessageReliably(managerServer.id, bufferMessage), 10 seconds)
    })

    assert(numReceivedServerMessages == 10)
    assert(numReceivedMessages == 0)

    manager.stop()
    managerServer.stop()
  }

  test("security mismatch password") {//不匹配的密码
    val conf = new SparkConf
     //是否启用内部身份验证
    conf.set("spark.authenticate", "true")
    conf.set("spark.app.id", "app-id")
    //设置组件之间进行身份验证的密钥
    conf.set("spark.authenticate.secret", "good")
    val securityManager = new SecurityManager(conf)
    val manager = new ConnectionManager(0, conf, securityManager)
    var numReceivedMessages = 0

    manager.onReceiveMessage((msg: Message, id: ConnectionManagerId) => {
      numReceivedMessages += 1
      None
    })
  //设置组件之间进行身份验证的密钥
    val badconf = conf.clone.set("spark.authenticate.secret", "bad")
    val badsecurityManager = new SecurityManager(badconf)
    val managerServer = new ConnectionManager(0, badconf, badsecurityManager)
    var numReceivedServerMessages = 0

    managerServer.onReceiveMessage((msg: Message, id: ConnectionManagerId) => {
      numReceivedServerMessages += 1
      None
    })
    //10485760字节是10MB
    val size = 10 * 1024 * 1024
    //ByteBuffer.allocate在能够读和写之前,必须有一个缓冲区,用静态方法 allocate() 来分配缓冲区
    val buffer = ByteBuffer.allocate(size).put(Array.tabulate[Byte](size)(x => x.toByte))
    buffer.flip
    val bufferMessage = Message.createBufferMessage(buffer.duplicate)
    // Expect managerServer to close connection, which we'll report as an error:
    //预期managerServer关闭连接,我们将作为一个错误报告：
    intercept[IOException] {
    //Await.result或者Await.ready会导致当前线程被阻塞,并等待actor通过它的应答来完成Future
      Await.result(manager.sendMessageReliably(managerServer.id, bufferMessage), 10 seconds)
    }

    assert(numReceivedServerMessages == 0)
    assert(numReceivedMessages == 0)

    manager.stop()
    managerServer.stop()
  }

  test("security mismatch auth off") {//不匹配的安全认证
    val conf = new SparkConf
     //是否启用内部身份验证
    conf.set("spark.authenticate", "false")
    //设置组件之间进行身份验证的密钥
    conf.set("spark.authenticate.secret", "good")
    val securityManager = new SecurityManager(conf)
    val manager = new ConnectionManager(0, conf, securityManager)
    var numReceivedMessages = 0

    manager.onReceiveMessage((msg: Message, id: ConnectionManagerId) => {
      numReceivedMessages += 1
      None
    })

    val badconf = new SparkConf
     //是否启用内部身份验证
    badconf.set("spark.authenticate", "true")
    //设置组件之间进行身份验证的密钥
    badconf.set("spark.authenticate.secret", "good")
    val badsecurityManager = new SecurityManager(badconf)
    val managerServer = new ConnectionManager(0, badconf, badsecurityManager)
    var numReceivedServerMessages = 0
    managerServer.onReceiveMessage((msg: Message, id: ConnectionManagerId) => {
      numReceivedServerMessages += 1
      None
    })
    //10485760字节是10MB
    val size = 10 * 1024 * 1024
    //ByteBuffer.allocate在能够读和写之前,必须有一个缓冲区,用静态方法 allocate() 来分配缓冲区
    val buffer = ByteBuffer.allocate(size).put(Array.tabulate[Byte](size)(x => x.toByte))
    buffer.flip
    val bufferMessage = Message.createBufferMessage(buffer.duplicate)
    (0 until 1).map(i => {
      val bufferMessage = Message.createBufferMessage(buffer.duplicate)
      manager.sendMessageReliably(managerServer.id, bufferMessage)
    }).foreach(f => {
      try {
        val g = Await.result(f, 1 second)
        assert(false)
      } catch {
        case i: IOException =>
          assert(true)
        case e: TimeoutException => {
          // we should timeout here since the client can't do the negotiation
          //我们应该在这里暂停,因为客户端无法进行协商
          assert(true)
        }
      }
    })

    assert(numReceivedServerMessages == 0)
    assert(numReceivedMessages == 0)
    manager.stop()
    managerServer.stop()
  }

  test("security auth off") {//安全认证
    val conf = new SparkConf
     //是否启用内部身份验证
    conf.set("spark.authenticate", "false")
    val securityManager = new SecurityManager(conf)
    val manager = new ConnectionManager(0, conf, securityManager)
    var numReceivedMessages = 0

    manager.onReceiveMessage((msg: Message, id: ConnectionManagerId) => {
      numReceivedMessages += 1
      None
    })

    val badconf = new SparkConf
    badconf.set("spark.authenticate", "false")
    val badsecurityManager = new SecurityManager(badconf)
    val managerServer = new ConnectionManager(0, badconf, badsecurityManager)
    var numReceivedServerMessages = 0

    managerServer.onReceiveMessage((msg: Message, id: ConnectionManagerId) => {
      numReceivedServerMessages += 1
      None
    })
    //10485760字节是10MB
    val size = 10 * 1024 * 1024
    //ByteBuffer.allocate在能够读和写之前,必须有一个缓冲区,用静态方法 allocate() 来分配缓冲区
    val buffer = ByteBuffer.allocate(size).put(Array.tabulate[Byte](size)(x => x.toByte))
    buffer.flip
    val bufferMessage = Message.createBufferMessage(buffer.duplicate)
    (0 until 10).map(i => {
      val bufferMessage = Message.createBufferMessage(buffer.duplicate)
      manager.sendMessageReliably(managerServer.id, bufferMessage)
    }).foreach(f => {
      try {
        val g = Await.result(f, 1 second)
      } catch {
        case e: Exception => {
          assert(false)
        }
      }
    })
    assert(numReceivedServerMessages == 10)
    assert(numReceivedMessages == 0)

    manager.stop()
    managerServer.stop()
  }

  test("Ack error message") {//ACK错误消息
    val conf = new SparkConf
     //是否启用内部身份验证
    conf.set("spark.authenticate", "false")
    val securityManager = new SecurityManager(conf)
    val manager = new ConnectionManager(0, conf, securityManager)
    val managerServer = new ConnectionManager(0, conf, securityManager)
    managerServer.onReceiveMessage((msg: Message, id: ConnectionManagerId) => {
      throw new Exception("Custom exception text")
    })
    //10485760字节是10MB
    val size = 10 * 1024 * 1024
    //ByteBuffer.allocate在能够读和写之前,必须有一个缓冲区,用静态方法 allocate() 来分配缓冲区
    val buffer = ByteBuffer.allocate(size).put(Array.tabulate[Byte](size)(x => x.toByte))
    buffer.flip
    val bufferMessage = Message.createBufferMessage(buffer)

    val future = manager.sendMessageReliably(managerServer.id, bufferMessage)

    val exception = intercept[IOException] {
      Await.result(future, 1 second)
    }
    assert(Utils.exceptionString(exception).contains("Custom exception text"))

    manager.stop()
    managerServer.stop()

  }

  test("sendMessageReliably timeout") {//sendMessageReliably超时
    val clientConf = new SparkConf
    clientConf.set("spark.authenticate", "false")
    val ackTimeoutS = 30
    clientConf.set("spark.core.connection.ack.wait.timeout", s"${ackTimeoutS}s")

    val clientSecurityManager = new SecurityManager(clientConf)
    val manager = new ConnectionManager(0, clientConf, clientSecurityManager)

    val serverConf = new SparkConf
    serverConf.set("spark.authenticate", "false")
    val serverSecurityManager = new SecurityManager(serverConf)
    val managerServer = new ConnectionManager(0, serverConf, serverSecurityManager)
    managerServer.onReceiveMessage((msg: Message, id: ConnectionManagerId) => {
      // sleep 60 sec > ack timeout for simulating server slow down or hang up
      //睡眠60秒> ack超时模拟服务器减慢或挂断
      Thread.sleep(ackTimeoutS * 3 * 1000)
      None
    })
    //10485760字节是10MB
    val size = 10 * 1024 * 1024
    //ByteBuffer.allocate在能够读和写之前,必须有一个缓冲区,用静态方法 allocate() 来分配缓冲区
    val buffer = ByteBuffer.allocate(size).put(Array.tabulate[Byte](size)(x => x.toByte))
    buffer.flip
    val bufferMessage = Message.createBufferMessage(buffer.duplicate)

    val future = manager.sendMessageReliably(managerServer.id, bufferMessage)

    // Future should throw IOException in 30 sec.
    // Otherwise TimeoutExcepton is thrown from Await.result.
    // We expect TimeoutException is not thrown.
    //Future应该在30秒内抛出IOException,否则TimeoutExcepton将从Await.result抛出,
    // 我们预计不会抛出TimeoutException。
    intercept[IOException] {
      Await.result(future, (ackTimeoutS * 2) second)
    }

    manager.stop()
    managerServer.stop()
  }

}

