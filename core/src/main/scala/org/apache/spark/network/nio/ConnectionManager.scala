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
import java.lang.ref.WeakReference
import java.net._
import java.nio._
import java.nio.channels._
import java.nio.channels.spi._
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{LinkedBlockingDeque, ThreadPoolExecutor, TimeUnit}

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, SynchronizedMap, SynchronizedQueue}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.language.postfixOps

import com.google.common.base.Charsets.UTF_8
import io.netty.util.{Timeout, TimerTask, HashedWheelTimer}

import org.apache.spark._
import org.apache.spark.network.sasl.{SparkSaslClient, SparkSaslServer}
import org.apache.spark.util.{ThreadUtils, Utils}

import scala.util.Try
import scala.util.control.NonFatal

private[nio] class ConnectionManager(
    port: Int,
    conf: SparkConf,
    securityManager: SecurityManager,
    name: String = "Connection manager")
  extends Logging {

  /**
   * Used by sendMessageReliably to track messages being sent.
   * 用于通过可靠地发送消息来跟踪发送的消息
   * @param message the message that was sent 发送的消息
   * @param connectionManagerId the connection manager that sent this message 发送此消息的连接管理器
   * @param completionHandler callback that's invoked when the send has completed or failed
   *			      发送已完成或失败时调用的回调函数
   */
  class MessageStatus(
      val message: Message,
      val connectionManagerId: ConnectionManagerId,
      completionHandler: Try[Message] => Unit) {

    def success(ackMessage: Message) {
      if (ackMessage == null) {
        failure(new NullPointerException)
      }
      else {
        completionHandler(scala.util.Success(ackMessage))
      }
    }

    def failWithoutAck() {
      completionHandler(scala.util.Failure(new IOException("Failed without being ACK'd")))
    }

    def failure(e: Throwable) {
      completionHandler(scala.util.Failure(e))
    }
  }

  private val selector = SelectorProvider.provider.openSelector()
  private val ackTimeoutMonitor =
    new HashedWheelTimer(ThreadUtils.namedThreadFactory("AckTimeoutMonitor"))

  private val ackTimeout =
    conf.getTimeAsSeconds("spark.core.connection.ack.wait.timeout",
      conf.get("spark.network.timeout", "120s"))

  // Get the thread counts from the Spark Configuration.
  // 从Spark配置获取线程计数
  // Even though the ThreadPoolExecutor constructor takes both a minimum and maximum value,
  // we only query for the minimum value because we are using LinkedBlockingDeque.
  // 虽然线程池的构造函数需要的最小和最大值,我们只查询的最小值,因为我们使用linkedblockingdeque
  // The JavaDoc for ThreadPoolExecutor points out that when using a LinkedBlockingDeque (which is
  // an unbounded queue) no more than corePoolSize threads will ever be created, so only the "min"
  // parameter is necessary.
  private val handlerThreadCount = conf.getInt("spark.core.connection.handler.threads.min", 20)
  private val ioThreadCount = conf.getInt("spark.core.connection.io.threads.min", 4)
  private val connectThreadCount = conf.getInt("spark.core.connection.connect.threads.min", 1)

  private val handleMessageExecutor = new ThreadPoolExecutor(
    handlerThreadCount,
    handlerThreadCount,
    conf.getInt("spark.core.connection.handler.threads.keepalive", 60), TimeUnit.SECONDS,
    new LinkedBlockingDeque[Runnable](),
    ThreadUtils.namedThreadFactory("handle-message-executor")) {

    override def afterExecute(r: Runnable, t: Throwable): Unit = {
      super.afterExecute(r, t)
      if (t != null && NonFatal(t)) {
        logError("Error in handleMessageExecutor is not handled properly", t)
      }
    }
  }

  private val handleReadWriteExecutor = new ThreadPoolExecutor(
    ioThreadCount,
    ioThreadCount,
    conf.getInt("spark.core.connection.io.threads.keepalive", 60), TimeUnit.SECONDS,
    new LinkedBlockingDeque[Runnable](),
    ThreadUtils.namedThreadFactory("handle-read-write-executor")) {

    override def afterExecute(r: Runnable, t: Throwable): Unit = {
      super.afterExecute(r, t)
      if (t != null && NonFatal(t)) {
        logError("Error in handleReadWriteExecutor is not handled properly", t)
      }
    }
  }

  // Use a different, yet smaller, thread pool - infrequently used with very short lived tasks :
  // which should be executed asap
  //使用一个不同的,但较小的,线程池-很少使用非常短的活动的任务,应尽快执行
  private val handleConnectExecutor = new ThreadPoolExecutor(
    connectThreadCount,
    connectThreadCount,
    conf.getInt("spark.core.connection.connect.threads.keepalive", 60), TimeUnit.SECONDS,
    new LinkedBlockingDeque[Runnable](),
    ThreadUtils.namedThreadFactory("handle-connect-executor")) {

    override def afterExecute(r: Runnable, t: Throwable): Unit = {
      super.afterExecute(r, t)
      if (t != null && NonFatal(t)) {
        logError("Error in handleConnectExecutor is not handled properly", t)
      }
    }
  }

  private val serverChannel = ServerSocketChannel.open()
  // used to track the SendingConnections waiting to do SASL negotiation
  //用于跟踪发送连接等待做SASL协商
  private val connectionsAwaitingSasl = new HashMap[ConnectionId, SendingConnection]
    with SynchronizedMap[ConnectionId, SendingConnection]
  private val connectionsByKey =
    new HashMap[SelectionKey, Connection] with SynchronizedMap[SelectionKey, Connection]
  private val connectionsById = new HashMap[ConnectionManagerId, SendingConnection]
    with SynchronizedMap[ConnectionManagerId, SendingConnection]
  // Tracks sent messages for which we are awaiting acknowledgements.  Entries are added to this
  // map when messages are sent and are removed when acknowledgement messages are received or when
  // acknowledgement timeouts expire
  private val messageStatuses = new HashMap[Int, MessageStatus]  // [MessageId, MessageStatus]
  private val keyInterestChangeRequests = new SynchronizedQueue[(SelectionKey, Int)]
  private val registerRequests = new SynchronizedQueue[SendingConnection]

  implicit val futureExecContext = ExecutionContext.fromExecutor(
    ThreadUtils.newDaemonCachedThreadPool("Connection manager future execution context"))

  @volatile
  private var onReceiveCallback: (BufferMessage, ConnectionManagerId) => Option[Message] = null

  private val authEnabled = securityManager.isAuthenticationEnabled()

  serverChannel.configureBlocking(false)
  serverChannel.socket.setReuseAddress(true)
  serverChannel.socket.setReceiveBufferSize(256 * 1024)

  private def startService(port: Int): (ServerSocketChannel, Int) = {
    serverChannel.socket.bind(new InetSocketAddress(port))
    (serverChannel, serverChannel.socket.getLocalPort)
  }
  Utils.startServiceOnPort[ServerSocketChannel](port, startService, conf, name)
  serverChannel.register(selector, SelectionKey.OP_ACCEPT)

  val id = new ConnectionManagerId(Utils.localHostName, serverChannel.socket.getLocalPort)
  logInfo("Bound socket to port " + serverChannel.socket.getLocalPort() + " with id = " + id)

  // used in combination with the ConnectionManagerId to create unique Connection ids
  // to be able to track asynchronous messages
  //使用与connectionmanagerid组合创造出唯一的连接IDS能够跟踪异步消息
  private val idCount: AtomicInteger = new AtomicInteger(1)

  private val writeRunnableStarted: HashSet[SelectionKey] = new HashSet[SelectionKey]()
  private val readRunnableStarted: HashSet[SelectionKey] = new HashSet[SelectionKey]()

  @volatile private var isActive = true
  private val selectorThread = new Thread("connection-manager-thread") {
    override def run(): Unit = ConnectionManager.this.run()
  }
  selectorThread.setDaemon(true)
  // start this thread last, since it invokes run(), which accesses members above
  //启动此线程上调用,因为它run(),访问以上成员
  selectorThread.start()

  private def triggerWrite(key: SelectionKey) {
    val conn = connectionsByKey.getOrElse(key, null)
    if (conn == null) return

    writeRunnableStarted.synchronized {
      // So that we do not trigger more write events while processing this one.
      // The write method will re-register when done.
      //因此,我们不会触发更多的写事件,而处理这一个,Write方法将重新登记完成
      if (conn.changeInterestForWrite()) conn.unregisterInterest()
      if (writeRunnableStarted.contains(key)) {
        // key.interestOps(key.interestOps() & ~ SelectionKey.OP_WRITE)
        return
      }

      writeRunnableStarted += key
    }
    handleReadWriteExecutor.execute(new Runnable {
      override def run() {
        try {
          var register: Boolean = false
          try {
            register = conn.write()
          } finally {
            writeRunnableStarted.synchronized {
              writeRunnableStarted -= key
              val needReregister = register || conn.resetForceReregister()
              if (needReregister && conn.changeInterestForWrite()) {
                conn.registerInterest()
              }
            }
          }
        } catch {
          case NonFatal(e) => {
            logError("Error when writing to " + conn.getRemoteConnectionManagerId(), e)
            conn.callOnExceptionCallbacks(e)
          }
        }
      }
    } )
  }


  private def triggerRead(key: SelectionKey) {
    val conn = connectionsByKey.getOrElse(key, null)
    if (conn == null) return

    readRunnableStarted.synchronized {
      // So that we do not trigger more read events while processing this one.
      //因此,我们不会触发更多的读取事件,而处理这一个
      // The read method will re-register when done.
      //读取方法将重新注册时完成
      if (conn.changeInterestForRead())conn.unregisterInterest()
      if (readRunnableStarted.contains(key)) {
        return
      }

      readRunnableStarted += key
    }
    handleReadWriteExecutor.execute(new Runnable {
      override def run() {
        try {
          var register: Boolean = false
          try {
            register = conn.read()
          } finally {
            readRunnableStarted.synchronized {
              readRunnableStarted -= key
              if (register && conn.changeInterestForRead()) {
                conn.registerInterest()
              }
            }
          }
        } catch {
          case NonFatal(e) => {
            logError("Error when reading from " + conn.getRemoteConnectionManagerId(), e)
            conn.callOnExceptionCallbacks(e)
          }
        }
      }
    } )
  }

  private def triggerConnect(key: SelectionKey) {
    val conn = connectionsByKey.getOrElse(key, null).asInstanceOf[SendingConnection]
    if (conn == null) return

    // prevent other events from being triggered 防止其他事件被触发
    // Since we are still trying to connect, we do not need to do the additional steps in
    //因为我们仍在试图连接,我们不需要做额外的步骤triggerwrite
    // triggerWrite
    conn.changeConnectionKeyInterest(0)

    handleConnectExecutor.execute(new Runnable {
      override def run() {
        try {
          var tries: Int = 10
          while (tries >= 0) {
            if (conn.finishConnect(false)) return
            // Sleep ?
            Thread.sleep(1)
            tries -= 1
          }

          // fallback to previous behavior : we should not really come here since this method was
          // triggered since channel became connectable : but at times, the first finishConnect need
          // not succeed : hence the loop to retry a few 'times'.
          //回到以前的行为：我们不应该真的来这里,因为这个方法被触发,因为通道可以连接：但有时,第一个finishConnect不需要成功：因此循环重试几次。
          conn.finishConnect(true)
        } catch {
          case NonFatal(e) => {
            logError("Error when finishConnect for " + conn.getRemoteConnectionManagerId(), e)
            conn.callOnExceptionCallbacks(e)
          }
        }
      }
    } )
  }

  // MUST be called within selector loop - else deadlock.
  //必须在选择器循环-否则死锁
  private def triggerForceCloseByException(key: SelectionKey, e: Exception) {
    try {
      key.interestOps(0)
    } catch {
      // ignore exceptions
      case e: Exception => logDebug("Ignoring exception", e)
    }

    val conn = connectionsByKey.getOrElse(key, null)
    if (conn == null) return

    // Pushing to connect threadpool
    //推动连接线程池
    handleConnectExecutor.execute(new Runnable {
      override def run() {
        try {
          conn.callOnExceptionCallbacks(e)
        } catch {
          // ignore exceptions
          case NonFatal(e) => logDebug("Ignoring exception", e)
        }
        try {
          conn.close()
        } catch {
          // ignore exceptions
          case NonFatal(e) => logDebug("Ignoring exception", e)
        }
      }
    })
  }


  def run() {
    try {
      while (isActive) {
        while (!registerRequests.isEmpty) {
          val conn: SendingConnection = registerRequests.dequeue()
          addListeners(conn)
          conn.connect()
          addConnection(conn)
        }

        while(!keyInterestChangeRequests.isEmpty) {
          val (key, ops) = keyInterestChangeRequests.dequeue()

          try {
            if (key.isValid) {
              val connection = connectionsByKey.getOrElse(key, null)
              if (connection != null) {
                val lastOps = key.interestOps()
                key.interestOps(ops)

                // hot loop - prevent materialization of string if trace not enabled.
		            //热度循环-防止字符串如果不启用跟踪。
                if (isTraceEnabled()) {
                  def intToOpStr(op: Int): String = {
                    val opStrs = ArrayBuffer[String]()
                    if ((op & SelectionKey.OP_READ) != 0) opStrs += "READ"
                    if ((op & SelectionKey.OP_WRITE) != 0) opStrs += "WRITE"
                    if ((op & SelectionKey.OP_CONNECT) != 0) opStrs += "CONNECT"
                    if ((op & SelectionKey.OP_ACCEPT) != 0) opStrs += "ACCEPT"
                    if (opStrs.size > 0) opStrs.reduceLeft(_ + " | " + _) else " "
                  }

                  logTrace("Changed key for connection to [" +
                    connection.getRemoteConnectionManagerId()  + "] changed from [" +
                      intToOpStr(lastOps) + "] to [" + intToOpStr(ops) + "]")
                }
              }
            } else {
              logInfo("Key not valid ? " + key)
              throw new CancelledKeyException()
            }
          } catch {
            case e: CancelledKeyException => {
              logInfo("key already cancelled ? " + key, e)
              triggerForceCloseByException(key, e)
            }
            case e: Exception => {
              logError("Exception processing key " + key, e)
              triggerForceCloseByException(key, e)
            }
          }
        }

        val selectedKeysCount =
          try {
            selector.select()
          } catch {
            // Explicitly only dealing with CancelledKeyException here since other exceptions
            // should be dealt with differently.
            //显然只处理CancelledKeyException,因为其他异常应该被不同的处理。
            case e: CancelledKeyException =>
              // Some keys within the selectors list are invalid/closed. clear them.
              //选择器列表中的某些键无效/关闭。 清除它们
              val allKeys = selector.keys().iterator()

              while (allKeys.hasNext) {
                val key = allKeys.next()
                try {
                  if (! key.isValid) {
                    logInfo("Key not valid ? " + key)
                    throw new CancelledKeyException()
                  }
                } catch {
                  case e: CancelledKeyException => {
                    logInfo("key already cancelled ? " + key, e)
                    triggerForceCloseByException(key, e)
                  }
                  case e: Exception => {
                    logError("Exception processing key " + key, e)
                    triggerForceCloseByException(key, e)
                  }
                }
              }
              0

            case e: ClosedSelectorException =>
              logDebug("Failed select() as selector is closed.", e)
              return
          }

        if (selectedKeysCount == 0) {
          logDebug("Selector selected " + selectedKeysCount + " of " + selector.keys.size +
            " keys")
        }
        if (selectorThread.isInterrupted) {
          logInfo("Selector thread was interrupted!")
          return
        }

        if (0 != selectedKeysCount) {
          val selectedKeys = selector.selectedKeys().iterator()
          while (selectedKeys.hasNext) {
            val key = selectedKeys.next
            selectedKeys.remove()
            try {
              if (key.isValid) {
                if (key.isAcceptable) {
                  acceptConnection(key)
                } else
                if (key.isConnectable) {
                  triggerConnect(key)
                } else
                if (key.isReadable) {
                  triggerRead(key)
                } else
                if (key.isWritable) {
                  triggerWrite(key)
                }
              } else {
                logInfo("Key not valid ? " + key)
                throw new CancelledKeyException()
              }
            } catch {
              // weird, but we saw this happening - even though key.isValid was true,
              // key.isAcceptable would throw CancelledKeyException.
              case e: CancelledKeyException => {
                logInfo("key already cancelled ? " + key, e)
                triggerForceCloseByException(key, e)
              }
              case e: Exception => {
                logError("Exception processing key " + key, e)
                triggerForceCloseByException(key, e)
              }
            }
          }
        }
      }
    } catch {
      case e: Exception => logError("Error in select loop", e)
    }
  }

  def acceptConnection(key: SelectionKey) {
    val serverChannel = key.channel.asInstanceOf[ServerSocketChannel]

    var newChannel = serverChannel.accept()

    // accept them all in a tight loop. non blocking accept with no processing, should be fine
    //接受他们所有在一个紧张的循环,非阻塞接受不加工,应该罚款
    while (newChannel != null) {
      try {
        val newConnectionId = new ConnectionId(id, idCount.getAndIncrement.intValue)
        val newConnection = new ReceivingConnection(newChannel, selector, newConnectionId,
          securityManager)
        newConnection.onReceive(receiveMessage)
        addListeners(newConnection)
        addConnection(newConnection)
        logInfo("Accepted connection from [" + newConnection.remoteAddress + "]")
      } catch {
        // might happen in case of issues with registering with selector
	//可能发生的情况下,与注册与选择器
        case e: Exception => logError("Error in accept loop", e)
      }

      newChannel = serverChannel.accept()
    }
  }

  private def addListeners(connection: Connection) {
    connection.onKeyInterestChange(changeConnectionKeyInterest)
    connection.onException(handleConnectionError)
    connection.onClose(removeConnection)
  }

  def addConnection(connection: Connection) {
    connectionsByKey += ((connection.key, connection))
  }

  def removeConnection(connection: Connection) {
    connectionsByKey -= connection.key

    try {
      connection match {
        case sendingConnection: SendingConnection =>
          val sendingConnectionManagerId = sendingConnection.getRemoteConnectionManagerId()
          logInfo("Removing SendingConnection to " + sendingConnectionManagerId)

          connectionsById -= sendingConnectionManagerId
          connectionsAwaitingSasl -= connection.connectionId

          messageStatuses.synchronized {
            messageStatuses.values.filter(_.connectionManagerId == sendingConnectionManagerId)
              .foreach(status => {
                logInfo("Notifying " + status)
                status.failWithoutAck()
              })

            messageStatuses.retain((i, status) => {
              status.connectionManagerId != sendingConnectionManagerId
            })
          }
        case receivingConnection: ReceivingConnection =>
          val remoteConnectionManagerId = receivingConnection.getRemoteConnectionManagerId()
          logInfo("Removing ReceivingConnection to " + remoteConnectionManagerId)

          val sendingConnectionOpt = connectionsById.get(remoteConnectionManagerId)
          if (!sendingConnectionOpt.isDefined) {
            logError(s"Corresponding SendingConnection to ${remoteConnectionManagerId} not found")
            return
          }

          val sendingConnection = sendingConnectionOpt.get
          connectionsById -= remoteConnectionManagerId
          sendingConnection.close()

          val sendingConnectionManagerId = sendingConnection.getRemoteConnectionManagerId()

          assert(sendingConnectionManagerId == remoteConnectionManagerId)

          messageStatuses.synchronized {
            for (s <- messageStatuses.values
                 if s.connectionManagerId == sendingConnectionManagerId) {
              logInfo("Notifying " + s)
              s.failWithoutAck()
            }

            messageStatuses.retain((i, status) => {
              status.connectionManagerId != sendingConnectionManagerId
            })
          }
        case _ => logError("Unsupported type of connection.")
      }
    } finally {
      // So that the selection keys can be removed.
      //使选择键可以被删除
      wakeupSelector()
    }
  }

  def handleConnectionError(connection: Connection, e: Throwable) {
    logInfo("Handling connection error on connection to " +
      connection.getRemoteConnectionManagerId())
    removeConnection(connection)
  }

  def changeConnectionKeyInterest(connection: Connection, ops: Int) {
    keyInterestChangeRequests += ((connection.key, ops))
    // so that registrations happen !
    //使注册发生
    wakeupSelector()
  }

  def receiveMessage(connection: Connection, message: Message) {
    val connectionManagerId = ConnectionManagerId.fromSocketAddress(message.senderAddress)
    logDebug("Received [" + message + "] from [" + connectionManagerId + "]")
    val runnable = new Runnable() {
      val creationTime = System.currentTimeMillis
      def run() {
        try {
          logDebug("Handler thread delay is " + (System.currentTimeMillis - creationTime) + " ms")
          handleMessage(connectionManagerId, message, connection)
          logDebug("Handling delay is " + (System.currentTimeMillis - creationTime) + " ms")
        } catch {
          case NonFatal(e) => {
            logError("Error when handling messages from " +
              connection.getRemoteConnectionManagerId(), e)
            connection.callOnExceptionCallbacks(e)
          }
        }
      }
    }
    handleMessageExecutor.execute(runnable)
    /* handleMessage(connection, message) */
  }

  private def handleClientAuthentication(
      waitingConn: SendingConnection,
      securityMsg: SecurityMessage,
      connectionId : ConnectionId) {
    if (waitingConn.isSaslComplete()) {
      logDebug("Client sasl completed for id: "  + waitingConn.connectionId)
      connectionsAwaitingSasl -= waitingConn.connectionId
      waitingConn.registerAfterAuth()
      wakeupSelector()
      return
    } else {
      var replyToken : Array[Byte] = null
      try {
        replyToken = waitingConn.sparkSaslClient.response(securityMsg.getToken)
        if (waitingConn.isSaslComplete()) {
          logDebug("Client sasl completed after evaluate for id: " + waitingConn.connectionId)
          connectionsAwaitingSasl -= waitingConn.connectionId
          waitingConn.registerAfterAuth()
          wakeupSelector()
          return
        }
        val securityMsgResp = SecurityMessage.fromResponse(replyToken,
          securityMsg.getConnectionId.toString)
        val message = securityMsgResp.toBufferMessage
        if (message == null) throw new IOException("Error creating security message")
        sendSecurityMessage(waitingConn.getRemoteConnectionManagerId(), message)
      } catch {
        case e: Exception =>
          logError("Error handling sasl client authentication", e)
          waitingConn.close()
          throw new IOException("Error evaluating sasl response: ", e)
      }
    }
  }

  private def handleServerAuthentication(
      connection: Connection,
      securityMsg: SecurityMessage,
      connectionId: ConnectionId) {
    if (!connection.isSaslComplete()) {
      logDebug("saslContext not established")
      var replyToken : Array[Byte] = null
      try {
        connection.synchronized {
          if (connection.sparkSaslServer == null) {
            logDebug("Creating sasl Server")
            connection.sparkSaslServer = new SparkSaslServer(conf.getAppId, securityManager, false)
          }
        }
        replyToken = connection.sparkSaslServer.response(securityMsg.getToken)
        if (connection.isSaslComplete()) {
          logDebug("Server sasl completed: " + connection.connectionId +
            " for: " + connectionId)
        } else {
          logDebug("Server sasl not completed: " + connection.connectionId +
            " for: " + connectionId)
        }
        if (replyToken != null) {
          val securityMsgResp = SecurityMessage.fromResponse(replyToken,
            securityMsg.getConnectionId)
          val message = securityMsgResp.toBufferMessage
          if (message == null) throw new Exception("Error creating security Message")
          sendSecurityMessage(connection.getRemoteConnectionManagerId(), message)
        }
      } catch {
        case e: Exception => {
          logError("Error in server auth negotiation: " + e)
          // It would probably be better to send an error message telling other side auth failed
          // but for now just close
 	        //这很可能是更好的发送错误消息告诉对方认证失败但现在关闭
          connection.close()
        }
      }
    } else {
      logDebug("connection already established for this connection id: " + connection.connectionId)
    }
  }


  private def handleAuthentication(conn: Connection, bufferMessage: BufferMessage): Boolean = {
    if (bufferMessage.isSecurityNeg) {
      logDebug("This is security neg message")

      // parse as SecurityMessage
      //解析作为安全消息
      val securityMsg = SecurityMessage.fromBufferMessage(bufferMessage)
      val connectionId = ConnectionId.createConnectionIdFromString(securityMsg.getConnectionId)

      connectionsAwaitingSasl.get(connectionId) match {
        case Some(waitingConn) => {
          // Client - this must be in response to us doing Send
	        //客户端-这必须是响应于我们做发送
          logDebug("Client handleAuth for id: " +  waitingConn.connectionId)
          handleClientAuthentication(waitingConn, securityMsg, connectionId)
        }
        case None => {
          // Server - someone sent us something and we haven't authenticated yet
	        //服务器-有人给我们发送的东西,我们还没有身份验证
          logDebug("Server handleAuth for id: " + connectionId)
          handleServerAuthentication(conn, securityMsg, connectionId)
        }
      }
      return true
    } else {
      if (!conn.isSaslComplete()) {
        // We could handle this better and tell the client we need to do authentication
        // negotiation, but for now just ignore them.
	      //我们可以更好地处理这个问题,并告诉客户端我们需要做身份验证协商,但现在只是忽略它们
        logError("message sent that is not security negotiation message on connection " +
                 "not authenticated yet, ignoring it!!")
        return true
      }
    }
    false
  }

  private def handleMessage(
      connectionManagerId: ConnectionManagerId,
      message: Message,
      connection: Connection) {
    logDebug("Handling [" + message + "] from [" + connectionManagerId + "]")
    message match {
      case bufferMessage: BufferMessage => {
        if (authEnabled) {
          val res = handleAuthentication(connection, bufferMessage)
          if (res) {
            // message was security negotiation so skip the rest
            //消息是安全协商，所以跳过其余的
            logDebug("After handleAuth result was true, returning")
            return
          }
        }
        if (bufferMessage.hasAckId()) {
          messageStatuses.synchronized {
            messageStatuses.get(bufferMessage.ackId) match {
              case Some(status) => {
                messageStatuses -= bufferMessage.ackId
                status.success(message)
              }
              case None => {
                /**
                 * We can fall down on this code because of following 2 cases
                 * 我们可以在这个代码上,因为以下2种情况
                 * (1) Invalid ack sent due to buggy code.
                 *     无效的ACK发送由于错误的代码
                 * (2) Late-arriving ack for a SendMessageStatus
		                  *	迟到的ACK发送消息的状态,避免不愿意迟到的应答
                 *     To avoid unwilling late-arriving ack
                 *     caused by long pause like GC, you can set
                 *     larger value than default to spark.core.connection.ack.wait.timeout
                  *     为了避免长时间停止像GC那样迟来的迟到,
                  *     您可以设置比默认值更大的值spark.core.connection.ack.wait.timeout
                 */
                logWarning(s"Could not find reference for received ack Message ${message.id}")
              }
            }
          }
        } else {
          var ackMessage : Option[Message] = None
          try {
            ackMessage = if (onReceiveCallback != null) {
              logDebug("Calling back")
              onReceiveCallback(bufferMessage, connectionManagerId)
            } else {
              logDebug("Not calling back as callback is null")
              None
            }

            if (ackMessage.isDefined) {
              if (!ackMessage.get.isInstanceOf[BufferMessage]) {
                logDebug("Response to " + bufferMessage + " is not a buffer message, it is of type "
                  + ackMessage.get.getClass)
              } else if (!ackMessage.get.asInstanceOf[BufferMessage].hasAckId) {
                logDebug("Response to " + bufferMessage + " does not have ack id set")
                ackMessage.get.asInstanceOf[BufferMessage].ackId = bufferMessage.id
              }
            }
          } catch {
            case e: Exception => {
              logError(s"Exception was thrown while processing message", e)
              ackMessage = Some(Message.createErrorMessage(e, bufferMessage.id))
            }
          } finally {
            sendMessage(connectionManagerId, ackMessage.getOrElse {
              Message.createBufferMessage(bufferMessage.id)
            })
          }
        }
      }
      case _ => throw new Exception("Unknown type message received")
    }
  }

  private def checkSendAuthFirst(connManagerId: ConnectionManagerId, conn: SendingConnection) {
    // see if we need to do sasl before writing
    //看看我们是否需要在写作之前做SASL
    // this should only be the first negotiation as the Client!!!
    //这应该是客户的第一次协商！！！
    if (!conn.isSaslComplete()) {
      conn.synchronized {
        if (conn.sparkSaslClient == null) {
          conn.sparkSaslClient = new SparkSaslClient(conf.getAppId, securityManager, false)
          var firstResponse: Array[Byte] = null
          try {
            firstResponse = conn.sparkSaslClient.firstToken()
            val securityMsg = SecurityMessage.fromResponse(firstResponse,
              conn.connectionId.toString())
            val message = securityMsg.toBufferMessage
            if (message == null) throw new Exception("Error creating security message")
            connectionsAwaitingSasl += ((conn.connectionId, conn))
            sendSecurityMessage(connManagerId, message)
            logDebug("adding connectionsAwaitingSasl id: " + conn.connectionId +
              " to: " + connManagerId)
          } catch {
            case e: Exception => {
              logError("Error getting first response from the SaslClient.", e)
              conn.close()
              throw new Exception("Error getting first response from the SaslClient")
            }
          }
        }
      }
    } else {
      logDebug("Sasl already established ")
    }
  }

  // allow us to add messages to the inbox for doing sasl negotiating
  //让我们添加消息收件箱做SASL协商
  private def sendSecurityMessage(connManagerId: ConnectionManagerId, message: Message) {
    def startNewConnection(): SendingConnection = {
      val inetSocketAddress = new InetSocketAddress(connManagerId.host, connManagerId.port)
      val newConnectionId = new ConnectionId(id, idCount.getAndIncrement.intValue)
      val newConnection = new SendingConnection(inetSocketAddress, selector, connManagerId,
        newConnectionId, securityManager)
      logInfo("creating new sending connection for security! " + newConnectionId )
      registerRequests.enqueue(newConnection)

      newConnection
    }
    // I removed the lookupKey stuff as part of merge ... should I re-add it ?
    // We did not find it useful in our test-env ...
    //我们没有发现它很有用,在我们的测试环境…
    // If we do re-add it, we should consistently use it everywhere I guess ?
    message.senderAddress = id.toSocketAddress()
    logTrace("Sending Security [" + message + "] to [" + connManagerId + "]")
    val connection = connectionsById.getOrElseUpdate(connManagerId, startNewConnection())

    // send security message until going connection has been authenticated
    //发送安全消息,直到连接已被验证
    connection.send(message)

    wakeupSelector()
  }

  private def sendMessage(connectionManagerId: ConnectionManagerId, message: Message) {
    def startNewConnection(): SendingConnection = {
      val inetSocketAddress = new InetSocketAddress(connectionManagerId.host,
        connectionManagerId.port)
      val newConnectionId = new ConnectionId(id, idCount.getAndIncrement.intValue)
      val newConnection = new SendingConnection(inetSocketAddress, selector, connectionManagerId,
        newConnectionId, securityManager)
      newConnection.onException {
        case (conn, e) => {
          logError("Exception while sending message.", e)
          reportSendingMessageFailure(message.id, e)
        }
      }
      logTrace("creating new sending connection: " + newConnectionId)
      registerRequests.enqueue(newConnection)

      newConnection
    }
    val connection = connectionsById.getOrElseUpdate(connectionManagerId, startNewConnection())

    message.senderAddress = id.toSocketAddress()
    logDebug("Before Sending [" + message + "] to [" + connectionManagerId + "]" + " " +
      "connectionid: "  + connection.connectionId)

    if (authEnabled) {
      try {
        checkSendAuthFirst(connectionManagerId, connection)
      } catch {
        case NonFatal(e) => {
          reportSendingMessageFailure(message.id, e)
        }
      }
    }
    logDebug("Sending [" + message + "] to [" + connectionManagerId + "]")
    connection.send(message)
    wakeupSelector()
  }

  private def reportSendingMessageFailure(messageId: Int, e: Throwable): Unit = {
    // need to tell sender it failed
    //需要告诉发送者它失败了
    messageStatuses.synchronized {
      val s = messageStatuses.get(messageId)
      s match {
        case Some(msgStatus) => {
          messageStatuses -= messageId
          logInfo("Notifying " + msgStatus.connectionManagerId)
          msgStatus.failure(e)
        }
        case None => {
          logError("no messageStatus for failed message id: " + messageId)
        }
      }
    }
  }

  private def wakeupSelector() {
    selector.wakeup()
  }

  /**
   * Send a message and block until an acknowledgment is received or an error occurs.
   * 发送消息和块,直到接收到或发生错误为止
   * @param connectionManagerId the message's destination 消息的目的地
   * @param message the message being sent 正在发送的消息
   * @return a Future that either returns the acknowledgment message or captures an exception.
   *	     要么返回确认消息或捕获异常的未来
   */
  def sendMessageReliably(connectionManagerId: ConnectionManagerId, message: Message)
      : Future[Message] = {
     //Future 表示一个可能还没有实际完成的异步任务的结果
     //针对这个结果可以添加 Callback 以便在任务执行成功或失败后做出对应的操作
     //而Promise 交由任务执行者,任务执行者通过 Promise 可以标记任务完成或者失败
    val promise = Promise[Message]()

    // It's important that the TimerTask doesn't capture a reference to `message`, which can cause
    // memory leaks since cancelled TimerTasks won't necessarily be garbage collected until the time
    // at which they would originally be scheduled to run.  Therefore, extract the message id
    // from outside of the TimerTask closure (see SPARK-4393 for more context).
    val messageId = message.id
    // Keep a weak reference to the promise so that the completed promise may be garbage-collected
    //保持promise弱的引用,以便完成的promise可能垃圾回收
    val promiseReference = new WeakReference(promise)
    val timeoutTask: TimerTask = new TimerTask {
      override def run(timeout: Timeout): Unit = {
        messageStatuses.synchronized {
          messageStatuses.remove(messageId).foreach { s =>
            val e = new IOException("sendMessageReliably failed because ack " +
              s"was not received within $ackTimeout sec")
            val p = promiseReference.get
            if (p != null) {
              // Attempt to fail the promise with a Timeout exception
	      //尝试以超时异常失败的promise
              if (!p.tryFailure(e)) {
                // If we reach here, then someone else has already signalled success or failure
                // on this promise, so log a warning:
		//如果我们到达这里,然后其他人已经暗示了这个promise的成功或失败,所以一个日志警告
                logError("Ignore error because promise is completed", e)
              }
            } else {
              // The WeakReference was empty, which should never happen because
	      //WeakReference是空的,这不应该发生
              // sendMessageReliably's caller should have a strong reference to promise.future;
	      //sendmessagereliably的调用者应该promise.future强引用
              logError("Promise was garbage collected; this should never happen!", e)
            }
          }
        }
      }
    }

    val timeoutTaskHandle = ackTimeoutMonitor.newTimeout(timeoutTask, ackTimeout, TimeUnit.SECONDS)

    val status = new MessageStatus(message, connectionManagerId, s => {
      timeoutTaskHandle.cancel()
      s match {
        case scala.util.Failure(e) =>
          // Indicates a failure where we either never sent or never got ACK'd
	  // 指示一个错误我们没有发送或没有ACK'd
          if (!promise.tryFailure(e)) {
            logWarning("Ignore error because promise is completed", e)
          }
        case scala.util.Success(ackMessage) =>
          if (ackMessage.hasError) {
            val errorMsgByteBuf = ackMessage.asInstanceOf[BufferMessage].buffers.head
            val errorMsgBytes = new Array[Byte](errorMsgByteBuf.limit())
            errorMsgByteBuf.get(errorMsgBytes)
            val errorMsg = new String(errorMsgBytes, UTF_8)
            val e = new IOException(
              s"sendMessageReliably failed with ACK that signalled a remote error: $errorMsg")
            if (!promise.tryFailure(e)) {
              logWarning("Ignore error because promise is completed", e)
            }
          } else {
            if (!promise.trySuccess(ackMessage)) {
              logWarning("Drop ackMessage because promise is completed")
            }
          }
      }
    })
    messageStatuses.synchronized {
      messageStatuses += ((message.id, status))
    }

    sendMessage(connectionManagerId, message)
    promise.future
  }

  def onReceiveMessage(callback: (Message, ConnectionManagerId) => Option[Message]) {
    onReceiveCallback = callback
  }

  def stop() {
    isActive = false
    ackTimeoutMonitor.stop()
    selector.close()
    selectorThread.interrupt()
    selectorThread.join()
    val connections = connectionsByKey.values
    connections.foreach(_.close())
    if (connectionsByKey.size != 0) {
      logWarning("All connections not cleaned up")
    }
    handleMessageExecutor.shutdown()
    handleReadWriteExecutor.shutdown()
    handleConnectExecutor.shutdown()
    logInfo("ConnectionManager stopped")
  }
}


private[spark] object ConnectionManager {
  import scala.concurrent.ExecutionContext.Implicits.global

  def main(args: Array[String]) {
    val conf = new SparkConf
    val manager = new ConnectionManager(9999, conf, new SecurityManager(conf))
    manager.onReceiveMessage((msg: Message, id: ConnectionManagerId) => {
      // scalastyle:off println
      println("Received [" + msg + "] from [" + id + "]")
      // scalastyle:on println
      None
    })

    /* testSequentialSending(manager) */
    /* System.gc() */

    /* testParallelSending(manager) */
    /* System.gc() */

    /* testParallelDecreasingSending(manager) */
    /* System.gc() */

    testContinuousSending(manager)
    System.gc()
  }

  // scalastyle:off println
  def testSequentialSending(manager: ConnectionManager) {
    println("--------------------------")
    println("Sequential Sending")
    println("--------------------------")
    val size = 10 * 1024 * 1024
    val count = 10
    //ByteBuffer.allocate在能够读和写之前,必须有一个缓冲区,用静态方法 allocate() 来分配缓冲区
    val buffer = ByteBuffer.allocate(size).put(Array.tabulate[Byte](size)(x => x.toByte))
    buffer.flip

    (0 until count).map(i => {
      val bufferMessage = Message.createBufferMessage(buffer.duplicate)
      //Await.result或者Await.ready会导致当前线程被阻塞,并等待actor通过它的应答来完成Future
      Await.result(manager.sendMessageReliably(manager.id, bufferMessage), Duration.Inf)
    })
    println("--------------------------")
    println()
  }

  def testParallelSending(manager: ConnectionManager) {
    println("--------------------------")
    println("Parallel Sending")
    println("--------------------------")
    val size = 10 * 1024 * 1024
    val count = 10
    //ByteBuffer.allocate在能够读和写之前,必须有一个缓冲区,用静态方法 allocate() 来分配缓冲区
    val buffer = ByteBuffer.allocate(size).put(Array.tabulate[Byte](size)(x => x.toByte))
    buffer.flip

    val startTime = System.currentTimeMillis
    (0 until count).map(i => {
      val bufferMessage = Message.createBufferMessage(buffer.duplicate)
      manager.sendMessageReliably(manager.id, bufferMessage)
    }).foreach(f => {
      f.onFailure {
        case e => println("Failed due to " + e)
      }
      Await.ready(f, 1 second)
    })
    val finishTime = System.currentTimeMillis

    val mb = size * count / 1024.0 / 1024.0
    val ms = finishTime - startTime
    val tput = mb * 1000.0 / ms
    println("--------------------------")
    println("Started at " + startTime + ", finished at " + finishTime)
    println("Sent " + count + " messages of size " + size + " in " + ms + " ms " +
      "(" + tput + " MB/s)")
    println("--------------------------")
    println()
  }

  def testParallelDecreasingSending(manager: ConnectionManager) {
    println("--------------------------")
    println("Parallel Decreasing Sending")
    println("--------------------------")
    val size = 10 * 1024 * 1024
    val count = 10
    val buffers = Array.tabulate(count) { i =>
      val bufferLen = size * (i + 1)
      val bufferContent = Array.tabulate[Byte](bufferLen)(x => x.toByte)
      //ByteBuffer.allocate在能够读和写之前,必须有一个缓冲区,用静态方法 allocate() 来分配缓冲区
      ByteBuffer.allocate(bufferLen).put(bufferContent)
    }
    buffers.foreach(_.flip)
    val mb = buffers.map(_.remaining).reduceLeft(_ + _) / 1024.0 / 1024.0

    val startTime = System.currentTimeMillis
    (0 until count).map(i => {
      val bufferMessage = Message.createBufferMessage(buffers(count - 1 - i).duplicate)
      manager.sendMessageReliably(manager.id, bufferMessage)
    }).foreach(f => {
      f.onFailure {
        case e => println("Failed due to " + e)
      }
      Await.ready(f, 1 second)
    })
    val finishTime = System.currentTimeMillis

    val ms = finishTime - startTime
    val tput = mb * 1000.0 / ms
    println("--------------------------")
    /* println("Started at " + startTime + ", finished at " + finishTime) */
    println("Sent " + mb + " MB in " + ms + " ms (" + tput + " MB/s)")
    println("--------------------------")
    println()
  }

  def testContinuousSending(manager: ConnectionManager) {
    println("--------------------------")
    println("Continuous Sending")
    println("--------------------------")
    val size = 10 * 1024 * 1024
    val count = 10
    //ByteBuffer.allocate在能够读和写之前,必须有一个缓冲区,用静态方法 allocate() 来分配缓冲区
    val buffer = ByteBuffer.allocate(size).put(Array.tabulate[Byte](size)(x => x.toByte))
    buffer.flip

    val startTime = System.currentTimeMillis
    while(true) {
      (0 until count).map(i => {
          val bufferMessage = Message.createBufferMessage(buffer.duplicate)
          manager.sendMessageReliably(manager.id, bufferMessage)
        }).foreach(f => {
          f.onFailure {
            case e => println("Failed due to " + e)
          }
          Await.ready(f, 1 second)
        })
      val finishTime = System.currentTimeMillis
      Thread.sleep(1000)
      val mb = size * count / 1024.0 / 1024.0
      val ms = finishTime - startTime
      val tput = mb * 1000.0 / ms
      println("Sent " + mb + " MB in " + ms + " ms (" + tput + " MB/s)")
      println("--------------------------")
      println()
    }
  }
  // scalastyle:on println
}
