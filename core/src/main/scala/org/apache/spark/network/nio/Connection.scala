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

import java.net._
import java.nio._
import java.nio.channels._
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.LinkedList

import scala.collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.util.control.NonFatal

import org.apache.spark._
import org.apache.spark.network.sasl.{SparkSaslClient, SparkSaslServer}

private[nio]
abstract class Connection(val channel: SocketChannel, val selector: Selector,
    val socketRemoteConnectionManagerId: ConnectionManagerId, val connectionId: ConnectionId,
    val securityMgr: SecurityManager)
  extends Logging {

  var sparkSaslServer: SparkSaslServer = null
  var sparkSaslClient: SparkSaslClient = null

  def this(channel_ : SocketChannel, selector_ : Selector, id_ : ConnectionId,
      securityMgr_ : SecurityManager) = {
    this(channel_, selector_,
      ConnectionManagerId.fromSocketAddress(
        channel_.socket.getRemoteSocketAddress.asInstanceOf[InetSocketAddress]),
        id_, securityMgr_)
  }

  channel.configureBlocking(false)
  channel.socket.setTcpNoDelay(true)
  channel.socket.setReuseAddress(true)
  channel.socket.setKeepAlive(true)
  /* channel.socket.setReceiveBufferSize(32768) */

  @volatile private var closed = false
  var onCloseCallback: Connection => Unit = null
  val onExceptionCallbacks = new ConcurrentLinkedQueue[(Connection, Throwable) => Unit]
  var onKeyInterestChangeCallback: (Connection, Int) => Unit = null

  val remoteAddress = getRemoteAddress()

  def isSaslComplete(): Boolean

  def resetForceReregister(): Boolean

  // Read channels typically do not register for write and write does not for read
  // Now, we do have write registering for read too (temporarily), but this is to detect
  // channel close NOT to actually read/consume data on it !
  // How does this work if/when we move to SSL ?
  //读通道通常不注册写和写不读现在，我们也有写入注册读(暂时),但这是检测
  //通道关闭不实际读取/使用数据!如果/何时迁移到SSL,该如何工作?

  // What is the interest to register with selector for when we want this connection to be selected
  //当我们希望选择此连接时,请注意选择器的兴趣
  def registerInterest()

  // What is the interest to register with selector for when we want this connection to
  // be de-selected
  //当我们希望这个连接被取消选择时，请注意选择器的兴趣
  // Traditionally, 0 - but in our case, for example, for close-detection on SendingConnection hack,
  // it will be SelectionKey.OP_READ (until we fix it properly)
  //传统上，0 - 但是在我们的例子中，例如对于SendingConnection hack进行近似检测,
  // 它将是SelectionKey.OP_READ（直到我们正确地修复它）
  def unregisterInterest()

  // On receiving a read event, should we change the interest for this channel or not ?
  //收到阅读活动后，我们是否应该改变这个频道的兴趣？
  // Will be true for ReceivingConnection, false for SendingConnection.
  //对于ReceivingConnection，对于SendingConnection将为true。
  def changeInterestForRead(): Boolean

  private def disposeSasl() {
    if (sparkSaslServer != null) {
      sparkSaslServer.dispose()
    }

    if (sparkSaslClient != null) {
      sparkSaslClient.dispose()
    }
  }

  // On receiving a write event, should we change the interest for this channel or not ?
  //在收到写入事件时，是否应该改变这个频道的兴趣？
  // Will be false for ReceivingConnection, true for SendingConnection.
  //对于ReceivingConnection将为false，对于SendingConnection为true。
  // Actually, for now, should not get triggered for ReceivingConnection
  //实际上,现在不应该为ReceivingConnection触发
  def changeInterestForWrite(): Boolean

  def getRemoteConnectionManagerId(): ConnectionManagerId = {
    socketRemoteConnectionManagerId
  }

  def key(): SelectionKey = channel.keyFor(selector)

  def getRemoteAddress(): InetSocketAddress = {
    channel.socket.getRemoteSocketAddress().asInstanceOf[InetSocketAddress]
  }

  // Returns whether we have to register for further reads or not.
  //返回是否需要注册进一步阅读
  def read(): Boolean = {
    throw new UnsupportedOperationException(
      "Cannot read on connection of type " + this.getClass.toString)
  }

  // Returns whether we have to register for further writes or not.
  //返回是否需要注册进一步写入
  def write(): Boolean = {
    throw new UnsupportedOperationException(
      "Cannot write on connection of type " + this.getClass.toString)
  }

  def close() {
    closed = true
    val k = key()
    if (k != null) {
      k.cancel()
    }
    channel.close()
    disposeSasl()
    callOnCloseCallback()
  }

  protected def isClosed: Boolean = closed

  def onClose(callback: Connection => Unit) {
    onCloseCallback = callback
  }

  def onException(callback: (Connection, Throwable) => Unit) {
    onExceptionCallbacks.add(callback)
  }

  def onKeyInterestChange(callback: (Connection, Int) => Unit) {
    onKeyInterestChangeCallback = callback
  }

  def callOnExceptionCallbacks(e: Throwable) {
    onExceptionCallbacks foreach {
      callback =>
        try {
          callback(this, e)
        } catch {
          case NonFatal(e) => {
            logWarning("Ignored error in onExceptionCallback", e)
          }
        }
    }
  }

  def callOnCloseCallback() {
    if (onCloseCallback != null) {
      onCloseCallback(this)
    } else {
      logWarning("Connection to " + getRemoteConnectionManagerId() +
        " closed and OnExceptionCallback not registered")
    }

  }

  def changeConnectionKeyInterest(ops: Int) {
    if (onKeyInterestChangeCallback != null) {
      onKeyInterestChangeCallback(this, ops)
    } else {
      throw new Exception("OnKeyInterestChangeCallback not registered")
    }
  }
  //打印剩余的缓冲区
  def printRemainingBuffer(buffer: ByteBuffer) {
    val bytes = new Array[Byte](buffer.remaining)
    val curPosition = buffer.position
    buffer.get(bytes)
    bytes.foreach(x => print(x + " "))
    buffer.position(curPosition)
    print(" (" + bytes.length + ")")
  }
  //打印缓冲区
  def printBuffer(buffer: ByteBuffer, position: Int, length: Int) {
    val bytes = new Array[Byte](length)
    val curPosition = buffer.position
    buffer.position(position)
    buffer.get(bytes)
    bytes.foreach(x => print(x + " "))
    print(" (" + position + ", " + length + ")")
    buffer.position(curPosition)
  }
}


private[nio]
class SendingConnection(val address: InetSocketAddress, selector_ : Selector,
    remoteId_ : ConnectionManagerId, id_ : ConnectionId,
    securityMgr_ : SecurityManager)
  extends Connection(SocketChannel.open, selector_, remoteId_, id_, securityMgr_) {

  def isSaslComplete(): Boolean = {
    if (sparkSaslClient != null) sparkSaslClient.isComplete() else false
  }

  private class Outbox {
    val messages = new LinkedList[Message]()
    val defaultChunkSize = 65536
    var nextMessageToBeUsed = 0

    def addMessage(message: Message) {
      messages.synchronized {
        messages.add(message)
        logDebug("Added [" + message + "] to outbox for sending to " +
          "[" + getRemoteConnectionManagerId() + "]")
      }
    }

    def getChunk(): Option[MessageChunk] = {
      messages.synchronized {
        while (!messages.isEmpty) {
          /* nextMessageToBeUsed = nextMessageToBeUsed % messages.size */
          /* val message = messages(nextMessageToBeUsed) */

          val message = if (securityMgr.isAuthenticationEnabled() && !isSaslComplete()) {
            // only allow sending of security messages until sasl is complete
            //只允许发送安全消息,直到sasl完成
            var pos = 0
            var securityMsg: Message = null
            while (pos < messages.size() && securityMsg == null) {
              if (messages.get(pos).isSecurityNeg) {
                securityMsg = messages.remove(pos)
              }
              pos = pos + 1
            }
            // didn't find any security messages and auth isn't completed so return
            //没有找到任何安全消息和auth没有完成，所以返回
            if (securityMsg == null) return None
            securityMsg
          } else {
            messages.removeFirst()
          }

          val chunk = message.getChunkForSending(defaultChunkSize)
          if (chunk.isDefined) {
            messages.add(message)
            nextMessageToBeUsed = nextMessageToBeUsed + 1
            if (!message.started) {
              logDebug(
                "Starting to send [" + message + "] to [" + getRemoteConnectionManagerId() + "]")
              message.started = true
              message.startTime = System.currentTimeMillis
            }
            logTrace(
              "Sending chunk from [" + message + "] to [" + getRemoteConnectionManagerId() + "]")
            return chunk
          } else {
            message.finishTime = System.currentTimeMillis
            logDebug("Finished sending [" + message + "] to [" + getRemoteConnectionManagerId() +
              "] in "  + message.timeTaken )
          }
        }
      }
      None
    }
  }

  // outbox is used as a lock - ensure that it is always used as a leaf (since methods which
  // lock it are invoked in context of other locks)
  //发件箱用作锁 - 确保它始终用作叶子（因为锁定它的方法在其他锁的上下文中调用
  private val outbox = new Outbox()
  /*
    This is orthogonal to whether we have pending bytes to write or not - and satisfies a slightly
    different purpose. This flag is to see if we need to force reregister for write even when we
    do not have any pending bytes to write to socket.
    这与我们是否有待写入的字节是正交的 - 并且稍微满足不同的目的 这个标志是看我们是否需要强制重新注册，即使我们
     没有任何挂起的字节写入套接字。
    This can happen due to a race between adding pending buffers, and checking for existing of
    data as detailed in https://github.com/mesos/spark/pull/791
    这可能由于添加挂起缓冲区之间的竞争而发生，
   */
  private var needForceReregister = false

  val currentBuffers = new ArrayBuffer[ByteBuffer]()

  /* channel.socket.setSendBufferSize(256 * 1024) */

  override def getRemoteAddress(): InetSocketAddress = address

  val DEFAULT_INTEREST = SelectionKey.OP_READ

  override def registerInterest() {
    // Registering read too - does not really help in most cases, but for some
    //注册阅读 - 在大多数情况下并不真正有帮助，但对某些人而言
    // it does - so let us keep it for now.
    //它是 - 所以让我们现在保持它。
    changeConnectionKeyInterest(SelectionKey.OP_WRITE | DEFAULT_INTEREST)
  }

  override def unregisterInterest() {
    changeConnectionKeyInterest(DEFAULT_INTEREST)
  }

  def registerAfterAuth(): Unit = {
    outbox.synchronized {
      needForceReregister = true
    }
    if (channel.isConnected) {
      registerInterest()
    }
  }

  def send(message: Message) {
    outbox.synchronized {
      outbox.addMessage(message)
      needForceReregister = true
    }
    if (channel.isConnected) {
      registerInterest()
    }
  }

  // return previous value after resetting it.
  //在重置之后返回上一个值
  def resetForceReregister(): Boolean = {
    outbox.synchronized {
      val result = needForceReregister
      needForceReregister = false
      result
    }
  }

  // MUST be called within the selector loop
  //必须在选择器循环中调用
  def connect() {
    try {
      channel.register(selector, SelectionKey.OP_CONNECT)
      channel.connect(address)
      logInfo("Initiating connection to [" + address + "]")
    } catch {
      case e: Exception =>
        logError("Error connecting to " + address, e)
        callOnExceptionCallbacks(e)
    }
  }

  def finishConnect(force: Boolean): Boolean = {
    try {
      // Typically, this should finish immediately since it was triggered by a connect
      // selection - though need not necessarily always complete successfully.
      //通常，这应该立即完成,因为它被连接选择触发 - 尽管不一定总是成功完成
      val connected = channel.finishConnect
      if (!force && !connected) {
        logInfo(
          "finish connect failed [" + address + "], " + outbox.messages.size + " messages pending")
        return false
      }

      // Fallback to previous behavior - assume finishConnect completed
      // This will happen only when finishConnect failed for some repeated number of times
      // (10 or so)
      //回退到以前的行为 - 假设finishConnect完成这将发生只有当finishConnect失败了一些重复的次数(10左右)
      // Is highly unlikely unless there was an unclean close of socket, etc
      //非常不可能，除非有一个不洁的关闭插座等
      registerInterest()
      logInfo("Connected to [" + address + "], " + outbox.messages.size + " messages pending")
    } catch {
      case e: Exception => {
        logWarning("Error finishing connection to " + address, e)
        callOnExceptionCallbacks(e)
      }
    }
    true
  }

  override def write(): Boolean = {
    try {
      while (true) {
        if (currentBuffers.size == 0) {
          outbox.synchronized {
            outbox.getChunk() match {
              case Some(chunk) => {
                val buffers = chunk.buffers
                // If we have 'seen' pending messages, then reset flag - since we handle that as
                // normal registering of event (below)
                //如果我们看到'等待的消息,那么重置标志 - 因为我们处理这个事件的正常注册(下面)
                if (needForceReregister && buffers.exists(_.remaining() > 0)) resetForceReregister()

                currentBuffers ++= buffers
              }
              case None => {
                // changeConnectionKeyInterest(0)
                /* key.interestOps(0) */
                return false
              }
            }
          }
        }

        if (currentBuffers.size > 0) {
          val buffer = currentBuffers(0)
          val remainingBytes = buffer.remaining
          val writtenBytes = channel.write(buffer)
          if (buffer.remaining == 0) {
            currentBuffers -= buffer
          }
          if (writtenBytes < remainingBytes) {
            // re-register for write.
            return true
          }
        }
      }
    } catch {
      case e: Exception => {
        logWarning("Error writing in connection to " + getRemoteConnectionManagerId(), e)
        callOnExceptionCallbacks(e)
        close()
        return false
      }
    }
    // should not happen - to keep scala compiler happy
    true
  }

  // This is a hack to determine if remote socket was closed or not.
  //这是一个黑客来确定远程套接字是否关闭。
  // SendingConnection DOES NOT expect to receive any data - if it does, it is an error
  // For a bunch of cases, read will return -1 in case remote socket is closed : hence we
  // register for reads to determine that.
  //SendingConnection不希望收到任何数据 - 如果是,它是一个错误对于一些情况，读取将返回-1，
  // 以防远程套接字关闭：因此我们注册读取来确定。
  override def read(): Boolean = {
    // We don't expect the other side to send anything; so, we just read to detect an error or EOF.
    //我们不希望对方发送任何东西,所以我们只是读取来检测错误或EOF。
    try {
      //ByteBuffer.allocate在能够读和写之前,必须有一个缓冲区,用静态方法 allocate() 来分配缓冲区
      val length = channel.read(ByteBuffer.allocate(1))
      if (length == -1) { // EOF
        close()
      } else if (length > 0) {
        logWarning(
          "Unexpected data read from SendingConnection to " + getRemoteConnectionManagerId())
      }
    } catch {
      case e: Exception =>
        logError("Exception while reading SendingConnection to " + getRemoteConnectionManagerId(),
          e)
        callOnExceptionCallbacks(e)
        close()
    }

    false
  }

  override def changeInterestForRead(): Boolean = false

  override def changeInterestForWrite(): Boolean = ! isClosed
}


// Must be created within selector loop - else deadlock
//必须在选择器循环中创建 - 否则是死锁
private[spark] class ReceivingConnection(
    channel_ : SocketChannel,
    selector_ : Selector,
    id_ : ConnectionId,
    securityMgr_ : SecurityManager)
    extends Connection(channel_, selector_, id_, securityMgr_) {

  def isSaslComplete(): Boolean = {
    if (sparkSaslServer != null) sparkSaslServer.isComplete() else false
  }

  class Inbox() {
    val messages = new HashMap[Int, BufferMessage]()

    def getChunk(header: MessageChunkHeader): Option[MessageChunk] = {

      def createNewMessage: BufferMessage = {
        val newMessage = Message.create(header).asInstanceOf[BufferMessage]
        newMessage.started = true
        newMessage.startTime = System.currentTimeMillis
        newMessage.isSecurityNeg = header.securityNeg == 1
        logDebug(
          "Starting to receive [" + newMessage + "] from [" + getRemoteConnectionManagerId() + "]")
        messages += ((newMessage.id, newMessage))
        newMessage
      }

      val message = messages.getOrElseUpdate(header.id, createNewMessage)
      logTrace(
        "Receiving chunk of [" + message + "] from [" + getRemoteConnectionManagerId() + "]")
      message.getChunkForReceiving(header.chunkSize)
    }

    def getMessageForChunk(chunk: MessageChunk): Option[BufferMessage] = {
      messages.get(chunk.header.id)
    }

    def removeMessage(message: Message) {
      messages -= message.id
    }
  }

  @volatile private var inferredRemoteManagerId: ConnectionManagerId = null

  override def getRemoteConnectionManagerId(): ConnectionManagerId = {
    val currId = inferredRemoteManagerId
    if (currId != null) currId else super.getRemoteConnectionManagerId()
  }

  // The receiver's remote address is the local socket on remote side : which is NOT
  // the connection manager id of the receiver.
  //接收者的远程地址是远程端的本地套接字：不是接收方的连接管理员ID。
  // We infer that from the messages we receive on the receiver socket.
  //我们从接收器插座上收到的消息推断出。
  private def processConnectionManagerId(header: MessageChunkHeader) {
    val currId = inferredRemoteManagerId
    if (header.address == null || currId != null) return

    val managerId = ConnectionManagerId.fromSocketAddress(header.address)

    if (managerId != null) {
      inferredRemoteManagerId = managerId
    }
  }


  val inbox = new Inbox()
  //ByteBuffer.allocate在能够读和写之前,必须有一个缓冲区,用静态方法 allocate() 来分配缓冲区
  val headerBuffer: ByteBuffer = ByteBuffer.allocate(MessageChunkHeader.HEADER_SIZE)
  var onReceiveCallback: (Connection, Message) => Unit = null
  var currentChunk: MessageChunk = null

  channel.register(selector, SelectionKey.OP_READ)

  override def read(): Boolean = {
    try {
      while (true) {
        if (currentChunk == null) {
          val headerBytesRead = channel.read(headerBuffer)
          if (headerBytesRead == -1) {
            close()
            return false
          }
          if (headerBuffer.remaining > 0) {
            // re-register for read event ...
            return true
          }
          headerBuffer.flip
          if (headerBuffer.remaining != MessageChunkHeader.HEADER_SIZE) {
            throw new Exception(
              "Unexpected number of bytes (" + headerBuffer.remaining + ") in the header")
          }
          val header = MessageChunkHeader.create(headerBuffer)
          headerBuffer.clear()

          processConnectionManagerId(header)

          header.typ match {
            case Message.BUFFER_MESSAGE => {
              if (header.totalSize == 0) {
                if (onReceiveCallback != null) {
                  onReceiveCallback(this, Message.create(header))
                }
                currentChunk = null
                // re-register for read event ...
                return true
              } else {
                currentChunk = inbox.getChunk(header).orNull
              }
            }
            case _ => throw new Exception("Message of unknown type received")
          }
        }

        if (currentChunk == null) throw new Exception("No message chunk to receive data")

        val bytesRead = channel.read(currentChunk.buffer)
        if (bytesRead == 0) {
          // re-register for read event ...
          //重新注册读事件...
          return true
        } else if (bytesRead == -1) {
          close()
          return false
        }

        /* logDebug("Read " + bytesRead + " bytes for the buffer") */

        if (currentChunk.buffer.remaining == 0) {
          /* println("Filled buffer at " + System.currentTimeMillis) */
          val bufferMessage = inbox.getMessageForChunk(currentChunk).get
          if (bufferMessage.isCompletelyReceived) {
            bufferMessage.flip()
            bufferMessage.finishTime = System.currentTimeMillis
            logDebug("Finished receiving [" + bufferMessage + "] from " +
              "[" + getRemoteConnectionManagerId() + "] in " + bufferMessage.timeTaken)
            if (onReceiveCallback != null) {
              onReceiveCallback(this, bufferMessage)
            }
            inbox.removeMessage(bufferMessage)
          }
          currentChunk = null
        }
      }
    } catch {
      case e: Exception => {
        logWarning("Error reading from connection to " + getRemoteConnectionManagerId(), e)
        callOnExceptionCallbacks(e)
        close()
        return false
      }
    }
    // should not happen - to keep scala compiler happy
    //不应该发生 - 保持scala编译器的快乐
    true
  }

  def onReceive(callback: (Connection, Message) => Unit) {onReceiveCallback = callback}

  // override def changeInterestForRead(): Boolean = ! isClosed
  //覆盖def changeInterestForRead()：Boolean =!关闭
  override def changeInterestForRead(): Boolean = true

  override def changeInterestForWrite(): Boolean = {
    throw new IllegalStateException("Unexpected invocation right now")
  }

  override def registerInterest() {
    // Registering read too - does not really help in most cases, but for some
    // it does - so let us keep it for now.
    //注册阅读 - 在大多数情况下并不真正有帮助,但对于某些情况而言,我们现在就保留这一点
    changeConnectionKeyInterest(SelectionKey.OP_READ)
  }

  override def unregisterInterest() {
    changeConnectionKeyInterest(0)
  }

  // For read conn, always false.
  //对于读取conn，总是false。
  override def resetForceReregister(): Boolean = false
}
