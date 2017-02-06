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

package org.apache.spark.streaming.receiver

import java.nio.ByteBuffer
import java.util.concurrent.CountDownLatch

import scala.collection.mutable.ArrayBuffer
import scala.concurrent._
import scala.util.control.NonFatal

import org.apache.spark.{SparkEnv, Logging, SparkConf}
import org.apache.spark.storage.StreamBlockId
import org.apache.spark.util.{Utils, ThreadUtils}

/**
 * Abstract class that is responsible for supervising a Receiver in the worker.
 * It provides all the necessary interfaces for handling the data received by the receiver.
 * 主要负责监督Worker对Receiver的控制,receiver收到数据后,交给ReceiverSupervisor存储数据
 * 它提供了用于处理接收到的数据的所有必要的接口
 */
private[streaming] abstract class ReceiverSupervisor(
    receiver: Receiver[_],
    conf: SparkConf
  ) extends Logging {

  /** 
   *  Enumeration to identify current state of the Receiver
   *  枚举类型标识接收器的当前状态
   *  */
  object ReceiverState extends Enumeration {
    type CheckpointState = Value
    val Initialized, Started, Stopped = Value
  }
  import ReceiverState._

  // Attach the supervisor to the receiver
  //将监督连接到接收器
  receiver.attachSupervisor(this)

  private val futureExecutionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("receiver-supervisor-future", 128))

  /** 
   *  Receiver id
   *  接收器ID 
   *  */
  protected val streamId = receiver.streamId

  /** 
   *  Has the receiver been marked for stop.
   *  接收器已被标记为停止
   *   */
  private val stopLatch = new CountDownLatch(1)

  /** 
   *  Time between a receiver is stopped and started again 
   *  接收器之间的时间停止并重新开始
   *  */
  private val defaultRestartDelay = conf.getInt("spark.streaming.receiverRestartDelay", 2000)

  /** 
   *  The current maximum rate limit for this receiver.
   *  此接收器的当前限制最大速率 
   *  */
  private[streaming] def getCurrentRateLimit: Long = Long.MaxValue

  /** 
   *  Exception associated with the stopping of the receiver
   *  与接收器停止相关的异常 
   *  */
  @volatile protected var stoppingError: Throwable = null

  /** 
   *  State of the receiver 
   *  接收器的状态
   *  */
  @volatile private[streaming] var receiverState = Initialized

  /** 
   *  Push a single data item to backend data store.
   *  将单个数据项推到后端数据存储区 
   *  */
  def pushSingle(data: Any)

  /** 
   *  Store the bytes of received data as a data block into Spark's memory.
   *  将接收到的数据的字节存储为Spark的内存中的数据块 
   *  */
  def pushBytes(
      bytes: ByteBuffer,
      optionalMetadata: Option[Any],
      optionalBlockId: Option[StreamBlockId]
    )

  /** 
   *  Store a iterator of received data as a data block into Spark's memory. 
   *  将接收到的数据作为数据块存储到Spark的内存中
   *  */
  def pushIterator(
      iterator: Iterator[_],
      optionalMetadata: Option[Any],
      optionalBlockId: Option[StreamBlockId]
    )

  /**
   *  Store an ArrayBuffer of received data as a data block into Spark's memory. 
   *  将接收到的数据以ArrayBufferd数据块存储到Spark的内存中。
   *  */
  def pushArrayBuffer(
      arrayBuffer: ArrayBuffer[_],
      optionalMetadata: Option[Any],
      optionalBlockId: Option[StreamBlockId]
    )

  /**
   * Create a custom [[BlockGenerator]] that the receiver implementation can directly control
   * using their provided [[BlockGeneratorListener]].
   * 创建一个自定义的[BlockGenerator],接收器的实现可以直接使用其提供的[blockgeneratorlistener]控制
   * Note: Do not explicitly start or stop the `BlockGenerator`, the `ReceiverSupervisorImpl`
   * will take care of it.
   */
  def createBlockGenerator(blockGeneratorListener: BlockGeneratorListener): BlockGenerator

  /** 
   *  Report errors.
   *  报告错误 
   *  */
  def reportError(message: String, throwable: Throwable)

  /**
   * Called when supervisor is started.
   * 当监督者开始时调用
   * Note that this must be called before the receiver.onStart() is called to ensure
   * things like [[BlockGenerator]]s are started before the receiver starts sending data.
   */
  protected def onStart() { }

  /**
   * Called when supervisor is stopped.
   * 当监督者停止时调用
   * Note that this must be called after the receiver.onStop() is called to ensure
   * things like [[BlockGenerator]]s are cleaned up after the receiver stops sending data.
   */
  protected def onStop(message: String, error: Option[Throwable]) { }

  /** 
   *  Called when receiver is started. Return true if the driver accepts us 
   *  当接收器启动时调用,如果Driver接受我们的话,返回真的
   *  */
  protected def onReceiverStart(): Boolean

  /** 
   *  Called when receiver is stopped 
   *  当接收器停止时调用
   *  */
  protected def onReceiverStop(message: String, error: Option[Throwable]) { }

  /** 
   *  Start the supervisor 
   *  监督者启动
   *  */
  def start() {
    onStart()
    startReceiver()
  }

  /** 
   *  Mark the supervisor and the receiver for stopping 
   *  标记监督者和停止的接收器
   *  */
  def stop(message: String, error: Option[Throwable]) {
    stoppingError = error.orNull
    stopReceiver(message, error)
    onStop(message, error)
    futureExecutionContext.shutdownNow()
    stopLatch.countDown()
  }

  /** 
   *  Start receiver
   *  开始接收
   *   */
  def startReceiver(): Unit = synchronized {
    try {
      if (onReceiverStart()) {
        logInfo("Starting receiver")
        receiverState = Started
        receiver.onStart()
        logInfo("Called receiver onStart")
      } else {
        // The driver refused us
        //Driver拒绝了我们
        stop("Registered unsuccessfully because Driver refused to start receiver " + streamId, None)
      }
    } catch {
      case NonFatal(t) =>
        stop("Error starting receiver " + streamId, Some(t))
    }
  }

  /** 
   *  Stop receiver 
   *  停止接收
   *  */
  def stopReceiver(message: String, error: Option[Throwable]): Unit = synchronized {
    try {
      logInfo("Stopping receiver with message: " + message + ": " + error.getOrElse(""))
      receiverState match {
        case Initialized =>
          logWarning("Skip stopping receiver because it has not yet stared")
        case Started =>
          receiverState = Stopped
          receiver.onStop()
          logInfo("Called receiver onStop")
          onReceiverStop(message, error)
        case Stopped =>
          logWarning("Receiver has been stopped")
      }
    } catch {
      case NonFatal(t) =>
        logError("Error stopping receiver " + streamId + t.getStackTraceString)
    }
  }

  /** 
   *  Restart receiver with delay 
   *  延迟的重新启动接收器
   *  */
  def restartReceiver(message: String, error: Option[Throwable] = None) {
    restartReceiver(message, error, defaultRestartDelay)
  }

  /** 
   *  Restart receiver with delay
   *  延迟的重新启动接收器 
   *  */
  def restartReceiver(message: String, error: Option[Throwable], delay: Int) {
    Future {
      // This is a blocking action so we should use "futureExecutionContext" which is a cached
      // thread pool.
      //这是一个阻塞活动所以应该用“futureexecutioncontext”缓存的线程池
      logWarning("Restarting receiver with delay " + delay + " ms: " + message,
        error.getOrElse(null))
      stopReceiver("Restarting receiver with delay " + delay + "ms: " + message, error)
      logDebug("Sleeping for " + delay)
      Thread.sleep(delay)
      logInfo("Starting receiver again")
      startReceiver()
      logInfo("Receiver started again")
    }(futureExecutionContext)
  }

  /** 
   *  Check if receiver has been marked for starting 
   *  检查如果接收器已被标记为启动
   *  */
  def isReceiverStarted(): Boolean = {
    logDebug("state = " + receiverState)
    receiverState == Started
  }

  /** 
   *  Check if receiver has been marked for stopping
   *  检查如果接收器已被标记为停止
   *   */
  def isReceiverStopped(): Boolean = {
    logDebug("state = " + receiverState)
    receiverState == Stopped
  }


  /** 
   *  Wait the thread until the supervisor is stopped 
   *  等待线程,直到监督者停止
   *  */
  def awaitTermination() {
    logInfo("Waiting for receiver to be stopped")
    stopLatch.await()
    if (stoppingError != null) {
      logError("Stopped receiver with error: " + stoppingError)
    } else {
      logInfo("Stopped receiver without error")
    }
    if (stoppingError != null) {
      throw stoppingError
    }
  }
}
