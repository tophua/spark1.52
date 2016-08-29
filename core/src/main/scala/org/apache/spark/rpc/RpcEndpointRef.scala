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

package org.apache.spark.rpc

import scala.concurrent.Future
import scala.reflect.ClassTag

import org.apache.spark.util.RpcUtils
import org.apache.spark.{SparkException, Logging, SparkConf}

/**
 * 用于发送消息
 * RpcEndpointRef 一个远程RpcEndpoint的引用，通过它可以给远程RpcEndpoint发送消息，可以是同步可以是异步
 * A reference for a remote [[RpcEndpoint]]. [[RpcEndpointRef]] is thread-safe(线程安全).
 * 
 * 1)处理RpcEndpointRef.send或RpcCallContext.reply方法，如果收到不匹配的消息，将抛出SparkException
 * 		def receive: PartialFunction[Any, Unit] = {
					case _ => throw new SparkException(self + " does not implement 'receive'")}
	 2)处理RpcEndpointRef.ask方法，如果不匹配消息，将抛出SparkException
			def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
					case _ => context.sendFailure(new SparkException(self + " won't reply anything"))}
	 3).当处理消息发生异常时
	 		def onError(cause: Throwable): Unit = {throw cause}
	 4)当远程地址连接到当前的节点地址时触发
	 		def onConnected(remoteAddress: RpcAddress): Unit = {}
	 5)当远程地址连接断开时触发def onDisconnected(remoteAddress: RpcAddress): Unit = {}
	 6)当远程地址和当前节点的连接发生网络异常时触发
			def onNetworkError(cause: Throwable, remoteAddress: RpcAddress): Unit = {}
 */
private[spark] abstract class RpcEndpointRef(@transient conf: SparkConf)
  extends Serializable with Logging {

  private[this] val maxRetries = RpcUtils.numRetries(conf)//最大重试次数
  private[this] val retryWaitMs = RpcUtils.retryWaitMs(conf)//重试等待3秒
  private[this] val defaultAskTimeout = RpcUtils.askRpcTimeout(conf)//返回默认RPC操作超时时间,默认120秒

  /**
   * return the address for the [[RpcEndpointRef]]
   */
  def address: RpcAddress

  def name: String

  /**
   * 发送单方面的异步消息
   * Sends a one-way asynchronous message. Fire-and-forget semantics.
   */
  def send(message: Any): Unit

  /**
   * 发送一个消息给RpcEndpoint.receiveAndReply并返回一个Future在指定的时间内接受响应，
   * Send a message to the corresponding(类似,相配) [[RpcEndpoint.receiveAndReply)]] and return a [[Future]] to
   * receive the reply within the specified timeout.
   * 本方法值请求一次
   * This method only sends the message once and never retries.
   */
  def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T]

  /**
   * Send a message to the corresponding [[RpcEndpoint.receiveAndReply)]] and return a [[Future]] to
   * receive the reply within a default timeout.
   *
   * This method only sends the message once and never(不再从试) retries.
   */
  def ask[T: ClassTag](message: Any): Future[T] = ask(message, defaultAskTimeout)

  /**
   * 发送消息给RpcEndpoint.receive并在默认的超时内得到结果，否则抛出SparkException，
   * 注意，本方法是一个阻塞操作可能消耗时间，所以不要早消息循环中调用它
   * Send a message to the corresponding(相当) [[RpcEndpoint]] and get its result within a default
   * timeout, or throw a SparkException if this fails even after the default number of retries.
   * The default `timeout` will be used in every trial of calling `sendWithReply`. Because this
   * method retries, the message handling in the receiver side should be idempotent.
   *注意，本方法是一个阻塞操作可能消耗时间，所以不要在消息循环中调用它
   * Note: this is a blocking action which may cost a lot of time,  so don't call it in an message
   * loop of [[RpcEndpoint]].
   *
   * @param message the message to send
   * @tparam T type of the reply message
   * @return the reply message from the corresponding [[RpcEndpoint]]
   */
  def askWithRetry[T: ClassTag](message: Any): T = askWithRetry(message, defaultAskTimeout)

  /**
   * 发送消息给RpcEndpoint.receive并在默认的超时内得到结果，否则抛出SparkException
   * Send a message to the corresponding [[RpcEndpoint.receive]] and get its result within a
   * specified timeout, throw a SparkException if this fails even after the specified number of
   * retries. `timeout` will be used in every trial of calling `sendWithReply`. Because this method
   * retries, the message handling in the receiver side should be idempotent.
   * 注意，本方法是一个阻塞操作可能消耗时间，所以不要在消息循环中调用它
   * Note: this is a blocking action which may cost a lot of time, so don't call it in an message
   * loop of [[RpcEndpoint]].
   *
   * @param message the message to send
   * @param timeout the timeout duration
   * @tparam T type of the reply message
   * @return the reply message from the corresponding [[RpcEndpoint]]
   */
  def askWithRetry[T: ClassTag](message: Any, timeout: RpcTimeout): T = {
    // TODO: Consider removing multiple attempts
    var attempts = 0 //重试次数
    var lastException: Exception = null
    //maxRetries最大重试次数
    while (attempts < maxRetries) {
      attempts += 1
      try {
        val future = ask[T](message, timeout)
        /**
         * 在规定时间内返回对象， Await是scala并发库中的一个对象，result在duration时间片内返回Awaitable的执行结果，
         * ready表示duration时间片内Awaitable的状态变成complete,两个方法都是阻塞的，Awaitable相当java中的future，
         * 当然scala也有future类，正是继承该类。它的伴生对象主要是配置文件中获取时间值然后生成该对象
         */
        val result = timeout.awaitResult(future)
        if (result == null) {
          throw new SparkException("RpcEndpoint returned null")
        }
        return result
      } catch {
        case ie: InterruptedException => throw ie
        case e: Exception =>
          lastException = e
          logWarning(s"Error sending message [message = $message] in $attempts attempts", e)
      }

      if (attempts < maxRetries) {
        Thread.sleep(retryWaitMs)
      }
    }

    throw new SparkException(
      s"Error sending message [message = $message]", lastException)
  }

}
