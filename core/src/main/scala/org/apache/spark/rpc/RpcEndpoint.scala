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

import org.apache.spark.SparkException

/**
 * A factory class to create the [[RpcEnv]]. It must have an empty constructor so that it can be
 * created using Reflection.
 */
private[spark] trait RpcEnvFactory {

  def create(config: RpcEnvConfig): RpcEnv
}

/**
 * A trait that requires RpcEnv thread-safely sending messages to it.
 *
 * Thread-safety means processing of one message happens before processing of the next message by
 * the same [[ThreadSafeRpcEndpoint]]. In the other words, changes to internal fields of a
 * [[ThreadSafeRpcEndpoint]] are visible when processing the next message, and fields in the
 * [[ThreadSafeRpcEndpoint]] need not be volatile or equivalent.
 *
 * However, there is no guarantee that the same thread will be executing the same
 * [[ThreadSafeRpcEndpoint]] for different messages.
 * receive能并发操作，如果你想要receive是线程安全的，请使用ThreadSafeRpcEndpoint
 */
private[spark] trait ThreadSafeRpcEndpoint extends RpcEndpoint


/**
 * 
 * 凡是继承RpcEndpoint，都是一个消息通讯体，能接收消息
 * 当一个消息到来时，方法调用顺序为  onStart, receive, onStop
        它的生命周期为constructor -> onStart -> receive* -> onStop  .当然还有一些其他方法，都是间触发方法

  RpcEndpoint定义了由消息触发的一些函数，`onStart`, `receive` and `onStop`的调用是顺序发生的。
		 它的声明周期是constructor -> onStart -> receive* -> onStop。注意，`receive`能并发操作，
 		 如果你想要`receive`是线程安全的，请使用ThreadSafeRpcEndpoint，如果RpcEndpoint抛出错误，
  	 它的`onError`方法将会触发。它有51个实现子类，我们比较熟悉的是Master、Worker、ClientEndpoint等。
 * An end point for the RPC that defines what functions to trigger given a message.
 *
 * It is guaranteed that `onStart`, `receive` and `onStop` will be called in sequence.
 *
 * The life-cycle of an endpoint is:
 *
 * constructor -> onStart -> receive* -> onStop
 *
 * Note: `receive` can be called concurrently. If you want `receive` to be thread-safe, please use
 * [[ThreadSafeRpcEndpoint]]
 *
 * If any error is thrown from one of [[RpcEndpoint]] methods except `onError`, `onError` will be
 * invoked with the cause. If `onError` throws an error, [[RpcEnv]] will ignore it.
 */
private[spark] trait RpcEndpoint {

  /**
   * The [[RpcEnv]] that this [[RpcEndpoint]] is registered to.
   * RpcEnv)是一个RpcEndpoints用于处理消息的环境，它管理着整个RpcEndpoints的声明周期：
   * (1)根据name或uri注册endpoints
   * (2)管理各种消息的处理
   * (3)停止endpoints
   * 
   * RpcEnv处理从RpcEndpointRef或远程节点发送过来的消息，然后把响应消息给RpcEndpoint
   * 对于Rpc捕获到的异常消息，RpcEnv将会用RpcCallContext.sendFailure将失败消息发送给发送者，
   * 或者将没有发送者、‘NotSerializableException’等记录到日志中
   */
  val rpcEnv: RpcEnv

  /** 
   * 定义self无参方法,返回RpcEndpointRef类型,
   * The [[RpcEndpointRef]] of this [[RpcEndpoint]]. `self` will become valid when `onStart` is
   * called. And `self` will become `null` when `onStop` is called.
   *
   * Note: Because before `onStart`, [[RpcEndpoint]] has not yet been registered and there is not
   * valid [[RpcEndpointRef]] for it. So don't call `self` before `onStart` is called.
   */
  final def self: RpcEndpointRef = {
    require(rpcEnv != null, "rpcEnv has not been initialized")
    rpcEnv.endpointRef(this)
  }

  /**
   * 处理RpcEndpointRef.send或RpcCallContext.reply方法，如果收到不匹配的消息，将抛出SparkException
   * Process messages from [[RpcEndpointRef.send]] or [[RpcCallContext.reply)]]. If receiving a
   * unmatched message, [[SparkException]] will be thrown and sent to `onError`.
   */
  def receive: PartialFunction[Any, Unit] = {
    case _ => throw new SparkException(self + " does not implement 'receive'")
  }

  /**
   * 处理RpcEndpointRef.ask方法，默认如果不匹配消息，将抛出SparkException
   * Process messages from [[RpcEndpointRef.ask]]. If receiving a unmatched message,
   * [[SparkException]] will be thrown and sent to `onError`.
   */
  def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case _ => context.sendFailure(new SparkException(self + " won't reply anything"))
  }

  /**
   * 当处理消息发生异常时
   * Invoked when any exception is thrown during handling messages.
   */
  def onError(cause: Throwable): Unit = {
    // By default, throw e and let RpcEnv handle it
    throw cause
  }

  /**
   * 启动RpcEndpoint处理任何消息
   * Invoked before [[RpcEndpoint]] starts to handle any message.
   */
  def onStart(): Unit = {
    // By default, do nothing.
  }

  /**
   * 停止RpcEndpoint
   * Invoked when [[RpcEndpoint]] is stopping.
   */
  def onStop(): Unit = {
    // By default, do nothing.
  }

  /**
   * 当远程地址连接到当前的节点地址时触发
   * Invoked when `remoteAddress` is connected to the current node.
   */
  def onConnected(remoteAddress: RpcAddress): Unit = {
    // By default, do nothing.
  }

  /**
   * 当远程地址连接断开时触发
   * Invoked when `remoteAddress` is lost.
   */
  def onDisconnected(remoteAddress: RpcAddress): Unit = {
    // By default, do nothing.
  }

  /**
   * 当远程地址和当前节点的连接发生网络异常时触发
   * Invoked when some network error happens in the connection between the current node and
   * `remoteAddress`.
   */
  def onNetworkError(cause: Throwable, remoteAddress: RpcAddress): Unit = {
    // By default, do nothing.
  }

  /**
   * 停止RpcEndpoint
   * A convenient method to stop [[RpcEndpoint]].
   */
  final def stop(): Unit = {
    val _self = self
    if (_self != null) {
      rpcEnv.stop(_self)
    }
  }
}
