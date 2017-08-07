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
  * 一个工厂类创建[[RpcEnv]]它必须有一个空构造函数,以便可以使用Reflection创建它。
 */
private[spark] trait RpcEnvFactory {

  def create(config: RpcEnvConfig): RpcEnv
}

/**
 * A trait that requires RpcEnv thread-safely sending messages to it.
 * 需要线程安全RpcEnv特征向其发送消息
 * Thread-safety means processing of one message happens before processing of the next message by
 * the same [[ThreadSafeRpcEndpoint]]. In the other words, changes to internal fields of a
 * [[ThreadSafeRpcEndpoint]] are visible when processing the next message, and fields in the
 * [[ThreadSafeRpcEndpoint]] need not be volatile or equivalent.
  *
  * 线程安全意味着一个消息的处理发生在使用相同[[ThreadSafeRpcEndpoint]]处理下一个消息之前,
  * 换句话说，对[[ThreadSafeRpcEndpoint]]的内部字段的更改在处理下一条消息时是可见的'
  * 而[[ThreadSafeRpcEndpoint]]中的字段不需要是volatile或等同的。
 *
 * However, there is no guarantee that the same thread will be executing the same
 * [[ThreadSafeRpcEndpoint]] for different messages.
  * 但是,不能保证同一个线程将针对不同的消息执行相同的[[ThreadSafeRpcEndpoint]]
 * receive能并发操作,如果你想要receive是线程安全的,请使用ThreadSafeRpcEndpoint
 */
private[spark] trait ThreadSafeRpcEndpoint extends RpcEndpoint


/**
 * 凡是继承RpcEndpoint,都是一个消息通讯体,能接收消息
 * 当一个消息到来时,方法调用顺序为  onStart, receive, onStop
      它的生命周期为constructor -> onStart -> receive* -> onStop  .当然还有一些其他方法,都是间触发方法
      RpcEndpoint定义了由消息触发的一些函数,`onStart`, `receive` and `onStop`的调用是顺序发生的。
		 它的声明周期是constructor -> onStart -> receive* -> onStop。

 * An end point for the RPC that defines what functions to trigger given a message.
  * RPC的终点，定义在给定消息的情况下触发什么功能
 *
 * It is guaranteed that onStart, receive and onStop will be called in sequence.
 * 确保onStart，receive和onStop按顺序调用
 * The life-cycle of an endpoint is:
 * 终端的生命周期是：
 * constructor -> onStart -> receive* -> onStop
 *
 * Note: receive can be called concurrently. If you want receive to be thread-safe, please use
 * [[ThreadSafeRpcEndpoint]]
  *
 * 注意：receive可以同时调用。 如果你想要“receive”是线程安全的，请使用[[ThreadSafeRpcEndpoint]]
  *
 * If any error is thrown from one of [[RpcEndpoint]] methods except onError, onError will be
 * invoked with the cause. If onError throws an error, [[RpcEnv]] will ignore it.
  *
  * 如果任何错误从[[RpcEndpoint]]方法之一抛出,除了onError之外,onError将会被调用。 如果onError引发错误，[[RpcEnv]]将忽略它。
 */
private[spark] trait RpcEndpoint {

  /**
   * The [[RpcEnv]] that this [[RpcEndpoint]] is registered to.
   * RpcEnv)是一个RpcEndpoints用于处理消息的环境,它管理着整个RpcEndpoints的声明周期：
   * (1)根据name或uri注册endpoints
   * (2)管理各种消息的处理
   * (3)停止endpoints
   * 
   * RpcEnv处理从RpcEndpointRef或远程节点发送过来的消息,然后把响应消息给RpcEndpoint
   * 对于Rpc捕获到的异常消息,RpcEnv将会用RpcCallContext.sendFailure将失败消息发送给发送者,
   * 或者将没有发送者、‘NotSerializableException’等记录到日志中
   */
  val rpcEnv: RpcEnv

  /**
   * The [[RpcEndpointRef]] of this [[RpcEndpoint]]. `self` will become valid when `onStart` is
   * called. And `self` will become `null` when `onStop` is called.
    *
    * [[RpcEndpointRef]] [[RpcEndpoint]]],onStart是self将变得有效调用,当onStop被调用时,self将变为null。
   *
   * Note: Because before onStart, [[RpcEndpoint]] has not yet been registered and there is not
   * valid [[RpcEndpointRef]] for it. So don't call self before onStart is called.
    *
    * 注意：因为在onStart之前,[[RpcEndpoint]]尚未注册,并且没有有效的[[RpcEndpointRef]]所以在onStart被调用之前不要调用self
   */
  final def self: RpcEndpointRef = {
    require(rpcEnv != null, "rpcEnv has not been initialized")
    rpcEnv.endpointRef(this)
  }

  /**
   * Process messages from [[RpcEndpointRef.send]] or [[RpcCallContext.reply)]]. If receiving a
   * unmatched message, [[SparkException]] will be thrown and sent to `onError`.
    *
    * 从[[RpcEndpointRef.send]]或[[RpcCallContext.reply]]处理消息],
    * 如果接收到不匹配的消息，[[SparkException]]将被抛出并发送到`onError`。
   */
  def receive: PartialFunction[Any, Unit] = {
    case _ => throw new SparkException(self + " does not implement 'receive'")
  }

  /**
   * Process messages from [[RpcEndpointRef.ask]]. If receiving a unmatched message,
   * [[SparkException]] will be thrown and sent to `onError`.
    * 从[[RpcEndpointRef.ask]]处理消息,如果接收到不匹配的消息,[[SparkException]]将被抛出并发送到onError
   */
  def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case _ => context.sendFailure(new SparkException(self + " won't reply anything"))
  }

  /**
   * 当处理消息发生异常时
   * Invoked when any exception is thrown during handling messages.
    * 在处理消息时抛出任何异常时调用。
   */
  def onError(cause: Throwable): Unit = {
    // By default, throw e and let RpcEnv handle it
    //默认情况下,扔e并让RpcEnv处理它
    throw cause
  }

  /**
   * 启动RpcEndpoint处理任何消息
   * Invoked before [[RpcEndpoint]] starts to handle any message.
   */
  def onStart(): Unit = {
    // By default, do nothing.
    //默认情况下,什么也不做
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
    * 当“remoteAddress”连接到当前节点时调用。
   */
  def onConnected(remoteAddress: RpcAddress): Unit = {
    // By default, do nothing.
  }

  /**
   * 当远程地址连接断开时触发
   * Invoked when `remoteAddress` is lost.
    * remoteAddress 丢失时调用
   */
  def onDisconnected(remoteAddress: RpcAddress): Unit = {
    // By default, do nothing.
    //默认情况下,什么也不做
  }

  /**
   * Invoked when some network error happens in the connection between the current node and
   * `remoteAddress`.
    * 当前节点和“remoteAddress”之间的连接发生某些网络错误时调用。
   */
  def onNetworkError(cause: Throwable, remoteAddress: RpcAddress): Unit = {
    // By default, do nothing.
    //默认情况下,什么也不做
  }

  /**
   * 停止RpcEndpoint
   * A convenient method to stop [[RpcEndpoint]].
    * 一个方便的方法来停止[[RpcEndpoint]]。
   */
  final def stop(): Unit = {
    val _self = self
    if (_self != null) {
      rpcEnv.stop(_self)
    }
  }
}
