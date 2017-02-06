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

import java.net.URI
import java.util.concurrent.TimeoutException

import scala.concurrent.{Awaitable, Await, Future}
import scala.concurrent.duration._
import scala.language.postfixOps

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.util.{RpcUtils, Utils}


/**
 * A RpcEnv implementation must have a [[RpcEnvFactory]] implementation with an empty constructor
 * so that it can be created via Reflection.
 *  RpcEnv是一个RpcEndpoints用于处理消息的环境,管理着整个RpcEndpoint的生命周期
   * 1)根据name或uri注册endpoints
   * 2)管理各种消息的处理
   * 3)停止endpoints
   * 4)RpcEnv必须通过工厂类create创建
   * 
   * RpcEnv里面有setupEndpoint方法, RpcEndpoint和RpcEndpointRef向RpcEnv进行注册。
		客户端通过RpcEndpointRef发消息,首先通过RpcEnv来处理这个消息,找到这个消息具体发给谁,	然后路由给RpcEndpoint实体。
		现在有两种方式,一种是AkkaRpcEnv,另一种是NettyRpcEnv。Spark默认使用更加高效的NettyRpcEnv。
          对于Rpc捕获到的异常消息,RpcEnv将会用RpcCallContext.sendFailure将失败消息发送给发送者,
          或者将没有发送者,‘NotSerializableException’等记录到日志中。
          同时,
    RpcEnv也提供了根据name或uri获取RpcEndpointRef的方法。
 */
private[spark] object RpcEnv {

  private def getRpcEnvFactory(conf: SparkConf): RpcEnvFactory = {
    // Add more RpcEnv implementations here
    val rpcEnvNames = Map("akka" -> "org.apache.spark.rpc.akka.AkkaRpcEnvFactory")
    val rpcEnvName = conf.get("spark.rpc", "akka")
    val rpcEnvFactoryClassName = rpcEnvNames.getOrElse(rpcEnvName.toLowerCase, rpcEnvName)
    //默认AkkaRpcEnvFactory工厂类
    Utils.classForName(rpcEnvFactoryClassName).newInstance().asInstanceOf[RpcEnvFactory]
  }

  def create(
      name: String,
      host: String,
      port: Int,
      conf: SparkConf,
      securityManager: SecurityManager): RpcEnv = {
    // Using Reflection to create the RpcEnv to avoid to depend on Akka directly
    val config = RpcEnvConfig(conf, name, host, port, securityManager)
    getRpcEnvFactory(conf).create(config)
  }

}
/**
 * RpcEnv处理从RpcEndpointRef或远程节点发送过来的消息,然后把响应消息给RpcEndpoint
        对于Rpc捕获到的异常消息,RpcEnv将会用RpcCallContext.sendFailure将失败消息发送给发送者,
        或者将没有发送者、‘NotSerializableException’等记录到日志中
 * An RPC environment. [[RpcEndpoint]]s need to register itself with a name to [[RpcEnv]] to
 * receives messages. Then [[RpcEnv]] will process messages sent from [[RpcEndpointRef]] or remote
 * nodes, and deliver them to corresponding [[RpcEndpoint]]s. For uncaught exceptions caught by
 * [[RpcEnv]], [[RpcEnv]] will use [[RpcCallContext.sendFailure]] to send exceptions back to the
 * sender, or logging them if no such sender or `NotSerializableException`.
 *
 * [[RpcEnv]] also provides some methods to retrieve [[RpcEndpointRef]]s given name or uri.
 */
private[spark] abstract class RpcEnv(conf: SparkConf) {
//返回默认RPC远程查找节点超时时间 ,默认120s秒
  private[spark] val defaultLookupTimeout = RpcUtils.lookupRpcTimeout(conf)

  /**
   * 根据RpcEndpoint返回RpcEndpointRef,具体实现在RpcEndpoint.self方法中,如果RpcEndpointRef不存在,将返回null
   * Return RpcEndpointRef of the registered [[RpcEndpoint]]. Will be used to implement
   * [[RpcEndpoint.self]]. Return `null` if the corresponding [[RpcEndpointRef]] does not exist.
   * endpoint(服务端点)
   */
  private[rpc] def endpointRef(endpoint: RpcEndpoint): RpcEndpointRef

  /**  
   * Return the address that [[RpcEnv]] is listening to.
   * 返回一个正在侦听的地址
   */
  def address: RpcAddress

  /**
   * RpcEnv是一个RpcEndpoints用于处理消息的环境,管理着整个RpcEndpoint的生命周期
   * 1)根据name或uri注册endpoints
   * 2)管理各种消息的处理
   * 3)停止endpoints
   * 
   * RpcEnv里面有setupEndpoint方法, RpcEndpoint和RpcEndpointRef向RpcEnv进行注册
   * 
   * 根据name注册RpcEndpoint到RpcEnv中并返回它的一个引用RpcEndpointRef
   * Register a [[RpcEndpoint]] with a name and return its [[RpcEndpointRef]]. [[RpcEnv]] does not
   * guarantee thread-safety.
   */
  def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef

  /**
   * 获取RpcEndpointRef的方法
       (1)通过url获取RpcEndpointRef
                         通过url异步获取RpcEndpointRef
   * Retrieve the [[RpcEndpointRef]] represented by `uri` asynchronously.
   */
  def asyncSetupEndpointRefByURI(uri: String): Future[RpcEndpointRef]

  /**   
   * 根据url同步获取RpcEndpointRef,这是一个阻塞操作
   * Retrieve the [[RpcEndpointRef]] represented by `uri`. This is a blocking action.
   */
  def setupEndpointRefByURI(uri: String): RpcEndpointRef = {
    defaultLookupTimeout.awaitResult(asyncSetupEndpointRefByURI(uri))
  }

  /**
   * 异步获取RpcEndpointRef 
   * 根据systemName、address、endpointName获取RpcEndpointRef,其实是将三者拼接为uri,
   * 根据uri获取异步获取
   * Retrieve the [[RpcEndpointRef]] represented by `systemName`, `address` and `endpointName`
   * asynchronously.
   */
  def asyncSetupEndpointRef(
      systemName: String, address: RpcAddress, endpointName: String): Future[RpcEndpointRef] = {
    asyncSetupEndpointRefByURI(uriOf(systemName, address, endpointName))
  }

  /**
   * 同步获取RpcEndpointRef
   * 根据systemName、address、endpointName获取RpcEndpointRef,其实是将三者拼接为uri
   * Retrieve the [[RpcEndpointRef]] represented by `systemName`, `address` and `endpointName`.
   * This is a blocking action.
   */
  def setupEndpointRef(
      systemName: String, address: RpcAddress, endpointName: String): RpcEndpointRef = {
    setupEndpointRefByURI(uriOf(systemName, address, endpointName))
  }

  /**
   * 根据RpcEndpointRef停止RpcEndpoint
   * Stop [[RpcEndpoint]] specified by `endpoint`.
   */
  def stop(endpoint: RpcEndpointRef): Unit

  /**
   * Shutdown this [[RpcEnv]] asynchronously. If need to make sure [[RpcEnv]] exits successfully,
   * 异步关闭RPC环境,如果需要成功确定退出
   * call [[awaitTermination()]] straight after [[shutdown()]].
   */
  def shutdown(): Unit

  /**
   * Wait until [[RpcEnv]] exits.
   * 等待直到RpcEnv退出
   * TODO do we need a timeout parameter?
   */
  def awaitTermination(): Unit

  /**
   * Create a URI used to create a [[RpcEndpointRef]]. Use this one to create the URI instead of
   * creating it manually because different [[RpcEnv]] may have different formats.
   * 
   */
  def uriOf(systemName: String, address: RpcAddress, endpointName: String): String

  /**
   * RpcEndpointRef需要RpcEnv来反序列化,所以当反序列化RpcEndpointRefs的任何object时,应该通过该方法来操作
   * [[RpcEndpointRef]] cannot be deserialized without [[RpcEnv]]. So when deserializing any object
   * that contains [[RpcEndpointRef]]s, the deserialization codes should be wrapped by this method.
   */
  def deserialize[T](deserializationAction: () => T): T
}


private[spark] case class RpcEnvConfig(
    conf: SparkConf,
    name: String,
    host: String,
    port: Int,
    securityManager: SecurityManager)


/**
 * Represents a host and port.
 * 代表一个主机和端口
 */
private[spark] case class RpcAddress(host: String, port: Int) {
  // TODO do we need to add the type of RpcEnv in the address?

  val hostPort: String = host + ":" + port

  override val toString: String = hostPort

  def toSparkURL: String = "spark://" + hostPort
}


private[spark] object RpcAddress {

  /**
   * Return the [[RpcAddress]] represented by `uri`.
   * 返回一个RpcAddress代表uri
   */
  def fromURI(uri: URI): RpcAddress = {
    RpcAddress(uri.getHost, uri.getPort)
  }

  /**
   * Return the [[RpcAddress]] represented by `uri`.
   */
  def fromURIString(uri: String): RpcAddress = {
    fromURI(new java.net.URI(uri))
  }
/**
 * 根据sparkUrl提取主机名称及端口
 */
  def fromSparkURL(sparkUrl: String): RpcAddress = {
    val (host, port) = Utils.extractHostPortFromSparkUrl(sparkUrl)
    RpcAddress(host, port)
  }
}


/**
 * An exception thrown if RpcTimeout modifies a [[TimeoutException]].
 */
private[rpc] class RpcTimeoutException(message: String, cause: TimeoutException)
  extends TimeoutException(message) { initCause(cause) }


/**
 * Associates a timeout with a description so that a when a TimeoutException occurs, additional
 * context about the timeout can be amended to the exception message.
 * @param duration timeout duration in seconds
 * @param timeoutProp the configuration property that controls this timeout
 */
private[spark] class RpcTimeout(val duration: FiniteDuration, val timeoutProp: String)
  extends Serializable {

  /** Amends the standard message of TimeoutException to include the description */
  private def createRpcTimeoutException(te: TimeoutException): RpcTimeoutException = {
    new RpcTimeoutException(te.getMessage() + ". This timeout is controlled by " + timeoutProp, te)
  }

  /**
   * PartialFunction to match a TimeoutException and add the timeout description to the message
   * PartialFunction 匹配一个TimeoutException和添加超时的描述信息
   * @note This can be used in the recover callback of a Future to add to a TimeoutException
   * Example:
   *    val timeout = new RpcTimeout(5 millis, "short timeout")
   *    Future(throw new TimeoutException).recover(timeout.addMessageIfTimeout)
   */
  def addMessageIfTimeout[T]: PartialFunction[Throwable, T] = {
    // The exception has already been converted to a RpcTimeoutException so just raise it
    case rte: RpcTimeoutException => throw rte
    // Any other TimeoutException get converted to a RpcTimeoutException with modified message
    case te: TimeoutException => throw createRpcTimeoutException(te)
  }

  /**
   * 在规定时间内返回对象, Await是scala并发库中的一个对象,result在duration时间片内返回Awaitable的执行结果。
   * future类继承于Awaitable类,
   * Wait for the completed result and return it. If the result is not available within this
   * timeout, throw a [[RpcTimeoutException]] to indicate which configuration controls the timeout.
   * @param  awaitable  the `Awaitable` to be awaited(可等待)
   * @throws RpcTimeoutException if after waiting for the specified time `awaitable`
   *         is still not ready
   */
  def awaitResult[T](awaitable: Awaitable[T]): T = {
    try {
      /**
       * Await 它有两个方法,一个是Await.ready,当Future的状态为完成时返回,
       *                    一种是Await.result,直接返回Future持有的结果
       * Future还提供了一些map,filter,foreach等操作
       * Await.result或者Await.ready会导致当前线程被阻塞,并等待actor通过它的应答来完成Future
       */
      Await.result(awaitable, duration)
    } catch addMessageIfTimeout
  }
}


private[spark] object RpcTimeout {

  /**
   * Lookup the timeout property in the configuration and create
   * a RpcTimeout with the property key in the description.
   * 查找超时属性配置在配置文件中创建一个rpctimeout的描述
   * @param conf configuration properties containing the timeout
   * @param timeoutProp property key for the timeout in seconds
   * @throws NoSuchElementException if property is not set
   */
  def apply(conf: SparkConf, timeoutProp: String): RpcTimeout = {
    val timeout = { conf.getTimeAsSeconds(timeoutProp) seconds }
    new RpcTimeout(timeout, timeoutProp)
  }

  /**
   * Lookup the timeout property in the configuration and create
   * a RpcTimeout with the property key in the description.
   * Uses the given default value if property is not set
   * @param conf configuration properties containing the timeout
   * @param timeoutProp property key for the timeout in seconds
   * @param defaultValue default timeout value in seconds if property not found
   */
  def apply(conf: SparkConf, timeoutProp: String, defaultValue: String): RpcTimeout = {
    val timeout = { conf.getTimeAsSeconds(timeoutProp, defaultValue) seconds }
    new RpcTimeout(timeout, timeoutProp)
  }

  /**
   * Lookup prioritized(优化) list of timeout properties in the configuration
   * and create a RpcTimeout with the first set property key in the
   * description.
   * Uses the given default value if property is not set
   * @param conf configuration properties containing the timeout
   * @param timeoutPropList prioritized list of property keys for the timeout in seconds
   * @param defaultValue default timeout value in seconds if no properties found
   */
  def apply(conf: SparkConf, timeoutPropList: Seq[String], defaultValue: String): RpcTimeout = {
    require(timeoutPropList.nonEmpty)

    // Find the first set property or use the default value with the first property
    val itr = timeoutPropList.iterator
    var foundProp: Option[(String, String)] = None
    while (itr.hasNext && foundProp.isEmpty){
      val propKey = itr.next()
     //设置属性值
      conf.getOption(propKey).foreach { prop => foundProp = Some(propKey, prop) }
    }
    //取出第一个列表值为Key,设置value默认值
    val finalProp = foundProp.getOrElse(timeoutPropList.head, defaultValue)
    //声明FiniteDuration类型,finalProp取出元组值,设置为秒
    val timeout = { Utils.timeStringAsSeconds(finalProp._2) seconds }
    //设置timeout超时时间,finalProp设置Key值
    new RpcTimeout(timeout, finalProp._1)
  }
}
