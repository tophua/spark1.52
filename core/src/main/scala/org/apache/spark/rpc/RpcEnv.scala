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
  * Spark基于这个思想在上述的Network的基础上实现一套自己的RPC Actor模型,从而取代Akka,
  * 其中RpcEndpoint对于Actor,RpcEndpointRef对应ActorRef,RpcEnv即对应了ActorSystem
  *
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
   *
   * Return RpcEndpointRef of the registered [[RpcEndpoint]]. Will be used to implement
   * [[RpcEndpoint.self]]. Return `null` if the corresponding [[RpcEndpointRef]] does not exist.
    *
    * 返回已注册的[[RpcEndpoint]]的RpcEndpointRef,将用于实现[[RpcEndpoint.self]],
    * 如果相应的[[RpcEndpointRef]]不存在，返回`null`
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
   *
   * Register a [[RpcEndpoint]] with a name and return its [[RpcEndpointRef]]. [[RpcEnv]] does not
   * guarantee thread-safety.
    * 使用名称注册[[RpcEndpoint]]并返回其[[RpcEndpointRef]]。 [[RpcEnv]]不保证线程安全。
    * 根据name注册RpcEndpoint到RpcEnv中并返回它的一个引用RpcEndpointRef, [[RpcEnv]]不保证线程安全。
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
    * 我们需要一个超时参数吗?
   */
  def awaitTermination(): Unit

  /**
   * Create a URI used to create a [[RpcEndpointRef]]. Use this one to create the URI instead of
   * creating it manually because different [[RpcEnv]] may have different formats.
    * 创建一个用于创建[[RpcEndpointRef]]的URI,使用这个来创建URI而不是手动创建它,因为不同的[[RpcEnv]]可能有不同的格式。
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
  //TODO是否需要在地址中添加RpcEnv的类型？
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
    * 返回由`uri`表示的[[RpcAddress]]
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
  * 如果RpcTimeout修改[[TimeoutException]]，抛出异常
 */
private[rpc] class RpcTimeoutException(message: String, cause: TimeoutException)
  //initCause()这个方法就是对异常来进行包装的,目的就是为了出了问题的时候能够追根究底
  extends TimeoutException(message) { initCause(cause) }


/**
 * Associates a timeout with a description so that a when a TimeoutException occurs, additional
 * context about the timeout can be amended to the exception message.
  * 将超时与描述相关联,以便在发生TimeoutException时,有关超时的其他上下文可以修改为异常消息,
 * @param duration timeout duration in seconds
 * @param timeoutProp the configuration property that controls this timeout
 */
private[spark] class RpcTimeout(val duration: FiniteDuration, val timeoutProp: String)
  extends Serializable {

  /** Amends the standard message of TimeoutException to include the description
    * 修改TimeoutException的标准消息以包含描述 */
  private def createRpcTimeoutException(te: TimeoutException): RpcTimeoutException = {
    new RpcTimeoutException(te.getMessage() + ". This timeout is controlled by " + timeoutProp, te)
  }

  /**
   * PartialFunction to match a TimeoutException and add the timeout description to the message
   * PartialFunction来匹配TimeoutException,并将超时描述添加到消息中
   * @note This can be used in the recover callback of a Future to add to a TimeoutException
    *       这可以用于将来的恢复回调来添加到TimeoutException
   * Example:
   *    val timeout = new RpcTimeout(5 millis, "short timeout")
   *    Future(throw new TimeoutException).recover(timeout.addMessageIfTimeout)
    *    PartialFunction偏函数是个特质其的类型为PartialFunction[A,B],其中接收一个类型为A的参数，返回一个类型为B的结果
   */
  def addMessageIfTimeout[T]: PartialFunction[Throwable, T] = {
    // The exception has already been converted to a RpcTimeoutException so just raise it
    //异常已经被转换为RpcTimeoutException，所以只是提高它
    case rte: RpcTimeoutException => throw rte
    // Any other TimeoutException get converted to a RpcTimeoutException with modified message
      //任何其他TimeoutException通过修改的消息转换为RpcTimeoutException
    case te: TimeoutException => throw createRpcTimeoutException(te)
  }

  /**
   * 在规定时间内返回对象, Await是scala并发库中的一个对象,result在duration时间片内返回Awaitable的执行结果。
   * future类继承于Awaitable类,
   * Wait for the completed result and return it. If the result is not available within this
   * timeout, throw a [[RpcTimeoutException]] to indicate which configuration controls the timeout.
    *
    *在规定时间内等待完成的结果并返回,如果结果在此超时期间不可用,则抛出一个[[RpcTimeoutException]]来指示哪个配置控制超时
    *
   * @param  awaitable  the `Awaitable` to be awaited
   * @throws RpcTimeoutException if after waiting for the specified time `awaitable`
   *         is still not ready  如果等待指定的时间“等待”仍然没有准备好
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
   * 在配置中查找timeout属性,并在描述中使用属性键创建一个RpcTimeout。
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
    *
    * 在配置中查找timeout属性,并在描述中使用属性键创建一个RpcTimeout,
    * 如果属性未设置,则使用给定的默认值
    *
   * @param conf configuration properties containing the timeout
   * @param timeoutProp property key for the timeout in seconds
   * @param defaultValue default timeout value in seconds if property not found
   */
  def apply(conf: SparkConf, timeoutProp: String, defaultValue: String): RpcTimeout = {
    val timeout = { conf.getTimeAsSeconds(timeoutProp, defaultValue) seconds }
    new RpcTimeout(timeout, timeoutProp)
  }

  /**
   * Lookup prioritized list of timeout properties in the configuration
   * and create a RpcTimeout with the first set property key in the
   * description.
    * 查找配置中超时属性的优先级列表,并在描述中创建具有第一个集合属性键的RpcTimeout,
   * Uses the given default value if property is not set
    * 如果未设置属性，则使用给定的默认值
   * @param conf configuration properties containing the timeout 包含超时的配置属性
   * @param timeoutPropList prioritized list of property keys for the timeout in seconds
    *                         用于超时的属性keys的优先级列表（以秒为单位）
   * @param defaultValue default timeout value in seconds if no properties found
    *                       默认超时值（以秒为单位），如果找不到属性
   */
  def apply(conf: SparkConf, timeoutPropList: Seq[String], defaultValue: String): RpcTimeout = {
    require(timeoutPropList.nonEmpty)

    // Find the first set property or use the default value with the first property
    //找到第一个属性或使用第一个属性的默认值
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
