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

package org.apache.spark.rpc.akka

import java.util.concurrent.ConcurrentHashMap

import scala.concurrent.Future
import scala.language.postfixOps
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import akka.actor.{ActorSystem, ExtendedActorSystem, Actor, ActorRef, Props, Address}
import akka.event.Logging.Error
import akka.pattern.{ask => akkaAsk}
import akka.remote.{AssociationEvent, AssociatedEvent, DisassociatedEvent, AssociationErrorEvent}
import akka.serialization.JavaSerializer

import org.apache.spark.{SparkException, Logging, SparkConf}
import org.apache.spark.rpc._
import org.apache.spark.util.{ActorLogReceive, AkkaUtils, ThreadUtils}

/**
 * A RpcEnv implementation based on Akka.
 *
 * TODO Once we remove all usages of Akka in other place, we can move this file to a new project and
 * remove Akka from the dependencies.
 * actorSystem是Akka提供用于创建分布式消息通信系统的基础类
 * @param actorSystem
 * @param conf
 * @param boundPort
 */
private[spark] class AkkaRpcEnv private[akka] (
    val actorSystem: ActorSystem, conf: SparkConf, boundPort: Int)
  extends RpcEnv(conf) with Logging {

  private val defaultAddress: RpcAddress = {
    val address = actorSystem.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress
    // In some test case, ActorSystem doesn't bind to any address.
    // So just use some default value since they are only some unit tests
    RpcAddress(address.host.getOrElse("localhost"), address.port.getOrElse(boundPort))
  }

  override val address: RpcAddress = defaultAddress

  /**
   * A lookup table to search a [[RpcEndpointRef]] for a [[RpcEndpoint]]. We need it to make
   * [[RpcEndpoint.self]] work.
   * 查找ConcurrentHashMap搜索RpcEndpointRef的RpcEndpoint,
   */
  private val endpointToRef = new ConcurrentHashMap[RpcEndpoint, RpcEndpointRef]()

  /**
   * Need this map to remove `RpcEndpoint` from `endpointToRef` via a `RpcEndpointRef`
   * 从Map删除RpcEndpoint
   */
  private val refToEndpoint = new ConcurrentHashMap[RpcEndpointRef, RpcEndpoint]()

  private def registerEndpoint(endpoint: RpcEndpoint, endpointRef: RpcEndpointRef): Unit = {
    endpointToRef.put(endpoint, endpointRef)
    refToEndpoint.put(endpointRef, endpoint)
  }

  private def unregisterEndpoint(endpointRef: RpcEndpointRef): Unit = {
    val endpoint = refToEndpoint.remove(endpointRef)
    if (endpoint != null) {
      endpointToRef.remove(endpoint)
    }
  }

  /**
   * Retrieve the [[RpcEndpointRef]] of `endpoint`.
   * 根据RpcEndpoint从endpointToRef(ConcurrentHashMap)获得RpcEndpointRef
   */
  override def endpointRef(endpoint: RpcEndpoint): RpcEndpointRef = endpointToRef.get(endpoint)

  override def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef = {
    @volatile var endpointRef: AkkaRpcEndpointRef = null
    // Use lazy because the Actor needs to use `endpointRef`.
    // So `actorRef` should be created after assigning `endpointRef`.   
    //使用延迟加载Actor,需要使用endpointRef,所以actorre被创建后,分配endpointref,
    //Props是一个配置类,它的作用是对创建角色确认选项
    lazy val actorRef = actorSystem.actorOf(Props(new Actor with ActorLogReceive with Logging {

      assert(endpointRef != null)

      override def preStart(): Unit = {
        // Listen for remote client network events
        //首先向ActionSystem订阅事件
        context.system.eventStream.subscribe(self, classOf[AssociationEvent])
        safelyCall(endpoint) {
          endpoint.onStart()
        }
      }

      override def receiveWithLogging: Receive = {
        case AssociatedEvent(_, remoteAddress, _) =>
          safelyCall(endpoint) {
            endpoint.onConnected(akkaAddressToRpcAddress(remoteAddress))
          }

        case DisassociatedEvent(_, remoteAddress, _) =>
          safelyCall(endpoint) {
            endpoint.onDisconnected(akkaAddressToRpcAddress(remoteAddress))
          }

        case AssociationErrorEvent(cause, localAddress, remoteAddress, inbound, _) =>
          safelyCall(endpoint) {
            endpoint.onNetworkError(cause, akkaAddressToRpcAddress(remoteAddress))
          }

        case e: AssociationEvent =>
          // TODO ignore?

        case m: AkkaMessage =>
          logDebug(s"Received RPC message: $m")
          safelyCall(endpoint) {
            processMessage(endpoint, m, sender)
          }

        case AkkaFailure(e) =>
          safelyCall(endpoint) {
            throw e
          }

        case message: Any => {
          logWarning(s"Unknown message: $message")
        }

      }

      override def postStop(): Unit = {
        unregisterEndpoint(endpoint.self)
        safelyCall(endpoint) {
          endpoint.onStop()
        }
      }

      }), name = name)
      //创建AkkaRpcEndpointRef对象
    endpointRef = new AkkaRpcEndpointRef(defaultAddress, actorRef, conf, initInConstructor = false)
    registerEndpoint(endpoint, endpointRef)
    // Now actorRef can be created safely
    // 现在创建安全actorref
    endpointRef.init()
  
    endpointRef
  }

  private def processMessage(endpoint: RpcEndpoint, m: AkkaMessage, _sender: ActorRef): Unit = {
    val message = m.message
    val needReply = m.needReply
    val pf: PartialFunction[Any, Unit] =
      if (needReply) {
        endpoint.receiveAndReply(new RpcCallContext {
          override def sendFailure(e: Throwable): Unit = {
            _sender ! AkkaFailure(e)
          }

          override def reply(response: Any): Unit = {
            _sender ! AkkaMessage(response, false)
          }

          // Some RpcEndpoints need to know the sender's address
          override val sender: RpcEndpointRef =
            new AkkaRpcEndpointRef(defaultAddress, _sender, conf)
        })
      } else {
        endpoint.receive
      }
    try {
      pf.applyOrElse[Any, Unit](message, { message =>
        throw new SparkException(s"Unmatched message $message from ${_sender}")
      })
    } catch {
      case NonFatal(e) =>
        _sender ! AkkaFailure(e)
        if (!needReply) {
          // If the sender does not require a reply, it may not handle the exception. So we rethrow
          // "e" to make sure it will be processed.
          throw e
        }
    }
  }

  /**
   * Run `action` safely to avoid to crash the thread. If any non-fatal exception happens, it will
   * call `endpoint.onError`. If `endpoint.onError` throws any non-fatal exception, just log it.
   * 运行"action"安全地避免崩溃的线程,如果任何非致命的异常.这将调用endpoint.onError方法
   * 如果ndpoint.onError抛出非致命异常,仅仅记录它
   */
  private def safelyCall(endpoint: RpcEndpoint)(action: => Unit): Unit = {
    try {
      action
    } catch {
      case NonFatal(e) => {
        try {
          endpoint.onError(e)
        } catch {
          case NonFatal(e) => logError(s"Ignore error: ${e.getMessage}", e)
        }
      }
    }
  }

  private def akkaAddressToRpcAddress(address: Address): RpcAddress = {
    RpcAddress(address.host.getOrElse(defaultAddress.host),
      address.port.getOrElse(defaultAddress.port))
  }

  override def asyncSetupEndpointRefByURI(uri: String): Future[RpcEndpointRef] = {
    import actorSystem.dispatcher
    //创建远程 Actor,resolveOne 方法来获得对应Actor的ActorRef。 如果指定的Actor存在,
    //那么返回一个和该匹配Actor对应的Future对象,反之返回一个[akka.actor.ActorNotFound]错误
    actorSystem.actorSelection(uri).resolveOne(defaultLookupTimeout.duration).
      map(new AkkaRpcEndpointRef(defaultAddress, _, conf)).
      // this is just in case there is a timeout from creating the future in resolveOne, we want the
      // exception to indicate the conf that determines the timeout
      recover(defaultLookupTimeout.addMessageIfTimeout)
  }

  override def uriOf(systemName: String, address: RpcAddress, endpointName: String): String = {
    AkkaUtils.address(
      AkkaUtils.protocol(actorSystem), systemName, address.host, address.port, endpointName)
  }

  override def shutdown(): Unit = {
    actorSystem.shutdown()
  }

  override def stop(endpoint: RpcEndpointRef): Unit = {
    require(endpoint.isInstanceOf[AkkaRpcEndpointRef])
    actorSystem.stop(endpoint.asInstanceOf[AkkaRpcEndpointRef].actorRef)
  }

  override def awaitTermination(): Unit = {
    actorSystem.awaitTermination()
  }

  override def toString: String = s"${getClass.getSimpleName}($actorSystem)"

  override def deserialize[T](deserializationAction: () => T): T = {
    JavaSerializer.currentSystem.withValue(actorSystem.asInstanceOf[ExtendedActorSystem]) {
      deserializationAction()
    }
  }
}

private[spark] class AkkaRpcEnvFactory extends RpcEnvFactory {

  def create(config: RpcEnvConfig): RpcEnv = {
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem(
      config.name, config.host, config.port, config.conf, config.securityManager)
    actorSystem.actorOf(Props(classOf[ErrorMonitor]), "ErrorMonitor")
    new AkkaRpcEnv(actorSystem, config.conf, boundPort)
  }
}

/**
 * Monitor errors reported by Akka and log them.
 * 监测报告的Akka错误和日志
 */
private[akka] class ErrorMonitor extends Actor with ActorLogReceive with Logging {

  override def preStart(): Unit = {
    //订阅
    context.system.eventStream.subscribe(self, classOf[Error])
  }

  override def receiveWithLogging: Actor.Receive = {
    case Error(cause: Throwable, _, _, message: String) => logDebug(message, cause)
  }
}

private[akka] class AkkaRpcEndpointRef(
    @transient defaultAddress: RpcAddress,
    @transient _actorRef: => ActorRef,
    @transient conf: SparkConf,
    @transient initInConstructor: Boolean = true)
  extends RpcEndpointRef(conf) with Logging {

  lazy val actorRef = _actorRef

  override lazy val address: RpcAddress = {
    val akkaAddress = actorRef.path.address
    RpcAddress(akkaAddress.host.getOrElse(defaultAddress.host),
      akkaAddress.port.getOrElse(defaultAddress.port))
  }

  override lazy val name: String = actorRef.path.name

  private[akka] def init(): Unit = {
    //Initialize the lazy vals
    //初始化延迟加载变量
    actorRef
    address
    name
  }

  if (initInConstructor) {//默认初始
    init()
  }

  override def send(message: Any): Unit = {
    actorRef ! AkkaMessage(message, false)
  }

  override def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T] = {
    actorRef.ask(AkkaMessage(message, true))(timeout.duration).flatMap {
      // The function will run in the calling thread, so it should be short and never block.
      // 函数的调用线程中运行,所以它应该是短暂,线程永远不会堵塞
      case msg @ AkkaMessage(message, reply) =>
        if (reply) {//判断是否有回复消息,则失败
          logError(s"Receive $msg but the sender cannot reply")
          Future.failed(new SparkException(s"Receive $msg but the sender cannot reply"))
        } else {
          Future.successful(message)
        }
      case AkkaFailure(e) =>
        Future.failed(e)
    }(ThreadUtils.sameThread).mapTo[T].
    recover(timeout.addMessageIfTimeout)(ThreadUtils.sameThread)
  }

  override def toString: String = s"${getClass.getSimpleName}($actorRef)"

  final override def equals(that: Any): Boolean = that match {
    case other: AkkaRpcEndpointRef => actorRef == other.actorRef
    case _ => false
  }

  final override def hashCode(): Int = if (actorRef == null) 0 else actorRef.hashCode()
}

/**
 * A wrapper to `message` so that the receiver knows if the sender expects a reply.
 * "消息"的一个包装器,接收发送方答复已知消息
 * @param message
 * @param needReply if the sender expects a reply message 否是接收发送回复的消息
 */
private[akka] case class AkkaMessage(message: Any, needReply: Boolean)

/**
 * A reply with the failure error from the receiver to the sender
 * 一个故障错误的答复从接收者到发送者
 */
private[akka] case class AkkaFailure(e: Throwable)
