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

import java.util.concurrent.{TimeUnit, CountDownLatch, TimeoutException}

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually._

import org.apache.spark.{SparkConf, SparkException, SparkFunSuite}

/**
 * Common tests for an RpcEnv implementation.
 * 一个rpcenv实现常见的测试
 */
abstract class RpcEnvSuite extends SparkFunSuite with BeforeAndAfterAll {

  var env: RpcEnv = _

  override def beforeAll(): Unit = {
    val conf = new SparkConf()
    //参数名称:ActorSystem名称local,配置文件:conf,端口:0,
    env = createRpcEnv(conf, "local", 0)
  }

  override def afterAll(): Unit = {
    if (env != null) {
      env.shutdown()
    }
  }
  //通过工厂方法模式创建了rpcEnv。
  def createRpcEnv(conf: SparkConf, name: String, port: Int): RpcEnv

  test("send a message locally") {//本地发送消息
    @volatile var message: String = null
    //setupEndpoint 根据name注册RpcEndpoint到RpcEnv中并返回它的一个引用RpcEndpointRef
    val rpcEndpointRef = env.setupEndpoint("send-locally", new RpcEndpoint {
      override val rpcEnv = env

      override def receive = {
        //收到发送的消息,赋值message
        case msg: String => message = msg
      }
    })
    //RpcEndpointRef 一个远程RpcEndpoint的引用,通过它可以给远程RpcEndpoint发送消息
    rpcEndpointRef.send("hello")
    eventually(timeout(5 seconds), interval(10 millis)) {
      assert("hello" === message)
    }
  }

  test("send a message remotely") {//远程发送消息
    @volatile var message: String = null
    // Set up a RpcEndpoint using env
    //向RpcEnv注册一个名称:send-remotely的RpcEndpoint
    env.setupEndpoint("send-remotely", new RpcEndpoint {
      override val rpcEnv = env

      override def receive: PartialFunction[Any, Unit] = {
        //收到发送的消息,赋值message
        case msg: String => message = msg
      }
    })
   //remote是ActorSystem,0是端口
    val anotherEnv = createRpcEnv(new SparkConf(), "remote", 0)
    // Use anotherEnv to find out the RpcEndpointRef
    //使用anotherEnv找到RpcEndpointRef
    //参数名称:ActorSystem:local,在启动的时候已经创建,远程地址:env.address,远程名称:send-remotely,
    //akka.tcp://local@localhost:51174/user/send-remotely
    val rpcEndpointRef = anotherEnv.setupEndpointRef("local", env.address, "send-remotely")
    try {
      //向远程发送消息
      rpcEndpointRef.send("hello")
      eventually(timeout(5 seconds), interval(10 millis)) {
        assert("hello" === message)
      }
    } finally {
      anotherEnv.shutdown()
      anotherEnv.awaitTermination()
    }
  }

  test("send a RpcEndpointRef") {//发送一个RPC终结点引用
    val endpoint = new RpcEndpoint {
      override val rpcEnv = env

      override def receiveAndReply(context: RpcCallContext) = {
        case "Hello" => context.reply(self)//响应
        case "Echo" => context.reply("Echo")//响应
      }
    }
    //向RpcEnv注册Actor名称send-ref的RpcEndpoint
    val rpcEndpointRef = env.setupEndpoint("send-ref", endpoint)
    //返回RpcEndpointRef对象,发送消息给RpcEndpoint.receive并在默认的超时内得到结果
    val newRpcEndpointRef = rpcEndpointRef.askWithRetry[RpcEndpointRef]("Hello")    
    //返回字符串
    val reply = newRpcEndpointRef.askWithRetry[String]("Echo")
    assert("Echo" === reply)
  }

  test("ask a message locally") {//本地询问消息
    //向RpcEnv注册一个名称:ask-locally的RpcEndpoint
    val rpcEndpointRef = env.setupEndpoint("ask-locally", new RpcEndpoint {
      override val rpcEnv = env

      override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
        case msg: String => {
          context.reply(msg)//响应
        }
      }
    })
    val reply = rpcEndpointRef.askWithRetry[String]("hello")
    assert("hello" === reply)
  }

  test("ask a message remotely") {//远程询问消息
    //向RpcEnv注册一个名称:ask-locally的RpcEndpoint
    env.setupEndpoint("ask-remotely", new RpcEndpoint {
      override val rpcEnv = env

      override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
        case msg: String => {
          context.reply(msg)//给发件人回复一个消息
        }
      }
    })
    //remote是ActorSystem,0是端口
    val anotherEnv = createRpcEnv(new SparkConf(), "remote", 0)
    // Use anotherEnv to find out the RpcEndpointRef
    //参数名称:ActorSystem:local,在启动的时候已经创建,远程地址:env.address,远程名称:send-remotely,
    //akka.tcp://local@localhost:51174/user/ask-remotely
    val rpcEndpointRef = anotherEnv.setupEndpointRef("local", env.address, "ask-remotely")
    try {
      //askWithRetry发送一个消息
      val reply = rpcEndpointRef.askWithRetry[String]("hello")
      assert("hello" === reply)
    } finally {
      anotherEnv.shutdown()
      anotherEnv.awaitTermination()
    }
  }

  test("ask a message timeout") {//询问消息超时
    env.setupEndpoint("ask-timeout", new RpcEndpoint {
      override val rpcEnv = env

      override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
        case msg: String => {
          Thread.sleep(100)
          context.reply(msg)
        }
      }
    })

    val conf = new SparkConf()
    val shortProp = "spark.rpc.short.timeout"//控制超时的配置属性
    conf.set("spark.rpc.retry.wait", "0")//重试等待时间
    conf.set("spark.rpc.numRetries", "1")//消息失败发送重试次数
    val anotherEnv = createRpcEnv(conf, "remote", 0)
    // Use anotherEnv to find out the RpcEndpointRef
    //用anotherEnv找到RpcEndpointRef(RPC终结点引用)
    val rpcEndpointRef = anotherEnv.setupEndpointRef("local", env.address, "ask-timeout")
    try {
      // Any exception thrown in askWithRetry is wrapped with a SparkException and set as the cause
      //设置的原因askwithretry抛出任何异常sparkexception
      val e = intercept[SparkException] {
        //RpcTimeout 控制超时的配置属性
        rpcEndpointRef.askWithRetry[String]("hello", new RpcTimeout(1 millis, shortProp))
      }
      // The SparkException cause should be a RpcTimeoutException with message indicating the
      // controlling timeout property
      //SparkException原因应该是一个RpcTimeoutException,其消息指示控制超时属性
      assert(e.getCause.isInstanceOf[RpcTimeoutException])
      assert(e.getCause.getMessage.contains(shortProp))//指示控制超时属性的消息
    } finally {
      anotherEnv.shutdown()
      anotherEnv.awaitTermination()
    }
  }

  test("onStart and onStop") {//开始和暂停
    //CountDownLatch这个类能够使一个线程等待其他线程完成各自的工作后再执行
    val stopLatch = new CountDownLatch(1)
    //可变ArrayBuffer
    val calledMethods = mutable.ArrayBuffer[String]()

    val endpoint = new RpcEndpoint {
      override val rpcEnv = env

      override def onStart(): Unit = {
        calledMethods += "start"
      }

      override def receive: PartialFunction[Any, Unit] = {
        case msg: String =>
      }

      override def onStop(): Unit = {
        calledMethods += "stop"
        stopLatch.countDown()
      }
    }
    val rpcEndpointRef = env.setupEndpoint("start-stop-test", endpoint)
    env.stop(rpcEndpointRef)
    //等待其他线程完成等待时间,超过10秒直接放弃
    stopLatch.await(10, TimeUnit.SECONDS)
    assert(List("start", "stop") === calledMethods)
  }

  test("onError: error in onStart") {//开始运行的错误
    @volatile var e: Throwable = null
    env.setupEndpoint("onError-onStart", new RpcEndpoint {
      override val rpcEnv = env

      override def onStart(): Unit = {
        throw new RuntimeException("Oops!")
      }

      override def receive: PartialFunction[Any, Unit] = {
        case m =>
      }

      override def onError(cause: Throwable): Unit = {
        e = cause
      }
    })

    eventually(timeout(5 seconds), interval(10 millis)) {
      assert(e.getMessage === "Oops!")
    }
  }

  test("onError: error in onStop") {//暂停的错误
    @volatile var e: Throwable = null
    val endpointRef = env.setupEndpoint("onError-onStop", new RpcEndpoint {
      override val rpcEnv = env

      override def receive: PartialFunction[Any, Unit] = {
        case m =>
      }

      override def onError(cause: Throwable): Unit = {
        e = cause
      }

      override def onStop(): Unit = {
        throw new RuntimeException("Oops!")
      }
    })

    env.stop(endpointRef)

    eventually(timeout(5 seconds), interval(10 millis)) {
      assert(e.getMessage === "Oops!")
    }
  }

  test("onError: error in receive") {//接收的错误
    @volatile var e: Throwable = null
    val endpointRef = env.setupEndpoint("onError-receive", new RpcEndpoint {
      override val rpcEnv = env

      override def receive: PartialFunction[Any, Unit] = {
        case m => throw new RuntimeException("Oops!")
      }

      override def onError(cause: Throwable): Unit = {
        e = cause
      }
    })

    endpointRef.send("Foo")
    //interval 间隔,timeout超时 
    eventually(timeout(5 seconds), interval(10 millis)) {
      assert(e.getMessage === "Oops!")
    }
  }

  test("self: call in onStart") {//在onStart调用
    @volatile var callSelfSuccessfully = false

    env.setupEndpoint("self-onStart", new RpcEndpoint {
      override val rpcEnv = env

      override def onStart(): Unit = {
        self
        callSelfSuccessfully = true
      }

      override def receive: PartialFunction[Any, Unit] = {
        case m =>
      }
    })

    eventually(timeout(5 seconds), interval(10 millis)) {
      // Calling `self` in `onStart` is fine
      //在`onStart`中调用`self`是不错的
      assert(callSelfSuccessfully === true)
    }
  }

  test("self: call in receive") {//接收到调用
    @volatile var callSelfSuccessfully = false

    val endpointRef = env.setupEndpoint("self-receive", new RpcEndpoint {
      override val rpcEnv = env

      override def receive: PartialFunction[Any, Unit] = {
        case m => {
          self
          callSelfSuccessfully = true
        }
      }
    })

    endpointRef.send("Foo")

    eventually(timeout(5 seconds), interval(10 millis)) {
      // Calling `self` in `receive` is fine
      //在`receive`中调用`self`是不错的
      assert(callSelfSuccessfully === true)
    }
  }

  test("self: call in onStop") {//调用暂停
    @volatile var selfOption: Option[RpcEndpointRef] = null

    val endpointRef = env.setupEndpoint("self-onStop", new RpcEndpoint {
      override val rpcEnv = env

      override def receive: PartialFunction[Any, Unit] = {
        case m =>
      }

      override def onStop(): Unit = {
        selfOption = Option(self)
      }

      override def onError(cause: Throwable): Unit = {
      }
    })

    env.stop(endpointRef)

    eventually(timeout(5 seconds), interval(10 millis)) {
      // Calling `self` in `onStop` will return null, so selfOption will be None
      //在`onStop'中调用`self`将返回null，因此selfOption将为None
      assert(selfOption == None)
    }
  }

  test("call receive in sequence") {//顺序调用接收
    // If a RpcEnv implementation breaks the `receive` contract, hope this test can expose it
    //如果一个RpcEnv实现打破了'receive`合同，希望这个测试能够暴露出来
    for (i <- 0 until 100) {
      @volatile var result = 0
      val endpointRef = env.setupEndpoint(s"receive-in-sequence-$i", new ThreadSafeRpcEndpoint {
        override val rpcEnv = env

        override def receive: PartialFunction[Any, Unit] = {
          case m => result += 1
        }

      })

      (0 until 10) foreach { _ =>
        new Thread {
          override def run() {
            (0 until 100) foreach { _ =>
              endpointRef.send("Hello")
            }
          }
        }.start()
      }

      eventually(timeout(5 seconds), interval(5 millis)) {
        assert(result == 1000)
      }

      env.stop(endpointRef)
    }
  }

  test("stop(RpcEndpointRef) reentrant") {//reentrant 折返
    @volatile var onStopCount = 0
    val endpointRef = env.setupEndpoint("stop-reentrant", new RpcEndpoint {
      override val rpcEnv = env

      override def receive: PartialFunction[Any, Unit] = {
        case m =>
      }

      override def onStop(): Unit = {
        onStopCount += 1
      }
    })

    env.stop(endpointRef)
    env.stop(endpointRef)

    eventually(timeout(5 seconds), interval(5 millis)) {
      // Calling stop twice should only trigger onStop once.
      //调用两次只能触发onStop一次停止
      assert(onStopCount == 1)
    }
  }
  /**
   * 发送和回复
   */
  test("sendWithReply") {
    val endpointRef = env.setupEndpoint("sendWithReply", new RpcEndpoint {
      override val rpcEnv = env

      override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
        case m => context.reply("ack")
      }
    })

    val f = endpointRef.ask[String]("Hi")
    //同步阻塞按规定时间获取结果,超时抛出异常
    val ack = Await.result(f, 5 seconds)
    assert("ack" === ack)

    env.stop(endpointRef)
  }

  test("sendWithReply: remotely") {//发送和回复 远程
    env.setupEndpoint("sendWithReply-remotely", new RpcEndpoint {
      override val rpcEnv = env

      override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
        case m => context.reply("ack")
      }
    })

    val anotherEnv = createRpcEnv(new SparkConf(), "remote", 0)
    // Use anotherEnv to find out the RpcEndpointRef
    //用anotherEnv找到RpcEndpointRef(RPC终结点引用)
    val rpcEndpointRef = anotherEnv.setupEndpointRef("local", env.address, "sendWithReply-remotely")
    try {
      val f = rpcEndpointRef.ask[String]("hello")
      //同步阻塞按规定时间获取结果,超时抛出异常
      val ack = Await.result(f, 5 seconds)
      assert("ack" === ack)
    } finally {
      anotherEnv.shutdown()
      anotherEnv.awaitTermination()
    }
  }

  test("sendWithReply: error") {//发送和响应 错误
    val endpointRef = env.setupEndpoint("sendWithReply-error", new RpcEndpoint {
      override val rpcEnv = env
      //接收和回复
      override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
        case m => context.sendFailure(new SparkException("Oops"))//抛出异常
      }
    })

    val f = endpointRef.ask[String]("Hi")//请求
    //响应异常
    val e = intercept[SparkException] {
      //同步阻塞按规定时间获取结果,超时抛出异常
      Await.result(f, 5 seconds)
    }
    assert("Oops" === e.getMessage)

    env.stop(endpointRef)
  }

  test("sendWithReply: remotely error") {//远程错误
    env.setupEndpoint("sendWithReply-remotely-error", new RpcEndpoint {
      override val rpcEnv = env
      //接收和回复
      override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
        case msg: String => context.sendFailure(new SparkException("Oops"))//抛出异常
      }
    })

    val anotherEnv = createRpcEnv(new SparkConf(), "remote", 0)
    // Use anotherEnv to find out the RpcEndpointRef
    //用anotherEnv找到RpcEndpointRef(RPC终结点引用)
    val rpcEndpointRef = anotherEnv.setupEndpointRef(
      "local", env.address, "sendWithReply-remotely-error")
    try {
      val f = rpcEndpointRef.ask[String]("hello")//请求
      val e = intercept[SparkException] {
        //Await.result或者Await.ready会导致当前线程被阻塞,并等待actor通过它的应答来完成Future
        Await.result(f, 5 seconds)
      }
      assert("Oops" === e.getMessage)
    } finally {
      anotherEnv.shutdown()
      anotherEnv.awaitTermination()
    }
  }

  test("network events") {//网络事件
    //可变
    val events = new mutable.ArrayBuffer[(Any, Any)] with mutable.SynchronizedBuffer[(Any, Any)]
    env.setupEndpoint("network-events", new ThreadSafeRpcEndpoint {//线程安全
      override val rpcEnv = env
      //接收
      override def receive: PartialFunction[Any, Unit] = {
        case "hello" =>
        case m => events += "receive" -> m
      }
      //连接成功
      override def onConnected(remoteAddress: RpcAddress): Unit = {
        events += "onConnected" -> remoteAddress//向events添加元数组形式
      }
      //断开连接
      override def onDisconnected(remoteAddress: RpcAddress): Unit = {
        events += "onDisconnected" -> remoteAddress
      }
      //网络错误
      override def onNetworkError(cause: Throwable, remoteAddress: RpcAddress): Unit = {
        events += "onNetworkError" -> remoteAddress
      }

    })

    val anotherEnv = createRpcEnv(new SparkConf(), "remote", 0)
    // Use anotherEnv to find out the RpcEndpointRef
    //用anotherEnv找到RpcEndpointRef(RPC终结点引用)
    val rpcEndpointRef = anotherEnv.setupEndpointRef(
      "local", env.address, "network-events")
    val remoteAddress = anotherEnv.address
    rpcEndpointRef.send("hello")
    eventually(timeout(5 seconds), interval(5 millis)) {
      assert(events === List(("onConnected", remoteAddress)))
    }

    anotherEnv.shutdown()
    anotherEnv.awaitTermination()
    eventually(timeout(5 seconds), interval(5 millis)) {
      assert(events === List(
        ("onConnected", remoteAddress),
        ("onNetworkError", remoteAddress),
        ("onDisconnected", remoteAddress)))
    }
  }

  test("sendWithReply: unserializable error") {//接收和回复抛出未序列化错误
    env.setupEndpoint("sendWithReply-unserializable-error", new RpcEndpoint {
      override val rpcEnv = env
      //接收和回复
      override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
        case msg: String => context.sendFailure(new UnserializableException)
      }
    })

    val anotherEnv = createRpcEnv(new SparkConf(), "remote", 0)
    // Use anotherEnv to find out the RpcEndpointRef
     //用anotherEnv找到RpcEndpointRef(RPC终结点引用)
    val rpcEndpointRef = anotherEnv.setupEndpointRef(
      "local", env.address, "sendWithReply-unserializable-error")
    try {
      val f = rpcEndpointRef.ask[String]("hello")
      intercept[TimeoutException] {//连接超时异常
        Await.result(f, 1 seconds)
      }
    } finally {
      anotherEnv.shutdown()
      anotherEnv.awaitTermination()
    }
  }

  test("construct RpcTimeout with conf property") {//构造RPC超时配置属性
    val conf = new SparkConf

    val testProp = "spark.ask.test.timeout"
    val testDurationSeconds = 30
    val secondaryProp = "spark.ask.secondary.timeout"

    conf.set(testProp, s"${testDurationSeconds}s")
    conf.set(secondaryProp, "100s")

    // Construct RpcTimeout with a single property
    //构建一个属性rpctimeout
    val rt1 = RpcTimeout(conf, testProp)//默认30秒
    assert( testDurationSeconds === rt1.duration.toSeconds )

    // Construct RpcTimeout with prioritized list of properties
    //构造RPC超时优先属性列表
    val rt2 = RpcTimeout(conf, Seq("spark.ask.invalid.timeout", testProp, secondaryProp), "1s")
    assert( testDurationSeconds === rt2.duration.toSeconds )

    // Construct RpcTimeout with default value,
    // 构建具有默认值rpctimeout
    val defaultProp = "spark.ask.default.timeout"//默认超时
    val defaultDurationSeconds = 1
    val rt3 = RpcTimeout(conf, Seq(defaultProp), defaultDurationSeconds.toString + "s")
    assert( defaultDurationSeconds === rt3.duration.toSeconds )//设置默认超时
    assert( rt3.timeoutProp.contains(defaultProp) )

    // Try to construct RpcTimeout with an unconfigured property
    //尝试构建rpctimeout配置的属性
    intercept[NoSuchElementException] {
      RpcTimeout(conf, "spark.ask.invalid.timeout")
    }
  }

  test("ask a message timeout on Future using RpcTimeout") {//在Future的使用rpctimeout消息超时
    case class NeverReply(msg: String)

    val rpcEndpointRef = env.setupEndpoint("ask-future", new RpcEndpoint {
      override val rpcEnv = env

      override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
        case msg: String => context.reply(msg)
        case _: NeverReply =>
      }
    })

    val longTimeout = new RpcTimeout(1 second, "spark.rpc.long.timeout")
    val shortTimeout = new RpcTimeout(10 millis, "spark.rpc.short.timeout")

    // Ask with immediate response, should complete successfully
    //要求立即回应,应该成功完成
    val fut1 = rpcEndpointRef.ask[String]("hello", longTimeout)
    val reply1 = longTimeout.awaitResult(fut1)
    assert("hello" === reply1)

    // Ask with a delayed response and wait for response immediately that should timeout
    //请求延迟响应,并等待响应立即超时
    val fut2 = rpcEndpointRef.ask[String](NeverReply("doh"), shortTimeout)
    val reply2 =
      intercept[RpcTimeoutException] {
        shortTimeout.awaitResult(fut2)
      }.getMessage

    // RpcTimeout.awaitResult should have added the property to the TimeoutException message
     //rpctimeout.awaitresult应该添加属性TimeoutException消息
    assert(reply2.contains(shortTimeout.timeoutProp))

    // Ask with delayed response and allow the Future to timeout before Await.result
    //问延迟反应和允许未来超时之前await.result
    val fut3 = rpcEndpointRef.ask[String](NeverReply("goodbye"), shortTimeout)

    // Allow future to complete with failure using plain Await.result, this will return
    // once the future is complete to verify addMessageIfTimeout was invoked
    //允许将来使用简单的Await.result完成失败,一旦未来完成,将返回验证addMessageIfTimeout是否被调用
    val reply3 =
      intercept[RpcTimeoutException] {
        Await.result(fut3, 200 millis)
      }.getMessage

    // When the future timed out, the recover callback should have used
    // RpcTimeout.addMessageIfTimeout to add the property to the TimeoutException message
    //当未来超时时,恢复回调应该使用RpcTimeout.addMessageIfTimeout将属性添加到TimeoutException消息
    assert(reply3.contains(shortTimeout.timeoutProp))

    // Use RpcTimeout.awaitResult to process Future, since it has already failed with
    // RpcTimeoutException, the same RpcTimeoutException should be thrown
    //使用RpcTimeout.awaitResult来处理Future,因为它已经失败了RpcTimeoutException，应该抛出相同的RpcTimeoutException
    val reply4 =
      intercept[RpcTimeoutException] {
        shortTimeout.awaitResult(fut3)
      }.getMessage

    // Ensure description is not in message twice after addMessageIfTimeout and awaitResult
    //在addMessageIfTimeout和awaitResult之后,请确保描述不在消息中两次
    assert(shortTimeout.timeoutProp.r.findAllIn(reply4).length === 1)
  }

}

class UnserializableClass

class UnserializableException extends Exception {
  private val unserializableField = new UnserializableClass
}
