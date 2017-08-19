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

package org.apache.spark.repl

import java.io.File
import java.net.{URL, URLClassLoader}

import scala.concurrent.duration._
import scala.language.implicitConversions
import scala.language.postfixOps

import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Interruptor
import org.scalatest.concurrent.Timeouts._
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._

import org.apache.spark._
import org.apache.spark.util.Utils

class ExecutorClassLoaderSuite
  extends SparkFunSuite
  with BeforeAndAfterAll
  with MockitoSugar
  with Logging {

  val childClassNames = List("ReplFakeClass1", "ReplFakeClass2")
  val parentClassNames = List("ReplFakeClass1", "ReplFakeClass2", "ReplFakeClass3")
  var tempDir1: File = _
  var tempDir2: File = _
  var url1: String = _
  var urls2: Array[URL] = _
  var classServer: HttpServer = _

  override def beforeAll() {
    super.beforeAll()
    tempDir1 = Utils.createTempDir()
    tempDir2 = Utils.createTempDir()
    url1 = "file://" + tempDir1
    urls2 = List(tempDir2.toURI.toURL).toArray
    childClassNames.foreach(TestUtils.createCompiledClass(_, tempDir1, "1"))
    parentClassNames.foreach(TestUtils.createCompiledClass(_, tempDir2, "2"))
  }

  override def afterAll() {
    super.afterAll()
    if (classServer != null) {
      classServer.stop()
    }
    Utils.deleteRecursively(tempDir1)
    Utils.deleteRecursively(tempDir2)
    SparkEnv.set(null)
  }

  test("child first") {
    val parentLoader = new URLClassLoader(urls2, null)
    val classLoader = new ExecutorClassLoader(new SparkConf(), url1, parentLoader, true)
    val fakeClass = classLoader.loadClass("ReplFakeClass2").newInstance()
    val fakeClassVersion = fakeClass.toString
    assert(fakeClassVersion === "1")
  }
//
  test("parent first") {
    val parentLoader = new URLClassLoader(urls2, null)
    val classLoader = new ExecutorClassLoader(new SparkConf(), url1, parentLoader, false)
    val fakeClass = classLoader.loadClass("ReplFakeClass1").newInstance()
    val fakeClassVersion = fakeClass.toString
    assert(fakeClassVersion === "2")
  }
  //子第一次可以倒退
  test("child first can fall back") {
    val parentLoader = new URLClassLoader(urls2, null)
    val classLoader = new ExecutorClassLoader(new SparkConf(), url1, parentLoader, true)
    val fakeClass = classLoader.loadClass("ReplFakeClass3").newInstance()
    val fakeClassVersion = fakeClass.toString
    assert(fakeClassVersion === "2")
  }
//子第一次可以失败
  test("child first can fail") {
    val parentLoader = new URLClassLoader(urls2, null)
    val classLoader = new ExecutorClassLoader(new SparkConf(), url1, parentLoader, true)
    intercept[java.lang.ClassNotFoundException] {
      classLoader.loadClass("ReplFakeClassDoesNotExist").newInstance()
    }
  }
  //无法从HTTP服务器获取类不应泄漏资源（SPARK-6209）
 test("failing to fetch classes from HTTP server should not leak resources (SPARK-6209)") {
    // This is a regression test for SPARK-6209, a bug where each failed attempt to load a class
    // from the driver's class server would leak a HTTP connection, causing the class server's
    // thread / connection pool to be exhausted.
   //这是SPARK-6209的回归测试,这是一个错误,每次尝试从驱动程序的类服务器加载类时都会泄漏HTTP连接,导致类服务器的线程/连接池耗尽。
    val conf = new SparkConf()
    val securityManager = new SecurityManager(conf)
    classServer = new HttpServer(conf, tempDir1, securityManager)
    classServer.start()
    // ExecutorClassLoader uses SparkEnv's SecurityManager, so we need to mock this
    //ExecutorClassLoader使用SparkEnv的SecurityManager,所以我们需要模拟这个
    val mockEnv = mock[SparkEnv]
    when(mockEnv.securityManager).thenReturn(securityManager)
    SparkEnv.set(mockEnv)
    // Create an ExecutorClassLoader that's configured to load classes from the HTTP server
   //创建一个配置为从HTTP服务器加载类的ExecutorClassLoader
    val parentLoader = new URLClassLoader(Array.empty, null)
    val classLoader = new ExecutorClassLoader(conf, classServer.uri, parentLoader, false)
    classLoader.httpUrlConnectionTimeoutMillis = 500
    // Check that this class loader can actually load classes that exist
   //检查这个类加载器是否可以实际加载存在的类
    val fakeClass = classLoader.loadClass("ReplFakeClass2").newInstance()
    val fakeClassVersion = fakeClass.toString
    assert(fakeClassVersion === "1")
    // Try to perform a full GC now, since GC during the test might mask resource leaks
   //现在尝试执行完整的GC,因为测试过程中的GC可能会屏蔽资源泄漏
    System.gc()
    // When the original bug occurs, the test thread becomes blocked in a classloading call
    // and does not respond to interrupts.  Therefore, use a custom ScalaTest interruptor to
    // shut down the HTTP server when the test times out
   //当原始错误发生时,测试线程在类加载调用中被阻止,并且不响应中断.
    // 因此,当测试超时时,使用自定义ScalaTest中断器关闭HTTP服务器
    val interruptor: Interruptor = new Interruptor {
      override def apply(thread: Thread): Unit = {
        classServer.stop()
        classServer = null
        thread.interrupt()
      }
    }
    def tryAndFailToLoadABunchOfClasses(): Unit = {
      // The number of trials here should be much larger than Jetty's thread / connection limit
      // in order to expose thread or connection leaks
      //这里的尝试次数应该远大于Jetty的线程/连接限制,以暴露线程或连接泄漏
      for (i <- 1 to 1000) {
        if (Thread.currentThread().isInterrupted) {
          throw new InterruptedException()
        }
        // Incorporate the iteration number into the class name in order to avoid any response
        // caching that might be added in the future
        //将迭代号并入类名,以避免将来可能添加的任何响应缓存
        intercept[ClassNotFoundException] {
          classLoader.loadClass(s"ReplFakeClassDoesNotExist$i").newInstance()
        }
      }
    }
    failAfter(10 seconds)(tryAndFailToLoadABunchOfClasses())(interruptor)
  }

}
