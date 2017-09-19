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

package org.apache.spark.ui

import java.net.ServerSocket

import scala.io.Source
import scala.util.{Failure, Success, Try}

import org.eclipse.jetty.servlet.ServletContextHandler
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._

import org.apache.spark.LocalSparkContext._
import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}

class UISuite extends SparkFunSuite {

  /**
   * Create a test SparkContext with the SparkUI enabled.
   * It is safe to `get` the SparkUI directly from the SparkContext returned here.
    * 在启用SparkUI的情况下创建一个测试SparkContext。
    *从这里返回的SparkContext直接“获取”SparkUI是安全的。
   */
  private def newSparkContext(): SparkContext = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      .set("spark.ui.enabled", "true")
    val sc = new SparkContext(conf)
    assert(sc.ui.isDefined)
    sc
  }

  test("basic ui visibility") {//基本可见
    //柯里化方法
    withSpark(newSparkContext()) { sc =>
      // test if the ui is visible, and all the expected tabs are visible
      //测试ui是否可见,并且所有预期的选项卡都可见
      eventually(timeout(10 seconds), interval(50 milliseconds)) {
        val html = Source.fromURL(sc.ui.get.appUIAddress).mkString
        println("===="+html)
        assert(!html.contains("random data that should not be present"))
        assert(html.toLowerCase.contains("stages"))
        assert(html.toLowerCase.contains("storage"))
        assert(html.toLowerCase.contains("environment"))
        assert(html.toLowerCase.contains("executors"))
      }
    }
  }

  test("visibility at localhost:4040") {//可见性
    withSpark(newSparkContext()) { sc =>
      // test if visible from http://localhost:4040
      //可以访问http://localhost:4040,本地启动spark-shell
      eventually(timeout(10 seconds), interval(50 milliseconds)) {
        val html = Source.fromURL("http://localhost:4040").mkString
        println("===="+html)
        assert(html.toLowerCase.contains("stages"))
      }
    }
  }
  //jetty选择处于争用不同的端口
  test("jetty selects different port under contention") {
    val server = new ServerSocket(0)
    val startPort = server.getLocalPort
    val serverInfo1 = JettyUtils.startJettyServer(
      "0.0.0.0", startPort, Seq[ServletContextHandler](), new SparkConf)
    val serverInfo2 = JettyUtils.startJettyServer(
      "0.0.0.0", startPort, Seq[ServletContextHandler](), new SparkConf)
    // Allow some wiggle room in case ports on the machine are under contention
    //如果机器上的端口受到争用，请允许一些摆动的空间
    val boundPort1 = serverInfo1.boundPort
    val boundPort2 = serverInfo2.boundPort
    assert(boundPort1 != startPort)
    assert(boundPort2 != startPort)
    assert(boundPort1 != boundPort2)
    serverInfo1.server.stop()
    serverInfo2.server.stop()
    server.close()
  }
  //jetty正确绑定到端口0
  test("jetty binds to port 0 correctly") {
    val serverInfo = JettyUtils.startJettyServer(
      "0.0.0.0", 0, Seq[ServletContextHandler](), new SparkConf)
    val server = serverInfo.server
    val boundPort = serverInfo.boundPort
    assert(server.getState === "STARTED")
    assert(boundPort != 0)
    Try { new ServerSocket(boundPort) } match {
      case Success(s) => fail("Port %s doesn't seem used by jetty server".format(boundPort))
      case Failure(e) =>
    }
  }
  //验证appUIAddress包含该http方案
  test("verify appUIAddress contains the scheme") {
    withSpark(newSparkContext()) { sc =>
      val ui = sc.ui.get
      val uiAddress = ui.appUIAddress

      val uiHostPort = ui.appUIHostPort
      //http://192.168.100.227:4042===192.168.100.227:4042
      println(uiAddress+"==="+uiHostPort)
      assert(uiAddress.equals("http://" + uiHostPort))
    }
  }
  //验证appUIAddress包含端口
  test("verify appUIAddress contains the port") {
    withSpark(newSparkContext()) { sc =>
      val ui = sc.ui.get
      //http://192.168.100.227:4042
      val splitUIAddress = ui.appUIAddress.split(':')
      val boundPort = ui.boundPort
      println(splitUIAddress.toList.mkString(",")+"==="+boundPort)
      assert(splitUIAddress(2).toInt == boundPort)
    }
  }
}
