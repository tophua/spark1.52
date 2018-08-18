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

package org.apache.spark.deploy

import java.net.URL

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.io.Source

import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.scheduler.{SparkListenerExecutorAdded, SparkListener}
import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkFunSuite}

class LogUrlsStandaloneSuite extends SparkFunSuite with LocalSparkContext {

  /** 
   *  Length of time to wait while draining listener events.
   *  侦听事件等待时间长度
   *  */
  private val WAIT_TIMEOUT_MILLIS = 10000
  //验证正确的日志网址从工作节点传递
  test("verify that correct log urls get propagated from workers") {
    //sc = new SparkContext("local-cluster[2,1,1024]", "test")
    sc = new SparkContext("local[*]", "test")
    val listener = new SaveExecutorInfo
    sc.addSparkListener(listener)

    // Trigger a job so that executors get added
    //触发工作,添加执行者
    sc.parallelize(1 to 100, 4).map(_.toString).count()
    //println("==="+"=====")
    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    listener.addedExecutorInfos.values.foreach { info =>
       //println("==="+info)
      assert(info.logUrlMap.nonEmpty)
      // Browse to each URL to check that it's valid
      //浏览到每个网址,以检查它的有效性
      info.logUrlMap.foreach { case (logType, logUrl) =>
      //  println("===" + logType)
        val html = Source.fromURL(logUrl).mkString
        assert(html.contains(s"$logType log page"))


    }
    }
  }
 //Spark master和workers使用的公共DNS（默认空）
  ignore("verify that log urls reflect SPARK_PUBLIC_DNS (SPARK-6175)") {
    val SPARK_PUBLIC_DNS = "public_dns"
    class MySparkConf extends SparkConf(false) {
      override def getenv(name: String): String = {
        if (name == "SPARK_PUBLIC_DNS") SPARK_PUBLIC_DNS
        else super.getenv(name)
      }

      override def clone: SparkConf = {
        new MySparkConf().setAll(getAll)
      }
    }
    val conf = new MySparkConf().set(
      "spark.extraListeners", classOf[SaveExecutorInfo].getName)
    //sc = new SparkContext("local-cluster[2,1,1024]", "test", conf)
    sc = new SparkContext("local[*]", "test", conf)

    // Trigger a job so that executors get added
    ///添加执行者触发工作
    sc.parallelize(1 to 100, 4).map(_.toString).count()

    sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
    val listeners = sc.listenerBus.findListenersByClass[SaveExecutorInfo]

    val listener = listeners(0)
    listener.addedExecutorInfos.values.foreach { info =>
      assert(info.logUrlMap.nonEmpty)

      info.logUrlMap.values.foreach { logUrl =>
       // println("==="+logUrl)
        assert(new URL(logUrl).getHost === SPARK_PUBLIC_DNS)

      }
    }
  }
}

private[spark] class SaveExecutorInfo extends SparkListener {
  val addedExecutorInfos = mutable.Map[String, ExecutorInfo]()

  override def onExecutorAdded(executor: SparkListenerExecutorAdded) {
    addedExecutorInfos(executor.executorId) = executor.executorInfo
  }
}
