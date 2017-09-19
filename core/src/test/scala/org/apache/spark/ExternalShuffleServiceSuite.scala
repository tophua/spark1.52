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

package org.apache.spark

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.network.TransportContext
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.server.TransportServer
import org.apache.spark.network.shuffle.{ExternalShuffleBlockHandler, ExternalShuffleClient}

/**
 * This suite creates an external shuffle server and routes all shuffle fetches through it.
 * Note that failures in this suite may arise due to changes in Spark that invalidate expectations
 * set up in [[ExternalShuffleBlockHandler]], such as changing the format of shuffle files or how
 * we hash files into folders.
 */
class ExternalShuffleServiceSuite extends ShuffleSuite with BeforeAndAfterAll {
  var server: TransportServer = _
  var rpcHandler: ExternalShuffleBlockHandler = _

  override def beforeAll() {
    val transportConf = SparkTransportConf.fromSparkConf(conf, numUsableCores = 2)
    rpcHandler = new ExternalShuffleBlockHandler(transportConf)
    val transportContext = new TransportContext(transportConf, rpcHandler)
    server = transportContext.createServer()

    conf.set("spark.shuffle.manager", "sort")
    conf.set("spark.shuffle.service.enabled", "true")
    conf.set("spark.shuffle.service.port", server.getPort.toString)//端口随机
  }

  override def afterAll() {
    server.close()
  }

  // This test ensures that the external shuffle service is actually in use for the other tests.
  //此测试确保外部shuffle服务,在其他测试中实际使用
  /**
  test("using external shuffle service") {
    sc = new SparkContext("local-cluster[2,1,1024]", "test", conf)
    sc.env.blockManager.externalShuffleServiceEnabled should equal(true)
    sc.env.blockManager.shuffleClient.getClass should equal(classOf[ExternalShuffleClient])

    // In a slow machine, one slave may register hundreds of milliseconds ahead of the other one.
    // If we don't wait for all slaves, it's possible that only one executor runs all jobs. Then
    // all shuffle blocks will be in this executor, ShuffleBlockFetcherIterator will directly fetch
    // local blocks from the local BlockManager and won't send requests to ExternalShuffleService.
    // In this case, we won't receive FetchFailed. And it will make this test fail.
    // Therefore, we should wait until all slaves are up
    //在一个缓慢的机器中,一个从节点可能会在另一个机器前登记数百毫秒
    //如果我们不等待所有的从节点,这是可能的,只有一个执行者运行所有的工作,然后所有的shuffle块将在这个执行者
    sc.jobProgressListener.waitUntilExecutorsUp(2, 10000)

    val rdd = sc.parallelize(0 until 1000, 10).map(i => (i, 1)).reduceByKey(_ + _)

    rdd.count()
    rdd.count()

    // Invalidate the registered executors, disallowing access to their shuffle blocks (without
    // deleting the actual shuffle files, so we could access them without the shuffle service).
    //无效的注册者,不允许访问他们的shuffle块
    rpcHandler.applicationRemoved(sc.conf.getAppId, false /* cleanupLocalDirs */)

    // Now Spark will receive FetchFailed, and not retry the stage due to "spark.test.noStageRetry"
    // being set.
    val e = intercept[SparkException] {
      rdd.count()
    }
    e.getMessage should include ("Fetch failure will not retry stage due to testing config")
  }**/
}
