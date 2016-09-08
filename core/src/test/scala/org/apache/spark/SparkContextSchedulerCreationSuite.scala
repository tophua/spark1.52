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

import org.scalatest.PrivateMethodTester

import org.apache.spark.util.Utils
import org.apache.spark.scheduler.{SchedulerBackend, TaskScheduler, TaskSchedulerImpl}
import org.apache.spark.scheduler.cluster.{SimrSchedulerBackend, SparkDeploySchedulerBackend}
import org.apache.spark.scheduler.cluster.mesos.{CoarseMesosSchedulerBackend, MesosSchedulerBackend}
import org.apache.spark.scheduler.local.LocalBackend
/**
 * 测试创建任务调度器TaskScheduler,同时SparkContext.SparkMasterRegex正测表达测试
 */
class SparkContextSchedulerCreationSuite
  extends SparkFunSuite with LocalSparkContext with PrivateMethodTester with Logging {

  def createTaskScheduler(master: String): TaskSchedulerImpl =
    createTaskScheduler(master, new SparkConf())

  def createTaskScheduler(master: String, conf: SparkConf): TaskSchedulerImpl = {
    // Create local SparkContext to setup a SparkEnv. We don't actually want to start() the
    // real schedulers, so we don't want to create a full SparkContext with the desired scheduler.
    //创建本地sparkcontext设置sparkenv,我们不想实际调用 start()方法,
    sc = new SparkContext("local", "test", conf)
    //Tuple2[SchedulerBackend, TaskScheduler]返回类型
    val createTaskSchedulerMethod =
      PrivateMethod[Tuple2[SchedulerBackend, TaskScheduler]]('createTaskScheduler)
    //invokePrivate反射调用createTaskScheduler方法
    val (_, sched) = SparkContext invokePrivate createTaskSchedulerMethod(sc, master)
    sched.asInstanceOf[TaskSchedulerImpl]//强制类型转换
  }
  //val LOCAL_N_REGEX = """local\[([0-9]+|\*)\]""".r正则表达式
  test("bad-master") {
    val e = intercept[SparkException] {
      createTaskScheduler("localhost:1234")
    }
    assert(e.getMessage.contains("Could not parse Master URL"))
  }

  test("local") {
    val sched = createTaskScheduler("local")
    sched.backend match {//match 偏函数
      // LocalBackend 响应Scheduler的receiveOffers请求，根据可用的CPU核的设定值local[N]直接生成
      //CPU资源返回给Scheduler，并通过executor类在线程池中依次启动和运行scheduler返回的任务列表
      case s: LocalBackend => assert(s.totalCores === 1)
      case _ => fail()
    }
  }

  test("local-*") {
    val sched = createTaskScheduler("local[*]")
    sched.backend match {
      //availableProcessors 返回到Java虚拟机的可用的处理器数量
      case s: LocalBackend => assert(s.totalCores === Runtime.getRuntime.availableProcessors())
      case _ => fail()
    }
  }

  test("local-n") {
    val sched = createTaskScheduler("local[5]")
    assert(sched.maxTaskFailures === 1)
    sched.backend match {
      case s: LocalBackend => assert(s.totalCores === 5)
      case _ => fail()
    }
  }
//val LOCAL_N_FAILURES_REGEX = """local\[([0-9]+|\*)\s*,\s*([0-9]+)\]""".r正则表达式
  test("local-*-n-failures") {
    val sched = createTaskScheduler("local[* ,2]")
    assert(sched.maxTaskFailures === 2)
    sched.backend match {
      //availableProcessors 返回到Java虚拟机的可用的处理器数量
      case s: LocalBackend => assert(s.totalCores === Runtime.getRuntime.availableProcessors())
      case _ => fail()
    }
  }

  test("local-n-failures") {
    val sched = createTaskScheduler("local[4, 2]")
    assert(sched.maxTaskFailures === 2)
    sched.backend match {
      case s: LocalBackend => assert(s.totalCores === 4)
      case _ => fail()
    }
  }

  test("bad-local-n") {
    val e = intercept[SparkException] {
      createTaskScheduler("local[2*]")
    }
    assert(e.getMessage.contains("Could not parse Master URL"))
  }

  test("bad-local-n-failures") {
    val e = intercept[SparkException] {
      createTaskScheduler("local[2*,4]")
    }
    assert(e.getMessage.contains("Could not parse Master URL"))
  }

  test("local-default-parallelism") {
    //默认并行数
    val conf = new SparkConf().set("spark.default.parallelism", "16")
    val sched = createTaskScheduler("local", conf)

    sched.backend match {
      case s: LocalBackend => assert(s.defaultParallelism() === 16)
      case _ => fail()
    }
  }
//val SIMR_REGEX = """simr://(.*)""".r
  test("simr") {
    createTaskScheduler("simr://uri").backend match {
      case s: SimrSchedulerBackend => // OK
      case _ => fail()
    }
  }
//val LOCAL_CLUSTER_REGEX = """local-cluster\[\s*([0-9]+)\s*,\s*([0-9]+)\s*,\s*([0-9]+)\s*]""".r
  test("local-cluster") {
    createTaskScheduler("local-cluster[3, 14, 1024]").backend match {
      case s: SparkDeploySchedulerBackend => // OK
      case _ => fail()
    }
  }

  def testYarn(master: String, expectedClassName: String) {
    try {
      val sched = createTaskScheduler(master)
      assert(sched.getClass === Utils.classForName(expectedClassName))
    } catch {
      case e: SparkException =>
        assert(e.getMessage.contains("YARN mode not available"))
        logWarning("YARN not available, could not test actual YARN scheduler creation")
      case e: Throwable => fail(e)
    }
  }
//
  test("yarn-cluster") {
    testYarn("yarn-cluster", "org.apache.spark.scheduler.cluster.YarnClusterScheduler")
  }

  test("yarn-standalone") {
    testYarn("yarn-standalone", "org.apache.spark.scheduler.cluster.YarnClusterScheduler")
  }

  test("yarn-client") {
    testYarn("yarn-client", "org.apache.spark.scheduler.cluster.YarnScheduler")
  }

  def testMesos(master: String, expectedClass: Class[_], coarse: Boolean) {
    val conf = new SparkConf().set("spark.mesos.coarse", coarse.toString)
    try {
      val sched = createTaskScheduler(master, conf)
      assert(sched.backend.getClass === expectedClass)
    } catch {
      case e: UnsatisfiedLinkError =>
        assert(e.getMessage.contains("mesos"))
        logWarning("Mesos not available, could not test actual Mesos scheduler creation")
      case e: Throwable => fail(e)
    }
  }
// MESOS_REGEX = """(mesos|zk)://.*""".r
  test("mesos fine-grained") {
    testMesos("mesos://localhost:1234", classOf[MesosSchedulerBackend], coarse = false)
  }

  test("mesos coarse-grained") {
    testMesos("mesos://localhost:1234", classOf[CoarseMesosSchedulerBackend], coarse = true)
  }

  test("mesos with zookeeper") {
    testMesos("zk://localhost:1234,localhost:2345", classOf[MesosSchedulerBackend], coarse = false)
  }
}
