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

package org.apache.spark.deploy.worker

import java.io.File

import scala.collection.JavaConversions._

import org.apache.spark.deploy.{ApplicationDescription, Command, ExecutorState}
import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
//Worker 通过持有 ExecutorRunner 对象来控制 CoarseGrainedExecutorBackend 的启停
class ExecutorRunnerTest extends SparkFunSuite {
  test("command includes appId") {//命令包括AppID
    val appId = "12345-worker321-9876"
    val conf = new SparkConf
    //System.getenv()和System.getProperties()的区别
    //System.getenv() 返回系统环境变量值 设置系统环境变量：当前登录用户主目录下的".bashrc"文件中可以设置系统环境变量
    //System.getProperties() 返回Java进程变量值 通过命令行参数的"-D"选项
    val sparkHome = sys.props.getOrElse("spark.test.home", fail("spark.test.home is not set!"))
    val appDesc = new ApplicationDescription("app name", Some(8), 500,
      Command("foo", Seq(appId), Map(), Seq(), Seq(), Seq()), "appUiUrl")
    //Worker 通过持有 ExecutorRunner 对象来控制 CoarseGrainedExecutorBackend 的启停
    val er = new ExecutorRunner(appId, 1, appDesc, 8, 500, null, "blah", "worker321", 123,
      "publicAddr", new File(sparkHome), new File("ooga"), "blah", conf, Seq("localDir"),
      ExecutorState.RUNNING)
  val builder = CommandUtils.buildProcessBuilder(
      appDesc.command, new SecurityManager(conf), 512, sparkHome, er.substituteVariables)
    assert(builder.command().last === appId)
  }
}
