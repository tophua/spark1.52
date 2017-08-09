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

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.deploy.Command
import org.apache.spark.util.Utils
import org.scalatest.{Matchers, PrivateMethodTester}

class CommandUtilsSuite extends SparkFunSuite with Matchers with PrivateMethodTester {

  ignore("set libraryPath correctly") {//正确设置库路径
    val appId = "12345-worker321-9876"
    //返回当前系统路径名,windows默认PATH
    val libraryPath = Utils.libraryPathEnvName
    //System.getenv()和System.getProperties()的区别
    //System.getenv() 返回系统环境变量值 设置系统环境变量：当前登录用户主目录下的".bashrc"文件中可以设置系统环境变量
    //System.getProperties() 返回Java进程变量值 通过命令行参数的"-D"选项
    //sys.env("spark.testing.home").getOrElse("/software/spark2.1")
    val sparkHome = sys.props.getOrElse("spark.test.home", "/software/spark1.6")
     // val sparkHome = sys.props.getOrElse("spark.test.home", "test.home")
    //Command
    val cmd = new Command("mainClass", Seq(), Map(), Seq(), Seq("libraryPathToB"), Seq())
    //构建ProcessBuilder
   val builder = CommandUtils.buildProcessBuilder(
      cmd, new SecurityManager(new SparkConf), 512, sparkHome, t => t)
   val env = builder.environment
    env.keySet should contain(libraryPath)
    assert(env.get(libraryPath).startsWith("libraryPathToB"))
  }

  test("auth secret shouldn't appear in java opts") {//认证密码不应该出现在java选择
    val buildLocalCommand = PrivateMethod[Command]('buildLocalCommand)
    val conf = new SparkConf
    val secret = "This is the secret sauce"
    // set auth secret 设置秘密
    conf.set(SecurityManager.SPARK_AUTH_SECRET_CONF, secret)
    val command = new Command("mainClass", Seq(), Map(), Seq(), Seq("lib"),
      Seq("-D" + SecurityManager.SPARK_AUTH_SECRET_CONF + "=" + secret))

    // auth is not set auth没有设置
    var cmd = CommandUtils invokePrivate buildLocalCommand(
      command, new SecurityManager(conf), (t: String) => t, Seq(), Map())
    assert(!cmd.javaOpts.exists(_.startsWith("-D" + SecurityManager.SPARK_AUTH_SECRET_CONF)))
    assert(!cmd.environment.contains(SecurityManager.ENV_AUTH_SECRET))

    // auth is set to false auth设置为false
    conf.set(SecurityManager.SPARK_AUTH_CONF, "false")
    cmd = CommandUtils invokePrivate buildLocalCommand(
      command, new SecurityManager(conf), (t: String) => t, Seq(), Map())
    assert(!cmd.javaOpts.exists(_.startsWith("-D" + SecurityManager.SPARK_AUTH_SECRET_CONF)))
    assert(!cmd.environment.contains(SecurityManager.ENV_AUTH_SECRET))

    // auth is set to true auth设置为true
    conf.set(SecurityManager.SPARK_AUTH_CONF, "true")
    cmd = CommandUtils invokePrivate buildLocalCommand(
      command, new SecurityManager(conf), (t: String) => t, Seq(), Map())
    assert(!cmd.javaOpts.exists(_.startsWith("-D" + SecurityManager.SPARK_AUTH_SECRET_CONF)))
    assert(cmd.environment(SecurityManager.ENV_AUTH_SECRET) === secret)
  }
}
