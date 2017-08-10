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

import java.io.File

import org.scalatest.concurrent.Timeouts
import org.scalatest.prop.TableDrivenPropertyChecks._
import org.scalatest.time.SpanSugar._

import org.apache.spark.util.Utils

class DriverSuite extends SparkFunSuite with Timeouts {

  test("driver should exit after finishing without cleanup (SPARK-530)") {
    //driver退出后无需清理
    //System.getenv()和System.getProperties()的区别
    //System.getenv() 返回系统环境变量值 设置系统环境变量：当前登录用户主目录下的".bashrc"文件中可以设置系统环境变量
    //System.getProperties() 返回Java进程变量值 通过命令行参数的"-D"选项
    //getOrElse("spark.test.home", fail("spark.test.home is not set!"))
    val sparkHome = sys.props.getOrElse("spark.test.home", "/software/spark152")

   // val masters = Table("master", "local", "local-cluster[2,1,1024]")
   val masters = Table("master", "local", "local[*]")
    forAll(masters) { (master: String) =>
      val process = Utils.executeCommand(
        Seq(s"$sparkHome/bin/spark-class", "org.apache.spark.DriverWithoutCleanup", master),
        new File(sparkHome),
        Map("SPARK_TESTING" -> "1", "SPARK_HOME" -> sparkHome))
      failAfter(60 seconds) { process.waitFor() }
      // Ensure we still kill the process in case it timed out
      //它超时,确保我们仍然杀死过程
      process.destroy()
    }
  }
}

/**
 * Program that creates a Spark driver but doesn't call SparkContext#stop() or
 * sys.exit() after finishing.
  * 创建Spark驱动程序但不调用SparkContext＃stop（）或的程序sys.exit（）完成后。
 */
object DriverWithoutCleanup {
  def main(args: Array[String]) {
    Utils.configTestLog4j("INFO")
    val conf = new SparkConf
    //val sc = new SparkContext(args(0), "DriverWithoutCleanup", conf)
    val sc = new SparkContext("local", "DriverWithoutCleanup", conf)
    sc.parallelize(1 to 100, 4).count()
  }
}
