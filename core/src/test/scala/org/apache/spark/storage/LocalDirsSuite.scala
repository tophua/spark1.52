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

package org.apache.spark.storage

import java.io.File

import org.apache.spark.util.Utils
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkConf, SparkFunSuite}


/**
 * Tests for the spark.local.dir and SPARK_LOCAL_DIRS configuration options.
  * 测试spark.local.dir和SPARK_LOCAL_DIRS配置选项。
 */
class LocalDirsSuite extends SparkFunSuite with BeforeAndAfter {

  before {
    Utils.clearLocalRootDirs()
  }
    //返回一个有效的目录,即使一些本地目录丢失
  test("Utils.getLocalDir() returns a valid directory, even if some local dirs are missing") {
    // Regression test for SPARK-2974
    assert(!new File("/NONEXISTENT_DIR").exists())
    val conf = new SparkConf(false)
      .set("spark.local.dir", s"/NONEXISTENT_PATH,${System.getProperty("java.io.tmpdir")}")
    println("===="+new File(Utils.getLocalDir(conf)).getName)
    assert(new File(Utils.getLocalDir(conf)).exists())
  }

  test("SPARK_LOCAL_DIRS override also affects driver") {//重写也会影响驱动程序
    // Regression test for SPARK-2975
    assert(!new File("/NONEXISTENT_DIR").exists())
    // SPARK_LOCAL_DIRS is a valid directory:
    //SPARK_LOCAL_DIRS是一个有效的目录
    class MySparkConf extends SparkConf(false) {
      override def getenv(name: String): String = {
        if (name == "SPARK_LOCAL_DIRS") System.getProperty("java.io.tmpdir")
        else super.getenv(name)
      }

      override def clone: SparkConf = {
        new MySparkConf().setAll(getAll)
      }
    }
    // spark.local.dir only contains invalid directories, but that's not a problem since
    // SPARK_LOCAL_DIRS will override it on both the driver and workers:
    //用于暂存空间的目录,该目录用于保存map输出文件或者转储RDD
    val conf = new MySparkConf().set("spark.local.dir", "/NONEXISTENT_PATH")
    assert(new File(Utils.getLocalDir(conf)).exists())
  }

}
