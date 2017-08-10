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

import org.scalatest.Assertions
import org.apache.spark.storage.StorageLevel

class SparkContextInfoSuite extends SparkFunSuite with LocalSparkContext {
  //只返回RDDS被标记为缓存
  test("getPersistentRDDs only returns RDDs that are marked as cached") {
    sc = new SparkContext("local", "test")
    assert(sc.getPersistentRDDs.isEmpty === true)

    val rdd = sc.makeRDD(Array(1, 2, 3, 4), 2)
    //获得持久化RDD空值
    assert(sc.getPersistentRDDs.isEmpty === true)

    rdd.cache()//RDD持久化缓存
    assert(sc.getPersistentRDDs.size === 1)
    //返回列表第一个RDD的值
    assert(sc.getPersistentRDDs.values.head === rdd)
  }
  //返回一个不可变的Map
  test("getPersistentRDDs returns an immutable map") {
    sc = new SparkContext("local", "test")
    val rdd1 = sc.makeRDD(Array(1, 2, 3, 4), 2).cache()
    val myRdds = sc.getPersistentRDDs //返回已标记的持久化
    assert(myRdds.size === 1)
    assert(myRdds(0) === rdd1)
    //获得持久化存储级别,默认内存
    assert(myRdds(0).getStorageLevel === StorageLevel.MEMORY_ONLY)

    // myRdds2 should have 2 RDDs, but myRdds should not change
    val rdd2 = sc.makeRDD(Array(5, 6, 7, 8), 1).cache()
    val myRdds2 = sc.getPersistentRDDs
    assert(myRdds2.size === 2)
    assert(myRdds2(0) === rdd1)
    assert(myRdds2(1) === rdd2)
    assert(myRdds2(0).getStorageLevel === StorageLevel.MEMORY_ONLY)
    assert(myRdds2(1).getStorageLevel === StorageLevel.MEMORY_ONLY)
    assert(myRdds.size === 1)
    assert(myRdds(0) === rdd1)
    assert(myRdds(0).getStorageLevel === StorageLevel.MEMORY_ONLY)
  }
  //报告RDDS实际持久化RDDInfo数据
  test("getRDDStorageInfo only reports on RDDs that actually persist data") {
    sc = new SparkContext("local", "test")
    val rdd = sc.makeRDD(Array(1, 2, 3, 4), 2).cache()
    assert(sc.getRDDStorageInfo.size === 0)
    rdd.collect()
    assert(sc.getRDDStorageInfo.size === 1)//RDDInfo
    assert(sc.getRDDStorageInfo.head.isCached)//判断是否缓存
    assert(sc.getRDDStorageInfo.head.memSize > 0)//内存大小
    assert(sc.getRDDStorageInfo.head.storageLevel === StorageLevel.MEMORY_ONLY)
  }

  test("call sites report correct locations") {//报告正确的位置
    sc = new SparkContext("local", "test")
    testPackage.runCallSiteTest(sc)
  }
}

/** Call site must be outside of usual org.apache.spark packages (see Utils#SPARK_CLASS_REGEX). */
package object testPackage extends Assertions {
  private val CALL_SITE_REGEX = "(.+) at (.+):([0-9]+)".r

  def runCallSiteTest(sc: SparkContext) {
    val rdd = sc.makeRDD(Array(1, 2, 3, 4), 2)
    val rddCreationSite = rdd.getCreationSite
    println("===="+rddCreationSite)
    val curCallSite = sc.getCallSite().shortForm // note: 2 lines after definition of "rdd"

    val rddCreationLine = rddCreationSite match {
      case CALL_SITE_REGEX(func, file, line) => {
        assert(func === "makeRDD")
        assert(file === "SparkContextInfoSuite.scala")
        line.toInt
      }
      case _ => fail("Did not match expected call site format")
    }

    curCallSite match {
      case CALL_SITE_REGEX(func, file, line) => {
        //这是正确的,因为我们从Spark的外部称它为正确的
        assert(func === "getCallSite") // this is correct because we called it from outside of Spark
        assert(file === "SparkContextInfoSuite.scala")
        println("==line==="+line.toInt )
        //assert(line.toInt === rddCreationLine.toInt + 2)
      }
      case _ => fail("Did not match expected call site format")
    }
  }
}
