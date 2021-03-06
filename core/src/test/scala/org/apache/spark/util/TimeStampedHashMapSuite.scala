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

package org.apache.spark.util

import java.lang.ref.WeakReference

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.apache.spark.SparkFunSuite

class TimeStampedHashMapSuite extends SparkFunSuite {

  // Test the testMap function - a Scala HashMap should obviously pass
  //试验测试映射函数
  testMap(new mutable.HashMap[String, String]())

  // Test TimeStampedHashMap basic functionality 基本功能测试
  testMap(new TimeStampedHashMap[String, String]())
  testMapThreadSafety(new TimeStampedHashMap[String, String]())

  // Test TimeStampedWeakValueHashMap basic functionality
  testMap(new TimeStampedWeakValueHashMap[String, String]())
  testMapThreadSafety(new TimeStampedWeakValueHashMap[String, String]())

  test("TimeStampedHashMap - clearing by timestamp") {//清理的时间
    // clearing by insertion time 清除插入时间
    //更新时间戳获取
    val map = new TimeStampedHashMap[String, String](updateTimeStampOnGet = false)
    map("k1") = "v1"
    assert(map("k1") === "v1")
    Thread.sleep(10)
    val threshTime = System.currentTimeMillis
    assert(map.getTimestamp("k1").isDefined)
    println(map.getTimestamp("k1").get +"====="+threshTime)
    assert(map.getTimestamp("k1").get < threshTime)
    map.clearOldValues(threshTime)//清除旧的值
    assert(map.get("k1") === None)

    // clearing by modification time
    //修改清理时间
    val map1 = new TimeStampedHashMap[String, String](updateTimeStampOnGet = true)
    map1("k1") = "v1"
    map1("k2") = "v2"
    assert(map1("k1") === "v1")
    Thread.sleep(10)
    val threshTime1 = System.currentTimeMillis
    Thread.sleep(10)
    //访问K2更新其访问时间> threshtime
    assert(map1("k2") === "v2")     // access k2 to update its access time to > threshTime
    assert(map1.getTimestamp("k1").isDefined)
    assert(map1.getTimestamp("k1").get < threshTime1)
    assert(map1.getTimestamp("k2").isDefined)
    assert(map1.getTimestamp("k2").get >= threshTime1)
    map1.clearOldValues(threshTime1) // should only clear k1,应该只清理k1
    assert(map1.get("k1") === None)
    assert(map1.get("k2").isDefined)
  }
  //按时间戳清除
  test("TimeStampedWeakValueHashMap - clearing by timestamp") {
    // clearing by insertion time 通过插入时间清除
    val map = new TimeStampedWeakValueHashMap[String, String](updateTimeStampOnGet = false)
    map("k1") = "v1"
    assert(map("k1") === "v1")
    Thread.sleep(10)
    val threshTime = System.currentTimeMillis
    assert(map.getTimestamp("k1").isDefined)
    assert(map.getTimestamp("k1").get < threshTime)
    map.clearOldValues(threshTime)
    assert(map.get("k1") === None)

    // clearing by modification time 按修改时间清除
    val map1 = new TimeStampedWeakValueHashMap[String, String](updateTimeStampOnGet = true)
    map1("k1") = "v1"
    map1("k2") = "v2"
    assert(map1("k1") === "v1")
    Thread.sleep(10)
    val threshTime1 = System.currentTimeMillis
    Thread.sleep(10)
    //访问k2将其访问时间更新为> threshTime
    assert(map1("k2") === "v2")     // access k2 to update its access time to > threshTime
    assert(map1.getTimestamp("k1").isDefined)
    assert(map1.getTimestamp("k1").get < threshTime1)
    assert(map1.getTimestamp("k2").isDefined)
    assert(map1.getTimestamp("k2").get >= threshTime1)
    map1.clearOldValues(threshTime1) // should only clear k1
    assert(map1.get("k1") === None)
    assert(map1.get("k2").isDefined)
  }

  test("TimeStampedWeakValueHashMap - clearing weak references") {//清除弱引用
    var strongRef = new Object
    val weakRef = new WeakReference(strongRef)
    val map = new TimeStampedWeakValueHashMap[String, Object]
    map("k1") = strongRef
    map("k2") = "v2"
    map("k3") = "v3"
    val isEquals = map("k1") == strongRef
    assert(isEquals)

    // clear strong reference to "k1"
    //清晰的强引用“K1”
    strongRef = null
    val startTime = System.currentTimeMillis
    //尽最大努力运行垃圾收集。 它通常*运行GC。
    System.gc() // Make a best effort to run the garbage collection. It *usually* runs GC.
    //尽力在所有清洁的对象上调用终结器。
    System.runFinalization()  // Make a best effort to call finalizer on all cleaned objects.
    while(System.currentTimeMillis - startTime < 10000 && weakRef.get != null) {
      System.gc()
      System.runFinalization()
      Thread.sleep(100)
    }
    assert(map.getReference("k1").isDefined)
    val ref = map.getReference("k1").get
    assert(ref.get === null)
    assert(map.get("k1") === None)

    // operations should only display non-null entries
    //操作应该只显示非空条目
    assert(map.iterator.forall { case (k, v) => k != "k1" })
    assert(map.filter { case (k, v) => k != "k2" }.size === 1)
    assert(map.filter { case (k, v) => k != "k2" }.head._1 === "k3")
    assert(map.toMap.size === 2)
    assert(map.toMap.forall { case (k, v) => k != "k1" })
    val buffer = new ArrayBuffer[String]
    map.foreach { case (k, v) => buffer += v.toString }
    assert(buffer.size === 2)
    assert(buffer.forall(_ != "k1"))
    val plusMap = map + (("k4", "v4"))
    assert(plusMap.size === 3)
    assert(plusMap.forall { case (k, v) => k != "k1" })
    val minusMap = map - "k2"
    assert(minusMap.size === 1)
    assert(minusMap.head._1 == "k3")

    // clear null values - should only clear k1
    //清空值应该只有明确K1
    map.clearNullValues()
    assert(map.getReference("k1") === None)
    assert(map.get("k1") === None)
    assert(map.get("k2").isDefined)
    assert(map.get("k2").get === "v2")
  }

  /** 
   *  Test basic operations of a Scala mutable Map.
   *  测试Scla一个可变Map基本操作
   *  */
  def testMap(hashMapConstructor: => mutable.Map[String, String]) {
    def newMap() = hashMapConstructor
    val testMap1 = newMap()
    val testMap2 = newMap()
    val name = testMap1.getClass.getSimpleName

    test(name + " - basic test") {
      // put, get, and apply
      //添加,获取
      testMap1 += (("k1", "v1"))
      assert(testMap1.get("k1").isDefined)
      assert(testMap1.get("k1").get === "v1")
      testMap1("k2") = "v2"
      assert(testMap1.get("k2").isDefined)
      assert(testMap1.get("k2").get === "v2")
      assert(testMap1("k2") === "v2")
      testMap1.update("k3", "v3")
      assert(testMap1.get("k3").isDefined)
      assert(testMap1.get("k3").get === "v3")

      // remove 删除
      testMap1.remove("k1")
      assert(testMap1.get("k1").isEmpty)
      testMap1.remove("k2")
      intercept[NoSuchElementException] {
        testMap1("k2") // Map.apply(<non-existent-key>) causes exception
      }
      testMap1 -= "k3"
      assert(testMap1.get("k3").isEmpty)

      // multi put
      //多种插入
      val keys = (1 to 100).map(_.toString)
      val pairs = keys.map(x => (x, x * 2))
      assert((testMap2 ++ pairs).iterator.toSet === pairs.toSet)
      testMap2 ++= pairs

      // iterator
      //迭代
      assert(testMap2.iterator.toSet === pairs.toSet)

      // filter
      //过虑
      val filtered = testMap2.filter { case (_, v) => v.toInt % 2 == 0 }
      val evenPairs = pairs.filter { case (_, v) => v.toInt % 2 == 0 }
      assert(filtered.iterator.toSet === evenPairs.toSet)

      // foreach
      //循环数组
      val buffer = new ArrayBuffer[(String, String)]
      testMap2.foreach(x => buffer += x)
      assert(testMap2.toSet === buffer.toSet)

      // multi remove
      //多种删除
      testMap2("k1") = "v1"
      testMap2 --= keys
      assert(testMap2.size === 1)
      assert(testMap2.iterator.toSeq.head === ("k1", "v1"))

      // +
      val testMap3 = testMap2 + (("k0", "v0"))
      assert(testMap3.size === 2)
      assert(testMap3.get("k1").isDefined)
      assert(testMap3.get("k1").get === "v1")
      assert(testMap3.get("k0").isDefined)
      assert(testMap3.get("k0").get === "v0")

      // -
      val testMap4 = testMap3 - "k0"
      assert(testMap4.size === 1)
      assert(testMap4.get("k1").isDefined)
      assert(testMap4.get("k1").get === "v1")
    }
  }

  /** 
   *  Test thread safety of a Scala mutable map. 
   *  测试线程安全可变Map
   *  */
  def testMapThreadSafety(hashMapConstructor: => mutable.Map[String, String]) {
    def newMap() = hashMapConstructor
    val name = newMap().getClass.getSimpleName
    val testMap = newMap()
    @volatile var error = false

    def getRandomKey(m: mutable.Map[String, String]): Option[String] = {
      val keys = testMap.keysIterator.toSeq
      if (keys.nonEmpty) {
        Some(keys(Random.nextInt(keys.size)))
      } else {
        None
      }
    }

    val threads = (1 to 25).map(i => new Thread() {
      override def run() {
        try {
          for (j <- 1 to 1000) {
            Random.nextInt(3) match {
              case 0 =>
                testMap(Random.nextString(10)) = Random.nextDouble().toString // put,插入
              case 1 =>
                getRandomKey(testMap).map(testMap.get) // get 获得
              case 2 =>
                getRandomKey(testMap).map(testMap.remove) // remove 删除
            }
          }
        } catch {
          case t: Throwable =>
            error = true
            throw t
        }
      }
    })

    test(name + " - threading safety test")  {//测试线程安全性
      threads.map(_.start)
      threads.map(_.join)
      assert(!error)
    }
  }
}
