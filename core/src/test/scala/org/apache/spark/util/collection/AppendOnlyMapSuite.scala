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

package org.apache.spark.util.collection

import java.util.Comparator

import scala.collection.mutable.HashSet

import org.apache.spark.SparkFunSuite
//迭代 AppendOnlyMap 中的元素的时候，从前到后扫描输出。
//如果 Array 的利用率达到 70%，那么就扩张一倍，并对所有 key 进行 rehash 后，重新排列每个 key 的位置。
//AppendOnlyMap 还有一个 destructiveSortedIterator(): Iterator[(K, V)] 方法，可以返回 Array 中排序后的 (K, V) pairs。
// 实现方法很简单：先将所有 (K, V) pairs compact 到 Array 的前端，并使得每个 (K, V) 占一个位置（原来占两个），
// 之后直接调用 Array.sort() 排序，不过这样做会破坏数组（key 的位置变化了）。
class AppendOnlyMapSuite extends SparkFunSuite {
  test("initialization") {//初始化
    val goodMap1 = new AppendOnlyMap[Int, Int](1)
    assert(goodMap1.size === 0)
    val goodMap2 = new AppendOnlyMap[Int, Int](255)
    assert(goodMap2.size === 0)
    val goodMap3 = new AppendOnlyMap[Int, Int](256)
    assert(goodMap3.size === 0)
    intercept[IllegalArgumentException] {
      new AppendOnlyMap[Int, Int](1 << 30) // Invalid map size: bigger than 2^29
    }
    intercept[IllegalArgumentException] {
      new AppendOnlyMap[Int, Int](-1)
    }
    intercept[IllegalArgumentException] {
      new AppendOnlyMap[Int, Int](0)
    }
  }

  test("object keys and values") {//对象键和值
    val map = new AppendOnlyMap[String, String]()
    for (i <- 1 to 100) {
      map("" + i) = "" + i
    }
    assert(map.size === 100)
    for (i <- 1 to 100) {
      assert(map("" + i) === "" + i)
    }
    assert(map("0") === null)
    assert(map("101") === null)
    assert(map(null) === null)
    val set = new HashSet[(String, String)]
    for ((k, v) <- map) {   // Test the foreach method
      set += ((k, v))
    }
    assert(set === (1 to 100).map(_.toString).map(x => (x, x)).toSet)
  }

  test("primitive keys and values") {//原始键和值
    val map = new AppendOnlyMap[Int, Int]()
    for (i <- 1 to 100) {
      map(i) = i
    }
    assert(map.size === 100)
    for (i <- 1 to 100) {
      assert(map(i) === i)
    }
    assert(map(0) === null)
    assert(map(101) === null)
    val set = new HashSet[(Int, Int)]
    for ((k, v) <- map) {   // Test the foreach method
      set += ((k, v))
    }
    assert(set === (1 to 100).map(x => (x, x)).toSet)
  }

  test("null keys") {//空键
    val map = new AppendOnlyMap[String, String]()
    for (i <- 1 to 100) {
      map("" + i) = "" + i
    }
    assert(map.size === 100)
    assert(map(null) === null)
    map(null) = "hello"
    assert(map.size === 101)
    assert(map(null) === "hello")
  }

  test("null values") {//空值
    val map = new AppendOnlyMap[String, String]()
    for (i <- 1 to 100) {
      map("" + i) = null
    }
    assert(map.size === 100)
    assert(map("1") === null)
    assert(map(null) === null)
    assert(map.size === 100)
    map(null) = null
    assert(map.size === 101)
    assert(map(null) === null)
  }

  test("changeValue") {
    val map = new AppendOnlyMap[String, String]()
    for (i <- 1 to 100) {
      map("" + i) = "" + i
    }
    assert(map.size === 100)
    for (i <- 1 to 100) {
      val res = map.changeValue("" + i, (hadValue, oldValue) => {
        assert(hadValue === true)
        assert(oldValue === "" + i)
        oldValue + "!"
      })
      assert(res === i + "!")
    }
    // Iterate from 101 to 400 to make sure the map grows a couple of times, because we had a
    // bug where changeValue would return the wrong result when the map grew on that insert
    //从101到400进行迭代以确保Map增长
    for (i <- 101 to 400) {
      val res = map.changeValue("" + i, (hadValue, oldValue) => {
        assert(hadValue === false)
        i + "!"
      })
      assert(res === i + "!")
    }
    assert(map.size === 400)
    assert(map(null) === null)
    map.changeValue(null, (hadValue, oldValue) => {
      assert(hadValue === false)
      "null!"
    })
    assert(map.size === 401)
    map.changeValue(null, (hadValue, oldValue) => {
      assert(hadValue === true)
      assert(oldValue === "null!")
      "null!!"
    })
    assert(map.size === 401)
  }

  test("inserting in capacity-1 map") {//插入容量-1的Map任务
    val map = new AppendOnlyMap[String, String](1)
    for (i <- 1 to 100) {
      map("" + i) = "" + i
    }
    assert(map.size === 100)
    for (i <- 1 to 100) {
      assert(map("" + i) === "" + i)
    }
  }

  test("destructive sort") {//破坏性排序
    val map = new AppendOnlyMap[String, String]()
    for (i <- 1 to 100) {
      map("" + i) = "" + i
    }
    map.update(null, "happy new year!")

    try {
      map.apply("1")
      map.update("1", "2013")
      map.changeValue("1", (hadValue, oldValue) => "2014")
      map.iterator
    } catch {
      case e: IllegalStateException => fail()
    }

    val it = map.destructiveSortedIterator(new Comparator[String] {
      def compare(key1: String, key2: String): Int = {
        val x = if (key1 != null) key1.toInt else Int.MinValue
        val y = if (key2 != null) key2.toInt else Int.MinValue
        x.compareTo(y)
      }
    })

    // Should be sorted by key
    //应按键分类
    assert(it.hasNext)
    var previous = it.next()
    assert(previous == (null, "happy new year!"))
    previous = it.next()
    assert(previous == ("1", "2014"))
    while (it.hasNext) {
      val kv = it.next()
      assert(kv._1.toInt > previous._1.toInt)
      previous = kv
    }

    // All subsequent calls to apply, update, changeValue and iterator should throw exception
    //所有后续的调用申请,更新,changevalue和迭代器应该抛出异常
    intercept[AssertionError] { map.apply("1") }
    intercept[AssertionError] { map.update("1", "2013") }
    intercept[AssertionError] { map.changeValue("1", (hadValue, oldValue) => "2014") }
    intercept[AssertionError] { map.iterator }
  }
}
