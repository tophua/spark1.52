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

import scala.reflect._

/**
 * A fast hash map implementation for primitive, non-null keys. This hash map supports
 * insertions and updates, but not deletions. This map is about an order of magnitude
 * faster than java.util.HashMap, while using much less space overhead.
 * 用于原始非空键的快速哈希映射实现,此散列图支持插入和更新,但不支持删除。
  * 这张Map大概是一个数量级的比java.util.HashMap快，同时使用更少的空间开销。
 * Under the hood, it uses our OpenHashSet implementation.
  * 下列方法,它使用我们的OpenHashSet实现。
 */
private[spark]
class PrimitiveKeyOpenHashMap[@specialized(Long, Int) K: ClassTag,
                              @specialized(Long, Int, Double) V: ClassTag](
    initialCapacity: Int)
  extends Iterable[(K, V)]
  with Serializable {

  def this() = this(64)

  require(classTag[K] == classTag[Long] || classTag[K] == classTag[Int])

  // Init in constructor (instead of in declaration) to work around a Scala compiler specialization
  // bug that would generate two arrays (one for Object and one for specialized T).
  //init在构造函数（而不是在声明中）来解决可以生成两个数组的Scala编译器专业化错误（一个用于Object，一个用于专门的T）。
  protected var _keySet: OpenHashSet[K] = _
  private var _values: Array[V] = _
  _keySet = new OpenHashSet[K](initialCapacity)
  _values = new Array[V](_keySet.capacity)

  private var _oldValues: Array[V] = null

  override def size: Int = _keySet.size

  /** Tests whether this map contains a binding for a key.
    * 测试此Map是否包含一个键的绑定
    *  */
  def contains(k: K): Boolean = {
    _keySet.getPos(k) != OpenHashSet.INVALID_POS
  }

  /** Get the value for a given key
    * 获取给定键的值
    *  */
  def apply(k: K): V = {
    val pos = _keySet.getPos(k)
    _values(pos)
  }

  /** Get the value for a given key, or returns elseValue if it doesn't exist.
    * 获取给定键的值,否则返回elseValue（如果不存在）
    *  */
  def getOrElse(k: K, elseValue: V): V = {
    val pos = _keySet.getPos(k)
    if (pos >= 0) _values(pos) else elseValue
  }

  /** Set the value for a key
    * 设置一个键的值
    * */
  def update(k: K, v: V) {
    val pos = _keySet.addWithoutResize(k) & OpenHashSet.POSITION_MASK
    _values(pos) = v
    _keySet.rehashIfNeeded(k, grow, move)
    _oldValues = null
  }

  /**
   * If the key doesn't exist yet in the hash map, set its value to defaultValue; otherwise,
   * set its value to mergeValue(oldValue).
    * 如果哈希映射中的键不存在,请将其值设置为defaultValue; 否则,将其值设置为mergeValue（oldValue）。
   *
   * @return the newly updated value.
   */
  def changeValue(k: K, defaultValue: => V, mergeValue: (V) => V): V = {
    val pos = _keySet.addWithoutResize(k)
    if ((pos & OpenHashSet.NONEXISTENCE_MASK) != 0) {
      val newValue = defaultValue
      _values(pos & OpenHashSet.POSITION_MASK) = newValue
      _keySet.rehashIfNeeded(k, grow, move)
      newValue
    } else {
      _values(pos) = mergeValue(_values(pos))
      _values(pos)
    }
  }

  override def iterator: Iterator[(K, V)] = new Iterator[(K, V)] {
    var pos = 0
    var nextPair: (K, V) = computeNextPair()

    /** Get the next value we should return from next(), or null if we're finished iterating
      * 获取next（）返回的下一个值，如果我们完成迭代，则返回null
      * */
    def computeNextPair(): (K, V) = {
      pos = _keySet.nextPos(pos)
      if (pos >= 0) {
        val ret = (_keySet.getValue(pos), _values(pos))
        pos += 1
        ret
      } else {
        null
      }
    }

    def hasNext: Boolean = nextPair != null

    def next(): (K, V) = {
      val pair = nextPair
      nextPair = computeNextPair()
      pair
    }
  }

  // The following member variables are declared as protected instead of private for the
  // specialization to work (specialized class extends the unspecialized one and needs access
  // to the "private" variables).
  //以下成员变量被声明为protected，而不是private专业化（专业类扩展了非专业化类，需要访问“私有”变量）。
  // They also should have been val's. We use var's because there is a Scala compiler bug that
  // would throw illegal access error at runtime if they are declared as val's.
  protected var grow = (newCapacity: Int) => {
    _oldValues = _values
    _values = new Array[V](newCapacity)
  }

  protected var move = (oldPos: Int, newPos: Int) => {
    _values(newPos) = _oldValues(oldPos)
  }
}
