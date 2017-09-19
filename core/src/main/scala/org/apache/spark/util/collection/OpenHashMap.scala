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

import scala.reflect.ClassTag

import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 * A fast hash map implementation for nullable keys. This hash map supports insertions and updates,
 * but not deletions. This map is about 5X faster than java.util.HashMap, while using much less
 * space overhead.
 * 用于可空键的快速哈希映射实现,此散列Map支持插入和更新，但不支持删除,
  * 这个Map比java.util.HashMap快5倍，而空间开销却少得多。
 * Under the hood, it uses our OpenHashSet implementation.
  * 在引擎盖下,它使用我们的OpenHashSet实现。
 */
@DeveloperApi
private[spark]
class OpenHashMap[K : ClassTag, @specialized(Long, Int, Double) V: ClassTag](
    initialCapacity: Int)
  extends Iterable[(K, V)]
  with Serializable {

  def this() = this(64)

  protected var _keySet = new OpenHashSet[K](initialCapacity)

  // Init in constructor (instead of in declaration) to work around a Scala compiler specialization
  // bug that would generate two arrays (one for Object and one for specialized T).
  //init在构造函数（而不是在声明中）来解决可以生成两个数组的Scala编译器专业化错误（一个用于Object，一个用于专门的T）。
  private var _values: Array[V] = _
  _values = new Array[V](_keySet.capacity)

  @transient private var _oldValues: Array[V] = null

  // Treat the null key differently so we can use nulls in "data" to represent empty items.
  //处理空键不同，以便我们可以使用“数据”中的空值来表示空项目
  private var haveNullValue = false
  private var nullValue: V = null.asInstanceOf[V]

  override def size: Int = if (haveNullValue) _keySet.size + 1 else _keySet.size

  /** Tests whether this map contains a binding for a key.测试此地图是否包含一个键的绑定 */
  def contains(k: K): Boolean = {
    if (k == null) {
      haveNullValue
    } else {
      _keySet.getPos(k) != OpenHashSet.INVALID_POS
    }
  }

  /** Get the value for a given key 获取给定键的值*/
  def apply(k: K): V = {
    if (k == null) {
      nullValue
    } else {
      val pos = _keySet.getPos(k)
      if (pos < 0) {
        null.asInstanceOf[V]
      } else {
        _values(pos)
      }
    }
  }

  /** Set the value for a key 设置一个键的值*/
  def update(k: K, v: V) {
    if (k == null) {
      haveNullValue = true
      nullValue = v
    } else {
      val pos = _keySet.addWithoutResize(k) & OpenHashSet.POSITION_MASK
      _values(pos) = v
      _keySet.rehashIfNeeded(k, grow, move)
      _oldValues = null
    }
  }

  /**
   * If the key doesn't exist yet in the hash map, set its value to defaultValue; otherwise,
   * set its value to mergeValue(oldValue).
    * 如果哈希映射中的键不存在,请将其值设置为defaultValue; 否则,将其值设置为mergeValue（oldValue）
   *
   * @return the newly updated value.
   */
  def changeValue(k: K, defaultValue: => V, mergeValue: (V) => V): V = {
    if (k == null) {
      if (haveNullValue) {
        nullValue = mergeValue(nullValue)
      } else {
        haveNullValue = true
        nullValue = defaultValue
      }
      nullValue
    } else {
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
  }

  override def iterator: Iterator[(K, V)] = new Iterator[(K, V)] {
    var pos = -1
    var nextPair: (K, V) = computeNextPair()

    /** Get the next value we should return from next(), or null if we're finished iterating
      * 获取next（）返回的下一个值,如果我们完成迭代,则返回null */
    def computeNextPair(): (K, V) = {
      //将位置-1视为空值
      if (pos == -1) {    // Treat position -1 as looking at the null value
        if (haveNullValue) {
          pos += 1
          return (null.asInstanceOf[K], nullValue)
        }
        pos += 1
      }
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
  // specialization to work (specialized class extends the non-specialized one and needs access
  // to the "private" variables).
  //以下成员变量被声明为受保护而不是专用于专业化(专用类扩展非专门类，并需要访问“私有”变量)。
  // They also should have been val's. We use var's because there is a Scala compiler bug that
  // would throw illegal access error at runtime if they are declared as val's.
  //他们也应该是val的,我们使用var,因为有一个Scala编译器错误,如果它们被声明为val,则会在运行时抛出非法访问错误
  protected var grow = (newCapacity: Int) => {
    _oldValues = _values
    _values = new Array[V](newCapacity)
  }

  protected var move = (oldPos: Int, newPos: Int) => {
    _values(newPos) = _oldValues(oldPos)
  }
}
