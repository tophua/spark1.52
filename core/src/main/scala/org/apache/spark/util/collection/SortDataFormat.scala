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

/**
 * Abstraction for sorting an arbitrary input buffer of data. This interface requires determining
 * the sort key for a given element index, as well as swapping elements and moving data from one
 * buffer to another.
 * 用于排序数据的任意输入缓冲区的抽象,该接口需要确定给定元素索引的排序键,以及交换元素并将数据从一个缓冲区移动到另一个缓冲区。
 * Example format: an array of numbers, where each element is also the key.
 * See [[KVArraySortDataFormat]] for a more exciting format.
 * 示例格式：一个数字数组，其中每个元素也是key.See [[KVArraySortDataFormat]]更令人兴奋的格式。
 * Note: Declaring and instantiating multiple subclasses of this class would prevent JIT inlining
 * overridden methods and hence decrease the shuffle performance.
 * 注意：声明和实例化此类的多个子类将阻止JIT内联重写方法,从而减少随机性。
 * @tparam K Type of the sort key of each element
 * @tparam Buffer Internal data structure used by a particular format (e.g., Array[Int]).
 */
// TODO: Making Buffer a real trait would be a better abstraction, but adds some complexity.
private[spark]
abstract class SortDataFormat[K, Buffer] {

  /**
   * Creates a new mutable key for reuse. This should be implemented if you want to override
    * 创建一个新的可key以供重用。 如果要覆盖，应该实现
   * [[getKey(Buffer, Int, K)]].
   */
  def newKey(): K = null.asInstanceOf[K]

  /** Return the sort key for the element at the given index.
    * 返回给定索引处元素的排序键
    *  */
  protected def getKey(data: Buffer, pos: Int): K

  /**
   * Returns the sort key for the element at the given index and reuse the input key if possible.
   * The default implementation ignores the reuse parameter and invokes [[getKey(Buffer, Int]].
   * If you want to override this method, you must implement [[newKey()]].
    * 返回给定索引处的元素的排序键,如果可能,重新使用输入键。
    * 默认实现将忽略重用参数，并调用[[getKey（Buffer，Int]]）。如果要覆盖此方法,则必须实现[[newKey（）]]。
   */
  def getKey(data: Buffer, pos: Int, reuse: K): K = {
    getKey(data, pos)
  }

  /** Swap two elements.
    * 交换两个元素
    * */
  def swap(data: Buffer, pos0: Int, pos1: Int): Unit

  /** Copy a single element from src(srcPos) to dst(dstPos).
    * 将单个元素从src（srcPos）复制到dst（dstPos）
    *  */
  def copyElement(src: Buffer, srcPos: Int, dst: Buffer, dstPos: Int): Unit

  /**
   * Copy a range of elements starting at src(srcPos) to dst, starting at dstPos.
   * Overlapping ranges are allowed.
    * 将从src（srcPos）开始的一系列元素复制到dst，从dstPos开始。 重叠范围是允许的。
   */
  def copyRange(src: Buffer, srcPos: Int, dst: Buffer, dstPos: Int, length: Int): Unit

  /**
   * Allocates a Buffer that can hold up to 'length' elements.
   * All elements of the buffer should be considered invalid until data is explicitly copied in.
    * 分配一个可以容纳“长度”元素的缓冲区。缓冲区的所有元素都应视为无效,直到数据被显式复制为止。
   */
  def allocate(length: Int): Buffer
}

/**
 * Supports sorting an array of key-value pairs where the elements of the array alternate between
 * keys and values, as used in [[AppendOnlyMap]].
 * 把所有的kv对移动数组的前端,然后进行排序
 * @tparam K Type of the sort key of each element
 * @tparam T Type of the Array we're sorting. Typically this must extend AnyRef, to support cases
 *           when the keys and values are not the same type.
 */
private[spark]
class KVArraySortDataFormat[K, T <: AnyRef : ClassTag] extends SortDataFormat[K, Array[T]] {

  override def getKey(data: Array[T], pos: Int): K = data(2 * pos).asInstanceOf[K]

  override def swap(data: Array[T], pos0: Int, pos1: Int) {
    val tmpKey = data(2 * pos0)
    val tmpVal = data(2 * pos0 + 1)
    data(2 * pos0) = data(2 * pos1)
    data(2 * pos0 + 1) = data(2 * pos1 + 1)
    data(2 * pos1) = tmpKey
    data(2 * pos1 + 1) = tmpVal
  }

  override def copyElement(src: Array[T], srcPos: Int, dst: Array[T], dstPos: Int) {
    dst(2 * dstPos) = src(2 * srcPos)
    dst(2 * dstPos + 1) = src(2 * srcPos + 1)
  }

  override def copyRange(src: Array[T], srcPos: Int, dst: Array[T], dstPos: Int, length: Int) {
    System.arraycopy(src, 2 * srcPos, dst, 2 * dstPos, 2 * length)
  }

  override def allocate(length: Int): Array[T] = {
    new Array[T](2 * length)
  }
}
