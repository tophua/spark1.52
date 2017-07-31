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

import org.apache.spark.util.collection.WritablePartitionedPairCollection._

/**
 * Append-only buffer of key-value pairs, each with a corresponding partition ID, that keeps track
 * of its estimated size in bytes.
 * 只添加键值对的缓冲区,每个都有一个相应的分区标识,保持其估计大小的字节数
 * The buffer can support up to `1073741823 (2 ^ 30 - 1)` elements.
 */
private[spark] class PartitionedPairBuffer[K, V](initialCapacity: Int = 64)
  extends WritablePartitionedPairCollection[K, V] with SizeTracker
{
  import PartitionedPairBuffer._

  require(initialCapacity <= MAXIMUM_CAPACITY,//Int.MaxValue / 2
    s"Can't make capacity bigger than ${MAXIMUM_CAPACITY} elements")
  require(initialCapacity >= 1, "Invalid initial capacity")

  // Basic growable array data structure. We use a single array of AnyRef to hold both the keys
  // and the values, so that we can sort them efficiently with KVArraySortDataFormat.
  //基本可生长数组数据结构,我们使用AnyRef的单个数组来保存键和值,以便我们可以使用KVArraySortDataFormat有效地对它们进行排序。
  private var capacity = initialCapacity
  private var curSize = 0
  private var data = new Array[AnyRef](2 * initialCapacity)

  /** Add an element into the buffer 
   *  计算结果元素添加缓存到数组中
   *  */
  def insert(partition: Int, key: K, value: V): Unit = {
    if (curSize == capacity) {
      growArray() //扩充容量
    }
    //计算结果元素添加缓存到数组中
    data(2 * curSize) = (partition, key.asInstanceOf[AnyRef])
    data(2 * curSize + 1) = value.asInstanceOf[AnyRef]
    curSize += 1//自增1
    afterUpdate()
  }

  /** 
   *  Double the size of the array because we've reached capacity
   *  新建2倍大小的新数组,我们已经扩展容量 
   * */  
  private def growArray(): Unit = {
    if (capacity >= MAXIMUM_CAPACITY) {//1073741823
      throw new IllegalStateException(s"Can't insert more than ${MAXIMUM_CAPACITY} elements")
    }
    val newCapacity =
      if (capacity * 2 < 0 || capacity * 2 > MAXIMUM_CAPACITY) { // Overflo溢出
        MAXIMUM_CAPACITY
      } else {
        capacity * 2
      }
    val newArray = new Array[AnyRef](2 * newCapacity)//扩大2倍数组
    System.arraycopy(data, 0, newArray, 0, 2 * capacity)//复制数组数据
    data = newArray//赋值
    capacity = newCapacity
    resetSamples()
  }

  /** Iterate through the data in a given order. For this class this is not really destructive.
    * 按照给定的顺序迭代数据, 对于这个类,这不是真正的破坏性
    * */
  override def partitionedDestructiveSortedIterator(keyComparator: Option[Comparator[K]])
    : Iterator[((Int, K), V)] = {
    val comparator = keyComparator.map(partitionKeyComparator).getOrElse(partitionComparator)
    new Sorter(new KVArraySortDataFormat[(Int, K), AnyRef]).sort(data, 0, curSize, comparator)
    iterator
  }

  private def iterator(): Iterator[((Int, K), V)] = new Iterator[((Int, K), V)] {
    var pos = 0

    override def hasNext: Boolean = pos < curSize

    override def next(): ((Int, K), V) = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      val pair = (data(2 * pos).asInstanceOf[(Int, K)], data(2 * pos + 1).asInstanceOf[V])
      pos += 1
      pair
    }
  }
}

private object PartitionedPairBuffer {
  val MAXIMUM_CAPACITY = Int.MaxValue / 2 // 2 ^ 30 - 1即1073741823
}
