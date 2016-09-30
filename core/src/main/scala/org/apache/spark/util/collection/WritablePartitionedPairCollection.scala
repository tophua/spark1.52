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

import org.apache.spark.storage.DiskBlockObjectWriter

/**
 * A common interface for size-tracking collections of key-value pairs that
 * 一个常见的接口跟踪集合的大小键值对
 * - Have an associated partition for each key-value pair.
 * - Support a memory-efficient sorted iterator
 * -支持一种内存高效的排序迭代器
 * - Support a WritablePartitionedIterator for writing the contents directly as bytes.
 * -支持一个直接写内容作为字节writablepartitionediterator
 */
private[spark] trait WritablePartitionedPairCollection[K, V] {
  /**
   * Insert a key-value pair with a partition into the collection
   * 将一个键值对插入到集合中的一个分区上
   * 
   */
  def insert(partition: Int, key: K, value: V): Unit

  /**
   * Iterate through the data in order of partition ID and then the given comparator. This may
   * destroy the underlying collection.
   * 给定比较器遍历分区ID顺序的数据,这可能破坏底层集合
   */
  def partitionedDestructiveSortedIterator(keyComparator: Option[Comparator[K]])
    : Iterator[((Int, K), V)]

  /**
   * Iterate through the data and write out the elements instead of returning them. Records are
   * returned in order of their partition ID and then the given comparator.
   * This may destroy the underlying collection.
   * 通过数据进行迭代写每个元素的内容而不是返回,记录被返回的顺序是分区标识,然后给定的比较器
   * 这可能会破坏底层的集合
   */
  def destructiveSortedWritablePartitionedIterator(keyComparator: Option[Comparator[K]])
    : WritablePartitionedIterator = {
    val it = partitionedDestructiveSortedIterator(keyComparator)
    new WritablePartitionedIterator {
      private[this] var cur = if (it.hasNext) it.next() else null

      def writeNext(writer: DiskBlockObjectWriter): Unit = {
        writer.write(cur._1._2, cur._2)
        cur = if (it.hasNext) it.next() else null
      }

      def hasNext(): Boolean = cur != null

      def nextPartition(): Int = cur._1._1
    }
  }
}

private[spark] object WritablePartitionedPairCollection {
  /**
   * A comparator for (Int, K) pairs that orders them by only their partition ID.
   * 按照分区ID进行比较
   */
  def partitionComparator[K]: Comparator[(Int, K)] = new Comparator[(Int, K)] {
    override def compare(a: (Int, K), b: (Int, K)): Int = {
      a._1 - b._1
    }
  }

  /**
   * A comparator for (Int, K) pairs that orders them both by their partition ID and a key ordering.
   * 先按照分区Id进行比较,再按照指定的Key进行二级排序,如果没有指定排序字段默认Key进行比较
   */
  def partitionKeyComparator[K](keyComparator: Comparator[K]): Comparator[(Int, K)] = {
    new Comparator[(Int, K)] {
      override def compare(a: (Int, K), b: (Int, K)): Int = {
        val partitionDiff = a._1 - b._1
        if (partitionDiff != 0) {
          partitionDiff
        } else {
          keyComparator.compare(a._2, b._2)
        }
      }
    }
  }
}

/**
 * Iterator that writes elements to a DiskBlockObjectWriter instead of returning them. Each element
 * has an associated partition.
 * 迭代器元素写到一个diskblockobjectwriter不是返回他们,每个元素都有一个关联的分区
 */
private[spark] trait WritablePartitionedIterator {
  /**
   * 写入Key和Value,并不写入Partition ID
   */
  def writeNext(writer: DiskBlockObjectWriter): Unit

  def hasNext(): Boolean

  def nextPartition(): Int
}
