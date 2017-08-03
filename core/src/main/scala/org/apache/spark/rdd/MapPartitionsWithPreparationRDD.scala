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

package org.apache.spark.rdd

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.spark.{Partition, Partitioner, TaskContext}

/**
 * An RDD that applies a user provided function to every partition of the parent RDD, and
 * additionally allows the user to prepare each partition before computing the parent partition.
  * 将用户提供的功能应用于父RDD的每个分区的RDD,并且还允许用户在计算父分区之前准备每个分区
 */
private[spark] class MapPartitionsWithPreparationRDD[U: ClassTag, T: ClassTag, M: ClassTag](
    prev: RDD[T],
    preparePartition: () => M,
    executePartition: (TaskContext, Int, M, Iterator[T]) => Iterator[U],
    preservesPartitioning: Boolean = false)//是否保存分区
  extends RDD[U](prev) {

  override val partitioner: Option[Partitioner] = {
    if (preservesPartitioning) firstParent[T].partitioner else None
  }

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  // In certain join operations, prepare can be called on the same partition multiple times.
  // In this case, we need to ensure that each call to compute gets a separate prepare argument.
  //在某些连接操作中，准备可以在同一分区上多次调用。
  //在这种情况下,我们需要确保每次对compute的调用都有一个单独的prepare参数。
  private[this] val preparedArguments: ArrayBuffer[M] = new ArrayBuffer[M]

  /**
   * Prepare a partition for a single call to compute.
   * 准备调用一个单独计算分区
   */
  def prepare(): Unit = {
    preparedArguments += preparePartition()
  }

  /**
   * Prepare a partition before computing it from its parent.
   * 准备一个分区计算的父分区
   */
  override def compute(partition: Partition, context: TaskContext): Iterator[U] = {
    val prepared =
      if (preparedArguments.isEmpty) {
        preparePartition()
      } else {
        preparedArguments.remove(0)
      }
    val parentIterator = firstParent[T].iterator(partition, context)
    executePartition(context, partition.index, prepared, parentIterator)
  }
}
