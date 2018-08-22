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

package org.apache.spark.graphx.impl

import scala.language.higherKinds
import scala.reflect.ClassTag

import org.apache.spark.util.collection.BitSet

import org.apache.spark.graphx._
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap

private[graphx] object VertexPartitionBase {
  /**
   * Construct the constituents of a VertexPartitionBase from the given vertices, merging duplicate
   * entries arbitrarily.
    * 从给定顶点构造VertexPartitionBase的组成部分,任意合并重复的条目
   */
  def initFrom[VD: ClassTag](iter: Iterator[(VertexId, VD)])
    : (VertexIdToIndexMap, Array[VD], BitSet) = {
    val map = new GraphXPrimitiveKeyOpenHashMap[VertexId, VD]
    iter.foreach { pair =>
      map(pair._1) = pair._2
    }
    (map.keySet, map._values, map.keySet.getBitSet)
  }

  /**
   * Construct the constituents of a VertexPartitionBase from the given vertices, merging duplicate
   * entries using `mergeFunc`.
    * 从给定顶点构造VertexPartitionBase的组成部分,使用`mergeFunc`合并重复条目
   */
  def initFrom[VD: ClassTag](iter: Iterator[(VertexId, VD)], mergeFunc: (VD, VD) => VD)
    : (VertexIdToIndexMap, Array[VD], BitSet) = {
    val map = new GraphXPrimitiveKeyOpenHashMap[VertexId, VD]
    iter.foreach { pair =>
      map.setMerge(pair._1, pair._2, mergeFunc)
    }
    (map.keySet, map._values, map.keySet.getBitSet)
  }
}

/**
 * An abstract map from vertex id to vertex attribute. [[VertexPartition]] is the corresponding
 * concrete implementation. [[VertexPartitionBaseOps]] provides a variety of operations for
 * VertexPartitionBase and subclasses that provide implicit evidence of membership in the
 * `VertexPartitionBaseOpsConstructor` typeclass (for example,
 * [[VertexPartition.VertexPartitionOpsConstructor]]).
 */
private[graphx] abstract class VertexPartitionBase[@specialized(Long, Int, Double) VD: ClassTag]
  extends Serializable {

  def index: VertexIdToIndexMap
  def values: Array[VD]
  def mask: BitSet

  val capacity: Int = index.capacity

  def size: Int = mask.cardinality()

  /** Return the vertex attribute for the given vertex ID. 返回给定顶点ID的顶点属性*/
  def apply(vid: VertexId): VD = values(index.getPos(vid))

  def isDefined(vid: VertexId): Boolean = {
    val pos = index.getPos(vid)
    pos >= 0 && mask.get(pos)
  }

  def iterator: Iterator[(VertexId, VD)] =
    mask.iterator.map(ind => (index.getValue(ind), values(ind)))
}

/**
 * A typeclass for subclasses of `VertexPartitionBase` representing the ability to wrap them in a
 * `VertexPartitionBaseOps`.
  * “VertexPartitionBase”子类的类型类,表示将它们包装在“VertexPartitionBaseOps”中的能力
 */
private[graphx] trait VertexPartitionBaseOpsConstructor[T[X] <: VertexPartitionBase[X]] {
  def toOps[VD: ClassTag](partition: T[VD]): VertexPartitionBaseOps[VD, T]
}
