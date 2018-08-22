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

import scala.reflect.ClassTag

import org.apache.spark.util.collection.{BitSet, PrimitiveVector}

import org.apache.spark.graphx._
import org.apache.spark.graphx.util.collection.GraphXPrimitiveKeyOpenHashMap

/** Stores vertex attributes to ship to an edge partition.
  * 存储顶点属性以发送到边缘分区*/
private[graphx]
class VertexAttributeBlock[VD: ClassTag](val vids: Array[VertexId], val attrs: Array[VD])
  extends Serializable {
  def iterator: Iterator[(VertexId, VD)] =
    (0 until vids.size).iterator.map { i => (vids(i), attrs(i)) }
}

private[graphx]
object ShippableVertexPartition {
  /** Construct a `ShippableVertexPartition` from the given vertices without any routing table.
    * 从给定的顶点构造一个`ShippableVertexPartition`，没有任何路由表*/
  def apply[VD: ClassTag](iter: Iterator[(VertexId, VD)]): ShippableVertexPartition[VD] =
    apply(iter, RoutingTablePartition.empty, null.asInstanceOf[VD], (a, b) => a)

  /**
   * Construct a `ShippableVertexPartition` from the given vertices with the specified routing
   * table, filling in missing vertices mentioned in the routing table using `defaultVal`.
    * 使用指定的路由表从给定顶点构造一个`ShippableVertexPartition`,
    * 使用`defaultVal`填充路由表中提到的缺失顶点。
   */
  def apply[VD: ClassTag](
      iter: Iterator[(VertexId, VD)], routingTable: RoutingTablePartition, defaultVal: VD)
    : ShippableVertexPartition[VD] =
    apply(iter, routingTable, defaultVal, (a, b) => a)

  /**
   * Construct a `ShippableVertexPartition` from the given vertices with the specified routing
   * table, filling in missing vertices mentioned in the routing table using `defaultVal`,
   * and merging duplicate vertex atrribute with mergeFunc.
    * 使用指定的路由表从给定顶点构造一个`ShippableVertexPartition`,
    * 使用`defaultVal`填充路由表中提到的缺失顶点,并使用mergeFunc合并重复的顶点atrribute
   */
  def apply[VD: ClassTag](
      iter: Iterator[(VertexId, VD)], routingTable: RoutingTablePartition, defaultVal: VD,
      mergeFunc: (VD, VD) => VD): ShippableVertexPartition[VD] = {
    val map = new GraphXPrimitiveKeyOpenHashMap[VertexId, VD]
    // Merge the given vertices using mergeFunc
    //使用mergeFunc合并给定顶点
    iter.foreach { pair =>
      map.setMerge(pair._1, pair._2, mergeFunc)
    }
    // Fill in missing vertices mentioned in the routing table
    //填写路由表中提到的缺少顶点
    routingTable.iterator.foreach { vid =>
      map.changeValue(vid, defaultVal, identity)
    }

    new ShippableVertexPartition(map.keySet, map._values, map.keySet.getBitSet, routingTable)
  }

  import scala.language.implicitConversions

  /**
   * Implicit conversion to allow invoking `VertexPartitionBase` operations directly on a
   * `ShippableVertexPartition`.
    * 隐式转换允许直接在`ShippableVertexPartition`上调用`VertexPartitionBase`操作
   */
  implicit def shippablePartitionToOps[VD: ClassTag](partition: ShippableVertexPartition[VD])
    : ShippableVertexPartitionOps[VD] = new ShippableVertexPartitionOps(partition)

  /**
   * Implicit evidence that `ShippableVertexPartition` is a member of the
   * `VertexPartitionBaseOpsConstructor` typeclass. This enables invoking `VertexPartitionBase`
   * operations on a `ShippableVertexPartition` via an evidence parameter, as in
   * [[VertexPartitionBaseOps]].
    *
    * 隐含的证据表明`ShippableVertexPartition`是`VertexPartitionBaseOpsConstructor`类型类的成员,
    * 这样可以通过证据参数在`ShippableVertexPartition`上调用`VertexPartitionBase`操作,
    * 如[[VertexPartitionBaseOps]]
   */
  implicit object ShippableVertexPartitionOpsConstructor
    extends VertexPartitionBaseOpsConstructor[ShippableVertexPartition] {
    def toOps[VD: ClassTag](partition: ShippableVertexPartition[VD])
      : VertexPartitionBaseOps[VD, ShippableVertexPartition] = shippablePartitionToOps(partition)
  }
}

/**
 * A map from vertex id to vertex attribute that additionally stores edge partition join sites for
 * each vertex attribute, enabling joining with an [[org.apache.spark.graphx.EdgeRDD]].
  * 从顶点id到顶点属性的映射,它还为每个顶点属性存储边缘分区连接站点,从而可以连接[[org.apache.spark.graphx.EdgeRDD]]
 */
private[graphx]
class ShippableVertexPartition[VD: ClassTag](
    val index: VertexIdToIndexMap,
    val values: Array[VD],
    val mask: BitSet,
    val routingTable: RoutingTablePartition)
  extends VertexPartitionBase[VD] {

  /** Return a new ShippableVertexPartition with the specified routing table.
    * 返回带有指定路由表的新ShippableVertexPartition*/
  def withRoutingTable(routingTable_ : RoutingTablePartition): ShippableVertexPartition[VD] = {
    new ShippableVertexPartition(index, values, mask, routingTable_)
  }

  /**
   * Generate a `VertexAttributeBlock` for each edge partition keyed on the edge partition ID. The
   * `VertexAttributeBlock` contains the vertex attributes from the current partition that are
   * referenced in the specified positions in the edge partition.
    * 为在边缘分区ID上键入的每个边缘分区生成“VertexAttributeBlock”,
    * `VertexAttributeBlock`包含当前分区中在边分区中指定位置引用的顶点属性
   */
  def shipVertexAttributes(
      shipSrc: Boolean, shipDst: Boolean): Iterator[(PartitionID, VertexAttributeBlock[VD])] = {
    Iterator.tabulate(routingTable.numEdgePartitions) { pid =>
      val initialSize = if (shipSrc && shipDst) routingTable.partitionSize(pid) else 64
      val vids = new PrimitiveVector[VertexId](initialSize)
      val attrs = new PrimitiveVector[VD](initialSize)
      var i = 0
      routingTable.foreachWithinEdgePartition(pid, shipSrc, shipDst) { vid =>
        if (isDefined(vid)) {
          vids += vid
          attrs += this(vid)
        }
        i += 1
      }
      (pid, new VertexAttributeBlock(vids.trim().array, attrs.trim().array))
    }
  }

  /**
   * Generate a `VertexId` array for each edge partition keyed on the edge partition ID. The array
   * contains the visible vertex ids from the current partition that are referenced in the edge
   * partition.
    * 为在边缘分区ID上键入的每个边缘分区生成一个“VertexId”数组,该数组包含边缘分区中引用的当前分区的可见顶点id
   */
  def shipVertexIds(): Iterator[(PartitionID, Array[VertexId])] = {
    Iterator.tabulate(routingTable.numEdgePartitions) { pid =>
      val vids = new PrimitiveVector[VertexId](routingTable.partitionSize(pid))
      var i = 0
      routingTable.foreachWithinEdgePartition(pid, true, true) { vid =>
        if (isDefined(vid)) {
          vids += vid
        }
        i += 1
      }
      (pid, vids.trim().array)
    }
  }
}

private[graphx] class ShippableVertexPartitionOps[VD: ClassTag](self: ShippableVertexPartition[VD])
  extends VertexPartitionBaseOps[VD, ShippableVertexPartition](self) {

  def withIndex(index: VertexIdToIndexMap): ShippableVertexPartition[VD] = {
    new ShippableVertexPartition(index, self.values, self.mask, self.routingTable)
  }

  def withValues[VD2: ClassTag](values: Array[VD2]): ShippableVertexPartition[VD2] = {
    new ShippableVertexPartition(self.index, values, self.mask, self.routingTable)
  }

  def withMask(mask: BitSet): ShippableVertexPartition[VD] = {
    new ShippableVertexPartition(self.index, self.values, mask, self.routingTable)
  }
}
