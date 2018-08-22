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

import scala.reflect.{classTag, ClassTag}

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import org.apache.spark.graphx._

/**
 * Manages shipping vertex attributes to the edge partitions of an
 * [[org.apache.spark.graphx.EdgeRDD]]. Vertex attributes may be partially shipped to construct a
 * triplet view with vertex attributes on only one side, and they may be updated. An active vertex
 * set may additionally be shipped to the edge partitions. Be careful not to store a reference to
 * `edges`, since it may be modified when the attribute shipping level is upgraded.
 */
private[impl]
class ReplicatedVertexView[VD: ClassTag, ED: ClassTag](
    var edges: EdgeRDDImpl[ED, VD],
    var hasSrcId: Boolean = false,
    var hasDstId: Boolean = false) {

  /**
   * Return a new `ReplicatedVertexView` with the specified `EdgeRDD`, which must have the same
   * shipping level.
    * 返回一个带有指定`EdgeRDD`的新`ReplicatedVertexView`,它必须具有相同的运输级别
   */
  def withEdges[VD2: ClassTag, ED2: ClassTag](
      edges_ : EdgeRDDImpl[ED2, VD2]): ReplicatedVertexView[VD2, ED2] = {
    new ReplicatedVertexView(edges_, hasSrcId, hasDstId)
  }

  /**
   * Return a new `ReplicatedVertexView` where edges are reversed and shipping levels are swapped to
   * match.
    * 返回一个新的`ReplicatedVertexView`,其中边缘被反转并且交换水平被交换以匹配
   */
  def reverse(): ReplicatedVertexView[VD, ED] = {
    val newEdges = edges.mapEdgePartitions((pid, part) => part.reverse)
    new ReplicatedVertexView(newEdges, hasDstId, hasSrcId)
  }

  /**
   * Upgrade the shipping level in-place to the specified levels by shipping vertex attributes from
   * `vertices`. This operation modifies the `ReplicatedVertexView`, and callers can access `edges`
   * afterwards to obtain the upgraded view.
    * 通过从“顶点”运送顶点属性，将装运级别就地升级到指定级别,
    * 此操作修改`ReplicatedVertexView`,然后调用者可以访问`edges`以获取升级后的视图
   */
  def upgrade(vertices: VertexRDD[VD], includeSrc: Boolean, includeDst: Boolean) {
    val shipSrc = includeSrc && !hasSrcId
    val shipDst = includeDst && !hasDstId
    if (shipSrc || shipDst) {
      val shippedVerts: RDD[(Int, VertexAttributeBlock[VD])] =
        vertices.shipVertexAttributes(shipSrc, shipDst)
          .setName("ReplicatedVertexView.upgrade(%s, %s) - shippedVerts %s %s (broadcast)".format(
            includeSrc, includeDst, shipSrc, shipDst))
          .partitionBy(edges.partitioner.get)
      val newEdges = edges.withPartitionsRDD(edges.partitionsRDD.zipPartitions(shippedVerts) {
        (ePartIter, shippedVertsIter) => ePartIter.map {
          case (pid, edgePartition) =>
            (pid, edgePartition.updateVertices(shippedVertsIter.flatMap(_._2.iterator)))
        }
      })
      edges = newEdges
      hasSrcId = includeSrc
      hasDstId = includeDst
    }
  }

  /**
   * Return a new `ReplicatedVertexView` where the `activeSet` in each edge partition contains only
   * vertex ids present in `actives`. This ships a vertex id to all edge partitions where it is
   * referenced, ignoring the attribute shipping level.
    *
    * 返回一个新的`ReplicatedVertexView`,其中每个边分区中的`activeSet`只包含`actives`中的顶点id。
    * 这会将顶点id发送到引用它的所有边缘分区，忽略属性传送级别。
   */
  def withActiveSet(actives: VertexRDD[_]): ReplicatedVertexView[VD, ED] = {
    val shippedActives = actives.shipVertexIds()
      .setName("ReplicatedVertexView.withActiveSet - shippedActives (broadcast)")
      .partitionBy(edges.partitioner.get)

    val newEdges = edges.withPartitionsRDD(edges.partitionsRDD.zipPartitions(shippedActives) {
      (ePartIter, shippedActivesIter) => ePartIter.map {
        case (pid, edgePartition) =>
          (pid, edgePartition.withActiveSet(shippedActivesIter.flatMap(_._2.iterator)))
      }
    })
    new ReplicatedVertexView(newEdges, hasSrcId, hasDstId)
  }

  /**
   * Return a new `ReplicatedVertexView` where vertex attributes in edge partition are updated using
   * `updates`. This ships a vertex attribute only to the edge partitions where it is in the
   * position(s) specified by the attribute shipping level.
    *
    *返回一个新的`ReplicatedVertexView`，其中边缘分区中的顶点属性使用`updates`更新。
    * 这会将顶点属性仅发送到边缘分区，在该边界分区中，它位于由属性送货级别指定的位置。
   */
  def updateVertices(updates: VertexRDD[VD]): ReplicatedVertexView[VD, ED] = {
    val shippedVerts = updates.shipVertexAttributes(hasSrcId, hasDstId)
      .setName("ReplicatedVertexView.updateVertices - shippedVerts %s %s (broadcast)".format(
        hasSrcId, hasDstId))
      .partitionBy(edges.partitioner.get)

    val newEdges = edges.withPartitionsRDD(edges.partitionsRDD.zipPartitions(shippedVerts) {
      (ePartIter, shippedVertsIter) => ePartIter.map {
        case (pid, edgePartition) =>
          (pid, edgePartition.updateVertices(shippedVertsIter.flatMap(_._2.iterator)))
      }
    })
    new ReplicatedVertexView(newEdges, hasSrcId, hasDstId)
  }
}
