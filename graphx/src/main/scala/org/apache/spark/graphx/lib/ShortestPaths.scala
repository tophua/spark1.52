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

package org.apache.spark.graphx.lib

import org.apache.spark.graphx._
import scala.reflect.ClassTag

/**
 * Computes shortest paths to the given set of landmark vertices, returning a graph where each
 * vertex attribute is a map containing the shortest-path distance to each reachable landmark.
  * 计算到给定地标顶点集的最短路径,返回图表,其中每个顶点属性是包含到每个可到达地标的最短路径距离的地图。
 */
object ShortestPaths {
  /** Stores a map from the vertex id of a landmark to the distance to that landmark.
    * 存储从地标的顶点id到到该地标的距离的地图*/
  type SPMap = Map[VertexId, Int]

  private def makeMap(x: (VertexId, Int)*) = Map(x: _*)

  private def incrementMap(spmap: SPMap): SPMap = spmap.map { case (v, d) => v -> (d + 1) }

  private def addMaps(spmap1: SPMap, spmap2: SPMap): SPMap =
    (spmap1.keySet ++ spmap2.keySet).map {
      k => k -> math.min(spmap1.getOrElse(k, Int.MaxValue), spmap2.getOrElse(k, Int.MaxValue))
    }.toMap

  /**
   * Computes shortest paths to the given set of landmark vertices.
    * 计算到给定地标顶点集的最短路径
   *
   * @tparam ED the edge attribute type (not used in the computation)
    *           边缘属性类型(未在计算中使用)
   *
   * @param graph the graph for which to compute the shortest paths 用于计算最短路径的图表
   * @param landmarks the list of landmark vertex ids. Shortest paths will be computed to each
   * landmark. 标志性顶点id的列表,将为每个地标计算最短路径。
   *
   * @return a graph where each vertex attribute is a map containing the shortest-path distance to
   * each reachable landmark vertex.
    *       图表,其中每个顶点属性是包含到每个可到达的地标顶点的最短路径距离的地图
   */
  def run[VD, ED: ClassTag](graph: Graph[VD, ED], landmarks: Seq[VertexId]): Graph[SPMap, ED] = {
    val spGraph = graph.mapVertices { (vid, attr) =>
      if (landmarks.contains(vid)) makeMap(vid -> 0) else makeMap()
    }

    val initialMessage = makeMap()

    def vertexProgram(id: VertexId, attr: SPMap, msg: SPMap): SPMap = {
      addMaps(attr, msg)
    }

    def sendMessage(edge: EdgeTriplet[SPMap, _]): Iterator[(VertexId, SPMap)] = {
      val newAttr = incrementMap(edge.dstAttr)
      if (edge.srcAttr != addMaps(newAttr, edge.srcAttr)) Iterator((edge.srcId, newAttr))
      else Iterator.empty
    }

    Pregel(spGraph, initialMessage)(vertexProgram, sendMessage, addMaps)
  }
}
