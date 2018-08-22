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

import scala.reflect.ClassTag

import org.apache.spark.graphx._

/**
 * Compute the number of triangles passing through each vertex.
  * 计算通过每个顶点的三角形数量
 *
 * The algorithm is relatively straightforward and can be computed in three steps:
 * 该算法相对简单,可以分三步计算：
 * <ul>
 * <li>Compute the set of neighbors for each vertex 计算每个顶点的邻居集
 * <li>For each edge compute the intersection of the sets and send the count to both vertices.
  * 对于每个边缘计算集合的交集并将计数发送到两个顶点。
 * <li> Compute the sum at each vertex and divide by two since each triangle is counted twice.
  * 计算每个顶点的总和并除以2,因为每个三角形计数两次
 * </ul>
 *
 * Note that the input graph should have its edges in canonical direction
 * (i.e. the `sourceId` less than `destId`). Also the graph must have been partitioned
 * using [[org.apache.spark.graphx.Graph#partitionBy]].
 */
object TriangleCount {

  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Graph[Int, ED] = {
    // Remove redundant edges
    //删除冗余边缘
    val g = graph.groupEdges((a, b) => a).cache()

    // Construct set representations of the neighborhoods
    //构建邻域的集合表示
    val nbrSets: VertexRDD[VertexSet] =
      g.collectNeighborIds(EdgeDirection.Either).mapValues { (vid, nbrs) =>
        val set = new VertexSet(4)
        var i = 0
        while (i < nbrs.size) {
          // prevent self cycle
          if (nbrs(i) != vid) {
            set.add(nbrs(i))
          }
          i += 1
        }
        set
      }
    // join the sets with the graph
    //用图表加入集合
    val setGraph: Graph[VertexSet, ED] = g.outerJoinVertices(nbrSets) {
      (vid, _, optSet) => optSet.getOrElse(null)
    }
    // Edge function computes intersection of smaller vertex with larger vertex
    //边缘函数计算较小顶点与较大顶点的交点
    def edgeFunc(ctx: EdgeContext[VertexSet, ED, Int]) {
      assert(ctx.srcAttr != null)
      assert(ctx.dstAttr != null)
      val (smallSet, largeSet) = if (ctx.srcAttr.size < ctx.dstAttr.size) {
        (ctx.srcAttr, ctx.dstAttr)
      } else {
        (ctx.dstAttr, ctx.srcAttr)
      }
      val iter = smallSet.iterator
      var counter: Int = 0
      while (iter.hasNext) {
        val vid = iter.next()
        if (vid != ctx.srcId && vid != ctx.dstId && largeSet.contains(vid)) {
          counter += 1
        }
      }
      ctx.sendToSrc(counter)
      ctx.sendToDst(counter)
    }
    // compute the intersection along edges
    //计算沿边的交点
    val counters: VertexRDD[Int] = setGraph.aggregateMessages(edgeFunc, _ + _)
    // Merge counters with the graph and divide by two since each triangle is counted twice
    //将计数器与图表合并并除以2,因为每个三角形都计算两次
    g.outerJoinVertices(counters) {
      (vid, _, optCounter: Option[Int]) =>
        val dblCount = optCounter.getOrElse(0)
        // double count should be even (divisible by two)
        assert((dblCount & 1) == 0)
        dblCount / 2
    }
  } // end of TriangleCount
}
