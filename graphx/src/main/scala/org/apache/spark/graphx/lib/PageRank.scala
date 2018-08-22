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
import scala.language.postfixOps

import org.apache.spark.Logging
import org.apache.spark.graphx._

/**
 * PageRank algorithm implementation. There are two implementations of PageRank implemented.
  * PageRank算法实现,实现了PageRank的两种实现
 *
 * The first implementation uses the standalone [[Graph]] interface and runs PageRank
 * for a fixed number of iterations:
  * 第一个实现使用独立的[[Graph]]接口并运行PageRank进行固定的迭代次数：
 * {{{
 * var PR = Array.fill(n)( 1.0 )
 * val oldPR = Array.fill(n)( 1.0 )
 * for( iter <- 0 until numIter ) {
 *   swap(oldPR, PR)
 *   for( i <- 0 until n ) {
 *     PR[i] = alpha + (1 - alpha) * inNbrs[i].map(j => oldPR[j] / outDeg[j]).sum
 *   }
 * }
 * }}}
 *
 * The second implementation uses the [[Pregel]] interface and runs PageRank until
 * convergence:
 *
 * {{{
 * var PR = Array.fill(n)( 1.0 )
 * val oldPR = Array.fill(n)( 0.0 )
 * while( max(abs(PR - oldPr)) > tol ) {
 *   swap(oldPR, PR)
 *   for( i <- 0 until n if abs(PR[i] - oldPR[i]) > tol ) {
 *     PR[i] = alpha + (1 - \alpha) * inNbrs[i].map(j => oldPR[j] / outDeg[j]).sum
 *   }
 * }
 * }}}
 *
 * `alpha` is the random reset probability (typically 0.15), `inNbrs[i]` is the set of
 * neighbors whick link to `i` and `outDeg[j]` is the out degree of vertex `j`.
 *
 * Note that this is not the "normalized" PageRank and as a consequence pages that have no
 * inlinks will have a PageRank of alpha.
 */
object PageRank extends Logging {


  /**
   * Run PageRank for a fixed number of iterations returning a graph
   * with vertex attributes containing the PageRank and edge
   * attributes the normalized edge weight.
    *
    * 运行PageRank一定数量的迭代,返回一个包含顶点属性的图形,
    * 其中包含PageRank和边缘属性的标准化边缘权重。
   *
   * @tparam VD the original vertex attribute (not used)
    *            原始顶点属性（未使用）
   * @tparam ED the original edge attribute (not used)
    *            原始边缘属性（未使用）
   *
   * @param graph the graph on which to compute PageRank
   * @param numIter the number of iterations of PageRank to run
   * @param resetProb the random reset probability (alpha)
   *
   * @return the graph containing with each vertex containing the PageRank and each edge
   *         containing the normalized weight.
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], numIter: Int,
    resetProb: Double = 0.15): Graph[Double, Double] =
  {
    runWithOptions(graph, numIter, resetProb)
  }

  /**
   * Run PageRank for a fixed number of iterations returning a graph
   * with vertex attributes containing the PageRank and edge
   * attributes the normalized edge weight.
    *
    * 运行PageRank一定数量的迭代,返回一个包含顶点属性的图形,
    * 其中包含PageRank和边缘属性的标准化边缘权重
   *
   * @tparam VD the original vertex attribute (not used)
    *            原始顶点属性（未使用）
   * @tparam ED the original edge attribute (not used)
    *            原始边缘属性（未使用）
   *
   * @param graph the graph on which to compute PageRank
    *              计算PageRank的图表
   * @param numIter the number of iterations of PageRank to run
    *                要运行的PageRank的迭代次数
   * @param resetProb the random reset probability (alpha)
    *                  随机重置概率（alpha）
   * @param srcId the source vertex for a Personalized Page Rank (optional)
    *              个性化页面排名的源顶点（可选）
   * @return the graph containing with each vertex containing the PageRank and each edge
   *         containing the normalized weight.
    *         包含每个顶点的图形包含PageRank,每个边缘包含标准化的权重
   *
   */
  def runWithOptions[VD: ClassTag, ED: ClassTag](
      graph: Graph[VD, ED], numIter: Int, resetProb: Double = 0.15,
      srcId: Option[VertexId] = None): Graph[Double, Double] =
  {
    // Initialize the PageRank graph with each edge attribute having
    // weight 1/outDegree and each vertex with attribute 1.0.
    //初始化PageRank图，每个边属性的权重为1/outDegree,每个顶点的属性为1.0
    var rankGraph: Graph[Double, Double] = graph
      // Associate the degree with each vertex
      //将度数与每个顶点相关联
      .outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0) }
      // Set the weight on the edges based on the degree
      //根据度数设置边缘的重量
      .mapTriplets( e => 1.0 / e.srcAttr, TripletFields.Src )
      // Set the vertex attributes to the initial pagerank values
      //将顶点属性设置为初始pagerank值
      .mapVertices( (id, attr) => resetProb )

    val personalized = srcId isDefined
    val src: VertexId = srcId.getOrElse(-1L)
    def delta(u: VertexId, v: VertexId): Double = { if (u == v) 1.0 else 0.0 }

    var iteration = 0
    var prevRankGraph: Graph[Double, Double] = null
    while (iteration < numIter) {
      rankGraph.cache()

      // Compute the outgoing rank contributions of each vertex, perform local preaggregation, and
      // do the final aggregation at the receiving vertices. Requires a shuffle for aggregation.

      //计算每个顶点的输出排名贡献,执行本地预先聚合,并在接收顶点进行最终聚合,需要一个shuffle进行聚合
      val rankUpdates = rankGraph.aggregateMessages[Double](
        ctx => ctx.sendToDst(ctx.srcAttr * ctx.attr), _ + _, TripletFields.Src)

      // Apply the final rank updates to get the new ranks, using join to preserve ranks of vertices
      // that didn't receive a message. Requires a shuffle for broadcasting updated ranks to the
      // edge partitions.
      //应用最终排名更新以获取新排名,使用连接来保留未接收消息的顶点排名,需要随机播放以将更新的排名广播到边缘分区
      prevRankGraph = rankGraph
      val rPrb = if (personalized) {
        (src: VertexId , id: VertexId) => resetProb * delta(src, id)
      } else {
        (src: VertexId, id: VertexId) => resetProb
      }

      rankGraph = rankGraph.joinVertices(rankUpdates) {
        (id, oldRank, msgSum) => rPrb(src, id) + (1.0 - resetProb) * msgSum
      }.cache()

      rankGraph.edges.foreachPartition(x => {}) // also materializes rankGraph.vertices
      logInfo(s"PageRank finished iteration $iteration.")
      prevRankGraph.vertices.unpersist(false)
      prevRankGraph.edges.unpersist(false)

      iteration += 1
    }

    rankGraph
  }

  /**
   * Run a dynamic version of PageRank returning a graph with vertex attributes containing the
   * PageRank and edge attributes containing the normalized edge weight.
    *
    * 运行PageRank的动态版本,返回包含PageRank和包含标准化边权重的边缘属性的顶点属性的图形。
   *
   * @tparam VD the original vertex attribute (not used)
   * @tparam ED the original edge attribute (not used)
   *
   * @param graph the graph on which to compute PageRank
   * @param tol the tolerance allowed at convergence (smaller => more accurate).
   * @param resetProb the random reset probability (alpha)
   *
   * @return the graph containing with each vertex containing the PageRank and each edge
   *         containing the normalized weight.
   */
  def runUntilConvergence[VD: ClassTag, ED: ClassTag](
    graph: Graph[VD, ED], tol: Double, resetProb: Double = 0.15): Graph[Double, Double] =
  {
      runUntilConvergenceWithOptions(graph, tol, resetProb)
  }

  /**
   * Run a dynamic version of PageRank returning a graph with vertex attributes containing the
   * PageRank and edge attributes containing the normalized edge weight.
    * 运行PageRank的动态版本,返回包含PageRank和包含标准化边权重的边缘属性的顶点属性的图形
   *
   * @tparam VD the original vertex attribute (not used)
   * @tparam ED the original edge attribute (not used)
   *
   * @param graph the graph on which to compute PageRank
   * @param tol the tolerance allowed at convergence (smaller => more accurate).
   * @param resetProb the random reset probability (alpha)
   * @param srcId the source vertex for a Personalized Page Rank (optional)
   *
   * @return the graph containing with each vertex containing the PageRank and each edge
   *         containing the normalized weight.
   */
  def runUntilConvergenceWithOptions[VD: ClassTag, ED: ClassTag](
      graph: Graph[VD, ED], tol: Double, resetProb: Double = 0.15,
      srcId: Option[VertexId] = None): Graph[Double, Double] =
  {
    // Initialize the pagerankGraph with each edge attribute
    // having weight 1/outDegree and each vertex with attribute 1.0.
    //初始化pagerankGraph,每个edge属性具有权重1/outDegree,每个顶点具有属性1.0。
    val pagerankGraph: Graph[(Double, Double), Double] = graph
      // Associate the degree with each vertex
      .outerJoinVertices(graph.outDegrees) {
        (vid, vdata, deg) => deg.getOrElse(0)
      }
      // Set the weight on the edges based on the degree
      //根据度数设置边缘的重量
      .mapTriplets( e => 1.0 / e.srcAttr )
      // Set the vertex attributes to (initalPR, delta = 0)
      .mapVertices( (id, attr) => (0.0, 0.0) )
      .cache()

    val personalized = srcId.isDefined
    val src: VertexId = srcId.getOrElse(-1L)


    // Define the three functions needed to implement PageRank in the GraphX
    //定义在GraphX中实现PageRank所需的三个功能
    // version of Pregel
    def vertexProgram(id: VertexId, attr: (Double, Double), msgSum: Double): (Double, Double) = {
      val (oldPR, lastDelta) = attr
      val newPR = oldPR + (1.0 - resetProb) * msgSum
      (newPR, newPR - oldPR)
    }

    def personalizedVertexProgram(id: VertexId, attr: (Double, Double),
      msgSum: Double): (Double, Double) = {
      val (oldPR, lastDelta) = attr
      var teleport = oldPR
      val delta = if (src==id) 1.0 else 0.0
      teleport = oldPR*delta

      val newPR = teleport + (1.0 - resetProb) * msgSum
      (newPR, newPR - oldPR)
    }

    def sendMessage(edge: EdgeTriplet[(Double, Double), Double]) = {
      if (edge.srcAttr._2 > tol) {
        Iterator((edge.dstId, edge.srcAttr._2 * edge.attr))
      } else {
        Iterator.empty
      }
    }

    def messageCombiner(a: Double, b: Double): Double = a + b

    // The initial message received by all vertices in PageRank
    //PageRank中所有顶点收到的初始消息
    val initialMessage = resetProb / (1.0 - resetProb)

    // Execute a dynamic version of Pregel.
    //执行Pregel的动态版本
    val vp = if (personalized) {
      (id: VertexId, attr: (Double, Double), msgSum: Double) =>
        personalizedVertexProgram(id, attr, msgSum)
    } else {
      (id: VertexId, attr: (Double, Double), msgSum: Double) =>
        vertexProgram(id, attr, msgSum)
    }

    Pregel(pagerankGraph, initialMessage, activeDirection = EdgeDirection.Out)(
      vp, sendMessage, messageCombiner)
      .mapVertices((vid, attr) => attr._1)
  } // end of deltaPageRank

}
