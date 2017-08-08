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

package org.apache.spark.ui.scope

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import org.apache.spark.Logging
import org.apache.spark.scheduler.StageInfo
import org.apache.spark.storage.StorageLevel

/**
 * A representation of a generic cluster graph used for storing information on RDD operations.
 * 用于存储关于RDD操作的信息的通用集群图的表示
 * Each graph is defined with a set of edges and a root cluster, which may contain children
 * nodes and children clusters. Additionally, a graph may also have edges that enter or exit
 * the graph from nodes that belong to adjacent graphs.
  * 每个图都使用一组边和一个根集群来定义,它可以包含子节点和子集,此外,图形还可能具有从属于相邻图形的节点进入或退出图形的边
 */
private[ui] case class RDDOperationGraph(
    edges: Seq[RDDOperationEdge],
    outgoingEdges: Seq[RDDOperationEdge],
    incomingEdges: Seq[RDDOperationEdge],
    rootCluster: RDDOperationCluster)

/** A node in an RDDOperationGraph. This represents an RDD.
  * RDDOperationGraph中的一个节点,这代表RDD*/
private[ui] case class RDDOperationNode(id: Int, name: String, cached: Boolean)

/**
 * A directed edge connecting two nodes in an RDDOperationGraph.
 * This represents an RDD dependency.
  * 连接RDDOperationGraph中的两个节点的有向边,这表示RDD相关性
 */
private[ui] case class RDDOperationEdge(fromId: Int, toId: Int)

/**
 * A cluster that groups nodes together in an RDDOperationGraph.
  * 在RDDOperationGraph中将节点组合在一起的集群
 *
 * This represents any grouping of RDDs, including operation scopes (e.g. textFile, flatMap),
 * stages, jobs, or any higher level construct. A cluster may be nested inside of other clusters.
  * 这表示任何分组的RDD,包括操作范围(例如textFile,flatMap),阶段,作业或任何更高级别的构造,集群可以嵌套在其他集群中,
 */
private[ui] class RDDOperationCluster(val id: String, private var _name: String) {
  private val _childNodes = new ListBuffer[RDDOperationNode]
  private val _childClusters = new ListBuffer[RDDOperationCluster]

  def name: String = _name
  def setName(n: String): Unit = { _name = n }

  def childNodes: Seq[RDDOperationNode] = _childNodes.iterator.toSeq
  def childClusters: Seq[RDDOperationCluster] = _childClusters.iterator.toSeq
  def attachChildNode(childNode: RDDOperationNode): Unit = { _childNodes += childNode }
  def attachChildCluster(childCluster: RDDOperationCluster): Unit = {
    _childClusters += childCluster
  }

  /** Return all the nodes which are cached.返回所有缓存的节点 */
  def getCachedNodes: Seq[RDDOperationNode] = {
    _childNodes.filter(_.cached) ++ _childClusters.flatMap(_.getCachedNodes)
  }
}

private[ui] object RDDOperationGraph extends Logging {

  val STAGE_CLUSTER_PREFIX = "stage_"

  /**
   * Construct a RDDOperationGraph for a given stage.
   * 为给定阶段构建一个RDDOperationGraph
   * The root cluster represents the stage, and all children clusters represent RDD operations.
   * Each node represents an RDD, and each edge represents a dependency between two RDDs pointing
   * from the parent to the child.
   * 根集群代表阶段，所有子集群表示RDD操作。 每个节点表示一个RDD，每个边表示两个从父对象指向子节点的RDD之间的依赖关系。
   * This does not currently merge common operation scopes across stages. This may be worth
   * supporting in the future if we decide to group certain stages within the same job under
   * a common scope (e.g. part of a SQL query).
    * 目前，这并不合并跨阶段的常用操作范围,如果我们决定在同一作业中将某些阶段分组在一个共同的范围内（例如SQL查询的一部分）,这可能值得支持
   */
  def makeOperationGraph(stage: StageInfo): RDDOperationGraph = {
    val edges = new ListBuffer[RDDOperationEdge]
    val nodes = new mutable.HashMap[Int, RDDOperationNode]
    val clusters = new mutable.HashMap[String, RDDOperationCluster] // indexed by cluster ID

    // Root cluster is the stage cluster 根集群是阶段集群
    // Use a special prefix here to differentiate this cluster from other operation clusters
    //在此处使用特殊的前缀来区分这个集群和其他操作集群
    val stageClusterId = STAGE_CLUSTER_PREFIX + stage.stageId
    val stageClusterName = s"Stage ${stage.stageId}" +
      { if (stage.attemptId == 0) "" else s" (attempt ${stage.attemptId})" }
    val rootCluster = new RDDOperationCluster(stageClusterId, stageClusterName)

    // Find nodes, edges, and operation scopes that belong to this stage
    //查找属于此阶段的节点,边和操作范围
    stage.rddInfos.foreach { rdd =>
      edges ++= rdd.parentIds.map { parentId => RDDOperationEdge(parentId, rdd.id) }

      // TODO: differentiate between the intention to cache an RDD and whether it's actually cached
      val node = nodes.getOrElseUpdate(
        rdd.id, RDDOperationNode(rdd.id, rdd.name, rdd.storageLevel != StorageLevel.NONE))

      if (rdd.scope.isEmpty) {
        // This RDD has no encompassing scope, so we put it directly in the root cluster
        // This should happen only if an RDD is instantiated outside of a public RDD API
        //这个RDD没有涵盖范围，所以我们将其直接放在根集群中这只有在RDD在公共RDD API之外实例化时才会发生
        rootCluster.attachChildNode(node)
      } else {
        // Otherwise, this RDD belongs to an inner cluster,
        //否则，此RDD属于内部集群
        // which may be nested inside of other clusters
        //其可以嵌套在其他集群内
        val rddScopes = rdd.scope.map { scope => scope.getAllScopes }.getOrElse(Seq.empty)
        val rddClusters = rddScopes.map { scope =>
          val clusterId = scope.id
          val clusterName = scope.name.replaceAll("\\n", "\\\\n")
          clusters.getOrElseUpdate(clusterId, new RDDOperationCluster(clusterId, clusterName))
        }
        // Build the cluster hierarchy for this RDD
        //构建此RDD的集群层次结构
        rddClusters.sliding(2).foreach { pc =>
          if (pc.size == 2) {
            val parentCluster = pc(0)
            val childCluster = pc(1)
            parentCluster.attachChildCluster(childCluster)
          }
        }
        // Attach the outermost cluster to the root cluster, and the RDD to the innermost cluster
        //将最外层的集群附加到根集群，将RDD连接到最内层集群
        rddClusters.headOption.foreach { cluster => rootCluster.attachChildCluster(cluster) }
        rddClusters.lastOption.foreach { cluster => cluster.attachChildNode(node) }
      }
    }

    // Classify each edge as internal, outgoing or incoming
    // This information is needed to reason about how stages relate to each other
    //将每个边缘分类为内部,外向或传入该信息需要说明阶段如何相互关联
    val internalEdges = new ListBuffer[RDDOperationEdge]
    val outgoingEdges = new ListBuffer[RDDOperationEdge]
    val incomingEdges = new ListBuffer[RDDOperationEdge]
    edges.foreach { case e: RDDOperationEdge =>
      val fromThisGraph = nodes.contains(e.fromId)
      val toThisGraph = nodes.contains(e.toId)
      (fromThisGraph, toThisGraph) match {
        case (true, true) => internalEdges += e
        case (true, false) => outgoingEdges += e
        case (false, true) => incomingEdges += e
        // should never happen
          //永远不会发生
        case _ => logWarning(s"Found an orphan edge in stage ${stage.stageId}: $e")
      }
    }

    RDDOperationGraph(internalEdges, outgoingEdges, incomingEdges, rootCluster)
  }

  /**
   * Generate the content of a dot file that describes the specified graph.
    * 生成描述指定图形的点文件的内容
   *
   * Note that this only uses a minimal subset of features available to the DOT specification.
   * Part of the styling must be done here because the rendering library must take certain
   * attributes into account when arranging the graph elements. More style is added in the
   * visualization later through post-processing in JavaScript.
    * 请注意，这仅使用DOT规范可用的功能的最小子集。必须在此处完成样式的部分，
    * 因为在排列图形元素时，渲染库必须考虑某些属性。 后来通过JavaScript中的后期处理，可以在可视化中添加更多样式。
   *
   * For the complete DOT specification, see http://www.graphviz.org/Documentation/dotguide.pdf.
   */
  def makeDotFile(graph: RDDOperationGraph): String = {
    val dotFile = new StringBuilder
    dotFile.append("digraph G {\n")
    dotFile.append(makeDotSubgraph(graph.rootCluster, indent = "  "))
    graph.edges.foreach { edge => dotFile.append(s"""  ${edge.fromId}->${edge.toId};\n""") }
    dotFile.append("}")
    val result = dotFile.toString()
    logDebug(result)
    result
  }

  /** Return the dot representation of a node in an RDDOperationGraph.
    * 返回RDDOperationGraph中节点的点表示*/
  private def makeDotNode(node: RDDOperationNode): String = {
    s"""${node.id} [label="${node.name} [${node.id}]"]"""
  }

  /** Return the dot representation of a subgraph in an RDDOperationGraph.
    * 在RDDOperationGraph中返回子图的点表示形式*/
  private def makeDotSubgraph(cluster: RDDOperationCluster, indent: String): String = {
    val subgraph = new StringBuilder
    subgraph.append(indent + s"subgraph cluster${cluster.id} {\n")
    subgraph.append(indent + s"""  label="${cluster.name}";\n""")
    cluster.childNodes.foreach { node =>
      subgraph.append(indent + s"  ${makeDotNode(node)};\n")
    }
    cluster.childClusters.foreach { cscope =>
      subgraph.append(makeDotSubgraph(cscope, indent + "  "))
    }
    subgraph.append(indent + "}\n")
    subgraph.toString()
  }
}
