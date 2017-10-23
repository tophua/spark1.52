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

// scalastyle:off println
package org.apache.spark.examples.graphx

// $example on$
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{GraphLoader, PartitionStrategy}
// $example off$


/**
 * A vertex is part of a triangle when it has two adjacent vertices with an edge between them.
 * GraphX implements a triangle counting algorithm in the [`TriangleCount` object][TriangleCount]
 * that determines the number of triangles passing through each vertex,
 * providing a measure of clustering.
 * We compute the triangle count of the social network dataset.
 *
 * Note that `TriangleCount` requires the edges to be in canonical orientation (`srcId < dstId`)
 * and the graph to be partitioned using [`Graph.partitionBy`][Graph.partitionBy].
  *
 * 点是三角形的一部分，当它有两个相邻的顶点之间有一个边。GraphX 在 TriangleCount 对象 中实现一个三角计数算法,
  * 用于确定通过每个顶点的三角形数量，提供聚类度量。我们从 PageRank 部分 计算社交网络数据集的三角形数。
  * 需要注意的是 TriangleCount 边缘要处于规范方向 (srcId < dstId)，而图形要使用 Graph.partitionBy。
 * Run with
 * {{{
 * bin/run-example graphx.TriangleCountingExample
 * }}}
 */
object TriangleCountingExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("My First Spark Graphx").setMaster("local")
    // Define Spark Context which we will use to initialize our SQL Context
    val sc = new SparkContext(conf)



    // $example on$
    // Load the edges in canonical order and partition the graph for triangle count
    /**
      * 例如说在微博上你关注的人也互相关注,大家的关注关系中就会有很多三角形，这说 明社区很强很稳定，大家的联系都比较紧密;如果说只是你一个人关注很多人，这说明你的 社交群体是非常小的。
      */
    //利用GraphLoader.edgeListFile函数从边List文件中建立图的基本结构（所有“顶点”+“边”），且顶点和边的属性都默认为1
    val graph = GraphLoader.edgeListFile(sc, "data/graphx/followers.txt", true)
      .partitionBy(PartitionStrategy.RandomVertexCut)
    // Find the triangle count for each vertex
    val triCounts = graph.triangleCount().vertices
    /*输出结果
    * (1,1)//顶点1有1个三角形
    * (3,2)//顶点3有2个三角形
    * (5,2)
    * (4,1)
    * (6,1)
    * (2,2)
    */
    // Join the triangle counts with the usernames
    val users = sc.textFile("data/graphx/users.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val triCountByUsername = users.join(triCounts).map { case (id, (username, tc)) =>
      (username, tc)
    }
    // Print the result
    println(triCountByUsername.collect().mkString("\n"))
    // $example off$
    sc.stop()
  }
}
// scalastyle:on println
