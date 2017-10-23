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
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.{SparkConf, SparkContext}
// $example off$

/**
 * Suppose I want to build a graph from some text files, restrict the graph
 * to important relationships and users, run page-rank on the sub-graph, and
 * then finally return attributes associated with the top users.
 * This example do all of this in just a few lines with GraphX.
 *
 * Run with
 * {{{
 * bin/run-example graphx.ComprehensiveExample
 * }}}
 */
object ComprehensiveExample {
  /**
    * 假设我想从一些文本文件中构建图形，将图形限制为重要的关系和用户，在 sub-graph 上运行 page-rank ,
    * 然后返回与顶级用户关联的属性。我可以用 GraphX 在几行内完成所有这些：

    */
  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val conf = new SparkConf()
    conf.setAppName("My First Spark Graphx").setMaster("local")
    // Define Spark Context which we will use to initialize our SQL Context
    val sc = new SparkContext(conf)

    // $example on$
    // Load my user data and parse into tuples of user id and attribute list
    //加载我的用户数据并解析成用户ID和属性列表的元组
    val users = (sc.textFile("data/graphx/users.txt")
      .map(line => line.split(",")).map( parts => (parts.head.toLong, parts.tail) ))

    // Parse the edge data which is already in userId -> userId format
    //解析边缘数据已经是userId>userId格式
    //利用GraphLoader.edgeListFile函数从边List文件中建立图的基本结构（所有“顶点”+“边”），且顶点和边的属性都默认为1
    val followerGraph = GraphLoader.edgeListFile(sc, "data/graphx/followers.txt")

    // Attach the user attributes
    //附加用户属性
    val graph = followerGraph.outerJoinVertices(users) {
      case (uid, deg, Some(attrList)) => attrList
      // Some users may not have attributes so we set them as empty
        //有些用户可能没有属性，所以我们将它们设为空。
      case (uid, deg, None) => Array.empty[String]
    }

    // Restrict the graph to users with usernames and names
    //限制图与用户名和用户名称
    val subgraph = graph.subgraph(vpred = (vid, attr) => attr.size == 2)

    // Compute the PageRank
    val pagerankGraph = subgraph.pageRank(0.001)

    // Get the attributes of the top pagerank users
    //获取顶点PageRank用户的属性
    val userInfoWithPageRank = subgraph.outerJoinVertices(pagerankGraph.vertices) {
      case (uid, attrList, Some(pr)) => (pr, attrList.toList)
      case (uid, attrList, None) => (0.0, attrList.toList)
    }

    println(userInfoWithPageRank.vertices.top(5)(Ordering.by(_._2._1)).mkString("\n"))
    // $example off$

    sc.stop()
  }
}
// scalastyle:on println
