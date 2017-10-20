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
import org.apache.spark.graphx.GraphLoader
// $example off$


/**
 * A PageRank example on social network dataset
 * Run with
 * {{{
 * bin/run-example graphx.PageRankExample
 * }}}
 */
object PageRankExample {
  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    //Creating Spark Configuration
    val conf = new SparkConf()
    conf.setAppName("My First Spark Graphx").setMaster("local")
    // Define Spark Context which we will use to initialize our SQL Context
    val sparkCtx = new SparkContext(conf)


    // $example on$
    // Load the edges as a graph
    val graph = GraphLoader.edgeListFile(sparkCtx, "data/graphx/followers.txt")
    // Run PageRank
    /**
      * PageRank 测量在图中每个顶点的重要性，假设从边缘 u 到 v 表示的认可 v 通过的重要性 u ,
      * 例如,如果 Twitter 用户遵循许多其他用户,则用户将被高度排名。
      */
    val ranks = graph.pageRank(0.0001).vertices
    // Join the ranks with the usernames
    //GraphX还包括一个可以运行 PageRank 的社交网络数据集示例。给出了一组用户 data/graphx/users.txt ,
    //并给出了一组用户之间的关系 data/graphx/followers.txt

    val users = sparkCtx.textFile("data/graphx/users.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val ranksByUsername = users.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }
    // Print the result
    println(ranksByUsername.collect().mkString("\n"))
    // $example off$
    sparkCtx.stop()
  }
}
// scalastyle:on println
