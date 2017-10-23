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
    //将边加载为图
    /**
      2 1
      4 1
      1 2
      6 3
      7 3
      7 6
      6 7
      3 7
      */
    //利用GraphLoader.edgeListFile函数从边List文件中建立图的基本结构（所有“顶点”+“边”），且顶点和边的属性都默认为1
    val graph = GraphLoader.edgeListFile(sparkCtx, "data/graphx/followers.txt")
    // Run PageRank
    /**
      * PageRank 测量在图中每个顶点的重要性，假设从边缘 u 到 v 表示的认可 v 通过的重要性 u ,
      * 例如,如果 Twitter 用户遵循许多其他用户,则用户将被高度排名。
      *
      * pageRank 方法的时候需要传入一个参数,传入的这个参数的值越小PageRank计算的值就越精确,
      * 如果数据量特别大而传入的参数值又特别小的情况下就会导致巨大的计算任务和计算时间。
      */
    println("==================")
    /**
      ((1,1),(2,1),1)
      ((2,1),(1,1),1)
      ((3,1),(7,1),1)
      ((4,1),(1,1),1)
      ((6,1),(3,1),1)
      ((6,1),(7,1),1)
      ((7,1),(3,1),1)
      ((7,1),(6,1),1)
      **/
    graph.triplets.foreach(println)
    println("=========end=========")


    println("=======pageRank======")
    /**
    ((1,1.4588814096664682),(2,1.390049198216498),1.0)
    ((2,1.390049198216498),(1,1.4588814096664682),1.0)
    ((3,0.9993442038507723),(7,1.2973176314422592),1.0)
    ((4,0.15),(1,1.4588814096664682),1.0)
    ((6,0.7013599933629602),(3,0.9993442038507723),0.5)
    ((6,0.7013599933629602),(7,1.2973176314422592),0.5)
    ((7,1.2973176314422592),(3,0.9993442038507723),0.5)
    ((7,1.2973176314422592),(6,0.7013599933629602),0.5)
      */
     graph.pageRank(0.0001).triplets.foreach(println)
    //PageRank 测量在图中每个顶点的重要性
    val ranks = graph.pageRank(0.0001).vertices
    println("=======pageRank end======")
    /**
        (4,0.15)
        (1,1.4588814096664682)
        (6,0.7013599933629602)
        (3,0.9993442038507723)
        (7,1.2973176314422592)
        (2,1.390049198216498)
      */
    ranks.foreach(println)
    println("======vertices===end=========")
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
    /**
    (justinbieber,0.15)
    (BarackObama,1.4588814096664682)
    (matei_zaharia,0.7013599933629602)
    (jeresig,0.9993442038507723)
    (odersky,1.2973176314422592)
    (ladygaga,1.390049198216498)
      */
    println(ranksByUsername.collect().mkString("\n"))
    // $example off$
    sparkCtx.stop()
  }
}
// scalastyle:on println
