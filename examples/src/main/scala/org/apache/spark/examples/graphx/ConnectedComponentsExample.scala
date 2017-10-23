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
import org.apache.log4j.{Level, Logger}

/**
 * A connected components algorithm example.
 * The connected components algorithm labels each connected component of the graph
 * with the ID of its lowest-numbered vertex.
 * For example, in a social network, connected components can approximate clusters.
 * GraphX contains an implementation of the algorithm in the
 * [`ConnectedComponents` object][ConnectedComponents],
 * and we compute the connected components of the example social network dataset.
 *
 * Run with
 * {{{
 * bin/run-example graphx.ConnectedComponentsExample
 * }}}
 */
object ConnectedComponentsExample {
  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val conf = new SparkConf()
    conf.setAppName("My First Spark Graphx").setMaster("local")
    // Define Spark Context which we will use to initialize our SQL Context
    val sc = new SparkContext(conf)
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    // $example on$
    // Load the graph as in the PageRank example
    //利用GraphLoader.edgeListFile函数从边List文件中建立图的基本结构（所有“顶点”+“边”），且顶点和边的属性都默认为1
    val graph = GraphLoader.edgeListFile(sc, "data/graphx/followers.txt")
    /**
      * ((1,1),(2,1),1)((2,1),(1,1),1)((3,1),(7,1),1)((4,1),(1,1),1)((6,1),(3,1),1)((6,1),(7,1),1)((7,1),(3,1),1)((7,1),(6,1),1)
      */
    graph.triplets.foreach(print)
    // Find the connected components
    //连接的组件算法将图中每个连接的组件与其最低编号顶点的ID进行标记
    val cc = graph.connectedComponents().vertices
    println("===========")
    /**
    (4,1)
    (1,1)
    (6,3)
    (3,3)
    (7,3)
    (2,1)**/
    cc.foreach(println)
    // Join the connected components with the usernames
    //加入连接组件与用户名
    val users = sc.textFile("data/graphx/users.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val ccByUsername = users.join(cc).map {
      case (id, (username, cc)) => (username, cc)
    }
    // Print the result
    /**
      (justinbieber,1)
      (BarackObama,1)
      (matei_zaharia,3)
      (jeresig,3)
      (odersky,3)
      (ladygaga,1)
      */
    println(ccByUsername.collect().mkString("\n"))
    // $example off$
    sc.stop()
  }
}
// scalastyle:on println
