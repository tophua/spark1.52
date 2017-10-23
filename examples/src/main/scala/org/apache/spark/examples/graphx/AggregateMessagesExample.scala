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
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.{Graph, TripletFields, VertexRDD}
import org.apache.spark.{SparkConf, SparkContext}
// $example off$

/**
 * An example use the [`aggregateMessages`][Graph.aggregateMessages] operator to
 * compute the average age of the more senior followers of each user
  * 在下面的例子中,我们使用aggregateMessages运算符来计算每个用户的资深追踪者的平均年龄。
 * Run with
 * {{{
 * bin/run-example graphx.AggregateMessagesExample
 * }}}
 */
object AggregateMessagesExample {

  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    //Creating Spark Configuration
    val conf = new SparkConf()
    conf.setAppName("My First Spark Graphx").setMaster("local")
    // Define Spark Context which we will use to initialize our SQL Context
    val sc = new SparkContext(conf)

    // $example on$
    // Create a graph with "age" as the vertex property.
    //创建一个“age”作为顶点属性的图形
    // Here we use a random graph for simplicity.
    //利用graphx提供的类函数随机产生数据集,第二个参数代表顶点个数,第三个参数近似表示边个数,产生的随机图顶点及边属性均默认为整型1。
    val graph: Graph[Double, Int] =
    // mapVertices 用来保存索引
      GraphGenerators.logNormalGraph(sc, numVertices = 100).mapVertices( (id, _) => id.toDouble )
    println("=================")
    /**

      ((0,0.0),(4,4.0),1)
      ((0,0.0),(4,4.0),1)
      ((0,0.0),(5,5.0),1)
      ((0,0.0),(9,9.0),1)
      */
    graph.triplets.foreach(println)
    println("=================")
    // Compute the number of older followers and their total age
    //计算年龄较大的追随者的数量及其总年龄
    //聚合消息(aggregateMessages),该运算符将用户定义的sendMsg函数应用于图中的每个边缘三元组,然后使用该mergeMsg函数在其目标顶点聚合这些消息。
    // aggregateMessages 运算符返回一个 VertexRDD[Msg] ,其中包含去往每个顶点的聚合消息（Msg类型）
    //计算年龄大于自己的关注者的总人数和总年龄
    //aggregateMessages它主要功能是向邻边发消息,合并邻边收到的消息,返回messageRDD。
    //aggregateMessages方法分为Map和Reduce两个阶段
    val olderFollowers: VertexRDD[(Int, Double)] = graph.aggregateMessages[(Int, Double)](
      //triplet用户定义的sendMsg函数接受一个EdgeContext,它将源和目标属性以及edge属性和函数(sendToSrc和sendToDst)一起发送到源和目标属性
      // 发消息函数
      triplet => { // Map Function
        if (triplet.srcAttr > triplet.dstAttr) {
          // Send message to destination vertex containing counter and age
          //发送消息到包含计数器和年龄的目标顶点
          triplet.sendToDst((1, triplet.srcAttr))
        }
      },
      // Add counter and age
      //添加counter和年龄
      //在 map-reduce中,将sendMsg作为map函数,用户定义的mergeMsg 函数需要两个发往同一顶点的消息,并产生一条消息
      //该函数用于在Map阶段每个edge分区中每个点收到的消息合并,并且它还用于reduce阶段,合并不同分区的消息,合并vertexId相同的消息。
      //合并消息函数
      (a, b) => (a._1 + b._1, a._2 + b._2) // Reduce Function
      //aggregateMessages 采用一个可选的tripletsFields,它们指示在EdgeContext中访问哪些数据(即源顶点属性,而不是目标顶点属性)。
      //例如,如果我们计算每个用户的追随者的平均年龄,我们只需要源字段,因此我们将用于TripletFields.Src表示我们只需要源字段。
      //tripletFields：定义发消息的方向
      //   TripletFields.Src
    )
    // Divide total age by number of older followers to get average age of older followers
    //计算年龄大于自己的关注者的平均年龄
    val avgAgeOfOlderFollowers: VertexRDD[Double] =
      olderFollowers.mapValues( (id, value) =>
        value match { case (count, totalAge) => totalAge / count } )
    // Display the results
    avgAgeOfOlderFollowers.collect.foreach(println(_))
    // $example off$

    sc.stop()
  }
}
// scalastyle:on println
