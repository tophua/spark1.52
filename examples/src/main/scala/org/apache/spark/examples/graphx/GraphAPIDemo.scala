package org.apache.spark.examples.graphx

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD
object GraphAPIDemo extends App{
  //Creating Spark Configuration
  val conf = new SparkConf()
  conf.setAppName("My First Spark Graphx").setMaster("local")
  // Define Spark Context which we will use to initialize our SQL Context
  val sc = new SparkContext(conf)
  // Create an RDD for the vertices
  val users: RDD[(VertexId, (String, String))] =
    sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
      (5L, ("franklin", "prof")), (2L, ("istoica", "prof")),
      (4L, ("peter", "student"))))
  // Create an RDD for edges
  val relationships: RDD[Edge[String]] =
    sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
      Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi"),
      Edge(4L, 0L, "student"),   Edge(5L, 0L, "colleague")))
  // Define a default user in case there are relationship with missing user
  val defaultUser = ("John Doe", "Missing")
  // Build the initial Graph
  val graph = Graph(users, relationships, defaultUser)
  val a=graph.inDegrees.reduce((a,b) => if (a._2 > b._2) a else b)
  //顶点Id:9711200==引入数=2414
  println("顶点Id:"+a._1+"==引入数="+a._2)
  val v = graph.pageRank(0.001).vertices
  val pageRank=v.reduce((a,b) => if (a._2 > b._2) a else b)
  //顶点Id:9207016===85.27317386053808
  println("顶点Id:"+pageRank._1+"==权值=="+pageRank._2)
  // Notice that there is a user 0 (for which we have no information) connected to users
  // 4 (peter) and 5 (franklin).
  /**
    istoica is the colleague of franklin
    rxin is the collab of jgonzal
    peter is the student of John Doe
    franklin is the colleague of John Doe
    franklin is the advisor of rxin
    franklin is the pi of jgonzal
    */
  graph.triplets.map(
    triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
  ).collect.foreach(println(_))
  println("=======Missing=======")
  /**
    ((2,(istoica,prof)),(5,(franklin,prof)),colleague)
    ((3,(rxin,student)),(7,(jgonzal,postdoc)),collab)
    ((4,(peter,student)),(0,(John Doe,Missing)),student)
    ((5,(franklin,prof)),(0,(John Doe,Missing)),colleague)
    ((5,(franklin,prof)),(3,(rxin,student)),advisor)
    ((5,(franklin,prof)),(7,(jgonzal,postdoc)),pi)
    */
  graph.triplets.foreach(println)
  // Remove missing vertices as well as the edges to connected to them
  //subgraph操作利用顶点和边的判断式（predicates）,返回的图仅仅包含满足顶点判断式的顶点、满足边判断式的边以及满足顶点判断式的triple
  //subgraph操作可以用于很多场景,如获取感兴趣的顶点和边组成的图或者获取清除断开连接后的图
  println("=======subgraph=======")
  //subgraph方法的实现分两步：先过滤VertexRDD，然后再过滤EdgeRDD
  val validGraph = graph.subgraph(vpred = (id, attr) =>{
    /**
    4===peter====student
    0===John Doe====Missing
    3===rxin====student
    7===jgonzal====postdoc
    5===franklin====prof
    2===istoica====prof
    subgraph方法的实现分两步：先过滤VertexRDD，然后再过滤EdgeRDD

      2===istoica====prof
      5===franklin====prof
      3===rxin====student
      7===jgonzal====postdoc
      4===peter====student
      0===John Doe====Missing
      5===franklin====prof
      0===John Doe====Missing
      5===franklin====prof
      3===rxin====student
      5===franklin====prof
      7===jgonzal====postdoc
      */
    println(id+"==="+attr._1+"===="+attr._2)
    attr._2 != "Missing"
  })
  // The valid subgraph will disconnect users 4 and 5 by removing user 0
  /**
  (4,(peter,student))
  (3,(rxin,student))
  (7,(jgonzal,postdoc))
  (5,(franklin,prof))
  (2,(istoica,prof))
    */
  validGraph.vertices.collect.foreach(println(_))
  /**
  istoica is the colleague of franklin
  rxin is the collab of jgonzal
  franklin is the advisor of rxin
  franklin is the pi of jgonzal
    */
  validGraph.triplets.map(
    triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
  ).collect.foreach(println(_))

  // Run Connected Components
  val ccGraph = graph.connectedComponents() // No longer contains missing field
  // Remove missing vertices as well as the edges to connected to them
  //subgraph操作利用顶点和边的判断式（predicates），返回的图仅仅包含满足顶点判断式的顶点、满足边判断式的边以及满足顶点判断式的triple
  val validGraphb = graph.subgraph(vpred = (id, attr) => attr._2 != "Missing")
  /**
    ((2,(istoica,prof)),(5,(franklin,prof)),colleague)
    ((3,(rxin,student)),(7,(jgonzal,postdoc)),collab)
    ((5,(franklin,prof)),(3,(rxin,student)),advisor)
    ((5,(franklin,prof)),(7,(jgonzal,postdoc)),pi)
    */
  validGraphb.triplets.foreach(println)
  // Restrict the answer to the valid subgraph
  //mask操作构造一个子图，这个子图包含输入图中包含的顶点和边。它的实现很简单，顶点和边均做inner join操作即可
  val validCCGraph = ccGraph.mask(validGraphb)
  println("=====mask=========")
  /**
     ((2,0),(5,0),colleague)
    ((3,0),(7,0),collab)
    ((5,0),(3,0),advisor)
    ((5,0),(7,0),pi)
    */
  validCCGraph.triplets.foreach(println)
}
