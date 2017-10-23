package org.apache.spark.examples.graphx

import org.apache.spark._

import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD
object SubGraphDemo extends  App{
  //Creating Spark Configuration
  val conf = new SparkConf()
  conf.setAppName("My First Spark Graphx").setMaster("local")
  // Define Spark Context which we will use to initialize our SQL Context
  val sparkCtx = new SparkContext(conf)

  // Create an RDD for the vertices
  //顶点属性可能包含用户名和职业
  //顶点RDD[顶点的id,顶点的属性值]


  // Create an RDD for edges
  //定义描述协作者之间关系之间的边(关系)
  // 边RDD[起始点id,终点id，边的属性（边的标注,边的权重等）]
  //假设graph有如下的顶点和边 顶点RDD(id,(name,age) 边上有一个Int权重（属性）
  //(4,(David,42))(6,(Fran,50))(2,(Bob,27)) (1,(Alice,28))(3,(Charlie,65))(5,(Ed,55))
  val usersa: RDD[(VertexId, (String, Int))] =
  //对于 users 这个 RDD 而言，其每一个元素包含一个 ID 和属性，属性是由 name 和 occupation 构成的元组
    sparkCtx.parallelize(Array(
      (4L, ("David", 42)),
      (6L, ("Fran", 50)),
      (1L,("Alice",28)),
      (2L,("Bob",27)),
      (3L,("Charlie",65)),
      (5L,("Ed",55))))

  /**Edge(5,3,8)Edge(2,1,7)Edge(3,2,4) Edge(5,6,3)Edge(3,6,3)**/
  val relationships: RDD[Edge[Int]] =
  //Edge case 类,边缘具有 srcId 和 dstId 对应于源和目标顶点标识符,此外,Edge 该类有一个 attr 存储边缘属性的成员
    sparkCtx.parallelize(Array(
      Edge(5,3,8),
      Edge(2,1,7),
      Edge(5,6,3),
      Edge(3,6,3)))

  val graph = Graph(usersa, relationships)
  //可以使用以下三种操作方法获取满足条件的子图
  //方法1，对顶点进行操作
  println("=====")
  /**
    ((2,(Bob,27)),(1,(Alice,28)),7)
    ((3,(Charlie,65)),(6,(Fran,50)),3)
    ((5,(Ed,55)),(3,(Charlie,65)),8)
    ((5,(Ed,55)),(6,(Fran,50)),3)
    */
  graph.triplets.foreach(println)
  //vpred=(id,attr)=>attr._2>30 顶点vpred第二个属性(age)>30岁
  val subGraph1=graph.subgraph(vpred=(id,attr)=>attr._2>30)
  println("==begin===")
  /**
    ((3,(Charlie,65)),(6,(Fran,50)),3)
    ((5,(Ed,55)),(3,(Charlie,65)),8)
    ((5,(Ed,55)),(6,(Fran,50)),3)
    **/
  subGraph1.triplets.foreach(println)
  println("==end===")
  //(4,(David,42))(6,(Fran,50))(3,(Charlie,65))(5,(Ed,55))
 subGraph1.vertices.foreach(println)
  println
  //Edge(3,6,3)Edge(5,3,8)Edge(5,6,3)
  subGraph1.edges.foreach {println}
  /**println
  输出结果：
  顶点：(4,(David,42))(6,(Fran,50))(3,(Charlie,65))(5,(Ed,55))
  边：Edge(3,6,3)Edge(5,3,8)Edge(5,6,3)**/

  //方法2--对EdgeTriplet进行操作
  //epred（边）的属性（权重）大于3
  val subGraph2=graph.subgraph(epred=>epred.attr>3)
  println("==epred(边)的属性(权重)大于3===")
  /**
  ((2,(Bob,27)),(1,(Alice,28)),7)
  ((5,(Ed,55)),(3,(Charlie,65)),8) */
  subGraph2.triplets.foreach(println)

    //也可以定义如下的操作
  //方法3--对顶点和边Triplet两种同时操作“，”号隔开epred和vpred
    val subGraph3=graph.subgraph(epred=>epred.attr>3,vpred=(id,attr)=>attr._2>30)
  /** 输出结果：
    顶点：(3,(Charlie,65))(5,(Ed,55))(4,(David,42))(6,(Fran,50))
    边：Edge(5,3,8)**/

}
