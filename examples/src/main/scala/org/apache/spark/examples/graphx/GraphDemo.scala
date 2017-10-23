package org.apache.spark.examples.graphx
import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD
//http://spark.apachecn.org/docs/cn/2.2.0/graphx-programming-guide.html
object GraphDemo extends  App{
  // Assume the SparkContext has already been constructed

  //Creating Spark Configuration
  val conf = new SparkConf()
  conf.setAppName("My First Spark Graphx").setMaster("local")
  // Define Spark Context which we will use to initialize our SQL Context
  val sparkCtx = new SparkContext(conf)

  // Create an RDD for the vertices
  //顶点属性可能包含用户名和职业
  val users: RDD[(VertexId, (String, String))] =
  //对于 users 这个 RDD 而言，其每一个元素包含一个 ID 和属性，属性是由 name 和 occupation 构成的元组
    sparkCtx.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
      (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
  // Create an RDD for edges
  //定义描述协作者之间关系之间的边(关系)
  val relationships: RDD[Edge[String]] =
  //Edge case 类,边缘具有 srcId 和 dstId 对应于源和目标顶点标识符,此外,Edge 该类有一个 attr 存储边缘属性的成员
    sparkCtx.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
      Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
  // Define a default user in case there are relationship with missing user
  //defaultUser其主要作用就在于当如果想描述一种关系中不存在的目标顶点的时候就会使用这个defaultUser，
  // 例如 5 到 0 这个 ralationship 是不存在的，那就会默认指向 defaultUser，
  val defaultUser = ("John Doe", "Missing")
  // Build the initial Graph
  //生成的图形
  //GraphX每个元素有源顶点 ID、 目标顶点 ID 和边的属性等三部分构成;
  val graph = Graph(users, relationships, defaultUser)
  println("==============")
  /**
    ((2,(istoica,prof)),(5,(franklin,prof)),colleague)
    ((3,(rxin,student)),(7,(jgonzal,postdoc)),collab)
    ((5,(franklin,prof)),(3,(rxin,student)),advisor)
    ((5,(franklin,prof)),(7,(jgonzal,postdoc)),pi)
    */
  graph.triplets.foreach(println)
  println("======inDegrees========")
  /**
    入度,表示3的顶点入度1个连接数
    (3,1)
     入度,表示7的顶点入度2个连接数
    (7,2)
    (5,1)

    */
  graph.inDegrees.foreach(println)
  println("======outDegrees========")
  /**
    出度,表示3的顶点出度1个连接数
    (3,1)
    (5,2)
    (2,1)
    */
  graph.outDegrees.foreach(println)
  println("====bengin==========")
  /**
    ((2,0.15),(5,0.27749999999999997),1.0)
    ((3,0.2679375),(7,0.49568437499999995),1.0)
    ((5,0.27749999999999997),(3,0.2679375),0.5)
    ((5,0.27749999999999997),(7,0.49568437499999995),0.5)
    */
  graph.pageRank(0.0001).triplets.foreach(println)
  println("====bengin====vertices======")
  /**
    *获得顶点权重
    (3,0.2679375)
    (7,0.49568437499999995)
    (5,0.27749999999999997)
    (2,0.15)
    */
  graph.pageRank(0.0001).vertices.foreach(println)
  println("=======end=======")
  /**
    * connectedComponents连接的组件算法将图中每个连接的组件与其最低编号顶点的ID进行标记
    ((2,2),(5,2),colleague)
    ((3,2),(7,2),collab)
    ((5,2),(3,2),advisor)
    ((5,2),(7,2),pi)
    */
  graph.connectedComponents().triplets.foreach(println)
  println("====connectedComponents==========")
  /**
      (3,2)
      (7,2)
      (5,2)
      (2,2)
    */
  graph.connectedComponents().vertices.foreach(println)
  // Count all users which are postdocs
  // graph.vertices 和 graph.edges 成员将图形解构成相应的顶点和边缘视图
  //graph.vertices 返回一个 VertexRDD[(String, String)] 扩展 RDD[(VertexId, (String, String))] ，所以我们使用 scala case 表达式来解构元组
  //看一下 occupation(职业) 为 pst.doc.的顶点数目
  //graph.vertices顶点的视图,graph.vertices返回 VertexRDD[(String, String)],所以我们使用Scala的case表达来解构元组
  val count=graph.vertices.filter { case (id, (name, pos)) => pos == "prof" }.count
  println("count:"+count)

  // Count all the edges where src > dst
  //graph.edges 返回一个EdgeRDD对象包含,源顶点 ID、目标顶点 ID 和边的属性等三部分构成
  //graph.edges得到图到边的视图
 val edgescount= graph.edges.filter(e => e.srcId > e.dstId).foreach(println )
  println("edgescount:"+edgescount)
  //我们也可以使用case类型构造函数,要计算一下生成的graph中源顶点ID大于目标顶点ID的数量
  //graph.edges返回 EdgeRDD包含 Edge[String]对象,我们可以也使用的如下的类型的构造器
  val edgescountd=graph.edges.filter { case Edge(src, dst, prop) => src > dst }.count
  println("edgescountd:"+edgescountd)
  //创建Edge对象方式
  graph.edges.filter { case Edge(src, dst, prop) => src > dst }.foreach(println)
  /**SELECT src.id, dst.id, src.attr, e.attr, dst.attr
  FROM edges AS e LEFT JOIN vertices AS src, vertices AS dst
  ON e.srcId = src.Id AND e.dstId = dst.Id**/
  //EdgeTriplet 类通过分别添加包含源和目标属性的 srcAttr 和 dstAttr 成员来扩展 Edge 类
  println("======triplets.map======")
  val facts: RDD[String] =
  //EdgeTriplet第一个元素是顶点属性类型,在我们的例子中就是(name, occupation) 的元组,第二个元素是边属性类型
    //该 EdgeTriplet类继承了 Edge并加入了类属性: srcAttr和 dstAttr,用于包含了源和目标属性
  /**
    istoica is the colleague of franklin
    rxin is the collab of jgonzal
    franklin is the advisor of rxin
    franklin is the pi of jgonzal
    */
    graph.triplets.map(triplet =>
      //srcAttr源顶点,取出元组("rxin", "student")
      //triplet.attr边的属性,collab
      //triplet.dstAttr目标顶点,取出元组("jgonzal", "postdoc")
      triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)
  facts.collect.foreach(println(_))
  println("======triplets.map end======")



  // Remove missing vertices as well as the edges to connected to them
  //删除了断开的链接：
  //在 subgraph 操作者需要的顶点和边缘的谓词,并返回包含只有满足谓词顶点的顶点的曲线图(评估为真),并且满足谓词边缘边缘并连接满足顶点谓词顶点
  /**
    (3,(rxin,student))
    (7,(jgonzal,postdoc))
    (5,(franklin,prof))
    (2,(istoica,prof))
    */
  val validGraph = graph.subgraph(vpred = (id, attr) => attr._2 != "Missing")
  // The valid subgraph will disconnect users 4 and 5 by removing user 0
  validGraph.vertices.collect.foreach(println(_))
  println("======validGraph.map ======")
  /**
    istoica is the colleague of franklin
    rxin is the collab of jgonzal
    franklin is the advisor of rxin
    franklin is the pi of jgonzal
    */
  validGraph.triplets.map(
    triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
  ).collect.foreach(println(_))
}
