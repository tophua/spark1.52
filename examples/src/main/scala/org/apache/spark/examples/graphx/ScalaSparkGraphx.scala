package chapter.six

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx._

import org.apache.spark.graphx.Graph.graphToGraphOps
import org.apache.spark.rdd.RDD

object ScalaSparkGraphx {

  def main(args: Array[String]) {

    //Creating Spark Configuration
    val conf = new SparkConf()
    conf.setAppName("My First Spark Graphx").setMaster("local")
    // Define Spark Context which we will use to initialize our SQL Context 
    val sparkCtx = new SparkContext(conf)

    //Define Vertices/ Nodes for Subjects
    //"parallelize()" is used to distribute a local Scala collection to form an RDD.
    //It acts lazily, i.e. 
    //if a mutable collection is altered after invoking "parallelize" but before
    //any invocation to the RDD operation then your resultant RDD will contain modified collection.

    //为学科定义顶点
    val subjects: RDD[(VertexId, (String))] = sparkCtx.parallelize(Array((1L, ("English")), (2L, ("Math")),(3L, ("Science"))))

    //Define Vertices/ Nodes for Teachers
    //为教师定义顶点
    val teachers: RDD[(VertexId, (String))] = sparkCtx.parallelize(Array((4L, ("Leena")), (5L, ("Kate")),(6L, ("Mary"))))
    //Define Vertices/ Nodes for Students
    //为学生定义顶点
    val students: RDD[(VertexId, (String))] = sparkCtx.parallelize(Array((7L, ("Adam")), (8L, ("Joseph")),(9L, ("Jessica")),(10L, ("Ram")),(11L, ("brooks")),(12L, ("Camily"))))
    //Join all Vertices and create 1 Vertice
    //合并所有顶点
    val vertices = subjects.union(teachers).union(students)
    
    //Define Edges/ Relationships between Subject vs Teachers
    //定义学科和教师之间的边(关系)
    val subjectsVSteachers: RDD[Edge[String]] = sparkCtx.parallelize(Array(Edge(4L,1L, "teaches"), Edge(5L,2L, "teaches"),Edge(6L, 3L, "teaches")))
     
    //Define Edges/ Relationships between Subject vs Students
    //定义学科和学生之间的边(关系)
    val subjectsVSstudents: RDD[Edge[String]] = sparkCtx.parallelize(
        Array(Edge(7L, 3L, "Enrolled"), Edge(8L, 3L, "Enrolled"),Edge(9L, 2L,  "Enrolled"),Edge(10L, 2L, "Enrolled"),Edge(11L, 1L, "Enrolled"),
            Edge(12L, 1L, "Enrolled"))) 
    //Join all Edges and create 1 Edge
    //合并所有边
    val edges = subjectsVSteachers.union(subjectsVSstudents)
    //Define Object of Graph
    //定义图对象
    val graph = Graph(vertices, edges)

    //Print total number of Vertices and Edges
    //打印顶点和边的总数
    println("Total vertices = " + graph.vertices.count()+", Total Edges = "+graph.edges.count())
    
    // Print Students and Teachers associated with Subject = Math
    //打印与学科==数学有关的学生和教师
    println("Students and Teachers associated with Math....")
    //EdgeTriplet represents an edge along with the vertex attributes of its neighboring vertices.triplets
    //Edges 具有原定点ID、目标顶点ID和自己的属性
    //EdgeTriplet继承自Edge[ED],同时EdgeTriplet有srcAttr来表示源顶点的属性和dstAttr来表示目标顶点的属性
    graph.triplets.filter(f=>f.dstAttr.equalsIgnoreCase("Math")).collect().foreach(println)
    
    sparkCtx.stop()
  }

}