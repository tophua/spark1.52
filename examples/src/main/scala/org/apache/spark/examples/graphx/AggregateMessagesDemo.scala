package org.apache.spark.examples.graphx


import org.apache.spark.graphx.{Edge, Graph, TripletFields, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AggregateMessagesDemo extends App{
  val conf = new SparkConf()
  conf.setAppName("My First Spark Graphx").setMaster("local")
  // Define Spark Context which we will use to initialize our SQL Context

  val sparkCtx = new SparkContext(conf)
  val usersa: RDD[(VertexId, (String, Int))] =
  //对于 users 这个 RDD 而言，其每一个元素包含一个 ID 和属性，属性是由 name 和 occupation 构成的元组
  //(4,(David,18))(1,(Alice,28))(6,(Fran,40))(3,(Charlie,30))(2,(Bob,70))(5,Ed,55))
    sparkCtx.parallelize(Array(
      (4L,("David",18)),
      (1L,("Alice",28)),
      (6L,("Fran",40)),
      (3L,("Charlie",30)),
      (2L,("Bob",70)),
      (5L,("Ed",55))))
  /**
    边：Edge(4,2,2)Edge(2,1,7)Edge(4,5,8)Edge(2,4,2)Edge(5,6,3)Edge(3,2,4)
       Edge(6,1,2)Edge(3,6,3)Edge(6,2,8)Edge(4,1,1)Edge(6,4,3)(4,(2,110))
    */
  val relationships: RDD[Edge[Int]] =
  //Edge case 类,边缘具有 srcId 和 dstId 对应于源和目标顶点标识符,此外,Edge 该类有一个 attr 存储边缘属性的成员
    sparkCtx.parallelize(Array(
      Edge(4,2,2),
      Edge(2,1,7),
      Edge(4,5,8),
      Edge(2,4,2),
      Edge(5,6,3),
      Edge(3,2,4),
      Edge(6,1,2),
      Edge(3,6,3),
      Edge(6,2,8),
      Edge(4,1,1),
      Edge(6,4,3)
    ))

  val graph = Graph(usersa, relationships)
  graph.triplets.foreach(println)
  /**
    ((2,(Bob,70)),(1,(Alice,28)),7)
    ((2,(Bob,70)),(4,(David,18)),2)
    ((3,(Charlie,30)),(2,(Bob,70)),4)
    ((3,(Charlie,30)),(6,(Fran,40)),3)
    ((4,(David,18)),(1,(Alice,28)),1)
    ((4,(David,18)),(2,(Bob,70)),2)
    ((4,(David,18)),(5,(Ed,55)),8)
    ((5,(Ed,55)),(6,(Fran,40)),3)
    ((6,(Fran,40)),(1,(Alice,28)),2)
    ((6,(Fran,40)),(2,(Bob,70)),8)
    ((6,(Fran,40)),(4,(David,18)),3)
    */
  //定义一个相邻聚合,统计比自己年纪大的粉丝数(count)及其平均年龄（totalAge/count)
  //方括号内的元组(Int,Int)是函数返回值的类型，也就是Reduce函数（mergeMsg )右侧得到的值（count，totalAge）
  val olderFollowers=graph.aggregateMessages[(Int,Int)](
    //(1)--函数左侧是边三元组，也就是对边三元组进行操作，有两种发送方式sendToSrc和 sendToDst
    triplet=> {
      /**
      =attr=7==srcAttr_1==Bob==srcAttr._2==70==dstId==1
      =attr=2==srcAttr_1==Bob==srcAttr._2==70==dstId==4
      =attr=4==srcAttr_1==Charlie==srcAttr._2==30==dstId==2
      =attr=3==srcAttr_1==Charlie==srcAttr._2==30==dstId==6
      =attr=1==srcAttr_1==David==srcAttr._2==18==dstId==1
      =attr=2==srcAttr_1==David==srcAttr._2==18==dstId==2
      =attr=8==srcAttr_1==David==srcAttr._2==18==dstId==5
      =attr=3==srcAttr_1==Ed==srcAttr._2==55==dstId==6
      =attr=2==srcAttr_1==Fran==srcAttr._2==40==dstId==1
      =attr=8==srcAttr_1==Fran==srcAttr._2==40==dstId==2
      =attr=3==srcAttr_1==Fran==srcAttr._2==40==dstId==4
        */
      println(triplet.dstId+"==srcAttr_1=="+triplet.srcAttr._1+"==srcAttr._2=="+triplet.srcAttr._2+"=attr="+triplet.attr)
      if(triplet.srcAttr._2>triplet.dstAttr._2){
        println("==="+(1,triplet.srcAttr._2))
        /**
          * ===(1,70)
          * ===(1,70)
          * ===(1,55)
          * ===(1,40)
          * ===(1,40)
          */
        triplet.sendToDst((1,triplet.srcAttr._2))
      }
    },
    //(2)相当于Reduce函数a，b各代表一个元组（count，Age）
    (a,b)=>{
      println("=a._1="+a._1+"==b._1=="+b._1+"==a._2=="+a._2+"==b._2=="+b._2)
      //=a._1=1==b._1==1==a._2==70==b._2==40
      //=a._1=1==b._1==1==a._2==70==b._2==40
      //对count和Age不断相加（reduce），最终得到总的count和totalAge
      (a._1+b._1,a._2+b._2)
    },
    TripletFields.All)//(3)可选项,TripletFields.All/Src/Dst
  /**
  //顶点Id=4的用户,有2个年龄比自己大的粉丝,同年龄是110岁
    (4,(2,110))
    (1,(2,110))
    (6,(1,55))
    */
  olderFollowers.collect().foreach(println)
  //计算平均年龄
  val averageOfOlderFollowers=olderFollowers.mapValues((id,value)=>value match{
    case (count,totalAge) =>(count,totalAge/count)//由于不是所有顶点都有结果,所以用match-case语句
  })
  //输出结果：
  //(1,(2,55))(4,(2,55))(6,(1,55))//Id=1的用户,有2个粉丝,平均年龄是55岁
  averageOfOlderFollowers.foreach(print)

  //Spark中,经常使用在map中使用case语句进行匹配None和Some
  //假设graph.Vertice:(id，（name，weight））如下：

  val usersab: RDD[(Long, (String,Option[Int]))]=
  //对于 users 这个 RDD 而言，其每一个元素包含一个 ID 和属性，属性是由 name 和 occupation 构成的元组
  //(4,(David,18))(1,(Alice,28))(6,(Fran,40))(3,(Charlie,30))(2,(Bob,70))(5,Ed,55))
    sparkCtx.parallelize(Array(
      (4L,("David",Some(2))),
      (3L,("Charlie",Some(2))),
      (6L,("Fran",Some(4))),
      (2L,("Bob",Some(4))),
      (1L,("Alice",Some(4))),
      (5L,("Ed",None))
      ))
  val graphba = Graph(usersab, relationships)
  //
  val weights=graphba.vertices.map{
    case (id,(name,Some(weight)))=>(id,weight)
    //id=5时,weight=None，其他的为Some
    case (id,(name,None))=>(id,0)
  }
  weights.foreach(print)
  println

  //输出结果如下(id,weight)：
  //(4,2)(1,4)(6,4)(3,2)(5,0)(2,4)
//上面例子可以用下面的语句获得同样的结果：

  val weightsb=graphba.vertices.map{
    attr=>(attr._1,attr._2._2.getOrElse(0))
    //如果attr._2._2！=None，返回attr._2._2（weight）的值，
    //否则（即attr._2._2==None），返回自己设置的函数参数（0）
  }
  weightsb.foreach(print)
  println

}
