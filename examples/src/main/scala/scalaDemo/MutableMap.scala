package scalaDemo

/**
  * Created by liush on 17-7-14.
  * 可变的 Map 在 scala.collection.mutable 里，不可变的在 scala.collection.immutable 里
  * Map 是 Scala 里另一种有用的集合类
  */
import scala.collection.mutable
import scala.collection.mutable.{HashMap, HashSet, Map}
object  MutableMap {
  def main(args: Array[String]):Unit= {
    val treasureMap = Map[Int, String]()
    //map追加操作
    treasureMap += (1 -> "Go to island.")
    treasureMap += (2 -> "Find big X on ground.")
    treasureMap += (3 -> "Dig.")
    println(treasureMap(2))
    val prices=Map("xiaomi"->5,"meizu"->10,"huawei"->20)
    //短短一行代码就可以把价格打九折返回给一个新的map
    val  disprice=for((k,v)<-prices) yield (k,v*0.9)
    for((k,v)<-disprice)
    {
      println(k+" : "+v)
    }
    //1使用match模式匹配：
    prices.get("iPhone") match {
      case Some(v)=> println(v)
      case None =>println("No Value")
    }
    //2使用isEmpty和get解决
    val num=disprice.get("hadoop")
    if (num.isEmpty) println("No Value") else println(num.get)
    //如果是map可以使用getOrElse方法 而不必使用上面1，2方法。
    //可以把Option当做是一个要么为空，要么有带有单个元素的集合
    disprice.get("hadoop").foreach(println _)

    println("=============")
    //判断prices里面是否有key为“aaa”的元素,没有的话则添加（“aaa”，20）到prices中
   var test= prices.getOrElseUpdate("aaa",20)
    test +=12
    //如果存在不更新value值
    prices.getOrElseUpdate("huawei",400)
    for((k,v)<-prices)
    {
      println(k+" : "+v)
    }

    println("=============")
    val executorsByHost = new HashMap[String, HashSet[String]]
    //判断executorsByHost里面是否有key为host的元素,没有的话则添加（“23”，20）到executorsByHost中
    //如果存在Key不更新value值,还回HashSet值
    val executorsOnHost = executorsByHost.getOrElseUpdate("23", new mutable.HashSet[String])
    executorsOnHost += "hostA"
   val testa=executorsByHost("23")
    //打印HashSet的值
    testa.foreach(println _)

  }
}
