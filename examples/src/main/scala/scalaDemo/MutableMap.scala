package scalaDemo

/**
  * Created by liush on 17-7-14.
  * 可变的 Map 在 scala.collection.mutable 里，不可变的在 scala.collection.immutable 里
  * Map 是 Scala 里另一种有用的集合类
  */
import scala.collection.mutable.Map
object  MutableMap {
  def main(args: Array[String]):Unit= {
    val treasureMap = Map[Int, String]()
    //map追加操作
    treasureMap += (1 -> "Go to island.")
    treasureMap += (2 -> "Find big X on ground.")
    treasureMap += (3 -> "Dig.")
    println(treasureMap(2))

  }
}
