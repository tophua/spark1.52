package scalaDemo

/**
  * Created by liush on 17-7-14.
  * 可变的Set 操作
  */
object MutableSet {
  def main(args: Array[String]):Unit= {
    //可变
    import scala.collection.mutable.Set
    val movieSet = Set("Hitch", "Poltergeist")
    movieSet += "Shrek"
    println(movieSet)
    //不可变
    import scala.collection.immutable.HashSet
    val hashSet = HashSet("Tomatoes", "Chilies")
    println(hashSet + "Coriander")
  }

}
