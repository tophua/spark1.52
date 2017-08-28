package scalaDemo

/**
  * Created by liush on 17-8-28.
  */
class ZipIndices extends App {
  "abcde".indices zip "abcde"  //indices返回所有有效索引值
  // List[(Int, Char)] = List((0,a), (1,b), (2,c), (3,d),(4,e))
  val zipped = "abcde" zip List(1, 2, 3)
 // zipped: List[(Char, Int)] = List((a,1), (b,2), (c,3)) //会自动截断
  //Queues


}
