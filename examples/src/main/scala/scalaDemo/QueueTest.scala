package scalaDemo;

import scala.App;

/**
 * Created by liush on 17-8-28.
 */
object QueueMutableTest extends App {
  //Queues

  import scala.collection.mutable.Queue //可变Queue
  val queuea = new Queue[String]
  queuea += "a"  //添加单个
  queuea ++= List("b", "c") //添加多个
  println(queuea.dequeue) //取出头元素, 只返回一个值
  //res22: String = a

}