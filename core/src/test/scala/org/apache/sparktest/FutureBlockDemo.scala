package org.apache.sparktest

import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
/**
 * 在Akka中, 一个Future是用来获取某个并发操作的结果的数据结构。
 * 这个操作通常是由Actor执行或由Dispatcher直接执行的. 这个结果可以以同步（阻塞）或异步（非阻塞）的方式访问。
 */
object FutureBlockDemo {
  //阻塞方式（Blocking）：通过scala.concurrent.Await使用 
  def main(args: Array[String]): Unit = {
    implicit val baseTime = System.currentTimeMillis
    /**
     * 1)被传递给Future的代码块会被缺省的Dispatcher所执行，代码块的返回结果会被用来完成Future。
     * 2)Await.result方法将阻塞1秒时间来等待Future结果返回，如果Future在规定时间内没有返回，将抛出java.util.concurrent.TimeoutException异常。
     * 3)通过导入scala.concurrent.duration._，可以用一种方便的方式来声明时间间隔，
     *   如100 nanos，500 millis，5 seconds、1 minute、1 hour，3 days。
     *   还可以通过Duration(100, MILLISECONDS)，Duration(200, "millis")来创建时间间隔。
     */
    // create a Future
    val f = Future {
      //500毫秒(ms)=0.5秒(s),一秒等于1000毫秒
      Thread.sleep(500) //休眠500毫秒,      
      1 + 1
    }
    val appId = "spark-application-" + System.currentTimeMillis
    println(appId)
    // this is blocking(blocking is bad)
    val result = Await.result(f, 1 second) //1秒
    // 如果Future没有在Await规定的时间里返回,
    // 将抛出java.util.concurrent.TimeoutException
    println(result)
    Thread.sleep(4000)
    println(">>>>>>>>>>>")
  }

}