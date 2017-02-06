package org.apache.sparktest
/**
 * 非阻塞方式（回调方式）
 * 有时你只需要监听Future的完成事件,对其进行响应,不是创建新的Future,而仅仅是产生副作用。
 * 通过onComplete,onSuccess,onFailure三个回调函数来异步执行Future任务,而后两者仅仅是第一项的特例。
 */
import scala.concurrent.{ Future }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{ Failure, Success }
import scala.util.Random
object FutureNotBlock {
/**
 *Future结构中随机延迟一段时间,然后返回结果或者抛出异常。然后在回调函数中进行相关处理。
 */
  def main(args: Array[String]): Unit = {
    println("starting calculation ...")
    val f = Future {
      Thread.sleep(Random.nextInt(500))
      42
    }

    println("before onComplete")
    f.onComplete {
      case Success(value) => println(s"Got the callback, meaning = $value")
      case Failure(e)     => e.printStackTrace
    }

    // do the rest of your work
    println("A ...")
    Thread.sleep(100)
    println("B ....")
    Thread.sleep(100)
    println("C ....")
    Thread.sleep(100)
    println("D ....")
    Thread.sleep(100)
    println("E ....")
    Thread.sleep(100)

    Thread.sleep(2000)
  }
}