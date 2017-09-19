package scalaDemo

import java.time.LocalTime

import scala.concurrent.Future
import java.time._
import java.time._
import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent._
import ExecutionContext.Implicits.global
/**
  * Created by liush on 17-7-21.
  */
object FutureDemo extends App {
  Future {
    Thread.sleep(10000)
    println(s"This is the future at ${LocalTime.now}")
  }
  println(s"This is the present at ${LocalTime.now}")

  Thread.sleep(11000)

  Future { for (i <- 1 to 100) { print("A"); Thread.sleep(10) } }
  Future { for (i <- 1 to 100) { print("B"); Thread.sleep(10) } }

  Thread.sleep(2000)

  val f = Future {
    Thread.sleep(10000)
    42
  }
  println(f)

  Thread.sleep(11000)

  println(f)

  val f23 = Future {
    if (LocalTime.now.getHour > 12)
      throw new Exception("too late")
    42
  }
  Thread.sleep(1000)
  println(f23)


  val f4 = Future { Thread.sleep(10000); 42 }
  val result = Await.result(f4, 11.seconds)
  println(result)

  val f2 = Future { Thread.sleep(10000); 42 }
  Await.ready(f2, 11.seconds)
  val Some(t) = f2.value
  println(t)
}
