package scalaDemo.threadConcurrency

import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration.Duration
import scala.language.postfixOps
import scala.util.{Failure, Success}

/**
 * JVM 并发性: Scala 中的异步事件处理,
 * http://www.ibm.com/developerworks/cn/java/j-jvmc4/
 * 
 * Unhappy不恰当的;
 */
object AsyncUnhappy extends App {
  import ExecutionContext.Implicits.global

  // task definitions
  def task1(input: Int) = TimedEvent.delayedSuccess(1, input + 1)
  def task2(input: Int) = TimedEvent.delayedSuccess(2, input + 2)
  def task3(input: Int) = TimedEvent.delayedSuccess(3, input + 3)
  def task4(input: Int) = TimedEvent.delayedFailure(1, "This won't work!")

  /** Run tasks with block waits. */
  def runBlocking() = {
    val result = Promise[Int]
    try {
      val v1 = Await.result(task1(1), Duration.Inf)//Duration.Inf 无线期等待
      val future2 = task2(v1)
      val future3 = task3(v1)
      val v2 = Await.result(future2, Duration.Inf)//Duration.Inf 无线期等待
      val v3 = Await.result(future3, Duration.Inf)//Duration.Inf 无线期等待
      val v4 = Await.result(task4(v2 + v3), Duration.Inf)//Duration.Inf 无线期等待
      result.success(v4)
    } catch {
      case t: Throwable => result.failure(t)
    }
    result.future
  }

  /** Run tasks with callbacks. */
  def runOnComplete() = {
    val result = Promise[Int]
    task1(1).onComplete(v => v match {
      case Success(v1) => {
        val a = task2(v1)
        val b = task3(v1)
        a.onComplete(v => v match {
          case Success(v2) =>
            b.onComplete(v => v match {
              case Success(v3) => task4(v2 + v3).onComplete(v4 => v4 match {
                case Success(x) => result.success(x)
                case Failure(t) => result.failure(t)
              })
              case Failure(t) => result.failure(t)
            })
          case Failure(t) => result.failure(t)
        })
      }
      case Failure(t) => result.failure(t)
    })
    result.future
  }

  /** Run tasks with flatMap. */
  def runFlatMap() = {
    task1(1) flatMap { v1 =>
      val a = task2(v1)
      val b = task3(v1)
      a flatMap { v2 =>
        b flatMap { v3 => task4(v2 + v3) }
      }
    }
  }

  /** Run tasks with async macro. */
  //scaa 2.11中
/*  def runAsync(): Future[Int] = {

    async {//async此调用将该代码块声明为异步执行的代码，并在默认情况下异步执行它，然后返回一个 future 表示该代码块的执行结果
      val v1 = await(task1(1))//await()方法显示了何处需要一个 future 的结果
      val a = task2(v1)
      val b = task3(v1)
      /**
       * async 宏将代码转换为状态机类的方式，该宏的使用有一些限制
       * 最明显的限制是，不能将 await() 嵌套在 async 代码块中的另一个对象或闭包内（包括一个函数定义）。
       * 也不能将 await() 嵌套在一个 try 或 catch 内
       */
      await(task4(await(a) + await(b)))
    }
  }*/

  def timeComplete(f: () => Future[Int], name: String) {
    println("Starting " + name)
    val start = System.currentTimeMillis
    val future = f()
    try {
      val result = Await.result(future, Duration.Inf)
      val time = System.currentTimeMillis - start
      println(name + " returned " + result + " in " + time + " ms.")
    } catch {
      case t: Throwable => {
        val time = System.currentTimeMillis - start
        println(name + " threw " + t.getClass.getName + ": " + t.getMessage + " after " + time + " ms.")
      }
    }
  }

  timeComplete(runBlocking, "runBlocking")
  timeComplete(runOnComplete, "runOnComplete")
  timeComplete(runFlatMap, "runFlatMap")
 // timeComplete(runAsync, "runAsync")

  // force everything to terminate
  System.exit(0)
}
